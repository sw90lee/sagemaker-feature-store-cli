"""SageMaker Feature Store 배치 업데이트 명령어"""

import click
import boto3
import pandas as pd
import time
import os
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Union, Callable
import logging
import re

logger = logging.getLogger(__name__)


class SageMakerFeatureStoreUpdater:
    """SageMaker Feature Store 오프라인 데이터 대량 수정 클래스"""
    
    def __init__(self, 
                 feature_group_name: str,
                 config,
                 batch_size: int = 1000,
                 deduplicate: bool = True):
        """
        초기화
        
        Args:
            feature_group_name: Feature Group 이름
            config: CLI 설정 객체
            batch_size: 배치 처리 크기
            deduplicate: 중복 레코드 제거 여부
        """
        self.feature_group_name = feature_group_name
        self.region_name = config.region
        self.batch_size = batch_size
        self.deduplicate = deduplicate
        
        # AWS 클라이언트 초기화
        self.session = config.session
        self.sagemaker_client = self.session.client('sagemaker')
        self.s3_client = self.session.client('s3')
        self.athena_client = self.session.client('athena')
        
        # Feature Group 정보 가져오기
        self.feature_group_info = self._get_feature_group_info()
        self.offline_store_config = self.feature_group_info.get('OfflineStoreConfig', {})
        self.s3_config = self.offline_store_config.get('S3StorageConfig', {})
        self.data_catalog_config = self.offline_store_config.get('DataCatalogConfig', {})
        
        # S3 경로 정보
        self.s3_uri = self.s3_config.get('ResolvedOutputS3Uri', '')
        self.bucket_name, self.prefix = self._parse_s3_uri(self.s3_uri)
        
        # Glue 테이블 정보
        self.database_name = self.data_catalog_config.get('Database', 'sagemaker_featurestore')
        self.table_name = self.data_catalog_config.get('TableName', '')
        
        click.echo(f"피처 그룹: {feature_group_name}")
        click.echo(f"S3 경로: {self.s3_uri}")
        click.echo(f"Glue 데이터베이스: {self.database_name}")
        click.echo(f"Glue 테이블: {self.table_name}")
    
    def _get_feature_group_info(self) -> Dict:
        """Feature Group 정보 가져오기"""
        try:
            response = self.sagemaker_client.describe_feature_group(
                FeatureGroupName=self.feature_group_name
            )
            return response
        except Exception as e:
            click.echo(f"피처 그룹 정보 조회 실패: {e}", err=True)
            raise
    
    def _parse_s3_uri(self, s3_uri: str) -> tuple:
        """S3 URI를 버킷과 prefix로 분리"""
        if not s3_uri.startswith('s3://'):
            raise ValueError(f"잘못된 S3 URI: {s3_uri}")
        
        path = s3_uri[5:]  # s3:// 제거
        parts = path.split('/', 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        
        return bucket, prefix
    
    def get_offline_data_paths(self) -> List[str]:
        """오프라인 스토어의 모든 Parquet 파일 경로 가져오기"""
        click.echo("오프라인 스토어 Parquet 파일 목록 조회 중...")
        
        parquet_files = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        try:
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.parquet'):
                            s3_path = f"s3://{self.bucket_name}/{obj['Key']}"
                            parquet_files.append(s3_path)
            
            click.echo(f"발견된 Parquet 파일 수: {len(parquet_files):,}")
            return parquet_files
            
        except Exception as e:
            click.echo(f"S3 파일 목록 조회 실패: {e}", err=True)
            raise
    
    def read_data_sample(self, sample_size: int = 1000) -> pd.DataFrame:
        """데이터 샘플 읽기 (구조 확인용)"""
        click.echo(f"데이터 샘플 {sample_size}개 읽기 중...")
        
        parquet_files = self.get_offline_data_paths()
        if not parquet_files:
            click.echo("Parquet 파일을 찾을 수 없습니다", err=True)
            return pd.DataFrame()
        
        # 첫 번째 파일에서 샘플 읽기
        try:
            # pyarrow 엔진을 우선 사용, 없으면 기본 엔진 사용
            try:
                sample_df = pd.read_parquet(parquet_files[0], engine='pyarrow')
            except ImportError:
                try:
                    sample_df = pd.read_parquet(parquet_files[0], engine='fastparquet')
                except ImportError:
                    sample_df = pd.read_parquet(parquet_files[0])
            if len(sample_df) > sample_size:
                sample_df = sample_df.head(sample_size)
            
            click.echo(f"샘플 데이터 크기: {sample_df.shape}")
            click.echo(f"컬럼 목록: {list(sample_df.columns)}")
            return sample_df
            
        except Exception as e:
            click.echo(f"❌ 샘플 데이터 읽기 실패: {parquet_files[0]}", err=True)
            click.echo(f"   오류 유형: {type(e).__name__}", err=True)
            click.echo(f"   오류 메시지: {str(e)}", err=True)
            
            if "parquet" in str(e).lower():
                click.echo("   → Parquet 파일 형식 오류", err=True)
            elif "s3" in str(e).lower():
                click.echo("   → S3 접근 권한 또는 네트워크 문제", err=True)
            elif "engine" in str(e).lower():
                click.echo("   → Parquet 엔진 문제 (pyarrow 또는 fastparquet 설치 확인)", err=True)
            elif "memory" in str(e).lower():
                click.echo("   → 메모리 부족", err=True)
            
            raise
    
    def load_mapping_from_file(self, mapping_file: str) -> Dict:
        """매핑 파일에서 값 변환 규칙 로드"""
        click.echo(f"매핑 파일 로드 중: {mapping_file}")
        
        try:
            if mapping_file.endswith('.json'):
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            elif mapping_file.endswith('.csv'):
                df = pd.read_csv(mapping_file)
                if 'old_value' not in df.columns or 'new_value' not in df.columns:
                    raise ValueError("CSV 파일에 'old_value', 'new_value' 컬럼이 필요합니다")
                mapping = dict(zip(df['old_value'], df['new_value']))
            else:
                raise ValueError("지원되는 파일 형식: .json, .csv")
            
            click.echo(f"매핑 규칙 {len(mapping)}개 로드 완료")
            return mapping
            
        except Exception as e:
            click.echo(f"매핑 파일 로드 실패: {e}", err=True)
            raise
    
    def create_conditional_mapping(self, conditional_mapping: Union[str, Dict]) -> Dict:
        """조건부 매핑 생성"""
        if isinstance(conditional_mapping, str):
            try:
                conditional_mapping = json.loads(conditional_mapping)
            except json.JSONDecodeError as e:
                click.echo(f"조건부 매핑 JSON 파싱 실패: {e}", err=True)
                raise
        
        click.echo(f"조건부 매핑 규칙: {conditional_mapping}")
        return conditional_mapping
    
    def count_matching_records(self, 
                             column_name: str, 
                             old_value: str = None,
                             value_mapping: Dict = None,
                             conditional_mapping: Dict = None,
                             transform_function: Callable = None,
                             filter_conditions: Optional[Dict] = None,
                             filter_null_only: bool = False) -> Dict:
        """변경 대상 레코드 수 계산"""
        click.echo(f"변경 대상 레코드 수 계산 중... (컬럼: {column_name})")
        
        # null 값만 필터링하는 경우
        if filter_null_only:
            # transform_function이 있으면 Parquet 파일 직접 처리 (복잡한 로직 때문)
            if transform_function:
                # Athena로 null 값 있는 파일만 찾고, Parquet에서 상세 카운팅
                parquet_files = self._get_target_files_for_null_update(column_name, filter_conditions)
                if not parquet_files:
                    return {'total_files': 0, 'match_counts': {}}
                # 아래 일반 카운팅 로직으로 계속 진행 (parquet_files 이미 필터링됨)
            else:
                # 단순 null 카운팅은 Athena로
                return self._count_null_records_with_athena(column_name, filter_conditions)
        else:
            # 일반적인 경우 모든 파일 처리
            parquet_files = self.get_offline_data_paths()
        result_counts = {'total_files': len(parquet_files), 'match_counts': {}, 'failed_files': [], 'failed_details': []}
        
        # 필요한 컬럼들 결정
        required_columns = []
        
        # 변환 함수인 경우 소스 컬럼도 포함
        if transform_function:
            if hasattr(transform_function, 'source_column') and transform_function.source_column:
                required_columns.append(transform_function.source_column)
            
            # 새로운 컬럼인 경우를 고려해서 기존 컬럼이 있으면 포함
            try:
                sample_df = pd.read_parquet(self.get_offline_data_paths()[0], nrows=1, engine='fastparquet')
                if column_name in sample_df.columns:
                    required_columns.append(column_name)
            except:
                pass
        else:
            required_columns = [column_name]
        
        if conditional_mapping:
            for condition_col in conditional_mapping.keys():
                if condition_col not in required_columns:
                    required_columns.append(condition_col)
        
        # 필터 조건 컬럼도 포함
        if filter_conditions:
            for filter_col in filter_conditions.keys():
                if filter_col not in required_columns:
                    required_columns.append(filter_col)
        
        def count_in_file(file_path: str) -> Dict:
            try:
                # 변환 함수의 경우 모든 컬럼 읽기 (required_columns가 비어있을 수 있음)
                if transform_function and not required_columns:
                    try:
                        df = pd.read_parquet(file_path, engine='fastparquet')
                    except ImportError:
                        try:
                            df = pd.read_parquet(file_path, engine='pyarrow')
                        except ImportError:
                            df = pd.read_parquet(file_path)
                else:
                    try:
                        df = pd.read_parquet(file_path, columns=required_columns, engine='fastparquet')
                    except ImportError:
                        try:
                            df = pd.read_parquet(file_path, columns=required_columns, engine='pyarrow')
                        except ImportError:
                            df = pd.read_parquet(file_path, columns=required_columns)
                
                file_counts = {}
                
                # 공통 필터 조건 적용
                base_mask = pd.Series([True] * len(df), index=df.index)
                if filter_conditions:
                    for filter_col, filter_val in filter_conditions.items():
                        if filter_col in df.columns:
                            base_mask = base_mask & (df[filter_col] == filter_val)
                
                if transform_function:
                    # 변환 함수의 경우 전체 레코드 수 반환
                    total_count = base_mask.sum()
                    file_counts['transform_function'] = total_count
                    
                elif old_value is not None:
                    # 단일 값 매칭
                    if column_name in df.columns:
                        mask = base_mask & (df[column_name] == old_value)
                        count = mask.sum()
                        file_counts[old_value] = count
                    else:
                        file_counts[old_value] = 0
                    
                elif value_mapping:
                    # 매핑 파일 기반
                    if column_name in df.columns:
                        for old_val in value_mapping.keys():
                            mask = base_mask & (df[column_name] == old_val)
                            count = mask.sum()
                            if count > 0:
                                file_counts[old_val] = count
                    else:
                        for old_val in value_mapping.keys():
                            file_counts[old_val] = 0
                            
                elif conditional_mapping:
                    # 조건부 매핑
                    for condition_col, condition_mappings in conditional_mapping.items():
                        if condition_col not in df.columns:
                            continue
                        for condition_val, value_map in condition_mappings.items():
                            condition_mask = base_mask & (df[condition_col] == condition_val)
                            for old_val in value_map.keys():
                                if column_name in df.columns:
                                    mask = condition_mask & (df[column_name] == old_val)
                                    count = mask.sum()
                                else:
                                    count = condition_mask.sum()  # 새 컬럼인 경우 조건만 적용
                                if count > 0:
                                    key = f"{condition_col}={condition_val}, {column_name}={old_val}"
                                    file_counts[key] = count
                
                return file_counts
                
            except Exception as e:
                click.echo(f"❌ 파일 읽기 실패: {file_path}", err=True)
                click.echo(f"   오류 유형: {type(e).__name__}", err=True)
                click.echo(f"   오류 메시지: {str(e)}", err=True)
                if "columns" in str(e).lower():
                    click.echo(f"   요청한 컬럼: {required_columns}", err=True)
                    click.echo("   → 파일에 해당 컬럼이 없을 수 있습니다", err=True)
                elif "parquet" in str(e).lower():
                    click.echo("   → 파일이 손상되었거나 Parquet 형식이 아닐 수 있습니다", err=True)
                elif "permission" in str(e).lower() or "access" in str(e).lower():
                    click.echo("   → 파일 접근 권한 문제일 수 있습니다", err=True)
                elif "memory" in str(e).lower():
                    click.echo("   → 메모리 부족 문제일 수 있습니다", err=True)
                
                # 실패 정보 저장 (count_matching_records용)
                return {
                    'error': True,
                    'file_path': file_path,
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
        
        # 병렬 처리로 각 파일의 매칭 레코드 수 계산
        max_workers = min(os.cpu_count(), 10)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(count_in_file, file_path) for file_path in parquet_files]
            
            with click.progressbar(length=len(futures), label='파일 분석') as bar:
                for future in as_completed(futures):
                    file_counts = future.result()
                    
                    # 오류인 경우 실패 정보 저장
                    if isinstance(file_counts, dict) and file_counts.get('error', False):
                        result_counts['failed_files'].append(file_counts['file_path'])
                        result_counts['failed_details'].append({
                            'file': file_counts['file_path'],
                            'error_type': file_counts['error_type'],
                            'error': file_counts['error_message'],
                            'timestamp': file_counts['timestamp']
                        })
                    else:
                        # 정상 결과 집계
                        for key, count in file_counts.items():
                            if key not in result_counts['match_counts']:
                                result_counts['match_counts'][key] = 0
                            result_counts['match_counts'][key] += count
                    
                    bar.update(1)
        
        # 결과 출력
        total_records = sum(result_counts['match_counts'].values())
        click.echo(f"총 변경 대상 레코드 수: {total_records:,}개")
        
        if len(result_counts['match_counts']) > 1:
            click.echo("세부 매칭 결과:")
            for key, count in result_counts['match_counts'].items():
                if count > 0:
                    click.echo(f"  - {key}: {count:,}개")
        
        # 분석 중 실패한 파일이 있으면 리포트 저장
        if result_counts['failed_files']:
            click.echo(f"파일 분석 중 실패한 파일: {len(result_counts['failed_files'])}개")
            failed_report = self._save_failed_files_report(result_counts['failed_details'], 
                                                         self.feature_group_name, column_name)
            if failed_report:
                click.echo(f"실패한 파일 분석 리포트가 저장되었습니다: {failed_report}")
        
        return result_counts
    
    def update_records_batch(self,
                           column_name: str,
                           old_value: str = None,
                           new_value: str = None,
                           value_mapping: Dict = None,
                           conditional_mapping: Dict = None,
                           transform_function: Callable = None,
                           dry_run: bool = True,
                           filter_conditions: Optional[Dict] = None,
                           filter_null_only: bool = False) -> Dict:
        """레코드 대량 업데이트"""
        click.echo(f"배치 업데이트 시작:")
        click.echo(f"  컬럼: {column_name}")
        
        if old_value and new_value:
            click.echo(f"  단일 값 변경: '{old_value}' -> '{new_value}'")
        elif value_mapping:
            click.echo(f"  매핑 기반 변경: {len(value_mapping)}개 규칙")
        elif conditional_mapping:
            click.echo(f"  조건부 변경: {len(conditional_mapping)}개 조건")
        elif transform_function:
            click.echo(f"  함수 기반 변환: 사용자 정의 함수")
        
        click.echo(f"  DRY RUN: {dry_run}")
        click.echo(f"  NULL 값만 필터링: {filter_null_only}")
        
        if filter_conditions:
            click.echo(f"  추가 필터: {filter_conditions}")
        
        # null 값만 필터링하는 경우 Athena로 대상 파일 목록 획득
        if filter_null_only:
            parquet_files = self._get_target_files_for_null_update(column_name, filter_conditions)
        else:
            parquet_files = self.get_offline_data_paths()
        results = {
            'total_files': len(parquet_files),
            'processed_files': 0,
            'updated_records': 0,
            'failed_files': [],
            'failed_details': [],  # 실패 상세 정보 저장용
            'backup_files': []
        }
        
        def process_file(file_path: str) -> Dict:
            """단일 파일 처리"""
            try:
                # 파일 읽기 (여러 엔진 시도)
                try:
                    df = pd.read_parquet(file_path, engine='fastparquet')
                except ImportError:
                    try:
                        df = pd.read_parquet(file_path, engine='pyarrow')
                    except ImportError:
                        df = pd.read_parquet(file_path)
                original_count = len(df)
                total_update_count = 0
                
                # 대상 컬럼이 존재하지 않는 경우 새로 추가
                if column_name not in df.columns:
                    df[column_name] = None
                
                # 데이터프레임 복사 (변경 추적용)
                df_updated = df.copy() if not dry_run else df
                
                # 중복 record_id 처리: EventTime 기준으로 최신 레코드만 유지 (선택적)
                if self.deduplicate:
                    df_updated = self._deduplicate_by_latest_event_time(df_updated)
                
                # 1. 단일 값 변경
                if old_value is not None and new_value is not None:
                    if filter_null_only:
                        # null 값만 대상으로 하는 경우
                        mask = df[column_name].isnull()
                    else:
                        mask = df[column_name] == old_value
                    
                    # 추가 필터 조건 적용
                    if filter_conditions:
                        for filter_col, filter_val in filter_conditions.items():
                            if filter_col in df.columns:
                                mask = mask & (df[filter_col] == filter_val)
                    
                    update_count = mask.sum()
                    total_update_count += update_count
                    
                    if update_count > 0 and not dry_run:
                        df_updated.loc[mask, column_name] = new_value
                        # EventTime 자동 업데이트 (Feature Store 동기화를 위해)
                        self._update_event_time(df_updated, mask)
                
                # 2. 매핑 기반 변경
                elif value_mapping:
                    for old_val, new_val in value_mapping.items():
                        if filter_null_only:
                            # null 값만 대상으로 하는 경우
                            mask = df[column_name].isnull()
                        else:
                            mask = df[column_name] == old_val
                        
                        # 추가 필터 조건 적용
                        if filter_conditions:
                            for filter_col, filter_val in filter_conditions.items():
                                if filter_col in df.columns:
                                    mask = mask & (df[filter_col] == filter_val)
                        
                        update_count = mask.sum()
                        total_update_count += update_count
                        
                        if update_count > 0 and not dry_run:
                            df_updated.loc[mask, column_name] = new_val
                            # EventTime 자동 업데이트 (Feature Store 동기화를 위해)
                            self._update_event_time(df_updated, mask)
                
                # 3. 조건부 매핑
                elif conditional_mapping:
                    for condition_col, condition_mappings in conditional_mapping.items():
                        if condition_col not in df.columns:
                            continue
                            
                        for condition_val, value_map in condition_mappings.items():
                            condition_mask = df[condition_col] == condition_val
                            
                            for old_val, new_val in value_map.items():
                                if filter_null_only:
                                    # null 값만 대상으로 하는 경우
                                    mask = condition_mask & df[column_name].isnull()
                                else:
                                    mask = condition_mask & (df[column_name] == old_val)
                                
                                # 추가 필터 조건 적용
                                if filter_conditions:
                                    for filter_col, filter_val in filter_conditions.items():
                                        if filter_col in df.columns:
                                            mask = mask & (df[filter_col] == filter_val)
                                
                                update_count = mask.sum()
                                total_update_count += update_count
                                
                                if update_count > 0 and not dry_run:
                                    df_updated.loc[mask, column_name] = new_val
                                    # EventTime 자동 업데이트 (Feature Store 동기화를 위해)
                                    self._update_event_time(df_updated, mask)
                
                # 4. 변환 함수 기반
                elif transform_function:
                    if filter_null_only:
                        # null 값만 대상으로 하는 경우
                        mask = df[column_name].isnull() if column_name in df.columns else pd.Series([True] * len(df), index=df.index)
                    else:
                        mask = pd.Series([True] * len(df), index=df.index)
                    
                    # 추가 필터 조건 적용
                    if filter_conditions:
                        for filter_col, filter_val in filter_conditions.items():
                            if filter_col in df.columns:
                                mask = mask & (df[filter_col] == filter_val)
                    
                    if mask.sum() > 0:
                        original_values = df.loc[mask, column_name] if column_name in df.columns else pd.Series([None] * mask.sum(), index=df.loc[mask].index)
                        
                        # copy_from_column 함수인지 확인
                        if hasattr(transform_function, 'is_copy_function') and transform_function.is_copy_function:
                            # copy_from_column의 경우 행 전체를 전달
                            transformed_values = df.loc[mask].apply(transform_function, axis=1)
                        else:
                            # 일반적인 변환 함수의 경우 컬럼 값만 전달
                            transformed_values = original_values.apply(transform_function)
                        
                        # 실제로 변경된 값만 카운트
                        try:
                            if column_name in df.columns:
                                changed_mask = original_values != transformed_values
                                total_update_count = changed_mask.sum()
                            else:
                                # 새로운 컬럼의 경우 모든 값이 변경된 것으로 처리
                                total_update_count = len(transformed_values)
                        except:
                            total_update_count = len(transformed_values)
                        
                        if total_update_count > 0 and not dry_run:
                            df_updated.loc[mask, column_name] = transformed_values
                            # EventTime 자동 업데이트 (Feature Store 동기화를 위해)
                            self._update_event_time(df_updated, mask)
                
                if total_update_count == 0:
                    return {
                        'file': file_path,
                        'status': 'no_updates',
                        'original_count': original_count,
                        'updated_count': 0
                    }
                
                if not dry_run:
                    # 로컬 백업 생성 (S3에는 업로드하지 않음)
                    timestamp = int(time.time())
                    local_backup_dir = os.path.join(os.getcwd(), 'backups')
                    os.makedirs(local_backup_dir, exist_ok=True)
                    
                    # 파일명에서 S3 경로 추출해서 로컬 파일명 생성
                    file_key = file_path.replace(f's3://{self.bucket_name}/', '')
                    safe_filename = file_key.replace('/', '_').replace('.parquet', f'_backup_{timestamp}.parquet')
                    backup_path = os.path.join(local_backup_dir, safe_filename)
                    
                    # S3에서 원본 파일을 로컬 백업으로 다운로드
                    try:
                        self.s3_client.download_file(
                            self.bucket_name, 
                            file_key, 
                            backup_path
                        )
                    except Exception as e:
                        click.echo(f"⚠️ 백업 생성 실패 (계속 진행): {e}", err=True)
                    
                    # 업데이트된 파일 저장
                    df_updated.to_parquet(file_path, engine='fastparquet', index=False)
                    
                    return {
                        'file': file_path,
                        'status': 'updated',
                        'original_count': original_count,
                        'updated_count': total_update_count,
                        'backup_path': backup_path
                    }
                else:
                    return {
                        'file': file_path,
                        'status': 'dry_run',
                        'original_count': original_count,
                        'updated_count': total_update_count
                    }
                    
            except Exception as e:
                click.echo(f"❌ 파일 처리 실패: {file_path}", err=True)
                click.echo(f"   오류 유형: {type(e).__name__}", err=True)
                click.echo(f"   오류 메시지: {str(e)}", err=True)
                
                # 상세한 오류 분석
                if "parquet" in str(e).lower():
                    click.echo("   → Parquet 파일 읽기 오류 (파일 손상 또는 형식 문제)", err=True)
                elif "columns" in str(e).lower() or "column" in str(e).lower():
                    click.echo(f"   → 컬럼 관련 오류 (대상 컬럼: {column_name})", err=True)
                elif "memory" in str(e).lower():
                    click.echo("   → 메모리 부족 오류", err=True)
                elif "s3" in str(e).lower():
                    click.echo("   → S3 접근 오류 (권한 또는 네트워크 문제)", err=True)
                elif "permission" in str(e).lower() or "access" in str(e).lower():
                    click.echo("   → 파일 접근 권한 오류", err=True)
                elif "timeout" in str(e).lower():
                    click.echo("   → 네트워크 타임아웃 오류", err=True)
                else:
                    click.echo("   → 기타 알 수 없는 오류", err=True)
                
                return {
                    'file': file_path,
                    'status': 'error',
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'original_count': 0,
                    'updated_count': 0,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
        
        # 병렬 처리
        max_workers = min(os.cpu_count(), 5)  # 동시 S3 작업 제한
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_file, file_path) for file_path in parquet_files]
            
            with click.progressbar(length=len(futures), label='파일 처리') as bar:
                for future in as_completed(futures):
                    file_result = future.result()
                    results['processed_files'] += 1
                    
                    if file_result['status'] in ['updated', 'dry_run']:
                        results['updated_records'] += file_result['updated_count']
                        if file_result.get('backup_path'):
                            results['backup_files'].append(file_result['backup_path'])
                    elif file_result['status'] == 'error':
                        results['failed_files'].append(file_result['file'])
                        results['failed_details'].append(file_result)  # 상세 정보도 저장
                    
                    bar.update(1)
        
        # 결과 요약
        click.echo("=== 업데이트 결과 ===")
        click.echo(f"총 파일 수: {results['total_files']:,}")
        click.echo(f"처리 완료: {results['processed_files']:,}")
        click.echo(f"업데이트된 레코드: {results['updated_records']:,}")
        click.echo(f"실패한 파일: {len(results['failed_files'])}")
        click.echo(f"백업 파일: {len(results['backup_files'])}")
        
        # DRY RUN 모드일 때 예상 시간 보고서 출력
        if dry_run and results['processed_files'] > 0:
            self._print_dry_run_time_estimate(results)
        
        if results['failed_files']:
            click.echo("실패한 파일 목록:")
            for failed_file in results['failed_files'][:10]:
                click.echo(f"  - {failed_file}")
            
            # 실패한 파일 목록을 현재 디렉토리에 저장
            failed_files_data = self._save_failed_files_report(results['failed_details'], self.feature_group_name, column_name)
            if failed_files_data:
                click.echo(f"상세한 실패 리포트가 저장되었습니다: {failed_files_data}")
        
        return results
    
    def validate_with_athena(self, 
                           column_name: str, 
                           value: str, 
                           expected_count: Optional[int] = None) -> Dict:
        """Athena 쿼리로 업데이트 결과 검증"""
        click.echo(f"Athena 쿼리 검증 시작: {column_name} = '{value}'")
        
        if not self.table_name:
            click.echo("Glue 테이블 정보를 찾을 수 없습니다", err=True)
            return {'status': 'error', 'message': 'Glue 테이블 정보 없음'}
        
        # Athena 쿼리 작성
        query = f"""
        SELECT COUNT(*) as record_count
        FROM "{self.database_name}"."{self.table_name}"
        WHERE {column_name} = '{value}'
        """
        
        try:
            # Athena 쿼리 실행
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database_name},
                ResultConfiguration={
                    'OutputLocation': f's3://{self.bucket_name}/athena-query-results/'
                }
            )
            
            query_execution_id = response['QueryExecutionId']
            click.echo(f"Athena 쿼리 실행 ID: {query_execution_id}")
            
            # 쿼리 완료 대기
            max_wait_time = 300  # 5분
            wait_interval = 5
            elapsed_time = 0
            
            with click.progressbar(length=max_wait_time, label='쿼리 실행 중') as bar:
                while elapsed_time < max_wait_time:
                    response = self.athena_client.get_query_execution(
                        QueryExecutionId=query_execution_id
                    )
                    
                    status = response['QueryExecution']['Status']['State']
                    
                    if status == 'SUCCEEDED':
                        break
                    elif status in ['FAILED', 'CANCELLED']:
                        error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                        click.echo(f"Athena 쿼리 실패: {error_msg}", err=True)
                        return {'status': 'error', 'message': error_msg}
                    
                    time.sleep(wait_interval)
                    elapsed_time += wait_interval
                    bar.update(wait_interval)
            
            if elapsed_time >= max_wait_time:
                click.echo("Athena 쿼리 타임아웃", err=True)
                return {'status': 'timeout', 'message': 'Query timeout'}
            
            # 결과 가져오기
            result = self.athena_client.get_query_results(
                QueryExecutionId=query_execution_id
            )
            
            # 결과 파싱
            rows = result['ResultSet']['Rows']
            if len(rows) >= 2:  # 헤더 + 데이터
                actual_count = int(rows[1]['Data'][0]['VarCharValue'])
                
                validation_result = {
                    'status': 'success',
                    'actual_count': actual_count,
                    'query_execution_id': query_execution_id
                }
                
                if expected_count is not None:
                    validation_result['expected_count'] = expected_count
                    validation_result['matches_expected'] = actual_count == expected_count
                    
                    if actual_count == expected_count:
                        click.echo(f"✅ 검증 성공: {actual_count:,}개 레코드")
                    else:
                        click.echo(f"⚠️ 검증 불일치: 예상 {expected_count:,}, 실제 {actual_count:,}")
                else:
                    click.echo(f"Athena 쿼리 결과: {actual_count:,}개 레코드")
                
                return validation_result
            else:
                click.echo("Athena 쿼리 결과가 비어있습니다", err=True)
                return {'status': 'error', 'message': 'Empty result'}
                
        except Exception as e:
            click.echo(f"Athena 쿼리 검증 실패: {e}", err=True)
            return {'status': 'error', 'message': str(e)}
    
    def create_transform_function(self, transform_type: str, **kwargs) -> Callable:
        """변환 함수 생성"""
        if transform_type == 'regex_replace':
            pattern = kwargs.get('pattern')
            replacement = kwargs.get('replacement', '')
            if not pattern:
                raise ValueError("정규식 패턴이 필요합니다")
            
            def regex_transform(value):
                if pd.isna(value):
                    return value
                return re.sub(pattern, replacement, str(value))
            
            return regex_transform
        
        elif transform_type == 'prefix_suffix':
            prefix = kwargs.get('prefix', '')
            suffix = kwargs.get('suffix', '')
            
            def prefix_suffix_transform(value):
                if pd.isna(value):
                    return value
                return f"{prefix}{value}{suffix}"
            
            return prefix_suffix_transform
        
        elif transform_type == 'uppercase':
            def upper_transform(value):
                if pd.isna(value):
                    return value
                return str(value).upper()
            return upper_transform
        
        elif transform_type == 'lowercase':
            def lower_transform(value):
                if pd.isna(value):
                    return value
                return str(value).lower()
            return lower_transform
        
        elif transform_type == 'copy_from_column':
            source_column = kwargs.get('source_column')
            if not source_column:
                raise ValueError("copy_from_column에는 source_column 파라미터가 필요합니다")
            
            def copy_from_column_transform(row):
                """다른 컬럼의 값을 복사하는 변환 함수 (row 전체를 받음)"""
                if hasattr(row, 'index') and source_column in row.index:
                    return row[source_column]
                return None
            
            copy_from_column_transform.source_column = source_column
            copy_from_column_transform.is_copy_function = True
            return copy_from_column_transform
        
        elif transform_type == 'extract_time_prefix':
            time_format = kwargs.get('time_format', 'auto')
            prefix_pattern = kwargs.get('prefix_pattern', r'(\d{4}-\d{2}-\d{2})')
            to_iso = kwargs.get('to_iso', True)
            source_column = kwargs.get('source_column')
            
            def extract_time_prefix_transform(row):
                """prefix에서 시간 정보를 추출해서 ISO 형식으로 변환"""
                if source_column and hasattr(row, 'index') and source_column in row.index:
                    value = row[source_column]
                else:
                    value = getattr(row, 'name', '') if hasattr(row, 'name') else str(row)
                
                if pd.isna(value):
                    return None
                
                value_str = str(value)
                
                # 정규식으로 시간 패턴 추출
                match = re.search(prefix_pattern, value_str)
                if not match:
                    if to_iso:
                        return datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
                    return None
                
                time_str = match.group(1)
                
                if not to_iso:
                    return time_str
                
                try:
                    # 시간 형식 감지 및 변환
                    if time_format == 'auto':
                        if re.match(r'\d{4}-\d{2}-\d{2}', time_str):
                            dt = datetime.strptime(time_str, '%Y-%m-%d')
                        elif re.match(r'\d{14}', time_str):  # YYYYMMDDHHMMSS
                            dt = datetime.strptime(time_str, '%Y%m%d%H%M%S')
                        elif re.match(r'\d{8}', time_str):
                            dt = datetime.strptime(time_str, '%Y%m%d')
                        elif re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', time_str):
                            dt = datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%S')
                        elif re.match(r'\d{10}', time_str):  # Unix timestamp
                            dt = datetime.fromtimestamp(int(time_str))
                        else:
                            dt = datetime.now()
                    else:
                        dt = datetime.strptime(time_str, time_format)
                    
                    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    
                except ValueError as e:
                    click.echo(f"시간 변환 실패 {time_str}: {e}", err=True)
                    return datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            
            if source_column:
                extract_time_prefix_transform.source_column = source_column
                extract_time_prefix_transform.is_copy_function = True
            
            return extract_time_prefix_transform
        
        else:
            raise ValueError(f"지원하지 않는 변환 타입: {transform_type}")
    
    def _deduplicate_by_latest_event_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """중복된 record_id 중 EventTime 기준으로 최신 레코드만 유지"""
        # record_id 컬럼 찾기 (대소문자 구분 없이)
        record_id_cols = [col for col in df.columns if col.lower() in ['record_identifier_value', 'recordidentifiervalue', 'record_id', 'recordid']]
        
        # EventTime 컬럼 찾기 (대소문자 구분 없이)
        event_time_cols = [col for col in df.columns if col.lower() in ['eventtime', 'event_time', 'time']]
        
        if not record_id_cols or not event_time_cols:
            # 중복 제거에 필요한 컬럼이 없으면 원본 반환
            return df
        
        record_id_col = record_id_cols[0]
        event_time_col = event_time_cols[0]
        
        try:
            # EventTime을 datetime으로 변환
            df_temp = df.copy()
            df_temp[event_time_col + '_parsed'] = pd.to_datetime(df_temp[event_time_col], errors='coerce')
            
            # record_id별로 최신 EventTime을 가진 레코드만 유지
            latest_records = df_temp.loc[df_temp.groupby(record_id_col)[event_time_col + '_parsed'].idxmax()]
            
            # 임시 컬럼 제거
            latest_records = latest_records.drop(columns=[event_time_col + '_parsed'])
            
            original_count = len(df)
            deduplicated_count = len(latest_records)
            
            # 중복 제거 로그 제거 - 너무 많이 나와서 보기 힘듬
            
            return latest_records
            
        except Exception as e:
            # 중복 제거 오류 로그도 제거
            return df
    
    def _count_null_records_with_athena(self, column_name: str, filter_conditions: Optional[Dict] = None) -> Dict:
        """Athena를 사용해서 null 값 레코드 수 계산"""
        click.echo(f"Athena로 null 값 레코드 수 계산: {column_name}")
        
        if not self.table_name:
            click.echo("Glue 테이블 정보를 찾을 수 없습니다. 기본 방식으로 처리합니다.", err=True)
            # 기본 방식으로 fallback
            parquet_files = self.get_offline_data_paths()
            return {'total_files': len(parquet_files), 'match_counts': {}}
        
        # WHERE 절 조건 구성
        where_conditions = [f"{column_name} IS NULL"]
        
        if filter_conditions:
            for filter_col, filter_val in filter_conditions.items():
                where_conditions.append(f"{filter_col} = '{filter_val}'")
        
        where_clause = " AND ".join(where_conditions)
        
        # Athena 쿼리 작성
        query = f"""
        SELECT COUNT(*) as null_record_count
        FROM "{self.database_name}"."{self.table_name}"
        WHERE {where_clause}
        """
        
        try:
            # Athena 쿼리 실행
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database_name},
                ResultConfiguration={
                    'OutputLocation': f's3://{self.bucket_name}/athena-query-results/'
                }
            )
            
            query_execution_id = response['QueryExecutionId']
            click.echo(f"Athena 쿼리 실행 ID: {query_execution_id}")
            
            # 쿼리 완료 대기
            max_wait_time = 300  # 5분
            wait_interval = 5
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                status = response['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    click.echo(f"Athena 쿼리 실패: {error_msg}", err=True)
                    # 기본 방식으로 fallback
                    parquet_files = self.get_offline_data_paths()
                    return {'total_files': len(parquet_files), 'match_counts': {}}
                
                time.sleep(wait_interval)
                elapsed_time += wait_interval
            
            if elapsed_time >= max_wait_time:
                click.echo("Athena 쿼리 타임아웃. 기본 방식으로 처리합니다.", err=True)
                # 기본 방식으로 fallback
                parquet_files = self.get_offline_data_paths()
                return {'total_files': len(parquet_files), 'match_counts': {}}
            
            # 결과 가져오기
            result = self.athena_client.get_query_results(
                QueryExecutionId=query_execution_id
            )
            
            # 결과 파싱
            rows = result['ResultSet']['Rows']
            if len(rows) >= 2:  # 헤더 + 데이터
                null_count = int(rows[1]['Data'][0]['VarCharValue'])
                
                click.echo(f"Athena 결과: null 값 레코드 {null_count:,}개")
                
                return {
                    'total_files': 0,  # Athena 사용시에는 파일 수 불확정
                    'match_counts': {'null_values': null_count}
                }
            else:
                click.echo("Athena 쿼리 결과가 비어있습니다. 기본 방식으로 처리합니다.", err=True)
                # 기본 방식으로 fallback
                parquet_files = self.get_offline_data_paths()
                return {'total_files': len(parquet_files), 'match_counts': {}}
                
        except Exception as e:
            click.echo(f"Athena 쿼리 실행 실패: {e}. 기본 방식으로 처리합니다.", err=True)
            # 기본 방식으로 fallback
            parquet_files = self.get_offline_data_paths()
            return {'total_files': len(parquet_files), 'match_counts': {}}
    
    def get_null_records_with_athena(self, column_name: str, filter_conditions: Optional[Dict] = None) -> List[Dict]:
        """Athena를 사용해서 null 값이 있는 레코드의 정보 가져오기 (record_id와 파일 위치)"""
        click.echo(f"Athena로 null 값 레코드 정보 조회: {column_name}")
        
        if not self.table_name:
            click.echo("Glue 테이블 정보를 찾을 수 없습니다", err=True)
            return []
        
        # record_id 컬럼 이름 추정 (일반적인 이름들)
        record_id_candidates = ['record_identifier_value', 'recordidentifiervalue', 'record_id', 'recordid']
        
        # WHERE 절 조건 구성
        where_conditions = [f"{column_name} IS NULL"]
        
        if filter_conditions:
            for filter_col, filter_val in filter_conditions.items():
                where_conditions.append(f"{filter_col} = '{filter_val}'")
        
        where_clause = " AND ".join(where_conditions)
        
        # 레코드 식별자 컬럼을 찾기 위한 쿼리 (샘플)
        sample_query = f"""
        SELECT * FROM "{self.database_name}"."{self.table_name}"
        LIMIT 1
        """
        
        try:
            # 먼저 샘플 쿼리로 컬럼명 확인
            response = self.athena_client.start_query_execution(
                QueryString=sample_query,
                QueryExecutionContext={'Database': self.database_name},
                ResultConfiguration={
                    'OutputLocation': f's3://{self.bucket_name}/athena-query-results/'
                }
            )
            
            query_execution_id = response['QueryExecutionId']
            
            # 쿼리 완료 대기
            max_wait_time = 60
            wait_interval = 2
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                status = response['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    click.echo("컬럼 정보 조회 실패", err=True)
                    return []
                
                time.sleep(wait_interval)
                elapsed_time += wait_interval
            
            # 컬럼 정보 가져오기
            result = self.athena_client.get_query_results(
                QueryExecutionId=query_execution_id
            )
            
            # 헤더에서 record_id 컬럼 찾기
            record_id_column = None
            if result['ResultSet']['Rows']:
                headers = [col['VarCharValue'] for col in result['ResultSet']['Rows'][0]['Data']]
                for candidate in record_id_candidates:
                    if candidate.lower() in [h.lower() for h in headers]:
                        record_id_column = candidate
                        break
                
                # 정확한 이름 찾기
                if not record_id_column:
                    for header in headers:
                        if 'record' in header.lower() and 'id' in header.lower():
                            record_id_column = header
                            break
            
            if not record_id_column:
                click.echo("Record ID 컬럼을 찾을 수 없습니다", err=True)
                return []
            
            click.echo(f"Record ID 컬럼 발견: {record_id_column}")
            
            # 실제 null 값 레코드 조회 쿼리
            records_query = f"""
            SELECT {record_id_column}, "$path" as file_path
            FROM "{self.database_name}"."{self.table_name}"
            WHERE {where_clause}
            LIMIT 10000
            """
            
            # null 값 레코드 조회 실행
            response = self.athena_client.start_query_execution(
                QueryString=records_query,
                QueryExecutionContext={'Database': self.database_name},
                ResultConfiguration={
                    'OutputLocation': f's3://{self.bucket_name}/athena-query-results/'
                }
            )
            
            query_execution_id = response['QueryExecutionId']
            click.echo(f"null 값 레코드 조회 실행 ID: {query_execution_id}")
            
            # 쿼리 완료 대기
            max_wait_time = 300
            wait_interval = 5
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                status = response['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    click.echo(f"null 값 레코드 조회 실패: {error_msg}", err=True)
                    return []
                
                time.sleep(wait_interval)
                elapsed_time += wait_interval
            
            if elapsed_time >= max_wait_time:
                click.echo("null 값 레코드 조회 타임아웃", err=True)
                return []
            
            # 결과 가져오기
            result = self.athena_client.get_query_results(
                QueryExecutionId=query_execution_id
            )
            
            null_records = []
            rows = result['ResultSet']['Rows']
            
            # 헤더 스킵하고 데이터 파싱
            for row in rows[1:]:  # 첫 번째 행은 헤더
                data = row['Data']
                if len(data) >= 2:
                    record_id = data[0].get('VarCharValue')
                    file_path = data[1].get('VarCharValue')
                    
                    if record_id and file_path:
                        null_records.append({
                            'record_id': record_id,
                            'file_path': file_path
                        })
            
            click.echo(f"null 값 레코드 {len(null_records)}개 발견")
            return null_records
                
        except Exception as e:
            click.echo(f"null 값 레코드 조회 실패: {e}", err=True)
            return []
    
    def _get_target_files_for_null_update(self, column_name: str, filter_conditions: Optional[Dict] = None) -> List[str]:
        """null 값 업데이트를 위한 대상 파일 목록 조회"""
        click.echo(f"null 값이 있는 파일 목록 조회: {column_name}")
        
        if not self.table_name:
            click.echo("Glue 테이블 정보가 없어 모든 파일을 대상으로 처리합니다", err=True)
            return self.get_offline_data_paths()
        
        # WHERE 절 조건 구성
        where_conditions = [f"{column_name} IS NULL"]
        
        if filter_conditions:
            for filter_col, filter_val in filter_conditions.items():
                where_conditions.append(f"{filter_col} = '{filter_val}'")
        
        where_clause = " AND ".join(where_conditions)
        
        # 파일 경로별 null 값 레코드 수 조회 쿼리
        query = f"""
        SELECT "$path" as file_path, COUNT(*) as null_count
        FROM "{self.database_name}"."{self.table_name}"
        WHERE {where_clause}
        GROUP BY "$path"
        HAVING COUNT(*) > 0
        ORDER BY null_count DESC
        """
        
        try:
            # Athena 쿼리 실행
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database_name},
                ResultConfiguration={
                    'OutputLocation': f's3://{self.bucket_name}/athena-query-results/'
                }
            )
            
            query_execution_id = response['QueryExecutionId']
            click.echo(f"파일 목록 쿼리 실행 ID: {query_execution_id}")
            
            # 쿼리 완료 대기
            max_wait_time = 300
            wait_interval = 5
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                status = response['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    click.echo(f"파일 목록 쿼리 실패: {error_msg}", err=True)
                    return self.get_offline_data_paths()
                
                time.sleep(wait_interval)
                elapsed_time += wait_interval
            
            if elapsed_time >= max_wait_time:
                click.echo("파일 목록 쿼리 타임아웃", err=True)
                return self.get_offline_data_paths()
            
            # 결과 가져오기
            result = self.athena_client.get_query_results(
                QueryExecutionId=query_execution_id
            )
            
            target_files = []
            total_null_count = 0
            rows = result['ResultSet']['Rows']
            
            # 헤더 스킵하고 데이터 파싱
            for row in rows[1:]:  # 첫 번째 행은 헤더
                data = row['Data']
                if len(data) >= 2:
                    file_path = data[0].get('VarCharValue')
                    null_count = int(data[1].get('VarCharValue', 0))
                    
                    if file_path and null_count > 0:
                        target_files.append(file_path)
                        total_null_count += null_count
            
            click.echo(f"null 값이 있는 파일: {len(target_files)}개 (전체 null 레코드: {total_null_count:,}개)")
            
            if not target_files:
                click.echo("null 값이 있는 파일이 없습니다")
                return []
            
            return target_files
                
        except Exception as e:
            click.echo(f"파일 목록 조회 실패: {e}", err=True)
            return self.get_offline_data_paths()
    
    def _save_failed_files_report(self, failed_details: List[Dict], feature_group_name: str, column_name: str) -> str:
        """실패한 파일들의 상세 리포트를 파일로 저장"""
        if not failed_details:
            return None
        
        try:
            # 현재 시간으로 파일명 생성
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"failed_files_{feature_group_name}_{column_name}_{timestamp}.json"
            
            # 현재 작업 디렉토리에 저장
            filepath = os.path.join(os.getcwd(), filename)
            
            # 리포트 데이터 구성
            report_data = {
                "metadata": {
                    "feature_group": feature_group_name,
                    "column": column_name,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "total_failed": len(failed_details)
                },
                "failed_files": failed_details,
                "summary": {
                    "error_types": {},
                    "common_errors": []
                }
            }
            
            # 오류 유형별 통계
            error_types = {}
            for detail in failed_details:
                error_type = detail.get('error_type', 'Unknown')
                error_types[error_type] = error_types.get(error_type, 0) + 1
            
            report_data["summary"]["error_types"] = error_types
            
            # 공통 오류 패턴 분석
            common_patterns = []
            for detail in failed_details:
                error_msg = str(detail.get('error', '')).lower()
                if 'parquet' in error_msg:
                    common_patterns.append('Parquet format issue')
                elif 'column' in error_msg:
                    common_patterns.append('Column not found')
                elif 's3' in error_msg or 'access' in error_msg:
                    common_patterns.append('S3 access issue')
                elif 'memory' in error_msg:
                    common_patterns.append('Memory issue')
                elif 'timeout' in error_msg:
                    common_patterns.append('Timeout issue')
            
            # 공통 패턴 중복 제거
            report_data["summary"]["common_errors"] = list(set(common_patterns))
            
            # JSON 파일로 저장
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            
            # 간단한 텍스트 버전도 저장
            txt_filename = f"failed_files_{feature_group_name}_{column_name}_{timestamp}.txt"
            txt_filepath = os.path.join(os.getcwd(), txt_filename)
            
            with open(txt_filepath, 'w', encoding='utf-8') as f:
                f.write(f"=== 실패한 파일 리포트 ===\n")
                f.write(f"Feature Group: {feature_group_name}\n")
                f.write(f"Column: {column_name}\n")
                f.write(f"생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"총 실패 파일: {len(failed_details)}개\n\n")
                
                f.write("=== 오류 유형별 통계 ===\n")
                for error_type, count in error_types.items():
                    f.write(f"{error_type}: {count}개\n")
                f.write("\n")
                
                if common_patterns:
                    f.write("=== 주요 오류 패턴 ===\n")
                    for pattern in set(common_patterns):
                        f.write(f"- {pattern}\n")
                    f.write("\n")
                
                f.write("=== 실패한 파일 상세 목록 ===\n")
                for i, detail in enumerate(failed_details, 1):
                    f.write(f"{i}. {detail['file']}\n")
                    f.write(f"   오류 유형: {detail.get('error_type', 'Unknown')}\n")
                    f.write(f"   오류 메시지: {detail.get('error', 'No message')}\n")
                    f.write(f"   시간: {detail.get('timestamp', 'Unknown')}\n")
                    f.write("\n")
            
            click.echo(f"JSON 리포트: {filepath}")
            click.echo(f"텍스트 리포트: {txt_filepath}")
            
            return f"{filename} (JSON), {txt_filename} (TXT)"
            
        except Exception as e:
            click.echo(f"실패 리포트 저장 중 오류: {e}", err=True)
            return None

    def _update_event_time(self, df, mask):
        """EventTime 자동 업데이트 (기존 시간에서 10초 추가)"""
        # EventTime 컬럼 찾기 (대소문자 구분 없이)
        event_time_cols = [col for col in df.columns if col.lower() in ['eventtime', 'event_time', 'time']]
        
        if event_time_cols:
            event_time_col = event_time_cols[0]  # 첫 번째 발견된 컬럼 사용
            
            try:
                # 기존 EventTime 값들을 파싱하고 10초 추가
                for idx in df.loc[mask].index:
                    original_time = df.loc[idx, event_time_col]
                    
                    try:
                        # ISO 형식 파싱
                        if isinstance(original_time, str):
                            if 'T' in original_time:
                                # ISO format: 2024-01-15T10:30:45Z
                                dt = datetime.strptime(original_time.rstrip('Z'), '%Y-%m-%dT%H:%M:%S')
                            else:
                                # Date only: 2024-01-15
                                dt = datetime.strptime(original_time, '%Y-%m-%d')
                        else:
                            # 현재 시간 사용 (파싱 실패시)
                            dt = datetime.now()
                        
                        # 5초 추가
                        new_time = dt + timedelta(seconds=5)
                        df.loc[idx, event_time_col] = new_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                        
                    except:
                        # 파싱 실패시 현재 시간 + 5초 사용
                        new_time = datetime.now() + timedelta(seconds=5)
                        df.loc[idx, event_time_col] = new_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                        
                # 로그 제거 - 너무 많이 나와서 보기 힘듬
                
            except Exception as e:
                click.echo(f"  EventTime 업데이트 중 오류: {e}", err=True)
    
    def cleanup_backup_files(self, backup_files: List[str], confirm: bool = False) -> Dict:
        """백업 파일 정리 (로컬 백업용 - 현재는 사용 안함)"""
        if not confirm:
            click.echo("백업 파일 정리를 위해서는 confirm=True로 설정하세요")
            return {'status': 'skipped', 'message': 'confirmation required'}
        
        click.echo(f"로컬 백업 파일 {len(backup_files)}개 삭제 중...")
        
        deleted_count = 0
        failed_count = 0
        
        for backup_file in backup_files:
            try:
                if os.path.exists(backup_file):
                    os.remove(backup_file)
                    deleted_count += 1
            except Exception as e:
                click.echo(f"로컬 백업 파일 삭제 실패 {backup_file}: {e}", err=True)
                failed_count += 1
        
        click.echo(f"로컬 백업 파일 정리 완료: 삭제 {deleted_count}, 실패 {failed_count}")
        return {
            'status': 'completed',
            'deleted_count': deleted_count,
            'failed_count': failed_count
        }
    
    def cleanup_s3_backup_files(self, confirm: bool = False) -> Dict:
        """S3에 있는 모든 _backup_ 파일들 정리"""
        if not confirm:
            click.echo("S3 백업 파일 정리를 위해서는 confirm=True로 설정하세요")
            return {'status': 'skipped', 'message': 'confirmation required'}
        
        click.echo("S3에서 _backup_ 파일들 검색 중...")
        
        # S3에서 _backup_ 파일들 찾기
        backup_files = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        try:
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if '_backup_' in obj['Key'] and obj['Key'].endswith('.parquet'):
                            backup_files.append(obj['Key'])
            
            if not backup_files:
                click.echo("S3에서 _backup_ 파일을 찾을 수 없습니다")
                return {'status': 'completed', 'deleted_count': 0, 'failed_count': 0}
            
            click.echo(f"발견된 S3 백업 파일 수: {len(backup_files):,}")
            
            # 삭제 진행
            deleted_count = 0
            failed_count = 0
            
            with click.progressbar(backup_files, label='S3 백업 파일 삭제') as bar:
                for backup_key in bar:
                    try:
                        self.s3_client.delete_object(Bucket=self.bucket_name, Key=backup_key)
                        deleted_count += 1
                    except Exception as e:
                        click.echo(f"S3 백업 파일 삭제 실패 {backup_key}: {e}", err=True)
                        failed_count += 1
            
            click.echo(f"S3 백업 파일 정리 완료: 삭제 {deleted_count}, 실패 {failed_count}")
            return {
                'status': 'completed',
                'deleted_count': deleted_count,
                'failed_count': failed_count
            }
            
        except Exception as e:
            click.echo(f"S3 백업 파일 목록 조회 실패: {e}", err=True)
            return {'status': 'error', 'message': str(e)}
    
    def _print_dry_run_time_estimate(self, results: Dict) -> None:
        """DRY RUN 모드에서 실제 작업 시 예상 소요 시간 보고서 출력"""
        click.echo("\n=== 📊 실제 작업 시 예상 소요 시간 보고서 ===")
        
        total_files = results['total_files']
        processed_files = results['processed_files']
        updated_records = results['updated_records']
        failed_files = len(results['failed_files'])
        
        if processed_files == 0:
            click.echo("⚠️ 처리된 파일이 없어 시간 예상이 불가능합니다.")
            return
        
        # 기본 시간 계산 (경험적 수치)
        # - 파일당 평균 처리 시간: 2-5초 (크기에 따라)
        # - 레코드당 평균 처리 시간: 0.001초
        # - S3 업로드 오버헤드: 파일당 1-3초
        # - 백업 생성 시간: 파일당 0.5-2초
        
        avg_records_per_file = updated_records / processed_files if processed_files > 0 else 0
        
        # 파일 크기별 예상 시간 (레코드 수 기준)
        if avg_records_per_file < 100:
            base_time_per_file = 2  # 작은 파일
            backup_time_per_file = 0.5
            upload_time_per_file = 1
        elif avg_records_per_file < 1000:
            base_time_per_file = 3  # 중간 파일
            backup_time_per_file = 1
            upload_time_per_file = 1.5
        elif avg_records_per_file < 10000:
            base_time_per_file = 4  # 큰 파일
            backup_time_per_file = 1.5
            upload_time_per_file = 2
        else:
            base_time_per_file = 5  # 매우 큰 파일
            backup_time_per_file = 2
            upload_time_per_file = 3
        
        # 병렬 처리 고려 (기본적으로 5개 워커)
        max_workers = min(os.cpu_count(), 5)
        
        # 실제 처리할 파일 수 (실패 파일 제외)
        successful_files = processed_files - failed_files
        
        # 총 예상 시간 계산
        total_processing_time = successful_files * base_time_per_file
        total_backup_time = successful_files * backup_time_per_file  
        total_upload_time = successful_files * upload_time_per_file
        
        # 병렬 처리로 인한 시간 단축
        parallel_processing_time = total_processing_time / max_workers
        parallel_backup_time = total_backup_time / max_workers
        parallel_upload_time = total_upload_time / max_workers
        
        # 총 예상 시간 (순차적이 아닌 병렬)
        estimated_total_seconds = max(parallel_processing_time, parallel_backup_time) + parallel_upload_time
        
        # 여유 시간 추가 (네트워크 지연, 예상치 못한 오버헤드)
        estimated_total_seconds *= 1.3
        
        # 시간 포맷팅
        hours = int(estimated_total_seconds // 3600)
        minutes = int((estimated_total_seconds % 3600) // 60)
        seconds = int(estimated_total_seconds % 60)
        
        click.echo(f"📋 분석 기준:")
        click.echo(f"  • 성공 파일: {successful_files:,}개")
        click.echo(f"  • 실패 파일: {failed_files:,}개")
        click.echo(f"  • 업데이트 레코드: {updated_records:,}개")
        click.echo(f"  • 파일당 평균 레코드 수: {avg_records_per_file:.1f}개")
        click.echo(f"  • 병렬 워커 수: {max_workers}개")
        
        click.echo(f"\n⏱️ 예상 소요 시간:")
        if hours > 0:
            click.echo(f"  총 소요 시간: {hours}시간 {minutes}분 {seconds}초")
        elif minutes > 0:
            click.echo(f"  총 소요 시간: {minutes}분 {seconds}초")
        else:
            click.echo(f"  총 소요 시간: {seconds}초")
        
        click.echo(f"\n📊 세부 예상 시간 (병렬 처리 기준):")
        click.echo(f"  • 데이터 처리: {int(parallel_processing_time // 60)}분 {int(parallel_processing_time % 60)}초")
        click.echo(f"  • 백업 생성: {int(parallel_backup_time // 60)}분 {int(parallel_backup_time % 60)}초") 
        click.echo(f"  • S3 업로드: {int(parallel_upload_time // 60)}분 {int(parallel_upload_time % 60)}초")
        
        # 주의사항
        click.echo(f"\n⚠️ 주의사항:")
        click.echo(f"  • 실제 시간은 파일 크기, 네트워크 상태, S3 성능에 따라 달라질 수 있습니다")
        click.echo(f"  • 백업 파일은 로컬에 저장되므로 충분한 디스크 공간이 필요합니다")
        if updated_records > 100000:
            click.echo(f"  • 대용량 데이터({updated_records:,}개 레코드)이므로 시간이 오래 걸릴 수 있습니다")
        
        # 권장사항
        click.echo(f"\n💡 권장사항:")
        if estimated_total_seconds > 1800:  # 30분 이상
            click.echo(f"  • 작업 시간이 오래 예상되므로 백그라운드에서 실행하는 것을 권장합니다")
            click.echo(f"  • nohup이나 screen을 사용하여 세션이 끊어져도 계속 실행되도록 하세요")
        if successful_files > 1000:
            click.echo(f"  • 파일 수가 많으므로({successful_files:,}개) 배치 크기 조정을 고려해보세요")
        if failed_files > 0:
            click.echo(f"  • {failed_files}개 파일이 실패했으므로 실제 실행 전에 원인을 확인해주세요")


def batch_update(config, feature_group_name: str, column_name: str, 
                old_value: str = None, new_value: str = None,
                mapping_file: str = None, conditional_mapping: str = None,
                transform_type: str = None, transform_options: dict = None,
                dry_run: bool = True, skip_validation: bool = False,
                filter_column: str = None, filter_value: str = None,
                filter_null_only: bool = False,
                cleanup_backups: bool = False, batch_size: int = 1000,
                deduplicate: bool = True):
    """배치 업데이트 실행"""
    start_time = time.time()  # 시작 시간 기록
    try:
        # Feature Store 업데이터 초기화
        updater = SageMakerFeatureStoreUpdater(
            feature_group_name=feature_group_name,
            config=config,
            batch_size=batch_size,
            deduplicate=deduplicate
        )
        
        # 데이터 구조 확인
        click.echo("=== 데이터 구조 확인 ===")
        sample_df = updater.read_data_sample(100)
        
        if sample_df.empty:
            click.echo("데이터를 읽을 수 없습니다", err=True)
            return
        
        # 업데이트 대상 컬럼 확인
        if column_name not in sample_df.columns:
            click.echo(f"⚠️ 컬럼 '{column_name}'을(를) 찾을 수 없습니다.")
            click.echo("사용 가능한 컬럼:")
            for col in sample_df.columns:
                click.echo(f"  - {col}")
            
            if not click.confirm(f"'{column_name}'을 새 컬럼으로 추가하시겠습니까?"):
                return
        
        # 업데이트 방식 결정 및 파라미터 준비
        value_mapping = None
        conditional_map = None
        transform_function = None
        
        if mapping_file:
            value_mapping = updater.load_mapping_from_file(mapping_file)
            
        elif conditional_mapping:
            conditional_map = updater.create_conditional_mapping(conditional_mapping)
            
        elif transform_type:
            transform_function = updater.create_transform_function(transform_type, **(transform_options or {}))
        
        # 추가 필터 조건 설정
        filter_conditions = None
        if filter_column and filter_value:
            filter_conditions = {filter_column: filter_value}
        
        # 변경 대상 레코드 수 확인
        click.echo("=== 변경 대상 레코드 수 계산 ===")
        count_result = updater.count_matching_records(
            column_name=column_name,
            old_value=old_value,
            value_mapping=value_mapping,
            conditional_mapping=conditional_map,
            transform_function=transform_function,
            filter_conditions=filter_conditions,
            filter_null_only=filter_null_only
        )
        
        total_target_count = sum(count_result['match_counts'].values())
        
        if transform_function:
            click.echo(f"변환 함수 '{transform_type}' 적용 대상: {total_target_count:,}개 레코드")
        elif total_target_count == 0:
            click.echo("변경 대상 레코드가 없습니다", err=True)
            return
        
        # 사용자 확인
        if not dry_run:
            click.echo(f"\n⚠️ 주의: {total_target_count:,}개의 레코드가 변경됩니다!")
            click.echo(f"컬럼: {column_name}")
            
            if old_value and new_value:
                click.echo(f"단일 변경: '{old_value}' -> '{new_value}'")
            elif mapping_file:
                click.echo(f"매핑 파일: {mapping_file}")
            elif conditional_mapping:
                click.echo("조건부 매핑")
            elif transform_type:
                click.echo(f"변환 함수: {transform_type}")
            
            if filter_conditions:
                click.echo(f"필터 조건: {filter_conditions}")
            
            if not click.confirm("계속하시겠습니까?"):
                click.echo("작업이 취소되었습니다")
                return
        
        # 업데이트 실행
        click.echo("=== 레코드 업데이트 실행 ===")
        update_results = updater.update_records_batch(
            column_name=column_name,
            old_value=old_value,
            new_value=new_value,
            value_mapping=value_mapping,
            conditional_mapping=conditional_map,
            transform_function=transform_function,
            dry_run=dry_run,
            filter_conditions=filter_conditions,
            filter_null_only=filter_null_only
        )
        
        # Athena 검증
        if not skip_validation and not dry_run and old_value and new_value:
            click.echo("=== Athena 쿼리 검증 ===")
            
            # 기존 값 확인
            old_validation = updater.validate_with_athena(column_name, old_value)
            
            # 새 값 확인
            new_validation = updater.validate_with_athena(
                column_name, 
                new_value, 
                expected_count=update_results['updated_records']
            )
            
            click.echo(f"기존 값 '{old_value}' 레코드 수: {old_validation.get('actual_count', 'N/A')}")
            click.echo(f"새 값 '{new_value}' 레코드 수: {new_validation.get('actual_count', 'N/A')}")
        
        # 백업 파일 정리
        if cleanup_backups and not dry_run:
            click.echo("=== 백업 파일 정리 ===")
            
            # 로컬 백업 파일 정리
            if update_results['backup_files']:
                if click.confirm(f"로컬 백업 파일 {len(update_results['backup_files'])}개를 삭제하시겠습니까?"):
                    cleanup_result = updater.cleanup_backup_files(
                        update_results['backup_files'], 
                        confirm=True
                    )
                    click.echo(f"로컬 백업 파일 정리: {cleanup_result}")
            
            # S3 백업 파일 정리
            if click.confirm("S3에 있는 모든 _backup_ 파일들을 삭제하시겠습니까? (주의: 되돌릴 수 없습니다)"):
                s3_cleanup_result = updater.cleanup_s3_backup_files(confirm=True)
                click.echo(f"S3 백업 파일 정리: {s3_cleanup_result}")
        
        # 총 소요 시간 계산 및 출력
        end_time = time.time()
        duration = end_time - start_time
        duration_minutes = int(duration // 60)
        duration_seconds = int(duration % 60)
        
        click.echo("=== 작업 완료 ===")
        if duration_minutes > 0:
            click.echo(f"총 소요 시간: {duration_minutes}분 {duration_seconds}초")
        else:
            click.echo(f"총 소요 시간: {duration_seconds}초")
        
    except KeyboardInterrupt:
        end_time = time.time()
        duration = end_time - start_time
        click.echo(f"작업이 사용자에 의해 중단되었습니다 (소요 시간: {int(duration)}초)")
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        click.echo(f"작업 중 오류 발생: {e} (소요 시간: {int(duration)}초)", err=True)
        raise