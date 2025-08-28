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
                 batch_size: int = 1000):
        """
        초기화
        
        Args:
            feature_group_name: Feature Group 이름
            config: CLI 설정 객체
            batch_size: 배치 처리 크기
        """
        self.feature_group_name = feature_group_name
        self.region_name = config.region
        self.batch_size = batch_size
        
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
            click.echo(f"샘플 데이터 읽기 실패: {e}", err=True)
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
                             conditional_mapping: Dict = None) -> Dict:
        """변경 대상 레코드 수 계산"""
        click.echo(f"변경 대상 레코드 수 계산 중... (컬럼: {column_name})")
        
        parquet_files = self.get_offline_data_paths()
        result_counts = {'total_files': len(parquet_files), 'match_counts': {}}
        
        # 필요한 컬럼들 결정
        required_columns = [column_name]
        if conditional_mapping:
            for condition_col in conditional_mapping.keys():
                if condition_col not in required_columns:
                    required_columns.append(condition_col)
        
        def count_in_file(file_path: str) -> Dict:
            try:
                df = pd.read_parquet(file_path, columns=required_columns, engine='fastparquet')
                
                file_counts = {}
                
                if old_value is not None:
                    # 단일 값 매칭
                    count = len(df[df[column_name] == old_value])
                    file_counts[old_value] = count
                    
                elif value_mapping:
                    # 매핑 파일 기반
                    for old_val in value_mapping.keys():
                        count = len(df[df[column_name] == old_val])
                        if count > 0:
                            file_counts[old_val] = count
                            
                elif conditional_mapping:
                    # 조건부 매핑
                    for condition_col, condition_mappings in conditional_mapping.items():
                        for condition_val, value_map in condition_mappings.items():
                            condition_mask = df[condition_col] == condition_val
                            for old_val in value_map.keys():
                                mask = condition_mask & (df[column_name] == old_val)
                                count = mask.sum()
                                if count > 0:
                                    key = f"{condition_col}={condition_val}, {column_name}={old_val}"
                                    file_counts[key] = count
                
                return file_counts
                
            except Exception as e:
                click.echo(f"파일 읽기 오류 {file_path}: {e}", err=True)
                return {}
        
        # 병렬 처리로 각 파일의 매칭 레코드 수 계산
        max_workers = min(os.cpu_count(), 10)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(count_in_file, file_path) for file_path in parquet_files]
            
            with click.progressbar(length=len(futures), label='파일 분석') as bar:
                for future in as_completed(futures):
                    file_counts = future.result()
                    
                    # 결과 집계
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
        
        return result_counts
    
    def update_records_batch(self,
                           column_name: str,
                           old_value: str = None,
                           new_value: str = None,
                           value_mapping: Dict = None,
                           conditional_mapping: Dict = None,
                           transform_function: Callable = None,
                           dry_run: bool = True,
                           filter_conditions: Optional[Dict] = None) -> Dict:
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
        
        if filter_conditions:
            click.echo(f"  추가 필터: {filter_conditions}")
        
        parquet_files = self.get_offline_data_paths()
        results = {
            'total_files': len(parquet_files),
            'processed_files': 0,
            'updated_records': 0,
            'failed_files': [],
            'backup_files': []
        }
        
        def process_file(file_path: str) -> Dict:
            """단일 파일 처리"""
            try:
                # 파일 읽기
                df = pd.read_parquet(file_path, engine='fastparquet')
                original_count = len(df)
                total_update_count = 0
                
                # 대상 컬럼이 존재하지 않는 경우 새로 추가
                if column_name not in df.columns:
                    df[column_name] = None
                
                # 데이터프레임 복사 (변경 추적용)
                df_updated = df.copy() if not dry_run else df
                
                # 1. 단일 값 변경
                if old_value is not None and new_value is not None:
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
                    # 백업 생성
                    backup_path = file_path.replace('.parquet', f'_backup_{int(time.time())}.parquet')
                    
                    # S3에서 백업 복사
                    copy_source = {
                        'Bucket': self.bucket_name,
                        'Key': file_path.replace(f's3://{self.bucket_name}/', '')
                    }
                    backup_key = backup_path.replace(f's3://{self.bucket_name}/', '')
                    
                    self.s3_client.copy_object(
                        CopySource=copy_source,
                        Bucket=self.bucket_name,
                        Key=backup_key
                    )
                    
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
                click.echo(f"파일 처리 오류 {file_path}: {e}", err=True)
                return {
                    'file': file_path,
                    'status': 'error',
                    'error': str(e),
                    'original_count': 0,
                    'updated_count': 0
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
                    
                    bar.update(1)
        
        # 결과 요약
        click.echo("=== 업데이트 결과 ===")
        click.echo(f"총 파일 수: {results['total_files']:,}")
        click.echo(f"처리 완료: {results['processed_files']:,}")
        click.echo(f"업데이트된 레코드: {results['updated_records']:,}")
        click.echo(f"실패한 파일: {len(results['failed_files'])}")
        click.echo(f"백업 파일: {len(results['backup_files'])}")
        
        if results['failed_files']:
            click.echo("실패한 파일 목록:")
            for failed_file in results['failed_files'][:10]:
                click.echo(f"  - {failed_file}")
        
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
                        
                        # 10초 추가
                        new_time = dt + timedelta(seconds=10)
                        df.loc[idx, event_time_col] = new_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                        
                    except:
                        # 파싱 실패시 현재 시간 + 10초 사용
                        new_time = datetime.now() + timedelta(seconds=10)
                        df.loc[idx, event_time_col] = new_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                        
                click.echo(f"  EventTime 컬럼 '{event_time_col}'을 자동 업데이트했습니다 (+10초)")
                
            except Exception as e:
                click.echo(f"  EventTime 업데이트 중 오류: {e}", err=True)
    
    def cleanup_backup_files(self, backup_files: List[str], confirm: bool = False) -> Dict:
        """백업 파일 정리"""
        if not confirm:
            click.echo("백업 파일 정리를 위해서는 confirm=True로 설정하세요")
            return {'status': 'skipped', 'message': 'confirmation required'}
        
        click.echo(f"백업 파일 {len(backup_files)}개 삭제 중...")
        
        deleted_count = 0
        failed_count = 0
        
        with click.progressbar(backup_files, label='백업 파일 삭제') as bar:
            for backup_file in bar:
                try:
                    key = backup_file.replace(f's3://{self.bucket_name}/', '')
                    self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
                    deleted_count += 1
                except Exception as e:
                    click.echo(f"백업 파일 삭제 실패 {backup_file}: {e}", err=True)
                    failed_count += 1
        
        click.echo(f"백업 파일 정리 완료: 삭제 {deleted_count}, 실패 {failed_count}")
        return {
            'status': 'completed',
            'deleted_count': deleted_count,
            'failed_count': failed_count
        }


def batch_update(config, feature_group_name: str, column_name: str, 
                old_value: str = None, new_value: str = None,
                mapping_file: str = None, conditional_mapping: str = None,
                transform_type: str = None, transform_options: dict = None,
                dry_run: bool = True, skip_validation: bool = False,
                filter_column: str = None, filter_value: str = None,
                cleanup_backups: bool = False, batch_size: int = 1000):
    """배치 업데이트 실행"""
    try:
        # Feature Store 업데이터 초기화
        updater = SageMakerFeatureStoreUpdater(
            feature_group_name=feature_group_name,
            config=config,
            batch_size=batch_size
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
        
        # 변경 대상 레코드 수 확인
        if column_name in sample_df.columns:  # 기존 컬럼인 경우에만 레코드 수 계산
            click.echo("=== 변경 대상 레코드 수 계산 ===")
            count_result = updater.count_matching_records(
                column_name=column_name,
                old_value=old_value,
                value_mapping=value_mapping,
                conditional_mapping=conditional_map
            )
            
            total_target_count = sum(count_result['match_counts'].values())
            if total_target_count == 0:
                click.echo("변경 대상 레코드가 없습니다", err=True)
                return
        else:
            click.echo("=== 새 컬럼 생성 모드 ===")
            total_target_count = "전체 레코드"
        
        # 사용자 확인
        if not dry_run:
            click.echo(f"\n⚠️ 주의: {total_target_count}개의 레코드가 변경됩니다!")
            click.echo(f"컬럼: {column_name}")
            
            if old_value and new_value:
                click.echo(f"단일 변경: '{old_value}' -> '{new_value}'")
            elif mapping_file:
                click.echo(f"매핑 파일: {mapping_file}")
            elif conditional_mapping:
                click.echo("조건부 매핑")
            elif transform_type:
                click.echo(f"변환 함수: {transform_type}")
            
            if not click.confirm("계속하시겠습니까?"):
                click.echo("작업이 취소되었습니다")
                return
        
        # 추가 필터 조건 설정
        filter_conditions = None
        if filter_column and filter_value:
            filter_conditions = {filter_column: filter_value}
        
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
            filter_conditions=filter_conditions
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
        if cleanup_backups and update_results['backup_files'] and not dry_run:
            click.echo("=== 백업 파일 정리 ===")
            if click.confirm(f"{len(update_results['backup_files'])}개의 백업 파일을 삭제하시겠습니까?"):
                cleanup_result = updater.cleanup_backup_files(
                    update_results['backup_files'], 
                    confirm=True
                )
                click.echo(f"백업 파일 정리: {cleanup_result}")
        
        click.echo("=== 작업 완료 ===")
        
    except KeyboardInterrupt:
        click.echo("작업이 사용자에 의해 중단되었습니다")
    except Exception as e:
        click.echo(f"작업 중 오류 발생: {e}", err=True)
        raise