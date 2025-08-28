#!/usr/bin/env python3
"""
SageMaker Feature Store Offline Data Batch Updater

대량의 SageMaker Feature Store offline 데이터를 수정하는 스크립트
- 특정 피처 record의 값을 다른 값으로 변경
- 배치 처리로 대용량 데이터 처리
- Athena 쿼리 검증 포함

사용 예시:
# 1. 단일 값 변경
python sagemaker_feature_store_batch_updater.py \
    --feature-group-name "your-feature-group" \
    --update-column "RB_Result" \
    --old-value "old_value" \
    --new-value "new_value" \
    --dry-run

# 2. 매핑 파일로 여러 값 변경
python sagemaker_feature_store_batch_updater.py \
    --feature-group-name "your-feature-group" \
    --update-column "RB_Result" \
    --mapping-file "value_mapping.json" \
    --dry-run

# 3. 조건부 변경 (다른 컬럼 조건)
python sagemaker_feature_store_batch_updater.py \
    --feature-group-name "your-feature-group" \
    --update-column "RB_Result" \
    --conditional-mapping '{"Category": {"A": {"old1": "new1", "old2": "new2"}, "B": {"old3": "new3"}}}' \
    --dry-run
"""

import argparse
import boto3
import pandas as pd
import time
import os
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Union, Callable
import logging
import re

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SageMakerFeatureStoreUpdater:
    """SageMaker Feature Store offline 데이터 대량 수정 클래스"""
    
    def __init__(self, 
                 feature_group_name: str,
                 region_name: str = "ap-northeast-2",
                 batch_size: int = 1000):
        """
        초기화
        
        Args:
            feature_group_name: Feature Group 이름
            region_name: AWS 리전
            batch_size: 배치 처리 크기
        """
        self.feature_group_name = feature_group_name
        self.region_name = region_name
        self.batch_size = batch_size
        
        # AWS 클라이언트 초기화
        self.session = boto3.Session(region_name=region_name)
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
        
        logger.info(f"Feature Group: {feature_group_name}")
        logger.info(f"S3 경로: {self.s3_uri}")
        logger.info(f"Glue 데이터베이스: {self.database_name}")
        logger.info(f"Glue 테이블: {self.table_name}")
    
    def _get_feature_group_info(self) -> Dict:
        """Feature Group 정보 가져오기"""
        try:
            response = self.sagemaker_client.describe_feature_group(
                FeatureGroupName=self.feature_group_name
            )
            return response
        except Exception as e:
            logger.error(f"Feature Group 정보 조회 실패: {e}")
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
        logger.info("오프라인 스토어 Parquet 파일 목록 조회 중...")
        
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
            
            logger.info(f"발견된 Parquet 파일 수: {len(parquet_files):,}")
            return parquet_files
            
        except Exception as e:
            logger.error(f"S3 파일 목록 조회 실패: {e}")
            raise
    
    def read_data_sample(self, sample_size: int = 1000) -> pd.DataFrame:
        """데이터 샘플 읽기 (구조 확인용)"""
        logger.info(f"데이터 샘플 {sample_size}개 읽기 중...")
        
        parquet_files = self.get_offline_data_paths()
        if not parquet_files:
            logger.warning("Parquet 파일을 찾을 수 없습니다")
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
            
            logger.info(f"샘플 데이터 크기: {sample_df.shape}")
            logger.info(f"컬럼 목록: {list(sample_df.columns)}")
            return sample_df
            
        except Exception as e:
            logger.error(f"샘플 데이터 읽기 실패: {e}")
            raise
    
    def load_mapping_from_file(self, mapping_file: str) -> Dict:
        """매핑 파일에서 값 변환 규칙 로드"""
        logger.info(f"매핑 파일 로드 중: {mapping_file}")
        
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
            
            logger.info(f"매핑 규칙 {len(mapping)}개 로드 완료")
            return mapping
            
        except Exception as e:
            logger.error(f"매핑 파일 로드 실패: {e}")
            raise
    
    def create_conditional_mapping(self, conditional_mapping: Union[str, Dict]) -> Dict:
        """조건부 매핑 생성"""
        if isinstance(conditional_mapping, str):
            try:
                conditional_mapping = json.loads(conditional_mapping)
            except json.JSONDecodeError as e:
                logger.error(f"조건부 매핑 JSON 파싱 실패: {e}")
                raise
        
        logger.info(f"조건부 매핑 규칙: {conditional_mapping}")
        return conditional_mapping
    
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
            
            # copy_from_column의 경우 특별 처리를 위해 소스 컬럼 정보를 함수에 저장
            def copy_from_column_transform(row):
                """다른 컬럼의 값을 복사하는 변환 함수 (row 전체를 받음)"""
                if hasattr(row, 'index') and source_column in row.index:
                    return row[source_column]
                return None
            
            # 소스 컬럼 정보를 함수 속성으로 저장
            copy_from_column_transform.source_column = source_column
            copy_from_column_transform.is_copy_function = True
            return copy_from_column_transform
        
        elif transform_type == 'extract_time_prefix':
            time_format = kwargs.get('time_format', 'auto')  # auto, timestamp, date 등
            prefix_pattern = kwargs.get('prefix_pattern', r'(\d{4}-\d{2}-\d{2})')  # 기본 YYYY-MM-DD 패턴
            to_iso = kwargs.get('to_iso', True)
            source_column = kwargs.get('source_column')
            
            def extract_time_prefix_transform(row):
                """prefix에서 시간 정보를 추출해서 ISO 형식으로 변환"""
                if source_column and hasattr(row, 'index') and source_column in row.index:
                    value = row[source_column]
                else:
                    # 현재 컬럼의 값에서 추출
                    value = getattr(row, 'name', '') if hasattr(row, 'name') else str(row)
                
                if pd.isna(value):
                    return None
                
                value_str = str(value)
                
                # 정규식으로 시간 패턴 추출
                match = re.search(prefix_pattern, value_str)
                if not match:
                    # 패턴이 매치되지 않으면 현재 시간을 ISO 형식으로 반환
                    if to_iso:
                        return datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
                    return None
                
                time_str = match.group(1)
                
                if not to_iso:
                    return time_str
                
                try:
                    # 시간 형식 감지 및 변환
                    if time_format == 'auto':
                        # 자동 형식 감지
                        if re.match(r'\d{4}-\d{2}-\d{2}', time_str):
                            dt = datetime.strptime(time_str, '%Y-%m-%d')
                        elif re.match(r'\d{8}', time_str):
                            dt = datetime.strptime(time_str, '%Y%m%d')
                        elif re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', time_str):
                            dt = datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%S')
                        elif re.match(r'\d{10}', time_str):  # Unix timestamp
                            dt = datetime.fromtimestamp(int(time_str))
                        else:
                            # 기본값으로 현재 시간 사용
                            dt = datetime.now()
                    else:
                        # 지정된 형식 사용
                        dt = datetime.strptime(time_str, time_format)
                    
                    # ISO 형식으로 변환
                    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    
                except ValueError as e:
                    logger.warning(f"시간 변환 실패 {time_str}: {e}")
                    # 변환 실패 시 현재 시간 반환
                    return datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            
            # 소스 컬럼 정보를 함수 속성으로 저장 (필요한 경우)
            if source_column:
                extract_time_prefix_transform.source_column = source_column
                extract_time_prefix_transform.is_copy_function = True
            
            return extract_time_prefix_transform
        
        else:
            raise ValueError(f"지원하지 않는 변환 타입: {transform_type}")
    
    def count_matching_records(self, 
                             column_name: str, 
                             old_value: str = None,
                             value_mapping: Dict = None,
                             conditional_mapping: Dict = None) -> Dict:
        """변경 대상 레코드 수 계산 (유동적 매핑 지원)"""
        logger.info(f"변경 대상 레코드 수 계산 중... (컬럼: {column_name})")
        
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
                    total_conditional_count = 0
                    for condition_col, condition_mappings in conditional_mapping.items():
                        for condition_val, value_map in condition_mappings.items():
                            condition_mask = df[condition_col] == condition_val
                            for old_val in value_map.keys():
                                mask = condition_mask & (df[column_name] == old_val)
                                count = mask.sum()
                                if count > 0:
                                    key = f"{condition_col}={condition_val}, {column_name}={old_val}"
                                    file_counts[key] = count
                                    total_conditional_count += count
                    
                    if total_conditional_count > 0:
                        file_counts['_total_conditional'] = total_conditional_count
                
                return file_counts
                
            except Exception as e:
                logger.error(f"파일 읽기 오류 {file_path}: {e}")
                return {}
        
        # 병렬 처리로 각 파일의 매칭 레코드 수 계산
        max_workers = min(os.cpu_count(), 10)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(count_in_file, file_path) for file_path in parquet_files]
            
            for i, future in enumerate(as_completed(futures)):
                file_counts = future.result()
                
                # 결과 집계
                for key, count in file_counts.items():
                    if key not in result_counts['match_counts']:
                        result_counts['match_counts'][key] = 0
                    result_counts['match_counts'][key] += count
                
                # 진행률 표시
                if (i + 1) % max(1, len(futures) // 10) == 0 or i == len(futures) - 1:
                    progress = (i + 1) / len(futures) * 100
                    logger.info(f"진행률: {progress:.1f}% ({i+1}/{len(futures)} 파일)")
        
        # 결과 출력
        total_records = sum(result_counts['match_counts'].values())
        logger.info(f"총 변경 대상 레코드 수: {total_records:,}개")
        
        if len(result_counts['match_counts']) > 1:
            logger.info("세부 매칭 결과:")
            for key, count in result_counts['match_counts'].items():
                if key != '_total_conditional' and count > 0:
                    logger.info(f"  - {key}: {count:,}개")
        
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
        """
        레코드 대량 업데이트 (유동적 매핑 지원)
        
        Args:
            column_name: 업데이트할 컬럼명
            old_value: 기존 값 (단일 값 변경용)
            new_value: 새로운 값 (단일 값 변경용)
            value_mapping: 값 매핑 딕셔너리 {old_val: new_val}
            conditional_mapping: 조건부 매핑 {condition_col: {condition_val: {old_val: new_val}}}
            transform_function: 변환 함수 (값을 동적으로 변환)
            dry_run: 실제 실행 여부 (True: 테스트만, False: 실제 실행)
            filter_conditions: 추가 필터 조건 {'column': 'value'}
        
        Returns:
            업데이트 결과 정보
        """
        logger.info(f"배치 업데이트 시작:")
        logger.info(f"  컬럼: {column_name}")
        
        if old_value and new_value:
            logger.info(f"  단일 값 변경: '{old_value}' -> '{new_value}'")
        elif value_mapping:
            logger.info(f"  매핑 기반 변경: {len(value_mapping)}개 규칙")
        elif conditional_mapping:
            logger.info(f"  조건부 변경: {len(conditional_mapping)}개 조건")
        elif transform_function:
            logger.info(f"  함수 기반 변환: 사용자 정의 함수")
        
        logger.info(f"  DRY RUN: {dry_run}")
        
        if filter_conditions:
            logger.info(f"  추가 필터: {filter_conditions}")
        
        parquet_files = self.get_offline_data_paths()
        results = {
            'total_files': len(parquet_files),
            'processed_files': 0,
            'updated_records': 0,
            'failed_files': [],
            'backup_files': []
        }
        
        def process_file(file_path: str) -> Dict:
            """단일 파일 처리 (유동적 매핑 지원)"""
            try:
                # 파일 읽기
                df = pd.read_parquet(file_path, engine='fastparquet')
                original_count = len(df)
                total_update_count = 0
                
                # 대상 컬럼이 존재하지 않는 경우 새로 추가
                if column_name not in df.columns:
                    df[column_name] = None  # 새 컬럼을 None으로 초기화
                
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
                
                # 4. 변환 함수 기반
                elif transform_function:
                    mask = pd.Series([True] * len(df), index=df.index)
                    
                    # 추가 필터 조건 적용
                    if filter_conditions:
                        for filter_col, filter_val in filter_conditions.items():
                            if filter_col in df.columns:
                                mask = mask & (df[filter_col] == filter_val)
                    
                    if mask.sum() > 0:
                        original_values = df.loc[mask, column_name]
                        
                        # copy_from_column 함수인지 확인
                        if hasattr(transform_function, 'is_copy_function') and transform_function.is_copy_function:
                            # copy_from_column의 경우 행 전체를 전달
                            transformed_values = df.loc[mask].apply(transform_function, axis=1)
                        else:
                            # 일반적인 변환 함수의 경우 컬럼 값만 전달
                            transformed_values = original_values.apply(transform_function)
                        
                        # 실제로 변경된 값만 카운트
                        try:
                            changed_mask = original_values != transformed_values
                            total_update_count = changed_mask.sum()
                        except:
                            # 새로운 컬럼의 경우 모든 값이 변경된 것으로 처리
                            total_update_count = len(transformed_values)
                        
                        if total_update_count > 0 and not dry_run:
                            df_updated.loc[mask, column_name] = transformed_values
                
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
                logger.error(f"파일 처리 오류 {file_path}: {e}")
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
            
            for i, future in enumerate(as_completed(futures)):
                file_result = future.result()
                results['processed_files'] += 1
                
                if file_result['status'] in ['updated', 'dry_run']:
                    results['updated_records'] += file_result['updated_count']
                    if file_result.get('backup_path'):
                        results['backup_files'].append(file_result['backup_path'])
                elif file_result['status'] == 'error':
                    results['failed_files'].append(file_result['file'])
                
                # 진행률 표시
                if (i + 1) % max(1, len(futures) // 20) == 0 or i == len(futures) - 1:
                    progress = (i + 1) / len(futures) * 100
                    logger.info(f"처리 진행률: {progress:.1f}% ({i+1}/{len(futures)} 파일)")
        
        # 결과 요약
        logger.info("=== 업데이트 결과 ===")
        logger.info(f"총 파일 수: {results['total_files']:,}")
        logger.info(f"처리 완료: {results['processed_files']:,}")
        logger.info(f"업데이트된 레코드: {results['updated_records']:,}")
        logger.info(f"실패한 파일: {len(results['failed_files'])}")
        logger.info(f"백업 파일: {len(results['backup_files'])}")
        
        if results['failed_files']:
            logger.warning("실패한 파일 목록:")
            for failed_file in results['failed_files'][:10]:  # 최대 10개만 표시
                logger.warning(f"  - {failed_file}")
        
        return results
    
    def validate_with_athena(self, 
                           column_name: str, 
                           value: str, 
                           expected_count: Optional[int] = None) -> Dict:
        """
        Athena 쿼리로 업데이트 결과 검증
        
        Args:
            column_name: 검증할 컬럼명
            value: 검증할 값
            expected_count: 예상 레코드 수 (선택적)
        
        Returns:
            검증 결과
        """
        logger.info(f"Athena 쿼리 검증 시작: {column_name} = '{value}'")
        
        if not self.table_name:
            logger.error("Glue 테이블 정보를 찾을 수 없습니다")
            return {'status': 'error', 'message': 'Glue 테이블 정보 없음'}
        
        # Athena 쿼리 작성
        query = f"""
        SELECT COUNT(*) as record_count
        FROM "{self.database_name}"."{self.table_name}"
        WHERE {column_name} = '{value}'
        """
        
        # 쿼리 실행 설정
        query_execution_id = None
        
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
            logger.info(f"Athena 쿼리 실행 ID: {query_execution_id}")
            
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
                    logger.error(f"Athena 쿼리 실패: {error_msg}")
                    return {'status': 'error', 'message': error_msg}
                
                time.sleep(wait_interval)
                elapsed_time += wait_interval
                logger.info(f"쿼리 실행 중... ({elapsed_time}초 경과)")
            
            if elapsed_time >= max_wait_time:
                logger.error("Athena 쿼리 타임아웃")
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
                        logger.info(f"✅ 검증 성공: {actual_count:,}개 레코드")
                    else:
                        logger.warning(f"⚠️ 검증 불일치: 예상 {expected_count:,}, 실제 {actual_count:,}")
                else:
                    logger.info(f"Athena 쿼리 결과: {actual_count:,}개 레코드")
                
                return validation_result
            else:
                logger.error("Athena 쿼리 결과가 비어있습니다")
                return {'status': 'error', 'message': 'Empty result'}
                
        except Exception as e:
            logger.error(f"Athena 쿼리 검증 실패: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def cleanup_backup_files(self, backup_files: List[str], confirm: bool = False) -> Dict:
        """백업 파일 정리"""
        if not confirm:
            logger.warning("백업 파일 정리를 위해서는 confirm=True로 설정하세요")
            return {'status': 'skipped', 'message': 'confirmation required'}
        
        logger.info(f"백업 파일 {len(backup_files)}개 삭제 중...")
        
        deleted_count = 0
        failed_count = 0
        
        for backup_file in backup_files:
            try:
                key = backup_file.replace(f's3://{self.bucket_name}/', '')
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
                deleted_count += 1
            except Exception as e:
                logger.error(f"백업 파일 삭제 실패 {backup_file}: {e}")
                failed_count += 1
        
        logger.info(f"백업 파일 정리 완료: 삭제 {deleted_count}, 실패 {failed_count}")
        return {
            'status': 'completed',
            'deleted_count': deleted_count,
            'failed_count': failed_count
        }


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='SageMaker Feature Store 대량 데이터 업데이트')
    
    # 필수 인자
    parser.add_argument('--feature-group-name', required=True, 
                       help='Feature Group 이름')
    parser.add_argument('--update-column', required=True,
                       help='업데이트할 컬럼명')
    
    # 단일 값 변경 인자 (상호 배타적 그룹)
    value_group = parser.add_mutually_exclusive_group(required=True)
    value_group.add_argument('--single-update', nargs=2, metavar=('OLD_VALUE', 'NEW_VALUE'),
                           help='단일 값 변경: --single-update "old_value" "new_value"')
    value_group.add_argument('--mapping-file', 
                           help='매핑 파일 경로 (.json 또는 .csv)')
    value_group.add_argument('--conditional-mapping', 
                           help='조건부 매핑 JSON 문자열')
    value_group.add_argument('--transform-function', choices=['regex_replace', 'prefix_suffix', 'uppercase', 'lowercase', 'copy_from_column', 'extract_time_prefix'],
                           help='변환 함수 타입')
    
    # 이전 버전 호환성을 위한 인자들 (deprecated)
    parser.add_argument('--old-value', help='[Deprecated] 기존 값 - --single-update 사용 권장')
    parser.add_argument('--new-value', help='[Deprecated] 새로운 값 - --single-update 사용 권장')
    
    # 선택적 인자
    parser.add_argument('--region', default='ap-northeast-2',
                       help='AWS 리전 (기본값: ap-northeast-2)')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='배치 크기 (기본값: 1000)')
    parser.add_argument('--dry-run', action='store_true',
                       help='실제 실행하지 않고 테스트만 수행')
    parser.add_argument('--skip-validation', action='store_true',
                       help='Athena 검증 건너뛰기')
    parser.add_argument('--filter-column', 
                       help='추가 필터 컬럼명')
    parser.add_argument('--filter-value',
                       help='추가 필터 값')
    parser.add_argument('--cleanup-backups', action='store_true',
                       help='백업 파일 자동 정리')
    
    # 변환 함수 옵션들
    parser.add_argument('--regex-pattern', 
                       help='정규식 패턴 (transform-function=regex_replace 시 필요)')
    parser.add_argument('--regex-replacement', default='',
                       help='정규식 치환 문자열 (기본값: 빈 문자열)')
    parser.add_argument('--prefix', default='',
                       help='접두사 (transform-function=prefix_suffix 시)')
    parser.add_argument('--suffix', default='',
                       help='접미사 (transform-function=prefix_suffix 시)')
    parser.add_argument('--source-column',
                       help='복사할 원본 컬럼명 (transform-function=copy_from_column 시 필요)')
    parser.add_argument('--prefix-pattern', default=r'(\d{4}-\d{2}-\d{2})',
                       help='시간 추출용 정규식 패턴 (extract_time_prefix 시 사용, 기본값: YYYY-MM-DD)')
    parser.add_argument('--time-format', default='auto',
                       help='시간 형식 (extract_time_prefix 시 사용, 기본값: auto)')
    parser.add_argument('--to-iso', action='store_true', default=True,
                       help='추출된 시간을 ISO 형식으로 변환 (extract_time_prefix 시 사용)')
    parser.add_argument('--auto-confirm', action='store_true',
                       help='누락된 컬럼을 자동으로 새 컬럼으로 처리 (대화형 확인 건너뛰기)')
    
    args = parser.parse_args()
    
    try:
        # 이전 버전 호환성 처리
        if args.old_value and args.new_value and not args.single_update:
            logger.warning("--old-value, --new-value는 deprecated입니다. --single-update 사용을 권장합니다.")
            args.single_update = [args.old_value, args.new_value]
        
        # Feature Store 업데이터 초기화
        updater = SageMakerFeatureStoreUpdater(
            feature_group_name=args.feature_group_name,
            region_name=args.region,
            batch_size=args.batch_size
        )
        
        # 데이터 구조 확인
        logger.info("=== 데이터 구조 확인 ===")
        sample_df = updater.read_data_sample(100)
        
        if sample_df.empty:
            logger.error("데이터를 읽을 수 없습니다")
            return
        
        # 업데이트 대상 컬럼 확인
        missing_columns = []
        if args.update_column not in sample_df.columns:
            missing_columns.append(args.update_column)
        
        # 원본 컬럼 확인 (copy_from_column의 경우)
        if args.transform_function == 'copy_from_column' and args.source_column:
            if args.source_column not in sample_df.columns:
                logger.error(f"원본 컬럼 '{args.source_column}'을 찾을 수 없습니다")
                logger.info(f"사용 가능한 컬럼: {list(sample_df.columns)}")
                return
        
        # 누락된 컬럼 처리
        if missing_columns:
            logger.warning(f"⚠️  오프라인 스토어에서 찾을 수 없는 컬럼: {missing_columns}")
            logger.info("이는 다음과 같은 경우에 발생할 수 있습니다:")
            logger.info("1. Feature Group 스키마에는 있지만 기존 데이터에는 없는 컬럼")
            logger.info("2. 최근에 추가된 컬럼으로 아직 데이터가 입력되지 않은 경우")
            
            print(f"\n📋 사용 가능한 컬럼 목록 ({len(sample_df.columns)}개):")
            for i, col in enumerate(sorted(sample_df.columns), 1):
                print(f"{i:2d}. {col}")
            
            if args.auto_confirm:
                logger.info(f"🔄 --auto-confirm 옵션으로 '{args.update_column}'을 새 컬럼으로 자동 처리합니다")
                logger.info("주의: 이 컬럼이 Feature Group 스키마에 정의되어 있는지 확인하세요")
            else:
                print(f"\n❓ '{args.update_column}' 컬럼이 오프라인 스토어에 없습니다.")
                print("다음 중 선택하세요:")
                print("1. 다른 컬럼명으로 변경")
                print("2. 새 컬럼으로 추가 (Feature Group 스키마에 정의된 경우)")
                print("3. 작업 취소")
                
                choice = input("선택 (1/2/3): ").strip()
                
                if choice == "1":
                    print("\n사용 가능한 컬럼에서 선택하거나 새로운 컬럼명을 입력하세요:")
                    new_column = input("새 컬럼명: ").strip()
                    if new_column:
                        args.update_column = new_column
                        logger.info(f"업데이트 컬럼을 '{new_column}'으로 변경했습니다")
                        if new_column not in sample_df.columns:
                            logger.info(f"'{new_column}'은 새로운 컬럼으로 처리됩니다")
                    else:
                        logger.info("작업이 취소되었습니다")
                        return
                elif choice == "2":
                    logger.info(f"'{args.update_column}'을 새 컬럼으로 처리합니다")
                    logger.info("주의: 이 컬럼이 Feature Group 스키마에 정의되어 있는지 확인하세요")
                else:
                    logger.info("작업이 취소되었습니다")
                    return
        
        # 업데이트 방식 결정 및 파라미터 준비
        old_value = None
        new_value = None
        value_mapping = None
        conditional_mapping = None
        transform_function = None
        
        if args.single_update:
            old_value, new_value = args.single_update
            logger.info(f"단일 값 변경: '{old_value}' -> '{new_value}'")
            
        elif args.mapping_file:
            value_mapping = updater.load_mapping_from_file(args.mapping_file)
            logger.info(f"매핑 파일 기반 변경: {len(value_mapping)}개 규칙")
            
        elif args.conditional_mapping:
            conditional_mapping = updater.create_conditional_mapping(args.conditional_mapping)
            logger.info(f"조건부 매핑: {len(conditional_mapping)}개 조건")
            
        elif args.transform_function:
            if args.transform_function == 'regex_replace':
                if not args.regex_pattern:
                    logger.error("regex_replace 변환에는 --regex-pattern이 필요합니다")
                    return
                transform_function = updater.create_transform_function(
                    'regex_replace', 
                    pattern=args.regex_pattern, 
                    replacement=args.regex_replacement
                )
            elif args.transform_function == 'prefix_suffix':
                transform_function = updater.create_transform_function(
                    'prefix_suffix',
                    prefix=args.prefix,
                    suffix=args.suffix
                )
            elif args.transform_function == 'copy_from_column':
                if not args.source_column:
                    logger.error("copy_from_column 변환에는 --source-column이 필요합니다")
                    return
                transform_function = updater.create_transform_function(
                    'copy_from_column',
                    source_column=args.source_column
                )
            elif args.transform_function == 'extract_time_prefix':
                transform_function = updater.create_transform_function(
                    'extract_time_prefix',
                    time_format=args.time_format,
                    prefix_pattern=args.prefix_pattern,
                    to_iso=args.to_iso,
                    source_column=args.source_column
                )
            else:
                transform_function = updater.create_transform_function(args.transform_function)
                
            logger.info(f"변환 함수 적용: {args.transform_function}")
        
        # copy_from_column이나 extract_time_prefix이고 대상 컬럼이 존재하지 않는 경우 레코드 수 계산 건너뛰기
        if (args.transform_function in ['copy_from_column', 'extract_time_prefix'] and 
            args.update_column not in sample_df.columns):
            logger.info("=== 새 컬럼 생성 모드: 레코드 수 계산 건너뛰기 ===")
            if args.transform_function == 'copy_from_column':
                logger.info(f"모든 레코드에 대해 {args.source_column} -> {args.update_column} 복사 작업을 수행합니다")
            elif args.transform_function == 'extract_time_prefix':
                logger.info(f"모든 레코드에 대해 시간 prefix 추출 -> {args.update_column} 변환 작업을 수행합니다")
            total_target_count = 999999  # 임시값 (실제 개수는 나중에 확인)
        else:
            # 변경 대상 레코드 수 확인
            logger.info("=== 변경 대상 레코드 수 계산 ===")
            count_result = updater.count_matching_records(
                column_name=args.update_column,
                old_value=old_value,
                value_mapping=value_mapping,
                conditional_mapping=conditional_mapping
            )
            
            total_target_count = sum(count_result['match_counts'].values())
            if total_target_count == 0:
                logger.warning("변경 대상 레코드가 없습니다")
                return
        
        # 사용자 확인
        if not args.dry_run:
            print(f"\n⚠️  주의: {total_target_count:,}개의 레코드가 변경됩니다!")
            print(f"컬럼: {args.update_column}")
            
            if args.single_update:
                print(f"단일 변경: '{old_value}' -> '{new_value}'")
            elif args.mapping_file:
                print(f"매핑 파일: {args.mapping_file} ({len(value_mapping)}개 규칙)")
            elif args.conditional_mapping:
                print(f"조건부 매핑: {len(conditional_mapping)}개 조건")
            elif args.transform_function:
                print(f"변환 함수: {args.transform_function}")
            
            if input("\n계속하시겠습니까? (y/N): ").lower() != 'y':
                logger.info("작업이 취소되었습니다")
                return
        
        # 추가 필터 조건 설정
        filter_conditions = None
        if args.filter_column and args.filter_value:
            filter_conditions = {args.filter_column: args.filter_value}
        
        # 업데이트 실행
        logger.info("=== 레코드 업데이트 실행 ===")
        update_results = updater.update_records_batch(
            column_name=args.update_column,
            old_value=old_value,
            new_value=new_value,
            value_mapping=value_mapping,
            conditional_mapping=conditional_mapping,
            transform_function=transform_function,
            dry_run=args.dry_run,
            filter_conditions=filter_conditions
        )
        
        # Athena 검증
        if not args.skip_validation and not args.dry_run:
            logger.info("=== Athena 쿼리 검증 ===")
            
            # 단일 값 변경의 경우만 Athena 검증
            if args.single_update:
                # 기존 값 확인
                old_validation = updater.validate_with_athena(args.update_column, old_value)
                
                # 새 값 확인
                new_validation = updater.validate_with_athena(
                    args.update_column, 
                    new_value, 
                    expected_count=update_results['updated_records']
                )
                
                logger.info(f"기존 값 '{old_value}' 레코드 수: {old_validation.get('actual_count', 'N/A')}")
                logger.info(f"새 값 '{new_value}' 레코드 수: {new_validation.get('actual_count', 'N/A')}")
            else:
                logger.info("복합 매핑의 경우 Athena 검증을 건너뜁니다. 수동으로 확인해주세요.")
        
        # 백업 파일 정리
        if args.cleanup_backups and update_results['backup_files'] and not args.dry_run:
            logger.info("=== 백업 파일 정리 ===")
            cleanup_result = updater.cleanup_backup_files(
                update_results['backup_files'], 
                confirm=True
            )
            logger.info(f"백업 파일 정리: {cleanup_result}")
        
        logger.info("=== 작업 완료 ===\n")
        logger.info("💡 추가 사용법:")
        logger.info("  📁 매핑 파일 예시:")
        logger.info("     JSON: {\"old1\": \"new1\", \"old2\": \"new2\"}")
        logger.info("     CSV: old_value,new_value 형식")
        logger.info("  🎯 조건부 매핑: --conditional-mapping '{\"Category\": {\"A\": {\"old1\": \"new1\"}}}'")
        logger.info("  🔧 변환 함수: --transform-function regex_replace --regex-pattern 'pattern' --regex-replacement 'replacement'")
        
    except KeyboardInterrupt:
        logger.info("작업이 사용자에 의해 중단되었습니다")
    except Exception as e:
        logger.error(f"작업 중 오류 발생: {e}")
        raise


def create_example_files():
    """예시 매핑 파일 생성"""
    # JSON 매핑 파일 예시
    json_example = {
        "OLD_VALUE_1": "NEW_VALUE_1",
        "OLD_VALUE_2": "NEW_VALUE_2",
        "ABNORMAL": "NORMAL",
        "ERROR": "SUCCESS"
    }
    
    with open('value_mapping_example.json', 'w', encoding='utf-8') as f:
        json.dump(json_example, f, indent=2, ensure_ascii=False)
    
    # CSV 매핑 파일 예시
    csv_example = pd.DataFrame({
        'old_value': ['OLD_VALUE_1', 'OLD_VALUE_2', 'ABNORMAL', 'ERROR'],
        'new_value': ['NEW_VALUE_1', 'NEW_VALUE_2', 'NORMAL', 'SUCCESS']
    })
    csv_example.to_csv('value_mapping_example.csv', index=False)
    
    print("예시 파일이 생성되었습니다:")
    print("  - value_mapping_example.json")
    print("  - value_mapping_example.csv")


if __name__ == '__main__':
    if len(os.sys.argv) > 1 and os.sys.argv[1] == '--create-examples':
        create_example_files()
    else:
        main()