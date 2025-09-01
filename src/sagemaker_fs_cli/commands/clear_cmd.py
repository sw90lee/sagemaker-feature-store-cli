"""Clear command implementation"""

import time
import click
from typing import Dict, Any, List, Optional, Tuple
from botocore.exceptions import ClientError
from urllib.parse import urlparse
from tqdm import tqdm

from ..config import Config


def _find_athena_table_name(config: Config, database: str, feature_group_name: str) -> Optional[str]:
    """Find the actual Athena table name for a feature group"""
    try:
        athena_client = config.session.client('athena')
        
        # List all tables in the database
        response = athena_client.list_table_metadata(
            CatalogName='AwsDataCatalog',
            DatabaseName=database
        )
        
        # Common transformation patterns SageMaker might use
        possible_names = [
            feature_group_name.replace('-', '_'),  # Original logic
            feature_group_name.replace('-', '_').lower(),  # Lowercase
            feature_group_name,  # No transformation
            feature_group_name.lower(),  # Just lowercase
        ]
        
        # Get actual table names
        table_names = [table['Name'] for table in response.get('TableMetadataList', [])]
        
        # Try exact matches first
        for possible_name in possible_names:
            if possible_name in table_names:
                return possible_name
        
        # Try partial matches (in case there are prefixes/suffixes)
        feature_group_base = feature_group_name.replace('-', '_').lower()
        for table_name in table_names:
            if feature_group_base in table_name.lower():
                return table_name
        
        # If no match found, show available tables for debugging
        click.echo(f"⚠️  사용 가능한 테이블: {table_names[:5]}...")  # Show first 5
        return None
        
    except Exception as e:
        click.echo(f"⚠️  테이블 목록 조회 실패: {e}")
        return None


def _get_athena_output_location(config: Config) -> str:
    """Get suitable S3 location for Athena query results"""
    try:
        # Try to use SageMaker default bucket
        session = config.session
        region = session.region_name or 'us-east-1'
        account_id = session.client('sts').get_caller_identity()['Account']
        
        # Try common SageMaker bucket patterns
        bucket_patterns = [
            f"sagemaker-{region}-{account_id}",
            f"aws-athena-query-results-{account_id}-{region}",
            f"sagemaker-studio-{account_id}-{region}"
        ]
        
        s3_client = session.client('s3')
        
        for bucket_name in bucket_patterns:
            try:
                s3_client.head_bucket(Bucket=bucket_name)
                return f"s3://{bucket_name}/athena-results/"
            except:
                continue
        
        # If no bucket found, try to list and use the first available bucket
        response = s3_client.list_buckets()
        if response['Buckets']:
            first_bucket = response['Buckets'][0]['Name']
            return f"s3://{first_bucket}/athena-results/"
        
        # Fallback - this will likely fail but gives a clear error
        return f"s3://sagemaker-{region}-{account_id}/athena-results/"
        
    except Exception as e:
        # Fallback to original
        return 's3://temp-query-results/'


def clear_feature_group(config: Config, feature_group_name: str, online_only: bool = False, 
                       offline_only: bool = False, force: bool = False, 
                       backup_s3: Optional[str] = None, dry_run: bool = False,
                       deduplicate_only: bool = False) -> None:
    """Clear all data from a feature group"""
    try:
        # Validate feature group exists and get details
        fg_details = _validate_feature_group(config, feature_group_name)
        
        # Handle deduplicate-only mode
        if deduplicate_only:
            _deduplicate_feature_group(config, feature_group_name, fg_details, dry_run, force)
            return
        
        # Determine what to clear
        clear_online = not offline_only
        clear_offline = not online_only
        
        # Validate options
        if not fg_details.get('OnlineStoreConfig') and clear_online:
            click.echo(f"경고: '{feature_group_name}'에 온라인 스토어가 구성되어 있지 않습니다.", err=True)
            clear_online = False
            
        if not fg_details.get('OfflineStoreConfig') and clear_offline:
            click.echo(f"경고: '{feature_group_name}'에 오프라인 스토어가 구성되어 있지 않습니다.", err=True)
            clear_offline = False
            
        if not clear_online and not clear_offline:
            click.echo("삭제할 스토어가 없습니다.", err=True)
            return
            
        # Show plan
        _show_clear_plan(feature_group_name, clear_online, clear_offline, backup_s3)
        
        if dry_run:
            click.echo("\n🔍 Dry-run 모드: 실제 삭제는 수행되지 않습니다.")
            return
            
        # Confirm deletion
        if not force and not _confirm_deletion(feature_group_name, clear_online, clear_offline):
            click.echo("작업이 취소되었습니다.")
            return
            
        # Backup if requested
        if backup_s3 and clear_offline:
            click.echo(f"\n📦 오프라인 데이터를 {backup_s3}에 백업 중...")
            _backup_to_s3(config, feature_group_name, backup_s3, fg_details)
            
        # Execute coordinated clear: offline record IDs → online deletion → offline deletion
        _execute_coordinated_clear(config, feature_group_name, fg_details, clear_online, clear_offline)
            
        click.echo(f"\n✅ '{feature_group_name}' 데이터 삭제가 완료되었습니다.")
        
    except ClientError as e:
        click.echo(f"AWS API 오류: {e}", err=True)
        raise click.Abort()
    except Exception as e:
        click.echo(f"예상치 못한 오류: {e}", err=True)
        raise click.Abort()


def _validate_feature_group(config: Config, feature_group_name: str) -> Dict[str, Any]:
    """Validate feature group exists and return details"""
    try:
        fg_details = config.sagemaker.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        return fg_details
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFound':
            click.echo(f"피처 그룹 '{feature_group_name}'을 찾을 수 없습니다.", err=True)
        else:
            click.echo(f"피처 그룹 정보 조회 실패: {e}", err=True)
        raise click.Abort()


def _show_clear_plan(feature_group_name: str, clear_online: bool, clear_offline: bool, backup_s3: Optional[str]) -> None:
    """Show what will be cleared"""
    click.echo(f"\n📋 삭제 계획: '{feature_group_name}'")
    click.echo("=" * 50)
    
    if clear_online:
        click.echo("• 온라인 스토어: 모든 레코드 삭제")
    if clear_offline:
        click.echo("• 오프라인 스토어: S3 데이터 삭제")
    if backup_s3:
        click.echo(f"• 백업 위치: {backup_s3}")


def _confirm_deletion(feature_group_name: str, clear_online: bool, clear_offline: bool) -> bool:
    """Ask user confirmation"""
    stores_to_clear = []
    if clear_online:
        stores_to_clear.append("온라인 스토어")
    if clear_offline:
        stores_to_clear.append("오프라인 스토어")
        
    stores_str = ", ".join(stores_to_clear)
    
    click.echo(f"\n⚠️  경고: '{feature_group_name}'의 {stores_str} 데이터가 완전히 삭제됩니다.")
    click.echo("이 작업은 되돌릴 수 없습니다!")
    
    return click.confirm("\n정말로 계속하시겠습니까?")


def _backup_to_s3(config: Config, feature_group_name: str, backup_path: str, fg_details: Dict[str, Any]) -> None:
    """Backup offline store data to S3"""
    if not fg_details.get('OfflineStoreConfig'):
        return
        
    try:
        s3_client = config.session.client('s3')
        
        # Get source S3 path
        source_s3_uri = fg_details['OfflineStoreConfig']['S3StorageConfig']['S3Uri']
        source_bucket, source_prefix = _parse_s3_uri(source_s3_uri)
        
        # Parse backup path
        backup_bucket, backup_prefix = _parse_s3_uri(backup_path)
        if backup_prefix and not backup_prefix.endswith('/'):
            backup_prefix += '/'
        backup_prefix += f"{feature_group_name}/"
        
        # Copy all objects
        paginator = s3_client.get_paginator('list_objects_v2')
        objects_copied = 0
        
        for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
            if 'Contents' not in page:
                continue
                
            for obj in tqdm(page['Contents'], desc="백업 중"):
                source_key = obj['Key']
                # Remove source prefix and add backup prefix
                relative_key = source_key[len(source_prefix):].lstrip('/')
                backup_key = backup_prefix + relative_key
                
                copy_source = {'Bucket': source_bucket, 'Key': source_key}
                s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=backup_bucket,
                    Key=backup_key
                )
                objects_copied += 1
                
        click.echo(f"✅ {objects_copied}개 객체가 백업되었습니다.")
        
    except Exception as e:
        click.echo(f"백업 실패: {e}", err=True)
        raise


def _execute_coordinated_clear(config: Config, feature_group_name: str, fg_details: Dict[str, Any],
                              clear_online: bool, clear_offline: bool) -> None:
    """Execute coordinated clear: offline record IDs → online deletion → offline deletion"""
    
    record_ids = []
    
    # Step 1: Get all record IDs from offline store (if exists and we need them)
    if fg_details.get('OfflineStoreConfig') and clear_online:
        click.echo("\n📊 오프라인 스토어에서 레코드 ID 조회 중...")
        record_ids = _get_record_ids_from_offline_athena(config, fg_details)
        click.echo(f"✅ {len(record_ids):,}개 record ID 조회 완료")
    
    # Step 2: Delete online records using the record IDs
    if clear_online:
        click.echo("\n🗑️  온라인 스토어 데이터 삭제 중...")
        if record_ids:
            _delete_online_records_by_ids(config, feature_group_name, record_ids)
        elif not fg_details.get('OfflineStoreConfig'):
            click.echo("⚠️  온라인 전용 피처그룹은 record ID 목록이 필요합니다.")
            click.echo("💡 대안: bulk-get으로 모든 데이터를 조회한 후 record ID를 추출하여 삭제하세요.")
        else:
            click.echo("⚠️  Record ID가 없어 온라인 삭제를 건너뜁니다.")
    
    # Step 3: Delete offline data
    if clear_offline:
        click.echo("\n🗑️  오프라인 스토어 데이터 삭제 중...")
        _delete_offline_s3_data(config, fg_details)


def _get_record_ids_from_offline_athena(config: Config, fg_details: Dict[str, Any]) -> List[str]:
    """Get all unique record IDs from offline store via Athena"""
    try:
        athena_client = config.session.client('athena')
        
        # Construct query to get unique record IDs
        database_name = "sagemaker_featurestore"
        feature_group_name = fg_details['FeatureGroupName']
        record_id_feature = fg_details['RecordIdentifierFeatureName']
        
        # Find actual table name
        table_name = _find_athena_table_name(config, database_name, feature_group_name)
        if not table_name:
            click.echo(f"⚠️  Athena 테이블을 찾을 수 없습니다: {feature_group_name}")
            return []
        
        query = f"SELECT DISTINCT {record_id_feature} FROM {database_name}.{table_name}"
        
        # Execute query
        response = athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': _get_athena_output_location(config)
            }
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # Wait for query completion
        _wait_for_athena_query_completion(athena_client, query_execution_id)
        
        # Get results
        record_ids = []
        paginator = athena_client.get_paginator('get_query_results')
        
        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            rows = page['ResultSet']['Rows'][1:]  # Skip header row
            for row in rows:
                if row['Data'] and row['Data'][0].get('VarCharValue'):
                    record_ids.append(row['Data'][0]['VarCharValue'])
        
        return record_ids
        
    except Exception as e:
        click.echo(f"Athena를 통한 레코드 ID 조회 실패: {e}", err=True)
        return []


def _delete_online_records_by_ids(config: Config, feature_group_name: str, record_ids: List[str]) -> None:
    """Delete online records by record IDs"""
    if not record_ids:
        click.echo("삭제할 레코드가 없습니다.")
        return
        
    featurestore_runtime = config.session.client('sagemaker-featurestore-runtime')
    failed_deletions = []
    
    with tqdm(total=len(record_ids), desc="온라인 레코드 삭제 중") as pbar:
        for record_id in record_ids:
            try:
                _handle_throttling(
                    featurestore_runtime.delete_record,
                    FeatureGroupName=feature_group_name,
                    RecordIdentifierValueAsString=str(record_id)
                )
                pbar.update(1)
            except Exception as e:
                failed_deletions.append((record_id, str(e)))
                pbar.update(1)
                
    if failed_deletions:
        click.echo(f"⚠️  {len(failed_deletions)}개 레코드 삭제 실패:")
        for record_id, error in failed_deletions[:5]:  # Show first 5 failures
            click.echo(f"  - {record_id}: {error}")
        if len(failed_deletions) > 5:
            click.echo(f"  ... 그 외 {len(failed_deletions) - 5}개")
    else:
        click.echo("✅ 모든 온라인 레코드가 삭제되었습니다.")


def _delete_offline_s3_data(config: Config, fg_details: Dict[str, Any]) -> None:
    """Delete all data from offline store (S3)"""
    if not fg_details.get('OfflineStoreConfig'):
        return
        
    try:
        s3_client = config.session.client('s3')
        
        # Get S3 path
        s3_uri = fg_details['OfflineStoreConfig']['S3StorageConfig']['S3Uri']
        bucket, prefix = _parse_s3_uri(s3_uri)
        
        # Delete all objects
        paginator = s3_client.get_paginator('list_objects_v2')
        objects_deleted = 0
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' not in page:
                continue
                
            # Delete in batches of 1000 (S3 limit)
            objects = page['Contents']
            for i in tqdm(range(0, len(objects), 1000), desc="S3 객체 삭제 중"):
                batch = objects[i:i+1000]
                delete_objects = [{'Key': obj['Key']} for obj in batch]
                
                s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': delete_objects}
                )
                objects_deleted += len(delete_objects)
                
        click.echo(f"✅ {objects_deleted}개 S3 객체가 삭제되었습니다.")
        
    except Exception as e:
        click.echo(f"오프라인 스토어 삭제 실패: {e}", err=True)
        raise


def _wait_for_athena_query_completion(athena_client, query_execution_id: str) -> None:
    """Wait for Athena query to complete"""
    max_wait_time = 300  # 5 minutes
    wait_time = 0
    
    while wait_time < max_wait_time:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        
        if status == 'SUCCEEDED':
            return
        elif status in ['FAILED', 'CANCELLED']:
            reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            raise Exception(f"Athena 쿼리 실패: {reason}")
        
        time.sleep(2)
        wait_time += 2
    
    raise Exception(f"Athena 쿼리 타임아웃 (>{max_wait_time}초)")




def _parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    """Parse S3 URI into bucket and prefix"""
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip('/')
    return bucket, prefix


def _handle_throttling(func, *args, **kwargs):
    """Handle API throttling with exponential backoff"""
    max_retries = 5
    base_delay = 1
    
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] in ['ThrottlingException', 'TooManyRequestsException']:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    time.sleep(delay)
                    continue
            raise
    
    raise Exception(f"Max retries ({max_retries}) exceeded")


def _deduplicate_feature_group(config: Config, feature_group_name: str, fg_details: Dict[str, Any], 
                              dry_run: bool = False, force: bool = False) -> None:
    """Remove duplicate record_ids keeping only latest EventTime records"""
    import pandas as pd
    from datetime import datetime, timedelta
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    if not fg_details.get('OfflineStoreConfig'):
        click.echo("❌ 중복 제거는 오프라인 스토어가 구성된 피처 그룹에서만 가능합니다.", err=True)
        return
    
    # Get S3 configuration
    offline_config = fg_details['OfflineStoreConfig']['S3StorageConfig']
    s3_uri = offline_config.get('S3Uri') or offline_config.get('ResolvedOutputS3Uri', '')
    bucket, prefix = _parse_s3_uri(s3_uri)
    
    # Get feature information
    record_id_feature = fg_details['RecordIdentifierFeatureName']
    event_time_feature = fg_details['EventTimeFeatureName']
    
    click.echo(f"\n🔧 중복 제거 작업 시작: '{feature_group_name}'")
    click.echo("=" * 60)
    click.echo(f"Record ID 컬럼: {record_id_feature}")
    click.echo(f"Event Time 컬럼: {event_time_feature}")
    click.echo(f"S3 경로: {s3_uri}")
    
    if dry_run:
        click.echo("🔍 Dry-run 모드: 실제 변경은 수행되지 않습니다.")
    
    if not dry_run and not force:
        click.echo(f"\n⚠️  경고: '{feature_group_name}'에서 중복된 record_id를 제거합니다.")
        click.echo("EventTime 기준으로 가장 최신 레코드만 유지되며, 오래된 레코드는 영구히 삭제됩니다!")
        click.echo("이 작업은 되돌릴 수 없습니다!")
        
        if not click.confirm("\n정말로 계속하시겠습니까?"):
            click.echo("작업이 취소되었습니다.")
            return
    
    try:
        s3_client = config.session.client('s3')
        
        # Get all parquet files
        click.echo("\n📁 Parquet 파일 목록 조회 중...")
        parquet_files = _get_parquet_files(s3_client, bucket, prefix)
        click.echo(f"발견된 파일 수: {len(parquet_files):,}")
        
        if not parquet_files:
            click.echo("처리할 Parquet 파일이 없습니다.")
            return
        
        # Process files in parallel
        stats = _process_parquet_files_for_dedup(
            s3_client, bucket, parquet_files, record_id_feature, event_time_feature, dry_run
        )
        
        # Show results
        _show_deduplication_results(stats)
        
        if not dry_run:
            click.echo(f"\n✅ '{feature_group_name}' 중복 제거가 완료되었습니다.")
        
    except Exception as e:
        click.echo(f"중복 제거 작업 실패: {e}", err=True)
        raise


def _get_parquet_files(s3_client, bucket: str, prefix: str) -> List[str]:
    """Get all parquet files from S3 path"""
    parquet_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        parquet_files.append(obj['Key'])
        return parquet_files
    except Exception as e:
        click.echo(f"S3 파일 목록 조회 실패: {e}", err=True)
        return []


def _process_parquet_files_for_dedup(s3_client, bucket: str, parquet_files: List[str], 
                                   record_id_feature: str, event_time_feature: str, 
                                   dry_run: bool = False) -> Dict:
    """Process parquet files to remove duplicates"""
    import pandas as pd
    from datetime import datetime
    import tempfile
    import time
    
    total_stats = {
        'files_processed': 0,
        'files_modified': 0,
        'total_records_before': 0,
        'total_records_after': 0,
        'duplicates_removed': 0,
        'failed_files': []
    }
    
    def process_single_file(file_key: str) -> Dict:
        temp_file = None
        try:
            # Download file to temporary location
            with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
                s3_client.download_fileobj(bucket, file_key, temp_file)
                temp_file_path = temp_file.name
            
            # Read parquet file
            df = pd.read_parquet(temp_file_path, engine='fastparquet')
            original_count = len(df)
            
            # Check if required columns exist
            if record_id_feature not in df.columns or event_time_feature not in df.columns:
                return {
                    'file': file_key,
                    'status': 'skipped',
                    'reason': 'missing_columns',
                    'original_count': original_count,
                    'final_count': original_count,
                    'duplicates_removed': 0
                }
            
            # Remove duplicates by keeping latest EventTime
            df_temp = df.copy()
            df_temp[event_time_feature + '_parsed'] = pd.to_datetime(df_temp[event_time_feature], errors='coerce')
            
            # Keep latest record for each record_id
            latest_records = df_temp.loc[df_temp.groupby(record_id_feature)[event_time_feature + '_parsed'].idxmax()]
            latest_records = latest_records.drop(columns=[event_time_feature + '_parsed'])
            
            final_count = len(latest_records)
            duplicates_removed = original_count - final_count
            
            if duplicates_removed == 0:
                return {
                    'file': file_key,
                    'status': 'no_duplicates',
                    'original_count': original_count,
                    'final_count': final_count,
                    'duplicates_removed': duplicates_removed
                }
            
            if not dry_run:
                # Create backup
                backup_key = file_key.replace('.parquet', f'_backup_{int(time.time())}.parquet')
                copy_source = {'Bucket': bucket, 'Key': file_key}
                s3_client.copy_object(CopySource=copy_source, Bucket=bucket, Key=backup_key)
                
                # Save deduplicated file back to S3
                latest_records.to_parquet(temp_file_path, engine='fastparquet', index=False)
                s3_client.upload_file(temp_file_path, bucket, file_key)
                
                return {
                    'file': file_key,
                    'status': 'deduplicated',
                    'original_count': original_count,
                    'final_count': final_count,
                    'duplicates_removed': duplicates_removed,
                    'backup_key': backup_key
                }
            else:
                return {
                    'file': file_key,
                    'status': 'dry_run',
                    'original_count': original_count,
                    'final_count': final_count,
                    'duplicates_removed': duplicates_removed
                }
                
        except Exception as e:
            return {
                'file': file_key,
                'status': 'error',
                'error': str(e),
                'original_count': 0,
                'final_count': 0,
                'duplicates_removed': 0
            }
        finally:
            # Clean up temp file
            if temp_file and os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
    
    # Process files in parallel
    max_workers = min(os.cpu_count(), 5)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_single_file, file_key) for file_key in parquet_files]
        
        with tqdm(total=len(futures), desc='파일 처리 중') as pbar:
            for future in as_completed(futures):
                result = future.result()
                
                total_stats['files_processed'] += 1
                total_stats['total_records_before'] += result['original_count']
                total_stats['total_records_after'] += result['final_count']
                total_stats['duplicates_removed'] += result['duplicates_removed']
                
                if result['status'] in ['deduplicated', 'dry_run'] and result['duplicates_removed'] > 0:
                    total_stats['files_modified'] += 1
                elif result['status'] == 'error':
                    total_stats['failed_files'].append(result['file'])
                
                pbar.update(1)
    
    return total_stats


def _show_deduplication_results(stats: Dict) -> None:
    """Show deduplication results"""
    click.echo("\n📊 중복 제거 결과:")
    click.echo("=" * 40)
    click.echo(f"처리된 파일 수: {stats['files_processed']:,}")
    click.echo(f"변경된 파일 수: {stats['files_modified']:,}")
    click.echo(f"처리 전 총 레코드 수: {stats['total_records_before']:,}")
    click.echo(f"처리 후 총 레코드 수: {stats['total_records_after']:,}")
    click.echo(f"제거된 중복 레코드 수: {stats['duplicates_removed']:,}")
    
    if stats['duplicates_removed'] > 0:
        reduction_percent = (stats['duplicates_removed'] / stats['total_records_before']) * 100
        click.echo(f"중복 제거율: {reduction_percent:.2f}%")
    
    if stats['failed_files']:
        click.echo(f"\n❌ 실패한 파일 수: {len(stats['failed_files'])}")
        for failed_file in stats['failed_files'][:5]:  # Show first 5
            click.echo(f"  - {failed_file}")
        if len(stats['failed_files']) > 5:
            click.echo(f"  ... 그 외 {len(stats['failed_files']) - 5}개")