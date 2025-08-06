"""Migration command implementation"""

import json
import time
import click
from typing import Dict, Any, List, Optional, Generator, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from tqdm import tqdm
import pandas as pd

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


def migrate_feature_group(config: Config, source_name: str, target_name: str,
                         clear_target: bool = False, batch_size: int = 100,
                         max_workers: int = 4, dry_run: bool = False,
                         filter_query: Optional[str] = None) -> None:
    """Migrate data from source feature group to target feature group"""
    try:
        click.echo(f"🚀 피처 그룹 마이그레이션 시작: {source_name} → {target_name}")
        
        # Step 1: Validate both feature groups
        click.echo("\n📋 피처 그룹 정보 확인 중...")
        source_fg = _validate_feature_group(config, source_name, "source")
        target_fg = _validate_feature_group(config, target_name, "target")
        
        # Step 2: Check compatibility
        click.echo("\n🔍 스키마 호환성 검사 중...")
        _validate_migration_compatibility(source_fg, target_fg)
        click.echo("✅ 스키마 호환성 확인 완료")
        
        # Step 3: Plan migration
        click.echo("\n📊 마이그레이션 계획 수립 중...")
        migration_plan = _plan_migration(config, source_fg, target_fg, {
            'batch_size': batch_size,
            'max_workers': max_workers,
            'filter_query': filter_query,
            'clear_target': clear_target
        })
        
        # Step 4: Show plan
        _show_migration_plan(source_name, target_name, migration_plan)
        
        if dry_run:
            click.echo("\n🔍 Dry-run 모드: 실제 마이그레이션은 수행되지 않습니다.")
            return
        
        # Step 5: Confirm migration
        if not _confirm_migration(source_name, target_name, migration_plan):
            click.echo("마이그레이션이 취소되었습니다.")
            return
        
        # Step 6: Clear target if requested
        if clear_target:
            click.echo(f"\n🗑️  타겟 피처 그룹 '{target_name}' 데이터 삭제 중...")
            _clear_target_feature_group(config, target_fg)
        
        # Step 7: Execute migration
        click.echo(f"\n📦 데이터 마이그레이션 실행 중...")
        result = _execute_migration(config, source_fg, target_fg, migration_plan)
        
        # Step 8: Show results
        _show_migration_results(result)
        
    except Exception as e:
        click.echo(f"\n❌ 마이그레이션 실패: {e}", err=True)
        raise click.Abort()


def _validate_feature_group(config: Config, feature_group_name: str, role: str) -> Dict[str, Any]:
    """Validate feature group exists and return details"""
    try:
        fg_details = config.sagemaker.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        
        # Check if it's in the right state
        status = fg_details['FeatureGroupStatus']
        if status != 'Created':
            raise ValueError(f"{role.capitalize()} 피처 그룹 '{feature_group_name}' 상태가 '{status}'입니다. 'Created' 상태여야 합니다.")
        
        return fg_details
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFound':
            raise ValueError(f"{role.capitalize()} 피처 그룹 '{feature_group_name}'을 찾을 수 없습니다.")
        else:
            raise ValueError(f"{role.capitalize()} 피처 그룹 정보 조회 실패: {e}")


def _validate_migration_compatibility(source_fg: Dict[str, Any], target_fg: Dict[str, Any]) -> None:
    """Validate that source and target feature groups are compatible"""
    
    # Compare feature definitions
    source_features = {f['FeatureName']: f for f in source_fg['FeatureDefinitions']}
    target_features = {f['FeatureName']: f for f in target_fg['FeatureDefinitions']}
    
    # Check for missing features in source
    missing_features = set(target_features.keys()) - set(source_features.keys())
    if missing_features:
        raise ValueError(f"타겟에 있지만 소스에 없는 피처: {missing_features}")
    
    # Check data type compatibility
    incompatible_features = []
    for fname, target_feat in target_features.items():
        if fname in source_features:
            source_feat = source_features[fname]
            if not _is_type_compatible(source_feat['FeatureType'], target_feat['FeatureType']):
                incompatible_features.append((fname, source_feat['FeatureType'], target_feat['FeatureType']))
    
    if incompatible_features:
        raise ValueError(f"호환되지 않는 피처 타입: {incompatible_features}")
    
    # Check RecordIdentifier and EventTime features
    if source_fg['RecordIdentifierFeatureName'] != target_fg['RecordIdentifierFeatureName']:
        raise ValueError(f"RecordIdentifierFeatureName이 다릅니다: {source_fg['RecordIdentifierFeatureName']} vs {target_fg['RecordIdentifierFeatureName']}")
    
    if source_fg['EventTimeFeatureName'] != target_fg['EventTimeFeatureName']:
        raise ValueError(f"EventTimeFeatureName이 다릅니다: {source_fg['EventTimeFeatureName']} vs {target_fg['EventTimeFeatureName']}")


def _is_type_compatible(source_type: str, target_type: str) -> bool:
    """Check if source and target feature types are compatible"""
    # Same type is always compatible
    if source_type == target_type:
        return True
    
    # Define compatible type mappings
    compatible_mappings = {
        'Integral': ['Integral', 'Fractional', 'String'],  # Numbers can convert to string
        'Fractional': ['Fractional', 'String'],
        'String': ['String']  # String can only go to string
    }
    
    return target_type in compatible_mappings.get(source_type, [])


def _plan_migration(config: Config, source_fg: Dict[str, Any], target_fg: Dict[str, Any], 
                   options: Dict[str, Any]) -> Dict[str, Any]:
    """Plan the migration strategy"""
    
    # Determine source and target store types
    source_online = bool(source_fg.get('OnlineStoreConfig'))
    source_offline = bool(source_fg.get('OfflineStoreConfig'))
    target_online = bool(target_fg.get('OnlineStoreConfig'))
    target_offline = bool(target_fg.get('OfflineStoreConfig'))
    
    if source_online and source_offline:
        source_type = 'online+offline'
    elif source_online:
        source_type = 'online'
    else:
        source_type = 'offline'
    
    if target_online and target_offline:
        target_type = 'online+offline'
    elif target_online:
        target_type = 'online'
    else:
        target_type = 'offline'
    
    # Estimate record count
    estimated_records = _estimate_record_count(config, source_fg, source_type)
    
    # Determine migration strategy
    if source_type == 'offline' and 'online' in target_type:
        strategy = 'offline_to_online'
        primary_source = 'offline'
    elif 'online' in source_type and 'online' in target_type:
        strategy = 'online_to_online'
        primary_source = 'online'  # Use online as primary for faster access
    else:
        raise ValueError(f"지원되지 않는 마이그레이션 타입: {source_type} → {target_type}")
    
    return {
        'source_type': source_type,
        'target_type': target_type,
        'strategy': strategy,
        'primary_source': primary_source,
        'estimated_records': estimated_records,
        'batch_size': options['batch_size'],
        'max_workers': options['max_workers'],
        'filter_query': options.get('filter_query'),
        'clear_target': options.get('clear_target', False)
    }


def _estimate_record_count(config: Config, source_fg: Dict[str, Any], source_type: str) -> int:
    """Estimate the number of records to migrate"""
    
    if 'offline' in source_type:
        # Try to get count from offline store via Athena
        try:
            return _get_offline_record_count(config, source_fg)
        except Exception as e:
            click.echo(f"⚠️  레코드 수 추정 실패: {e}")
            return 0
    else:
        # Online store doesn't have a direct count API
        click.echo("⚠️  온라인 스토어의 정확한 레코드 수를 미리 알 수 없습니다.")
        return 0


def _get_offline_record_count(config: Config, source_fg: Dict[str, Any]) -> int:
    """Get record count from offline store via Athena"""
    
    athena_client = config.session.client('athena')
    database = 'sagemaker_featurestore'
    feature_group_name = source_fg['FeatureGroupName']
    
    # Try to find the actual table name
    table_name = _find_athena_table_name(config, database, feature_group_name)
    if not table_name:
        click.echo(f"⚠️  Athena 테이블을 찾을 수 없습니다: {feature_group_name}")
        return 0
    
    query = f"SELECT COUNT(*) as record_count FROM {database}.{table_name}"
    
    # Execute query
    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': _get_athena_output_location(config)
        }
    )
    
    query_execution_id = response['QueryExecutionId']
    
    # Wait for query completion
    _wait_for_query_completion(athena_client, query_execution_id)
    
    # Get result
    response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    rows = response['ResultSet']['Rows']
    
    if len(rows) > 1 and rows[1]['Data']:
        return int(rows[1]['Data'][0]['VarCharValue'])
    
    return 0


def _show_migration_plan(source_name: str, target_name: str, plan: Dict[str, Any]) -> None:
    """Display migration plan"""
    
    click.echo(f"\n📋 마이그레이션 계획")
    click.echo("=" * 60)
    click.echo(f"소스: {source_name} ({plan['source_type']})")
    click.echo(f"타겟: {target_name} ({plan['target_type']})")
    click.echo(f"전략: {plan['strategy']}")
    click.echo(f"주요 데이터 소스: {plan['primary_source']}")
    click.echo(f"예상 레코드 수: {plan['estimated_records']:,}")
    click.echo(f"배치 크기: {plan['batch_size']}")
    click.echo(f"동시 워커 수: {plan['max_workers']}")
    
    if plan['filter_query']:
        click.echo(f"필터: {plan['filter_query']}")
    
    if plan['clear_target']:
        click.echo("⚠️  타겟 데이터 삭제: 예")


def _confirm_migration(source_name: str, target_name: str, plan: Dict[str, Any]) -> bool:
    """Ask for user confirmation"""
    
    click.echo(f"\n⚠️  경고: '{source_name}'의 데이터를 '{target_name}'으로 마이그레이션합니다.")
    
    if plan['clear_target']:
        click.echo(f"⚠️  타겟 피처 그룹 '{target_name}'의 기존 데이터가 삭제됩니다!")
    
    click.echo("이 작업은 시간이 오래 걸릴 수 있습니다.")
    
    return click.confirm("\n계속하시겠습니까?")


def _clear_target_feature_group(config: Config, target_fg: Dict[str, Any]) -> None:
    """Clear target feature group data"""
    
    # Import clear functionality
    from . import clear_cmd
    
    target_name = target_fg['FeatureGroupName']
    
    try:
        # Use clear command with force flag
        clear_cmd.clear_feature_group(
            config=config,
            feature_group_name=target_name,
            online_only=False,
            offline_only=False,
            force=True,  # Skip confirmation
            backup_s3=None,
            dry_run=False
        )
    except Exception as e:
        click.echo(f"타겟 데이터 삭제 실패: {e}", err=True)
        raise


def _execute_migration(config: Config, source_fg: Dict[str, Any], target_fg: Dict[str, Any],
                      migration_plan: Dict[str, Any]) -> Dict[str, Any]:
    """Execute the actual migration"""
    
    strategy = migration_plan['strategy']
    batch_size = migration_plan['batch_size']
    max_workers = migration_plan['max_workers']
    
    total_processed = 0
    total_failed = 0
    failed_records = []
    
    # Progress bar setup
    estimated_records = migration_plan['estimated_records']
    if estimated_records > 0:
        progress_bar = tqdm(total=estimated_records, desc="마이그레이션 진행")
    else:
        progress_bar = tqdm(desc="마이그레이션 진행")
    
    try:
        # Execute based on strategy
        if strategy == 'offline_to_online':
            batches = _extract_from_offline_store(config, source_fg, migration_plan)
        else:  # online_to_online
            batches = _extract_from_online_store(config, source_fg, migration_plan)
        
        # Process each batch
        for batch in batches:
            if not batch:
                continue
                
            # Load batch to target
            success_count, failures = _load_to_target_store(config, target_fg, batch, max_workers)
            
            total_processed += len(batch)
            total_failed += len(failures)
            failed_records.extend(failures)
            
            # Update progress
            progress_bar.update(len(batch))
            progress_bar.set_postfix({
                'Success': f'{success_count}/{len(batch)}',
                'Failed': len(failures)
            })
    
    finally:
        progress_bar.close()
    
    return {
        'total_processed': total_processed,
        'total_success': total_processed - total_failed,
        'total_failed': total_failed,
        'failed_records': failed_records,
        'success_rate': (total_processed - total_failed) / total_processed if total_processed > 0 else 0
    }


def _extract_from_offline_store(config: Config, source_fg: Dict[str, Any], 
                               migration_plan: Dict[str, Any]) -> Generator[List[Dict], None, None]:
    """Extract data from offline store via Athena"""
    
    athena_client = config.session.client('athena')
    database = 'sagemaker_featurestore'
    feature_group_name = source_fg['FeatureGroupName']
    batch_size = migration_plan['batch_size']
    
    # Find actual table name
    table_name = _find_athena_table_name(config, database, feature_group_name)
    if not table_name:
        click.echo(f"⚠️  Athena 테이블을 찾을 수 없습니다: {feature_group_name}")
        return
    
    # Build query
    base_query = f"SELECT * FROM {database}.{table_name}"
    filter_query = migration_plan.get('filter_query')
    if filter_query:
        query = f"{base_query} {filter_query}"
    else:
        query = base_query
    
    # Execute query
    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': _get_athena_output_location(config)
        }
    )
    
    query_execution_id = response['QueryExecutionId']
    
    # Wait for completion
    _wait_for_query_completion(athena_client, query_execution_id)
    
    # Read results in batches
    paginator = athena_client.get_paginator('get_query_results')
    current_batch = []
    
    for page in paginator.paginate(QueryExecutionId=query_execution_id):
        rows = page['ResultSet']['Rows']
        
        # Skip header row
        if page['ResultSet']['Rows'] and not current_batch:
            rows = rows[1:]
        
        for row in rows:
            try:
                record = _convert_athena_row_to_record(row, source_fg['FeatureDefinitions'])
                current_batch.append(record)
                
                if len(current_batch) >= batch_size:
                    yield current_batch
                    current_batch = []
                    
            except Exception as e:
                click.echo(f"⚠️  레코드 변환 실패: {e}")
                continue
    
    # Yield remaining records
    if current_batch:
        yield current_batch


def _extract_from_online_store(config: Config, source_fg: Dict[str, Any], 
                              migration_plan: Dict[str, Any]) -> Generator[List[Dict], None, None]:
    """Extract data from online store"""
    
    # This is more complex as we need to get all record IDs first
    # For now, we'll use the same Athena approach if offline store exists
    if source_fg.get('OfflineStoreConfig'):
        # Use offline store to get record IDs, then fetch from online
        yield from _extract_hybrid_approach(config, source_fg, migration_plan)
    else:
        raise ValueError("온라인 전용 피처 그룹에서 모든 데이터를 추출하는 기능은 현재 지원되지 않습니다. "
                        "대신 record ID 목록을 제공하거나 bulk-get을 사용해주세요.")


def _extract_hybrid_approach(config: Config, source_fg: Dict[str, Any], 
                            migration_plan: Dict[str, Any]) -> Generator[List[Dict], None, None]:
    """Extract using both online and offline stores"""
    
    # Get record IDs from offline store
    record_ids = _get_record_ids_from_offline(config, source_fg, migration_plan)
    
    # Fetch records from online store in batches
    featurestore_runtime = config.session.client('sagemaker-featurestore-runtime')
    source_name = source_fg['FeatureGroupName']
    batch_size = migration_plan['batch_size']
    
    current_batch = []
    
    for record_id in record_ids:
        try:
            response = featurestore_runtime.get_record(
                FeatureGroupName=source_name,
                RecordIdentifierValueAsString=str(record_id)
            )
            
            if response.get('Record'):
                record = {item['FeatureName']: item['ValueAsString'] 
                         for item in response['Record']}
                current_batch.append(record)
                
                if len(current_batch) >= batch_size:
                    yield current_batch
                    current_batch = []
                    
        except Exception as e:
            click.echo(f"⚠️  레코드 {record_id} 조회 실패: {e}")
            continue
    
    if current_batch:
        yield current_batch


def _get_record_ids_from_offline(config: Config, source_fg: Dict[str, Any], 
                                migration_plan: Dict[str, Any]) -> List[str]:
    """Get all record IDs from offline store"""
    
    athena_client = config.session.client('athena')
    database = 'sagemaker_featurestore'
    feature_group_name = source_fg['FeatureGroupName']
    record_id_feature = source_fg['RecordIdentifierFeatureName']
    
    # Find actual table name
    table_name = _find_athena_table_name(config, database, feature_group_name)
    if not table_name:
        click.echo(f"⚠️  Athena 테이블을 찾을 수 없습니다: {feature_group_name}")
        return []
    
    # Build query to get unique record IDs
    base_query = f"SELECT DISTINCT {record_id_feature} FROM {database}.{table_name}"
    filter_query = migration_plan.get('filter_query')
    if filter_query:
        query = f"{base_query} {filter_query}"
    else:
        query = base_query
    
    # Execute query
    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': _get_athena_output_location(config)
        }
    )
    
    query_execution_id = response['QueryExecutionId']
    _wait_for_query_completion(athena_client, query_execution_id)
    
    # Get results
    record_ids = []
    paginator = athena_client.get_paginator('get_query_results')
    
    for page in paginator.paginate(QueryExecutionId=query_execution_id):
        rows = page['ResultSet']['Rows'][1:]  # Skip header
        for row in rows:
            if row['Data'] and row['Data'][0].get('VarCharValue'):
                record_ids.append(row['Data'][0]['VarCharValue'])
    
    return record_ids


def _load_to_target_store(config: Config, target_fg: Dict[str, Any], records: List[Dict],
                         max_workers: int) -> Tuple[int, List[Dict]]:
    """Load batch of records to target store"""
    
    if not target_fg.get('OnlineStoreConfig'):
        raise ValueError("타겟 피처 그룹에 온라인 스토어가 없습니다.")
    
    featurestore_runtime = config.session.client('sagemaker-featurestore-runtime')
    target_name = target_fg['FeatureGroupName']
    
    success_count = 0
    failures = []
    
    # Process records in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_record = {
            executor.submit(_put_single_record, featurestore_runtime, target_name, record): record
            for record in records
        }
        
        for future in as_completed(future_to_record):
            record = future_to_record[future]
            try:
                future.result()
                success_count += 1
            except Exception as e:
                failures.append({
                    'record': record,
                    'error': str(e)
                })
    
    return success_count, failures


def _put_single_record(client, feature_group_name: str, record: Dict) -> None:
    """Put a single record with retry logic"""
    
    max_retries = 3
    base_delay = 1
    
    for attempt in range(max_retries):
        try:
            # Convert record to SageMaker format
            sagemaker_record = [
                {'FeatureName': k, 'ValueAsString': str(v)}
                for k, v in record.items() if v is not None
            ]
            
            client.put_record(
                FeatureGroupName=feature_group_name,
                Record=sagemaker_record
            )
            return
            
        except ClientError as e:
            if e.response['Error']['Code'] in ['ThrottlingException', 'TooManyRequestsException']:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    time.sleep(delay)
                    continue
            raise


def _convert_athena_row_to_record(row: Dict, feature_definitions: List[Dict]) -> Dict:
    """Convert Athena query result row to feature record"""
    
    record = {}
    
    # Create a mapping of feature names to their positions
    # This assumes the query selected features in the same order as feature definitions
    for i, feature_def in enumerate(feature_definitions):
        feature_name = feature_def['FeatureName']
        
        if i < len(row['Data']) and row['Data'][i].get('VarCharValue'):
            record[feature_name] = row['Data'][i]['VarCharValue']
    
    return record


def _wait_for_query_completion(athena_client, query_execution_id: str) -> None:
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


def _show_migration_results(result: Dict[str, Any]) -> None:
    """Display migration results"""
    
    click.echo(f"\n📊 마이그레이션 결과")
    click.echo("=" * 50)
    click.echo(f"총 처리된 레코드: {result['total_processed']:,}")
    click.echo(f"성공: {result['total_success']:,}")
    click.echo(f"실패: {result['total_failed']:,}")
    click.echo(f"성공률: {result['success_rate']:.1%}")
    
    if result['failed_records']:
        click.echo(f"\n⚠️  실패한 레코드 (처음 5개):")
        for i, failure in enumerate(result['failed_records'][:5]):
            record_id = failure['record'].get('record_id', 'Unknown')
            click.echo(f"  {i+1}. {record_id}: {failure['error']}")
        
        if len(result['failed_records']) > 5:
            click.echo(f"  ... 그 외 {len(result['failed_records']) - 5}개")
    
    if result['total_failed'] == 0:
        click.echo("\n✅ 마이그레이션이 성공적으로 완료되었습니다!")
    else:
        click.echo(f"\n⚠️  마이그레이션이 완료되었지만 {result['total_failed']}개 레코드 실패")