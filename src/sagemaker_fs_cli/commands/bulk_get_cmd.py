"""Bulk get command implementation for offline store"""

import click
import os
import time
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from botocore.exceptions import ClientError
from ..config import Config
from ..utils.file_handler import FileHandler
from ..utils.formatter import OutputFormatter


def bulk_get_records(config: Config, feature_group_name: str, input_file: str, 
                    output_file: Optional[str], feature_names: Optional[List[str]], 
                    current_time: bool = False) -> None:
    """Bulk get records from offline store using Athena and input file"""
    try:
        # Validate input file exists
        if not os.path.exists(input_file):
            click.echo(f"입력 파일 '{input_file}'을 찾을 수 없습니다", err=True)
            raise click.Abort()
        
        # Read input file
        try:
            input_data = FileHandler.read_file(input_file)
        except Exception as e:
            click.echo(f"입력 파일 읽기 오류: {e}", err=True)
            raise click.Abort()
        
        if not input_data:
            click.echo("입력 파일이 비어있습니다", err=True)
            raise click.Abort()
        
        # Get feature group details
        fg_details = config.sagemaker.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        
        # Check if offline store is enabled
        if not fg_details.get('OfflineStoreConfig'):
            click.echo(f"피처 그룹 '{feature_group_name}'에 오프라인 스토어가 활성화되어 있지 않습니다", err=True)
            raise click.Abort()
        
        # Extract record identifiers
        record_ids = []
        for record in input_data:
            if isinstance(record, dict):
                # Try to find record identifier in various common field names
                record_id = (record.get('record_id') or 
                           record.get('id') or 
                           record.get('RecordIdentifier') or 
                           record.get('record_identifier'))
                if record_id is not None:
                    record_ids.append(str(record_id))
                else:
                    # If no identifier field found, use the first value
                    if record:
                        first_key = next(iter(record))
                        record_ids.append(str(record[first_key]))
            else:
                record_ids.append(str(record))
        
        if not record_ids:
            click.echo("입력 파일에서 레코드 식별자를 찾을 수 없습니다", err=True)
            raise click.Abort()
        
        click.echo(f"{len(record_ids)}개 레코드를 오프라인 스토어에서 조회 중...")
        
        # Get records from offline store using Athena
        results = _bulk_get_records_from_athena(config, feature_group_name, record_ids, feature_names, fg_details)
        
        if not results:
            click.echo("조회된 레코드가 없습니다", err=True)
            raise click.Abort()
        
        # Replace Time field with current timestamp if requested
        if current_time:
            current_timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            for record in results:
                if 'Time' in record:
                    record['Time'] = current_timestamp
        
        # Output results
        if output_file:
            try:
                FileHandler.write_file(results, output_file)
                click.echo(f"결과가 '{output_file}'에 저장되었습니다")
                click.echo(f"{len(results)}개 레코드가 성공적으로 조회되었습니다")
            except Exception as e:
                click.echo(f"출력 파일 쓰기 오류: {e}", err=True)
                raise click.Abort()
        else:
            # Print to stdout
            output = OutputFormatter.format_json(results)
            click.echo(output)
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFound':
            click.echo(f"피처 그룹 '{feature_group_name}'을 찾을 수 없습니다", err=True)
        else:
            click.echo(f"AWS 오류: {e}", err=True)
        raise click.Abort()
    except Exception as e:
        click.echo(f"예상치 못한 오류: {e}", err=True)
        raise click.Abort()


def _bulk_get_records_from_athena(config: Config, feature_group_name: str, record_ids: List[str], 
                                 feature_names: Optional[List[str]], fg_details: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get multiple records from offline store using Athena"""
    try:
        athena_client = config.session.client('athena')
        
        # Find the Athena table name
        database_name = "sagemaker_featurestore"
        table_name = _find_athena_table_name(config, database_name, feature_group_name)
        if not table_name:
            click.echo(f"오류: Athena 테이블을 찾을 수 없습니다: {feature_group_name}", err=True)
            return []
        
        # Get record identifier feature name
        record_identifier_feature = fg_details.get('RecordIdentifierFeatureName')
        if not record_identifier_feature:
            click.echo("오류: 레코드 식별자 피처 이름을 확인할 수 없습니다", err=True)
            return []
        
        # Build query for multiple record IDs
        if feature_names:
            columns = ', '.join(feature_names)
        else:
            columns = '*'
        
        # Create IN clause with quoted record IDs
        record_ids_str = "', '".join(record_ids)
        query = f"SELECT {columns} FROM {database_name}.{table_name} WHERE {record_identifier_feature} IN ('{record_ids_str}')"
        
        click.echo(f"Athena 쿼리 실행 중... ({len(record_ids)}개 레코드 조회)")
        
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
        results = []
        paginator = athena_client.get_paginator('get_query_results')
        
        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            rows = page['ResultSet']['Rows']
            if len(rows) > 0:
                # First row is header
                headers = [col.get('VarCharValue', '') for col in rows[0]['Data']]
                
                # Process data rows
                for row in rows[1:]:  # Skip header row
                    record_data = {}
                    for i, header in enumerate(headers):
                        value = ''
                        if i < len(row['Data']) and row['Data'][i].get('VarCharValue'):
                            value = row['Data'][i]['VarCharValue']
                        record_data[header] = value
                    
                    if record_data:  # Only add non-empty records
                        results.append(record_data)
        
        click.echo(f"✅ {len(results)}개 레코드를 성공적으로 조회했습니다")
        return results
        
    except Exception as e:
        click.echo(f"Athena 조회 오류: {e}", err=True)
        return []


def _find_athena_table_name(config: Config, database: str, feature_group_name: str) -> Optional[str]:
    """Find the actual Athena table name for a feature group"""
    try:
        athena_client = config.session.client('athena')
        
        # Get account ID for table name pattern
        account_id = config.session.client('sts').get_caller_identity()['Account']
        
        # Possible table name patterns
        table_name_patterns = [
            feature_group_name.replace('-', '_'),
            feature_group_name.lower().replace('-', '_'),
            f"{feature_group_name.replace('-', '_')}_{account_id}",
            f"{feature_group_name.lower().replace('-', '_')}_{account_id}"
        ]
        
        # Get actual table names
        response = athena_client.list_table_metadata(
            CatalogName='AwsDataCatalog',
            DatabaseName=database
        )
        
        existing_tables = [table['Name'] for table in response.get('TableMetadataList', [])]
        
        # Try exact matches first
        for pattern in table_name_patterns:
            for table_name in existing_tables:
                if pattern.lower() == table_name.lower():
                    return table_name
        
        # Try partial matches
        feature_group_base = feature_group_name.replace('-', '_').lower()
        for table_name in existing_tables:
            if feature_group_base in table_name.lower():
                return table_name
        
        # Show available tables for debugging
        click.echo(f"사용 가능한 테이블: {existing_tables[:5]}...")
        return None
        
    except Exception:
        return None


def _get_athena_output_location(config: Config) -> str:
    """Get suitable S3 location for Athena query results"""
    try:
        session = config.session
        region = session.region_name or 'us-east-1'
        account_id = session.client('sts').get_caller_identity()['Account']
        
        bucket_name = f"sagemaker-{region}-{account_id}"
        return f"s3://{bucket_name}/athena-results/"
        
    except Exception:
        return 's3://temp-query-results/'


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