"""Get command implementation for offline store"""

import click
import time
from typing import List, Optional, Dict, Any
from botocore.exceptions import ClientError
from ..config import Config
from ..utils.formatter import OutputFormatter


def get_record(config: Config, feature_group_name: str, record_identifier_value: str, 
               feature_names: Optional[List[str]], output_format: str) -> None:
    """Get a single record from the offline store using Athena"""
    try:
        # Get feature group details
        fg_details = config.sagemaker.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        
        # Check if offline store is enabled
        if not fg_details.get('OfflineStoreConfig'):
            click.echo(f"피처 그룹 '{feature_group_name}'에 오프라인 스토어가 활성화되어 있지 않습니다", err=True)
            raise click.Abort()
        
        # Get record identifier feature name
        record_identifier_feature = fg_details.get('RecordIdentifierFeatureName')
        if not record_identifier_feature:
            click.echo("오류: 레코드 식별자 피처 이름을 확인할 수 없습니다", err=True)
            raise click.Abort()
        
        # Query from offline store using Athena
        record_data = _get_record_from_athena(config, feature_group_name, record_identifier_feature, record_identifier_value, feature_names)
        
        if not record_data:
            click.echo(f"식별자 '{record_identifier_value}'에 해당하는 레코드를 오프라인 스토어에서 찾을 수 없습니다")
            return
        
        if output_format == 'table':
            # Convert to list format for table display
            table_data = [{'Feature': k, 'Value': v} for k, v in record_data.items()]
            output = OutputFormatter.format_table(table_data, ['Feature', 'Value'])
        else:  # json
            output = OutputFormatter.format_json(record_data)
        
        click.echo(output)
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFound':
            click.echo(f"피처 그룹 '{feature_group_name}'을 찾을 수 없습니다", err=True)
        elif error_code == 'ValidationException':
            click.echo(f"유효성 검사 오류: {e.response['Error']['Message']}", err=True)
        else:
            click.echo(f"AWS 오류: {e}", err=True)
        raise click.Abort()
    except Exception as e:
        click.echo(f"예상치 못한 오류: {e}", err=True)
        raise click.Abort()


def _get_record_from_athena(config: Config, feature_group_name: str, record_identifier_feature: str, 
                           record_identifier_value: str, feature_names: Optional[List[str]]) -> Optional[Dict[str, Any]]:
    """Get a single record from offline store using Athena"""
    try:
        athena_client = config.session.client('athena')
        
        # Find the Athena table name
        database_name = "sagemaker_featurestore"
        table_name = _find_athena_table_name(config, database_name, feature_group_name)
        if not table_name:
            click.echo(f"오류: Athena 테이블을 찾을 수 없습니다: {feature_group_name}", err=True)
            return None
        
        # Build query
        if feature_names:
            columns = ', '.join(feature_names)
        else:
            columns = '*'
        
        query = f"SELECT {columns} FROM {database_name}.{table_name} WHERE {record_identifier_feature} = '{record_identifier_value}' LIMIT 1"
        
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
        paginator = athena_client.get_paginator('get_query_results')
        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            rows = page['ResultSet']['Rows']
            if len(rows) > 1:  # Skip header row
                headers = [col['VarCharValue'] for col in rows[0]['Data']]
                data_row = rows[1]['Data']
                
                record_data = {}
                for i, header in enumerate(headers):
                    value = data_row[i].get('VarCharValue', '') if i < len(data_row) else ''
                    record_data[header] = value
                
                return record_data
        
        return None
        
    except Exception as e:
        click.echo(f"Athena 조회 오류: {e}", err=True)
        return None


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
    max_wait_time = 60  # 1 minute
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