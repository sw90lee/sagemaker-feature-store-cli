"""Put command implementation for offline store"""

import json
import time
import os
import tempfile
from datetime import datetime
from typing import Dict, Any
import click
from botocore.exceptions import ClientError
from ..config import Config
from urllib.parse import urlparse


def put_record(config: Config, feature_group_name: str, record: str) -> None:
    """Put a single record to the offline store (S3)"""
    try:
        # Parse the JSON record
        try:
            record_data = json.loads(record)
        except json.JSONDecodeError as e:
            click.echo(f"잘못된 JSON 레코드: {e}", err=True)
            raise click.Abort()
        
        if not isinstance(record_data, dict):
            click.echo("레코드는 JSON 객체 형태여야 합니다", err=True)
            raise click.Abort()
        
        # Get feature group details to understand the schema
        fg_details = config.sagemaker.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        
        # Check if offline store is enabled
        if not fg_details.get('OfflineStoreConfig'):
            click.echo(f"피처 그룹 '{feature_group_name}'에 오프라인 스토어가 활성화되어 있지 않습니다", err=True)
            raise click.Abort()
        
        # Prepare the record for putting
        feature_definitions = {fd['FeatureName']: fd for fd in fg_details['FeatureDefinitions']}
        formatted_record = []
        
        # Add EventTime if not provided (required for SageMaker FeatureStore)
        if 'EventTime' not in record_data:
            record_data['EventTime'] = str(int(time.time()))
        
        for feature_name, value in record_data.items():
            if feature_name not in feature_definitions:
                click.echo(f"경고: 피처 '{feature_name}'이 피처 그룹 스키마에서 찾을 수 없습니다", err=True)
                continue
            
            formatted_record.append({
                'FeatureName': feature_name,
                'ValueAsString': str(value)
            })
        
        if not formatted_record:
            click.echo("레코드에서 유효한 피처를 찾을 수 없습니다", err=True)
            raise click.Abort()
        
        # Put the record to offline store (S3)
        _put_record_to_s3(config, feature_group_name, formatted_record, fg_details)
        
        click.echo(f"피처 그룹 '{feature_group_name}'의 오프라인 스토어(S3)에 레코드가 성공적으로 저장되었습니다")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFound':
            click.echo(f"피처 그룹 '{feature_group_name}'을 찾을 수 없습니다", err=True)
        elif error_code == 'ValidationException':
            click.echo(f"유효성 검사 오류: {e.response['Error']['Message']}", err=True)
        elif error_code == 'AccessDeniedException':
            click.echo(f"S3 접근 거부됨: {e.response['Error']['Message']}", err=True)
        else:
            click.echo(f"AWS 오류: {e}", err=True)
        raise click.Abort()
    except Exception as e:
        click.echo(f"예상치 못한 오류: {e}", err=True)
        raise click.Abort()


def _put_record_to_s3(config: Config, feature_group_name: str, formatted_record: list, fg_details: Dict[str, Any]) -> None:
    """Put a single record to S3 in the offline store format"""
    try:
        # Get offline store S3 location
        offline_config = fg_details['OfflineStoreConfig']
        s3_uri = offline_config['S3StorageConfig']['S3Uri']
        
        # Parse S3 URI
        parsed_uri = urlparse(s3_uri)
        bucket_name = parsed_uri.netloc
        prefix = parsed_uri.path.lstrip('/')
        
        # Create record data as JSON
        record_data = {}
        for feature in formatted_record:
            record_data[feature['FeatureName']] = feature['ValueAsString']
        
        # Generate S3 key with timestamp for uniqueness
        timestamp = datetime.now().strftime('%Y/%m/%d/%H')
        record_id = record_data.get('record_id', str(int(time.time())))
        s3_key = f"{prefix.rstrip('/')}/year={timestamp.split('/')[0]}/month={timestamp.split('/')[1]}/day={timestamp.split('/')[2]}/hour={timestamp.split('/')[3]}/{record_id}_{int(time.time())}.json"
        
        # Upload to S3
        s3_client = config.session.client('s3')
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(record_data, ensure_ascii=False),
            ContentType='application/json'
        )
        
        click.echo(f"S3 위치: s3://{bucket_name}/{s3_key}")
        
    except Exception as e:
        click.echo(f"S3 저장 오류: {e}", err=True)
        raise