"""Put command implementation"""

import json
import time
import click
from typing import Dict, Any
from botocore.exceptions import ClientError
from ..config import Config


def put_record(config: Config, feature_group_name: str, record: str) -> None:
    """Put a single record to the feature group"""
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
        
        # Check if online store is enabled
        if not fg_details.get('OnlineStoreConfig'):
            click.echo(f"피처 그룹 '{feature_group_name}'에 온라인 스토어가 활성화되어 있지 않습니다", err=True)
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
        
        # Put the record
        response = config.featurestore_runtime.put_record(
            FeatureGroupName=feature_group_name,
            Record=formatted_record
        )
        
        click.echo(f"피처 그룹 '{feature_group_name}'에 레코드가 성공적으로 저장되었습니다")
        if response.get('ResponseMetadata', {}).get('RequestId'):
            click.echo(f"Request ID: {response['ResponseMetadata']['RequestId']}")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFound':
            click.echo(f"피처 그룹 '{feature_group_name}'을 찾을 수 없습니다", err=True)
        elif error_code == 'ValidationException':
            click.echo(f"유효성 검사 오류: {e.response['Error']['Message']}", err=True)
        elif error_code == 'AccessDeniedException':
            click.echo(f"접근 거부됨: {e.response['Error']['Message']}", err=True)
        else:
            click.echo(f"AWS 오류: {e}", err=True)
        raise click.Abort()
    except Exception as e:
        click.echo(f"예상치 못한 오류: {e}", err=True)
        raise click.Abort()