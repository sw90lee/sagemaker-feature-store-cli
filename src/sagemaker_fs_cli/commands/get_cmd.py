"""Get command implementation"""

import click
from typing import List, Optional
from botocore.exceptions import ClientError
from ..config import Config
from ..utils.formatter import OutputFormatter


def get_record(config: Config, feature_group_name: str, record_identifier_value: str, 
               feature_names: Optional[List[str]], output_format: str) -> None:
    """Get a single record from the feature group"""
    try:
        # First, get feature group details to find the record identifier name
        fg_details = config.sagemaker.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        
        # Find the record identifier feature name
        record_identifier_feature = None
        for feature in fg_details['FeatureDefinitions']:
            if feature.get('FeatureName') and 'record_identifier' in feature.get('FeatureType', '').lower():
                record_identifier_feature = feature['FeatureName']
                break
        
        # If not found by type, use the first feature as record identifier (common pattern)
        if not record_identifier_feature and fg_details['FeatureDefinitions']:
            record_identifier_feature = fg_details['FeatureDefinitions'][0]['FeatureName']
        
        if not record_identifier_feature:
            click.echo("오류: 레코드 식별자 피처 이름을 확인할 수 없습니다", err=True)
            raise click.Abort()
        
        # Prepare the get_record request
        request_params = {
            'FeatureGroupName': feature_group_name,
            'RecordIdentifierValueAsString': record_identifier_value
        }
        
        if feature_names:
            request_params['FeatureNames'] = feature_names
        
        # Get the record
        response = config.featurestore_runtime.get_record(**request_params)
        
        if not response.get('Record'):
            click.echo(f"식별자 '{record_identifier_value}'에 해당하는 레코드를 찾을 수 없습니다")
            return
        
        # Format the record
        record_data = {}
        for feature in response['Record']:
            feature_name = feature['FeatureName']
            feature_value = feature['ValueAsString']
            record_data[feature_name] = feature_value
        
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