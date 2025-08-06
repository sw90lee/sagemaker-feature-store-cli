"""List command implementation"""

import click
from typing import List, Dict, Any
from botocore.exceptions import ClientError
from ..config import Config
from ..utils.formatter import OutputFormatter


def list_feature_groups(config: Config, output_format: str) -> None:
    """List all feature groups with online or offline store enabled"""
    try:
        feature_groups = []
        paginator = config.sagemaker.get_paginator('list_feature_groups')
        
        for page in paginator.paginate():
            for fg in page['FeatureGroupSummaries']:
                # Get detailed information about each feature group
                try:
                    fg_details = config.sagemaker.describe_feature_group(
                        FeatureGroupName=fg['FeatureGroupName']
                    )
                    
                    # Include feature groups with online or offline store enabled
                    online_config = fg_details.get('OnlineStoreConfig', {})
                    offline_config = fg_details.get('OfflineStoreConfig', {})
                    
                    if online_config or offline_config:
                        # Extract detailed online store information (if available)
                        storage_type = online_config.get('StorageType', 'N/A') if online_config else 'N/A'
                        ttl_duration = online_config.get('TtlDuration') if online_config else None
                        if ttl_duration:
                            ttl_value = f"{ttl_duration.get('Value', 'N/A')} {ttl_duration.get('Unit', '')}"
                        else:
                            ttl_value = 'N/A'
                        
                        # Extract offline store information
                        offline_s3_uri = offline_config.get('S3StorageConfig', {}).get('S3Uri', 'Not configured') if offline_config else 'Not configured'
                        offline_table_format = offline_config.get('TableFormat', 'Unknown') if offline_config else 'N/A'
                        
                        # Determine ingest mode based on store configuration
                        ingest_mode = []
                        if online_config:
                            ingest_mode.append('Online')
                        if offline_config:
                            ingest_mode.append('Offline')
                        ingest_mode_str = ' + '.join(ingest_mode) if ingest_mode else 'Unknown'
                        
                        feature_groups.append({
                            'FeatureGroupName': fg['FeatureGroupName'],
                            'FeatureGroupStatus': fg['FeatureGroupStatus'],
                            'IngestMode': ingest_mode_str,
                            'StorageType': storage_type,
                            'TTLValue': ttl_value,
                            'EventTimeFeatureName': fg_details.get('EventTimeFeatureName', 'N/A'),
                            'RecordIdentifierFeatureName': fg_details.get('RecordIdentifierFeatureName', 'N/A'),
                            'CreationTime': fg['CreationTime'].strftime('%Y-%m-%d %H:%M:%S') if fg.get('CreationTime') else '',
                            'OfflineS3Uri': offline_s3_uri,
                            'TableFormat': offline_table_format,
                            'OnlineStoreConfig': online_config,
                            'OfflineStoreConfig': offline_config
                        })
                except ClientError as e:
                    click.echo(f"경고: 피처 그룹 {fg['FeatureGroupName']} 정보를 가져올 수 없습니다: {e}", err=True)
        
        if not feature_groups:
            click.echo("피처 그룹을 찾을 수 없습니다.")
            return
        
        if output_format == 'table':
            output = OutputFormatter.format_feature_groups(feature_groups)
        else:  # json
            output = OutputFormatter.format_json(feature_groups)
        
        click.echo(output)
        
    except ClientError as e:
        click.echo(f"피처 그룹 목록 조회 오류: {e}", err=True)
        raise click.Abort()
    except Exception as e:
        click.echo(f"예상치 못한 오류: {e}", err=True)
        raise click.Abort()