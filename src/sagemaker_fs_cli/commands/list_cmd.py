"""List command implementation"""

import click
from typing import List, Dict, Any
from botocore.exceptions import ClientError
from ..config import Config
from ..utils.formatter import OutputFormatter


def list_feature_groups(config: Config, output_format: str) -> None:
    """List all feature groups with online store enabled"""
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
                    
                    # Only include feature groups with online store enabled
                    if fg_details.get('OnlineStoreConfig'):
                        feature_groups.append({
                            'FeatureGroupName': fg['FeatureGroupName'],
                            'FeatureGroupStatus': fg['FeatureGroupStatus'],
                            'OnlineStoreConfig': fg_details.get('OnlineStoreConfig'),
                            'CreationTime': fg['CreationTime'].strftime('%Y-%m-%d %H:%M:%S') if fg.get('CreationTime') else '',
                            'OfflineStoreConfig': fg_details.get('OfflineStoreConfig', {}).get('S3StorageConfig', {}).get('S3Uri', 'Not configured') if fg_details.get('OfflineStoreConfig') else 'Not configured'
                        })
                except ClientError as e:
                    click.echo(f"경고: 피처 그룹 {fg['FeatureGroupName']} 정보를 가져올 수 없습니다: {e}", err=True)
        
        if not feature_groups:
            click.echo("온라인 피처 그룹을 찾을 수 없습니다.")
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