"""Add features command implementation"""

import click
import json
import time
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError
import boto3


def add_features(feature_group_name: str, features_file: str, 
                dry_run: bool = False, wait: bool = True) -> None:
    """Feature Group에 새로운 feature들을 추가합니다.
    
    Args:
        feature_group_name: 대상 feature group 이름
        features_file: 추가할 feature definition JSON 파일
        dry_run: 실제 수행하지 않고 미리보기만 진행
        wait: 업데이트 완료까지 대기 여부
    """
    try:
        click.echo("🚀 Feature 추가 프로세스 시작...")
        
        # SageMaker 클라이언트 초기화
        sagemaker_client = boto3.client('sagemaker')
        
        # 현재 feature group 정보 조회
        try:
            fg_details = sagemaker_client.describe_feature_group(
                FeatureGroupName=feature_group_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFound':
                click.echo(f"❌ Feature Group '{feature_group_name}'을 찾을 수 없습니다.", err=True)
                return
            raise
        
        current_features = fg_details.get('FeatureDefinitions', [])
        current_feature_names = {f['FeatureName'] for f in current_features}
        
        click.echo(f"✓ 현재 Feature Group: {feature_group_name}")
        click.echo(f"✓ 기존 feature 수: {len(current_features)}")
        
        # 새로운 feature definition 로드
        try:
            with open(features_file, 'r', encoding='utf-8') as f:
                new_features_data = json.load(f)
        except FileNotFoundError:
            click.echo(f"❌ 파일을 찾을 수 없습니다: {features_file}", err=True)
            return
        except json.JSONDecodeError as e:
            click.echo(f"❌ JSON 파싱 실패: {e}", err=True)
            return
        
        # 새로운 feature definition 검증
        if not isinstance(new_features_data, list):
            click.echo("❌ feature definition은 배열 형태여야 합니다.", err=True)
            return
        
        # 추가할 feature들만 필터링 (기존에 없는 것들)
        features_to_add = []
        duplicates = []
        
        for new_feature in new_features_data:
            feature_name = new_feature.get('FeatureName')
            if not feature_name:
                click.echo(f"⚠️  FeatureName이 없는 feature 무시: {new_feature}")
                continue
                
            if feature_name in current_feature_names:
                duplicates.append(feature_name)
            else:
                features_to_add.append(new_feature)
        
        if duplicates:
            click.echo(f"⚠️  이미 존재하는 feature들 (무시됨): {', '.join(duplicates)}")
        
        if not features_to_add:
            click.echo("ℹ️  추가할 새로운 feature가 없습니다.")
            return
        
        click.echo(f"✓ 추가할 feature 수: {len(features_to_add)}")
        for feature in features_to_add:
            click.echo(f"  + {feature['FeatureName']} ({feature['FeatureType']})")
        
        if dry_run:
            click.echo("🔍 [DRY RUN] 실제 추가하지 않고 미리보기만 실행합니다.")
            return
        
        # Feature 추가 실행
        try:
            click.echo("📝 Feature 추가 중...")
            response = sagemaker_client.update_feature_group(
                FeatureGroupName=feature_group_name,
                FeatureAdditions=features_to_add
            )
            
            click.echo("✓ Feature 추가 요청이 성공적으로 전송되었습니다!")
            
            if wait:
                click.echo("⏳ Feature 추가 완료까지 대기 중...")
                _wait_for_update(sagemaker_client, feature_group_name)
                click.echo("✅ Feature 추가가 완료되었습니다!")
            else:
                click.echo("ℹ️  Feature 추가가 백그라운드에서 진행됩니다.")
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            if error_code == 'ValidationException':
                click.echo(f"❌ 검증 오류: {error_message}", err=True)
            elif error_code == 'ConflictException':
                click.echo(f"❌ 충돌 오류: {error_message}", err=True)
            else:
                click.echo(f"❌ AWS 오류 ({error_code}): {error_message}", err=True)
            return
            
    except Exception as e:
        click.echo(f"❌ 예상치 못한 오류: {e}", err=True)
        return


def _wait_for_update(sagemaker_client: boto3.client, feature_group_name: str):
    """Feature Group 업데이트 완료까지 대기"""
    while True:
        try:
            response = sagemaker_client.describe_feature_group(
                FeatureGroupName=feature_group_name
            )
            
            status = response['FeatureGroupStatus']
            
            if status == 'Created':
                break
            elif status == 'UpdateFailed':
                failure_reason = response.get('FailureReason', '알 수 없는 오류')
                raise click.ClickException(f"Feature 업데이트 실패: {failure_reason}")
            elif status in ['Updating']:
                time.sleep(10)
            else:
                click.echo(f"현재 상태: {status}")
                time.sleep(10)
                
        except Exception as e:
            if "ResourceNotFound" in str(e):
                time.sleep(5)
            else:
                raise


def show_schema(feature_group_name: str, output_format: str = 'table') -> None:
    """Feature group의 현재 스키마를 조회하고 출력합니다."""
    try:
        sagemaker_client = boto3.client('sagemaker')
        fg_details = sagemaker_client.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        
        features = fg_details.get('FeatureDefinitions', [])
        
        if not features:
            click.echo(f"피처 그룹 '{feature_group_name}'에서 feature definition을 찾을 수 없습니다.")
            return
        
        # 기본 정보 출력
        click.echo(f"\n📊 Feature Group: {feature_group_name}")
        click.echo(f"Status: {fg_details.get('FeatureGroupStatus', 'Unknown')}")
        click.echo(f"Record Identifier: {fg_details.get('RecordIdentifierFeatureName', 'N/A')}")
        click.echo(f"Event Time Feature: {fg_details.get('EventTimeFeatureName', 'N/A')}")
        click.echo(f"Total Features: {len(features)}")
        
        # Feature 목록 출력
        if output_format == 'table':
            click.echo(f"\n📋 Feature Definitions:")
            click.echo("-" * 80)
            click.echo(f"{'Feature Name':<30} {'Type':<15} {'Collection Type':<15} {'Collection Name':<15}")
            click.echo("-" * 80)
            
            for feature in features:
                collection_type = feature.get('CollectionType', '')
                collection_name = ''
                
                if collection_type == 'List':
                    collection_name = feature.get('CollectionConfig', {}).get('VectorConfig', {}).get('Dimension', '')
                elif collection_type == 'Set':
                    collection_name = 'Set'
                    
                click.echo(f"{feature['FeatureName']:<30} {feature['FeatureType']:<15} {collection_type:<15} {str(collection_name):<15}")
        
        else:  # json
            output_data = {
                'FeatureGroupName': feature_group_name,
                'FeatureGroupStatus': fg_details.get('FeatureGroupStatus'),
                'RecordIdentifierFeatureName': fg_details.get('RecordIdentifierFeatureName'),
                'EventTimeFeatureName': fg_details.get('EventTimeFeatureName'),
                'FeatureDefinitions': features,
                'TotalFeatures': len(features)
            }
            click.echo(json.dumps(output_data, indent=2, ensure_ascii=False, default=str))
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFound':
            click.echo(f"오류: 피처 그룹 '{feature_group_name}'을 찾을 수 없습니다.", err=True)
        else:
            click.echo(f"AWS 오류: {e}", err=True)
        raise click.Abort()
    except Exception as e:
        click.echo(f"예상치 못한 오류: {e}", err=True)
        raise click.Abort()


def generate_feature_template(output_file: str = 'feature_template.json') -> None:
    """새로운 feature definition을 위한 템플릿 파일을 생성합니다."""
    template = [
        {
            "FeatureName": "example_string_feature",
            "FeatureType": "String",
            "Description": "예시 문자열 feature"
        },
        {
            "FeatureName": "example_integral_feature", 
            "FeatureType": "Integral",
            "Description": "예시 정수형 feature"
        },
        {
            "FeatureName": "example_fractional_feature",
            "FeatureType": "Fractional",
            "Description": "예시 실수형 feature"
        },
        {
            "FeatureName": "example_list_feature",
            "FeatureType": "String",
            "CollectionType": "List",
            "CollectionConfig": {
                "VectorConfig": {
                    "Dimension": 128
                }
            },
            "Description": "예시 벡터 리스트 feature"
        }
    ]
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(template, f, indent=2, ensure_ascii=False)
        
        click.echo(f"✅ Feature definition 템플릿이 생성되었습니다: {output_file}")
        click.echo("\n📝 템플릿 사용법:")
        click.echo("  1. 템플릿 파일을 편집하여 원하는 feature들을 정의하세요")
        click.echo("  2. 'fs add-features <feature-group-name> <template-file>' 명령으로 비교하세요")
        click.echo("\n📖 FeatureType 옵션:")
        click.echo("  - String: 문자열 데이터")
        click.echo("  - Integral: 정수형 데이터") 
        click.echo("  - Fractional: 실수형 데이터")
        click.echo("\n📖 CollectionType 옵션:")
        click.echo("  - List: 벡터나 배열 형태의 데이터")
        click.echo("  - Set: 집합 형태의 데이터")
        
    except Exception as e:
        click.echo(f"템플릿 파일 생성 오류: {e}", err=True)
        raise click.Abort()