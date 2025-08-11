"""Add features command implementation"""

import click
import json
import time
from typing import List, Dict, Any, Optional, Tuple
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


def add_features_from_flags(feature_group_name: str, features: List[str], 
                           dry_run: bool = False, wait: bool = True) -> None:
    """CLI 플래그로 전달된 feature 정의를 사용하여 Feature Group에 새로운 feature들을 추가합니다.
    
    Args:
        feature_group_name: 대상 feature group 이름
        features: feature 정의 문자열 리스트 (name:type:description 형식)
        dry_run: 실제 수행하지 않고 미리보기만 진행
        wait: 업데이트 완료까지 대기 여부
    """
    try:
        click.echo("🚀 CLI 플래그 기반 Feature 추가 프로세스 시작...")
        
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
        
        # CLI 플래그에서 feature 정의 파싱
        new_features_data = []
        parsing_errors = []
        
        for feature_def in features:
            parsed_feature, error = _parse_feature_definition(feature_def)
            if error:
                parsing_errors.append(f"'{feature_def}': {error}")
            elif parsed_feature:
                new_features_data.append(parsed_feature)
        
        if parsing_errors:
            click.echo("❌ Feature 정의 파싱 오류:", err=True)
            for error in parsing_errors:
                click.echo(f"  - {error}", err=True)
            return
        
        if not new_features_data:
            click.echo("❌ 유효한 feature 정의가 없습니다.", err=True)
            return
        
        # 벡터 feature가 있는 경우 Iceberg 테이블 여부 확인
        vector_features = [f for f in new_features_data if f.get('CollectionType') == 'List']
        if vector_features:
            offline_store_config = fg_details.get('OfflineStoreConfig', {})
            table_format = offline_store_config.get('TableFormat')
            
            if table_format != 'Iceberg':
                vector_names = [f['FeatureName'] for f in vector_features]
                click.echo(f"❌ 벡터(List) 타입 feature는 Iceberg 테이블 형식에서만 지원됩니다.", err=True)
                click.echo(f"   벡터 features: {', '.join(vector_names)}", err=True)
                click.echo(f"   현재 테이블 형식: {table_format or 'Glue'}", err=True)
                click.echo("   해결방법: Feature Group을 Iceberg 형식으로 생성하거나 벡터 feature를 제거하세요.", err=True)
                return
        
        # 추가할 feature들만 필터링 (기존에 없는 것들)
        features_to_add = []
        duplicates = []
        
        for new_feature in new_features_data:
            feature_name = new_feature.get('FeatureName')
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
            collection_info = ""
            if feature.get('CollectionType'):
                collection_info = f" [{feature['CollectionType']}]"
                if feature.get('CollectionConfig', {}).get('VectorConfig', {}).get('Dimension'):
                    collection_info += f"({feature['CollectionConfig']['VectorConfig']['Dimension']}D)"
            click.echo(f"  + {feature['FeatureName']} ({feature['FeatureType']}{collection_info})")
            if feature.get('Description'):
                click.echo(f"    설명: {feature['Description']}")
        
        if dry_run:
            click.echo("🔍 [DRY RUN] 실제 추가하지 않고 미리보기만 실행합니다.")
            return
        
        # Feature 추가 실행
        try:
            click.echo("📝 Feature 추가 중...")
            
            # AWS API는 Description을 지원하지 않으므로 제거
            api_features = []
            for feature in features_to_add:
                api_feature = {k: v for k, v in feature.items() if k != 'Description'}
                api_features.append(api_feature)
            
            response = sagemaker_client.update_feature_group(
                FeatureGroupName=feature_group_name,
                FeatureAdditions=api_features
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


def _parse_feature_definition(feature_def: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Feature 정의 문자열을 파싱합니다.
    
    지원하는 형식:
    1. 기본: name:type[:description]
    2. 벡터: name:type:list:dimension[:description] 
    3. 집합: name:type:set[:description]
    
    Args:
        feature_def: Feature 정의 문자열
        
    Returns:
        (parsed_feature_dict, error_message)
    """
    try:
        parts = feature_def.split(':')
        
        if len(parts) < 2:
            return None, "최소 'name:type' 형식이 필요합니다"
        
        feature_name = parts[0].strip()
        feature_type = parts[1].strip()
        
        # Feature 이름 검증
        if not feature_name:
            return None, "Feature 이름이 비어있습니다"
        
        if not feature_name.replace('_', '').replace('-', '').isalnum():
            return None, "Feature 이름에는 영문, 숫자, 언더스코어, 하이픈만 사용할 수 있습니다"
        
        # Feature 타입 검증
        valid_types = ['String', 'Integral', 'Fractional', 'string', 'integral', 'fractional']
        if feature_type not in valid_types:
            return None, f"유효하지 않은 타입 '{feature_type}'. 지원 타입: String, Integral, Fractional"
        
        # 타입 정규화
        feature_type = feature_type.capitalize()
        
        feature_dict = {
            'FeatureName': feature_name,
            'FeatureType': feature_type
        }
        
        # 추가 옵션 파싱
        if len(parts) >= 3:
            collection_or_desc = parts[2].strip().lower()
            
            # Collection Type 처리
            if collection_or_desc == 'list':
                feature_dict['CollectionType'] = 'List'
                
                # Dimension 처리
                if len(parts) >= 4:
                    try:
                        dimension = int(parts[3].strip())
                        if dimension <= 0:
                            return None, "Dimension은 양수여야 합니다"
                        if dimension > 10000:
                            return None, "Dimension은 10000 이하여야 합니다"
                        
                        feature_dict['CollectionConfig'] = {
                            'VectorConfig': {
                                'Dimension': dimension
                            }
                        }
                        
                        # Description 처리 (5번째 인덱스)
                        if len(parts) >= 5:
                            description = ':'.join(parts[4:]).strip()
                            if description:
                                feature_dict['Description'] = description
                    except ValueError:
                        return None, f"잘못된 dimension 값: '{parts[3]}'. 숫자여야 합니다"
                else:
                    return None, "List 타입에는 dimension이 필요합니다 (예: name:String:list:128)"
                    
            elif collection_or_desc == 'set':
                feature_dict['CollectionType'] = 'Set'
                
                # Description 처리 (4번째 인덱스부터)
                if len(parts) >= 4:
                    description = ':'.join(parts[3:]).strip()
                    if description:
                        feature_dict['Description'] = description
                        
            else:
                # Collection type이 아니면 description으로 처리
                description = ':'.join(parts[2:]).strip()
                if description:
                    feature_dict['Description'] = description
        
        return feature_dict, None
        
    except Exception as e:
        return None, f"파싱 오류: {str(e)}"


def parse_json_features(json_strings: List[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """JSON 문자열 형태의 feature 정의들을 파싱합니다.
    
    Args:
        json_strings: JSON 형태의 feature 정의 문자열 리스트
        
    Returns:
        (parsed_features, error_messages)
    """
    parsed_features = []
    errors = []
    
    for i, json_str in enumerate(json_strings):
        try:
            # JSON 파싱 시도
            feature_dict = json.loads(json_str)
            
            # 필수 필드 검증
            if not isinstance(feature_dict, dict):
                errors.append(f"Feature #{i+1}: JSON 객체여야 합니다")
                continue
                
            if 'FeatureName' not in feature_dict:
                errors.append(f"Feature #{i+1}: FeatureName 필드가 필요합니다")
                continue
                
            if 'FeatureType' not in feature_dict:
                errors.append(f"Feature #{i+1}: FeatureType 필드가 필요합니다")
                continue
            
            # 타입 검증
            valid_types = ['String', 'Integral', 'Fractional']
            if feature_dict['FeatureType'] not in valid_types:
                errors.append(f"Feature #{i+1}: 유효하지 않은 FeatureType '{feature_dict['FeatureType']}'")
                continue
            
            # Collection 설정 검증
            if 'CollectionType' in feature_dict:
                collection_type = feature_dict['CollectionType']
                if collection_type not in ['List', 'Set']:
                    errors.append(f"Feature #{i+1}: 유효하지 않은 CollectionType '{collection_type}'")
                    continue
                
                if collection_type == 'List':
                    if 'CollectionConfig' not in feature_dict:
                        errors.append(f"Feature #{i+1}: List 타입에는 CollectionConfig가 필요합니다")
                        continue
                    
                    vector_config = feature_dict.get('CollectionConfig', {}).get('VectorConfig', {})
                    dimension = vector_config.get('Dimension')
                    
                    if not dimension or not isinstance(dimension, int) or dimension <= 0:
                        errors.append(f"Feature #{i+1}: List 타입에는 유효한 Dimension이 필요합니다")
                        continue
            
            parsed_features.append(feature_dict)
            
        except json.JSONDecodeError as e:
            errors.append(f"Feature #{i+1}: JSON 파싱 오류 - {str(e)}")
        except Exception as e:
            errors.append(f"Feature #{i+1}: 처리 오류 - {str(e)}")
    
    return parsed_features, errors


def add_features_from_json_strings(feature_group_name: str, json_features: List[str],
                                  dry_run: bool = False, wait: bool = True) -> None:
    """JSON 문자열로 전달된 feature 정의를 사용하여 Feature Group에 새로운 feature들을 추가합니다.
    
    Args:
        feature_group_name: 대상 feature group 이름
        json_features: JSON 형태의 feature 정의 문자열 리스트
        dry_run: 실제 수행하지 않고 미리보기만 진행
        wait: 업데이트 완료까지 대기 여부
    """
    try:
        click.echo("🚀 JSON 문자열 기반 Feature 추가 프로세스 시작...")
        
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
        
        # JSON 문자열에서 feature 정의 파싱
        new_features_data, parsing_errors = parse_json_features(json_features)
        
        if parsing_errors:
            click.echo("❌ Feature 정의 파싱 오류:", err=True)
            for error in parsing_errors:
                click.echo(f"  - {error}", err=True)
            return
        
        if not new_features_data:
            click.echo("❌ 유효한 feature 정의가 없습니다.", err=True)
            return
        
        # 벡터 feature가 있는 경우 Iceberg 테이블 여부 확인
        vector_features = [f for f in new_features_data if f.get('CollectionType') == 'List']
        if vector_features:
            offline_store_config = fg_details.get('OfflineStoreConfig', {})
            table_format = offline_store_config.get('TableFormat')
            
            if table_format != 'Iceberg':
                vector_names = [f['FeatureName'] for f in vector_features]
                click.echo(f"❌ 벡터(List) 타입 feature는 Iceberg 테이블 형식에서만 지원됩니다.", err=True)
                click.echo(f"   벡터 features: {', '.join(vector_names)}", err=True)
                click.echo(f"   현재 테이블 형식: {table_format or 'Glue'}", err=True)
                click.echo("   해결방법: Feature Group을 Iceberg 형식으로 생성하거나 벡터 feature를 제거하세요.", err=True)
                return
        
        # 추가할 feature들만 필터링 (기존에 없는 것들)
        features_to_add = []
        duplicates = []
        
        for new_feature in new_features_data:
            feature_name = new_feature.get('FeatureName')
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
            collection_info = ""
            if feature.get('CollectionType'):
                collection_info = f" [{feature['CollectionType']}]"
                if feature.get('CollectionConfig', {}).get('VectorConfig', {}).get('Dimension'):
                    collection_info += f"({feature['CollectionConfig']['VectorConfig']['Dimension']}D)"
            click.echo(f"  + {feature['FeatureName']} ({feature['FeatureType']}{collection_info})")
            if feature.get('Description'):
                click.echo(f"    설명: {feature['Description']}")
        
        if dry_run:
            click.echo("🔍 [DRY RUN] 실제 추가하지 않고 미리보기만 실행합니다.")
            return
        
        # Feature 추가 실행
        try:
            click.echo("📝 Feature 추가 중...")
            
            # AWS API는 Description을 지원하지 않으므로 제거
            api_features = []
            for feature in features_to_add:
                api_feature = {k: v for k, v in feature.items() if k != 'Description'}
                api_features.append(api_feature)
            
            response = sagemaker_client.update_feature_group(
                FeatureGroupName=feature_group_name,
                FeatureAdditions=api_features
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