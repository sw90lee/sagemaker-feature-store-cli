"""
Feature Store 생성 명령어 구현
"""
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import boto3
import click
from tqdm import tqdm



@click.command()
@click.argument('feature_group_name')
@click.option('--description', type=str, help='Feature group 설명')
@click.option('--record-identifier-name', default='id', help='레코드 식별자 필드명 (기본값: id)')
@click.option('--event-time-feature-name', default='event_time', help='이벤트 시간 필드명 (기본값: event_time)')
@click.option('--schema-file', type=click.Path(exists=True), required=True, help='스키마 정의 JSON 파일 경로 (필수)')
@click.option('--online-store/--no-online-store', default=True, help='Online store 활성화 여부 (기본값: True)')
@click.option('--ttl-duration', type=int, help='Online store TTL 기간 (단위: 일, 1-365일)')
@click.option('--offline-store/--no-offline-store', default=True, help='Offline store 활성화 여부 (기본값: True)')
@click.option('--s3-uri', type=str, help='Offline store S3 URI (offline store 사용시 필수)')
@click.option('--role-arn', type=str, required=True, help='IAM 역할 ARN (필수)')
@click.option('--enable-encryption/--no-encryption', default=False, help='암호화 활성화 여부 (기본값: False)')
@click.option('--kms-key-id', type=str, help='KMS 키 ID (암호화 사용시)')
@click.option('--table-format', type=click.Choice(['Iceberg', 'Glue']), default='Glue', help='테이블 형식 (기본값: Glue)')
@click.option('--throughput-mode', type=click.Choice(['OnDemand', 'Provisioned']), default='OnDemand', help='처리량 모드 (기본값: OnDemand)')
@click.option('--read-capacity-units', type=int, help='읽기 용량 단위 (Provisioned 모드에서만)')
@click.option('--write-capacity-units', type=int, help='쓰기 용량 단위 (Provisioned 모드에서만)')
@click.option('--tags', multiple=True, help='태그 (key=value 형식, 다중 지정 가능)')
@click.option('--wait/--no-wait', default=True, help='생성 완료까지 대기 여부 (기본값: True)')
def create(
    feature_group_name: str,
    description: Optional[str],
    record_identifier_name: str,
    event_time_feature_name: str,
    schema_file: str,
    online_store: bool,
    ttl_duration: Optional[int],
    offline_store: bool,
    s3_uri: Optional[str],
    role_arn: str,
    enable_encryption: bool,
    kms_key_id: Optional[str],
    table_format: str,
    throughput_mode: str,
    read_capacity_units: Optional[int],
    write_capacity_units: Optional[int],
    tags: Tuple[str, ...],
    wait: bool
):
    """Feature Group을 생성합니다.
    
    FEATURE_GROUP_NAME: 생성할 Feature Group의 이름
    
    \b
    예시:
      # 기본 생성 (online + offline)
      fs create my-feature-group \\
        --schema-file schema.json \\
        --role-arn arn:aws:iam::123456789012:role/SageMakerRole \\
        --s3-uri s3://my-bucket/feature-store/
      
      # Online store만 생성
      fs create my-online-feature-group \\
        --schema-file schema.json \\
        --role-arn arn:aws:iam::123456789012:role/SageMakerRole \\
        --no-offline-store
      
      # 고급 설정으로 생성
      fs create my-advanced-feature-group \\
        --schema-file schema.json \\
        --role-arn arn:aws:iam::123456789012:role/SageMakerRole \\
        --s3-uri s3://my-bucket/feature-store/ \\
        --description "고객 프로필 피처 그룹" \\
        --record-identifier-name customer_id \\
        --event-time-feature-name timestamp \\
        --ttl-duration 365 \\
        --enable-encryption \\
        --kms-key-id alias/sagemaker-key \\
        --table-format Glue \\
        --throughput-mode Provisioned \\
        --read-capacity-units 5 \\
        --write-capacity-units 5 \\
        --tags environment=production \\
        --tags team=ml
    """
    try:
        click.echo("🚀 Feature Group 생성 시작...")
        
        # 설정 검증
        _validate_configuration(online_store, offline_store, s3_uri, throughput_mode, 
                              read_capacity_units, write_capacity_units, enable_encryption, kms_key_id, ttl_duration)
        
        # 스키마 로드 및 검증
        schema_data = _load_and_validate_schema(schema_file)
        click.echo(f"✓ 스키마 검증 완료 ({len(schema_data)}개 필드)")
        
        # IAM 역할 검증
        _validate_iam_role(role_arn)
        click.echo("✓ IAM 역할 검증 완료")
        
        # 태그 파싱
        parsed_tags = _parse_tags(tags)
        
        # Feature Group 생성
        sagemaker_client = boto3.client('sagemaker')
        
        config = _create_feature_group_config(
            feature_group_name=feature_group_name,
            description=description,
            record_identifier_name=record_identifier_name,
            event_time_feature_name=event_time_feature_name,
            schema_data=schema_data,
            online_store=online_store,
            offline_store=offline_store,
            s3_uri=s3_uri,
            role_arn=role_arn,
            enable_encryption=enable_encryption,
            kms_key_id=kms_key_id,
            table_format=table_format,
            throughput_mode=throughput_mode,
            read_capacity_units=read_capacity_units,
            write_capacity_units=write_capacity_units,
            ttl_duration=ttl_duration,
            tags=parsed_tags
        )
        
        click.echo(f"✓ Feature Group 생성 시작: {feature_group_name}")
        
        response = sagemaker_client.create_feature_group(**config)
        
        if wait:
            _wait_for_creation(sagemaker_client, feature_group_name)
        else:
            click.echo("⏳ Feature Group 생성이 백그라운드에서 진행됩니다.")
        
        # 생성된 Feature Group 정보 표시
        _display_feature_group_info(sagemaker_client, feature_group_name)
        
        click.echo("✅ Feature Group 생성이 완료되었습니다!")
        
    except Exception as e:
        raise click.ClickException(f"Feature Group 생성 중 오류 발생: {str(e)}")


def _validate_configuration(
    online_store: bool,
    offline_store: bool,
    s3_uri: Optional[str],
    throughput_mode: str,
    read_capacity_units: Optional[int],
    write_capacity_units: Optional[int],
    enable_encryption: bool,
    kms_key_id: Optional[str],
    ttl_duration: Optional[int]
):
    """설정값 검증"""
    # 최소 하나의 store는 활성화되어야 함
    if not online_store and not offline_store:
        raise click.ClickException("Online store 또는 Offline store 중 최소 하나는 활성화되어야 합니다.")
    
    # Offline store 사용시 S3 URI 필수
    if offline_store and not s3_uri:
        raise click.ClickException("Offline store를 사용할 경우 --s3-uri를 지정해야 합니다.")
    
    # Provisioned 모드시 capacity units 필수
    if throughput_mode == 'Provisioned':
        if not read_capacity_units or not write_capacity_units:
            raise click.ClickException("Provisioned 모드에서는 --read-capacity-units과 --write-capacity-units를 지정해야 합니다.")
        
        if read_capacity_units < 1 or write_capacity_units < 1:
            raise click.ClickException("Capacity units는 1 이상이어야 합니다.")
    
    # 암호화 사용시 KMS 키 확인
    if enable_encryption and not kms_key_id:
        raise click.ClickException("암호화를 활성화할 경우 --kms-key-id를 지정해야 합니다.")
    
    # TTL 검증
    if ttl_duration is not None:
        if not online_store:
            raise click.ClickException("TTL은 Online store가 활성화된 경우에만 설정할 수 있습니다.")
        if ttl_duration < 1 or ttl_duration > 365:
            raise click.ClickException("TTL 기간은 1-365일 사이여야 합니다.")


def _load_and_validate_schema(schema_file: str) -> List[Dict[str, str]]:
    """스키마 파일 로드 및 검증"""
    try:
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
    except json.JSONDecodeError as e:
        raise click.ClickException(f"스키마 파일 JSON 형식 오류: {str(e)}")
    except Exception as e:
        raise click.ClickException(f"스키마 파일 읽기 오류: {str(e)}")
    
    if not isinstance(schema_data, list):
        raise click.ClickException("스키마는 배열 형태여야 합니다.")
    
    if not schema_data:
        raise click.ClickException("스키마에 최소 하나의 필드가 있어야 합니다.")
    
    valid_types = {'String', 'Integral', 'Fractional'}
    
    for idx, field in enumerate(schema_data):
        if not isinstance(field, dict):
            raise click.ClickException(f"스키마 필드 {idx + 1}: 딕셔너리 형태여야 합니다.")
        
        if 'FeatureName' not in field:
            raise click.ClickException(f"스키마 필드 {idx + 1}: 'FeatureName'이 필요합니다.")
        
        if 'FeatureType' not in field:
            raise click.ClickException(f"스키마 필드 {idx + 1}: 'FeatureType'이 필요합니다.")
        
        if field['FeatureType'] not in valid_types:
            raise click.ClickException(f"스키마 필드 '{field['FeatureName']}': 지원되지 않는 타입 '{field['FeatureType']}'. 지원 타입: {', '.join(valid_types)}")
    
    return schema_data


def _validate_iam_role(role_arn: str):
    """IAM 역할 검증"""
    try:
        iam_client = boto3.client('iam')
        
        # ARN에서 역할 이름 추출
        role_name = role_arn.split('/')[-1]
        
        # 역할 존재 확인
        iam_client.get_role(RoleName=role_name)
        
    except iam_client.exceptions.NoSuchEntityException:
        raise click.ClickException(f"IAM 역할을 찾을 수 없습니다: {role_arn}")
    except Exception as e:
        raise click.ClickException(f"IAM 역할 검증 중 오류: {str(e)}")


def _parse_tags(tags: Tuple[str, ...]) -> List[Dict[str, str]]:
    """태그 문자열 파싱"""
    parsed_tags = []
    
    for tag in tags:
        if '=' not in tag:
            raise click.ClickException(f"잘못된 태그 형식: '{tag}'. 'key=value' 형식이어야 합니다.")
        
        key, value = tag.split('=', 1)
        parsed_tags.append({'Key': key.strip(), 'Value': value.strip()})
    
    return parsed_tags


def _create_feature_group_config(
    feature_group_name: str,
    description: Optional[str],
    record_identifier_name: str,
    event_time_feature_name: str,
    schema_data: List[Dict[str, str]],
    online_store: bool,
    offline_store: bool,
    s3_uri: Optional[str],
    role_arn: str,
    enable_encryption: bool,
    kms_key_id: Optional[str],
    table_format: str,
    throughput_mode: str,
    read_capacity_units: Optional[int],
    write_capacity_units: Optional[int],
    ttl_duration: Optional[int],
    tags: List[Dict[str, str]]
) -> Dict[str, Any]:
    """Feature Group 생성 설정 구성"""
    
    config = {
        'FeatureGroupName': feature_group_name,
        'RecordIdentifierFeatureName': record_identifier_name,
        'EventTimeFeatureName': event_time_feature_name,
        'FeatureDefinitions': schema_data,
        'RoleArn': role_arn
    }
    
    if description:
        config['Description'] = description
    
    # Online Store 설정
    if online_store:
        online_store_config = {
            'EnableOnlineStore': True
        }
        
        if enable_encryption and kms_key_id:
            online_store_config['SecurityConfig'] = {
                'KmsKeyId': kms_key_id
            }
        
        # TTL 설정
        if ttl_duration is not None:
            online_store_config['TtlDuration'] = {
                'Unit': 'Days',
                'Value': ttl_duration
            }
        
        config['OnlineStoreConfig'] = online_store_config
    
    # Offline Store 설정
    if offline_store:
        offline_store_config = {
            'S3StorageConfig': {
                'S3Uri': s3_uri
            },
            'TableFormat': table_format
        }
        
        if enable_encryption and kms_key_id:
            offline_store_config['S3StorageConfig']['KmsKeyId'] = kms_key_id
        
        config['OfflineStoreConfig'] = offline_store_config
    
    # 태그 추가
    if tags:
        config['Tags'] = tags
    
    return config


def _wait_for_creation(sagemaker_client: boto3.client, feature_group_name: str):
    """Feature Group 생성 완료까지 대기"""
    click.echo("⏳ Feature Group 생성 진행 상황 모니터링...")
    
    with tqdm(desc="생성 중", unit="초") as pbar:
        while True:
            try:
                response = sagemaker_client.describe_feature_group(
                    FeatureGroupName=feature_group_name
                )
                
                status = response['FeatureGroupStatus']
                pbar.set_description(f"상태: {status}")
                
                if status == 'Created':
                    pbar.set_description("완료")
                    click.echo("✓ Feature Group 생성 완료!")
                    break
                elif status == 'CreateFailed':
                    failure_reason = response.get('FailureReason', '알 수 없는 오류')
                    raise click.ClickException(f"Feature Group 생성 실패: {failure_reason}")
                elif status in ['Creating']:
                    time.sleep(10)
                    pbar.update(10)
                else:
                    raise click.ClickException(f"예상치 못한 상태: {status}")
                    
            except sagemaker_client.exceptions.ResourceNotFound:
                time.sleep(5)
                pbar.update(5)
            except Exception as e:
                if "ResourceNotFound" in str(e):
                    time.sleep(5)
                    pbar.update(5)
                else:
                    raise


def _display_feature_group_info(sagemaker_client: boto3.client, feature_group_name: str):
    """생성된 Feature Group 정보 표시"""
    try:
        response = sagemaker_client.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        
        click.echo("\n📋 Feature Group 정보:")
        click.echo(f"  이름: {response['FeatureGroupName']}")
        click.echo(f"  상태: {response['FeatureGroupStatus']}")
        
        # Online Store 정보
        online_config = response.get('OnlineStoreConfig')
        if online_config:
            click.echo("  Online Store: 활성화됨")
            throughput_mode = online_config.get('ThroughputConfig', {}).get('ThroughputMode', 'N/A')
            click.echo(f"    처리량 모드: {throughput_mode}")
        else:
            click.echo("  Online Store: 비활성화됨")
        
        # Offline Store 정보
        offline_config = response.get('OfflineStoreConfig')
        if offline_config:
            s3_uri = offline_config.get('S3StorageConfig', {}).get('S3Uri', 'N/A')
            table_format = offline_config.get('TableFormat', 'N/A')
            click.echo(f"  Offline Store: 활성화됨 ({s3_uri})")
            click.echo(f"    테이블 형식: {table_format}")
        else:
            click.echo("  Offline Store: 비활성화됨")
        
        # 생성 시간
        creation_time = response.get('CreationTime')
        if creation_time:
            formatted_time = creation_time.strftime('%Y-%m-%d %H:%M:%S KST')
            click.echo(f"  생성 시간: {formatted_time}")
        
        # 설명
        description = response.get('Description')
        if description:
            click.echo(f"  설명: {description}")
            
    except Exception as e:
        click.echo(f"⚠️  정보 표시 중 오류: {str(e)}")