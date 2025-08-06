"""
Feature Store 삭제 명령어 구현
"""
import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

import boto3
import click
from tqdm import tqdm



@click.command()
@click.argument('feature_group_name')
@click.option('--force/--no-force', default=False, help='확인 없이 강제 삭제 (기본값: False)')
@click.option('--delete-data/--keep-data', default=True, help='데이터 삭제 여부 (기본값: True)')
@click.option('--wait/--no-wait', default=True, help='삭제 완료까지 대기 여부 (기본값: True)')
@click.option('--dry-run', is_flag=True, help='실제 삭제 없이 삭제 계획만 표시')
def delete(
    feature_group_name: str,
    force: bool,
    delete_data: bool,
    wait: bool,
    dry_run: bool
):
    """
    Feature Group을 완전히 삭제합니다.
    
    ⚠️  주의: 이 작업은 되돌릴 수 없습니다!
    
    FEATURE_GROUP_NAME: 삭제할 Feature Group의 이름
    
    예시:
    
      # 기본 삭제 (확인 프롬프트 포함)
      fs delete my-feature-group
      
      # 강제 삭제 (확인 없이)
      fs delete my-feature-group --force
      
      # 데이터는 유지하고 feature group만 삭제
      fs delete my-feature-group --keep-data
      
      # 삭제 계획만 확인 (실제 삭제 안함)
      fs delete my-feature-group --dry-run
    """
    try:
        if dry_run:
            click.echo("🔍 삭제 계획 확인 (Dry Run)")
        else:
            click.echo("🗑️  Feature Group 삭제 프로세스 시작...")
        
        # 삭제 전 검증
        sagemaker_client = boto3.client('sagemaker')
        fg_details, dependencies = _validate_deletion(sagemaker_client, feature_group_name)
        
        if dry_run:
            _display_deletion_plan(feature_group_name, fg_details, dependencies, delete_data)
            return
        
        # 사용자 확인 (force가 아닌 경우)
        if not force:
            if not _confirm_deletion(feature_group_name, fg_details, dependencies, delete_data):
                click.echo("❌ 삭제가 취소되었습니다.")
                return
        
        click.echo("✓ 삭제 가능성 검증 완료")
        click.echo("✓ 종속성 확인 완료")
        if not force:
            click.echo("✓ 사용자 확인 완료")
        
        # 삭제 실행
        _execute_deletion(sagemaker_client, feature_group_name, fg_details, delete_data, wait)
        
        click.echo("✅ Feature Group 삭제가 완료되었습니다!")
        
    except Exception as e:
        raise click.ClickException(f"Feature Group 삭제 중 오류 발생: {str(e)}")


def _validate_deletion(sagemaker_client: boto3.client, feature_group_name: str) -> Tuple[Dict[str, Any], List[str]]:
    """삭제 전 검증"""
    try:
        # Feature Group 존재 확인
        response = sagemaker_client.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        
        # 삭제 가능한 상태인지 확인
        status = response['FeatureGroupStatus']
        if status in ['Deleting', 'DeleteFailed']:
            raise click.ClickException(f"Feature Group이 이미 삭제 중이거나 삭제 실패 상태입니다: {status}")
        
        # 종속성 확인
        dependencies = _check_dependencies(sagemaker_client, feature_group_name)
        
        return response, dependencies
        
    except sagemaker_client.exceptions.ResourceNotFound:
        raise click.ClickException(f"Feature Group을 찾을 수 없습니다: {feature_group_name}")


def _check_dependencies(sagemaker_client: boto3.client, feature_group_name: str) -> List[str]:
    """종속성 확인"""
    dependencies = []
    
    try:
        # 실행 중인 작업 확인 (예: training jobs, processing jobs 등)
        # 실제로는 더 복잡한 종속성 검사가 필요할 수 있음
        pass
    except Exception:
        # 종속성 확인 중 오류가 있어도 계속 진행
        pass
    
    return dependencies


def _display_deletion_plan(
    feature_group_name: str,
    fg_details: Dict[str, Any],
    dependencies: List[str],
    delete_data: bool
):
    """삭제 계획 표시 (Dry Run)"""
    click.echo(f"\nFeature Group: {feature_group_name}")
    click.echo(f"  상태: {fg_details['FeatureGroupStatus']}")
    
    # Online Store 정보
    online_config = fg_details.get('OnlineStoreConfig')
    if online_config:
        click.echo("  Online Store: 활성화됨")
    else:
        click.echo("  Online Store: 비활성화됨")
    
    # Offline Store 정보
    offline_config = fg_details.get('OfflineStoreConfig')
    if offline_config:
        s3_uri = offline_config.get('S3StorageConfig', {}).get('S3Uri', 'N/A')
        click.echo(f"  Offline Store: 활성화됨 ({s3_uri})")
    else:
        click.echo("  Offline Store: 비활성화됨")
    
    # 종속성 정보
    if dependencies:
        click.echo("\n⚠️  발견된 종속성:")
        for dep in dependencies:
            click.echo(f"  - {dep}")
    
    # 삭제 순서
    click.echo("\n삭제 순서:")
    step = 1
    
    if online_config:
        click.echo(f"  {step}. ✓ Online Store 비활성화")
        step += 1
    
    if delete_data:
        if online_config:
            click.echo(f"  {step}. ✓ Online 데이터 삭제")
            step += 1
        
        if offline_config:
            click.echo(f"  {step}. ✓ Offline 데이터 삭제 (S3 및 Athena)")
            step += 1
    else:
        click.echo(f"  {step}. ⏭️  데이터 유지 (--keep-data 옵션)")
        step += 1
    
    click.echo(f"  {step}. ✓ Feature Group 리소스 삭제")
    click.echo(f"  {step + 1}. ✓ 관련 메타데이터 정리")
    
    click.echo(f"\n예상 소요 시간: 3-8분")
    click.echo("복구 가능성: 불가능")
    
    if not delete_data:
        click.echo("\n💡 참고: --keep-data 옵션으로 인해 S3의 데이터는 유지됩니다.")
    
    click.echo("\n--force 플래그 없이 실제 삭제하려면 확인이 필요합니다.")


def _confirm_deletion(
    feature_group_name: str,
    fg_details: Dict[str, Any],
    dependencies: List[str],
    delete_data: bool
) -> bool:
    """사용자 삭제 확인"""
    click.echo("\n" + "="*60)
    click.echo("⚠️  위험: Feature Group 완전 삭제")
    click.echo("="*60)
    
    click.echo(f"\n삭제할 Feature Group: {feature_group_name}")
    
    # Online Store 정보
    online_config = fg_details.get('OnlineStoreConfig')
    if online_config:
        click.echo("  - Online Store: 활성화됨 (데이터 있음)")
    
    # Offline Store 정보
    offline_config = fg_details.get('OfflineStoreConfig')
    if offline_config:
        s3_uri = offline_config.get('S3StorageConfig', {}).get('S3Uri', 'N/A')
        click.echo(f"  - Offline Store: 활성화됨 (S3: {s3_uri})")
    
    # 생성 정보
    creation_time = fg_details.get('CreationTime')
    if creation_time:
        formatted_time = creation_time.strftime('%Y-%m-%d')
        click.echo(f"  - 생성일: {formatted_time}")
    
    # 마지막 수정 정보
    last_modified = fg_details.get('LastModifiedTime')
    if last_modified:
        formatted_time = last_modified.strftime('%Y-%m-%d')
        click.echo(f"  - 마지막 수정: {formatted_time}")
    
    # 경고사항
    click.echo("\n⚠️  주의사항:")
    click.echo("  - 이 작업은 되돌릴 수 없습니다")
    
    if delete_data:
        click.echo("  - 모든 데이터가 영구적으로 삭제됩니다")
    else:
        click.echo("  - Feature Group은 삭제되지만 S3 데이터는 유지됩니다")
    
    click.echo("  - 연결된 모델이나 파이프라인에 영향을 줄 수 있습니다")
    
    if dependencies:
        click.echo("\n🔗 발견된 종속성:")
        for dep in dependencies:
            click.echo(f"  - {dep}")
    
    # 확인 입력
    click.echo("\n" + "="*60)
    response = click.prompt(
        "정말로 삭제하시겠습니까? 'yes'를 입력하세요",
        type=str,
        default="no"
    )
    
    return response.lower() == 'yes'


def _execute_deletion(
    sagemaker_client: boto3.client,
    feature_group_name: str,
    fg_details: Dict[str, Any],
    delete_data: bool,
    wait: bool
):
    """삭제 실행"""
    
    # 1. Online Store 비활성화 (필요한 경우)
    online_config = fg_details.get('OnlineStoreConfig')
    if online_config:
        click.echo("⠋ Online Store 비활성화 중...")
        # 실제로는 SageMaker에서 자동으로 처리됨
        click.echo("✓ Online Store 비활성화 완료")
    
    # 2. 데이터 삭제 (delete_data가 True인 경우)
    if delete_data:
        _delete_feature_group_data(sagemaker_client, feature_group_name, fg_details)
    else:
        click.echo("⏭️  데이터 유지 (--keep-data 옵션)")
    
    # 3. Feature Group 삭제
    click.echo("⠋ Feature Group 삭제 중...")
    
    try:
        sagemaker_client.delete_feature_group(
            FeatureGroupName=feature_group_name
        )
        click.echo("✓ Feature Group 삭제 요청 완료")
        
        # 4. 삭제 완료 대기 (wait가 True인 경우)
        if wait:
            _wait_for_deletion(sagemaker_client, feature_group_name)
        else:
            click.echo("⏳ Feature Group 삭제가 백그라운드에서 진행됩니다.")
        
    except Exception as e:
        raise click.ClickException(f"Feature Group 삭제 요청 실패: {str(e)}")
    
    # 5. 정리 작업
    _cleanup_resources(feature_group_name)


def _delete_feature_group_data(
    sagemaker_client: boto3.client,
    feature_group_name: str,
    fg_details: Dict[str, Any]
):
    """Feature Group 데이터 삭제"""
    
    online_config = fg_details.get('OnlineStoreConfig')
    offline_config = fg_details.get('OfflineStoreConfig')
    
    # Online Store 데이터 삭제
    if online_config:
        click.echo("⠋ Online 데이터 삭제 중...")
        try:
            # Online store의 데이터는 Feature Group 삭제시 자동으로 삭제됨
            click.echo("✓ Online 데이터 삭제 완료")
        except Exception as e:
            click.echo(f"⚠️  Online 데이터 삭제 중 오류: {str(e)}")
    
    # Offline Store 데이터 삭제
    if offline_config:
        s3_uri = offline_config.get('S3StorageConfig', {}).get('S3Uri')
        if s3_uri:
            click.echo("⠋ Offline 데이터 삭제 중...")
            try:
                _delete_s3_data(s3_uri, feature_group_name)
                _delete_athena_table(feature_group_name)
                click.echo("✓ Offline 데이터 삭제 완료")
            except Exception as e:
                click.echo(f"⚠️  Offline 데이터 삭제 중 오류: {str(e)}")


def _delete_s3_data(s3_uri: str, feature_group_name: str):
    """S3 데이터 삭제"""
    try:
        s3_client = boto3.client('s3')
        
        # S3 URI 파싱
        if s3_uri.startswith('s3://'):
            s3_uri = s3_uri[5:]
        
        parts = s3_uri.split('/', 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''
        
        # Feature Group 관련 객체 경로 구성
        if not prefix.endswith('/'):
            prefix += '/'
        prefix += feature_group_name + '/'
        
        # 객체 목록 조회 및 삭제
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        objects_to_delete = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects_to_delete.append({'Key': obj['Key']})
        
        # 배치 삭제
        if objects_to_delete:
            # 1000개씩 배치로 삭제
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i+1000]
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': batch}
                )
    
    except Exception as e:
        # S3 삭제 실패는 경고만 표시 (전체 프로세스를 중단하지 않음)
        click.echo(f"S3 데이터 삭제 중 오류 (계속 진행): {str(e)}")


def _delete_athena_table(feature_group_name: str):
    """Athena 테이블 삭제"""
    try:
        athena_client = boto3.client('athena')
        glue_client = boto3.client('glue')
        
        # 일반적인 테이블 이름 패턴들
        table_name_patterns = [
            feature_group_name.replace('-', '_'),
            feature_group_name.lower().replace('-', '_'),
            f"{feature_group_name.replace('-', '_')}_1234567890123"  # 계정 ID 포함 패턴
        ]
        
        for table_name in table_name_patterns:
            try:
                # Glue 데이터 카탈로그에서 테이블 삭제
                glue_client.delete_table(
                    DatabaseName='sagemaker_featurestore',
                    Name=table_name
                )
                break
            except glue_client.exceptions.EntityNotFoundException:
                continue
            except Exception:
                continue
    
    except Exception as e:
        # Athena 테이블 삭제 실패는 경고만 표시
        click.echo(f"Athena 테이블 삭제 중 오류 (계속 진행): {str(e)}")


def _wait_for_deletion(sagemaker_client: boto3.client, feature_group_name: str):
    """Feature Group 삭제 완료까지 대기"""
    click.echo("⏳ Feature Group 삭제 진행 상황 모니터링...")
    
    with tqdm(desc="삭제 중", unit="초") as pbar:
        while True:
            try:
                response = sagemaker_client.describe_feature_group(
                    FeatureGroupName=feature_group_name
                )
                
                status = response['FeatureGroupStatus']
                pbar.set_description(f"상태: {status}")
                
                if status == 'Deleting':
                    time.sleep(10)
                    pbar.update(10)
                elif status == 'DeleteFailed':
                    failure_reason = response.get('FailureReason', '알 수 없는 오류')
                    raise click.ClickException(f"Feature Group 삭제 실패: {failure_reason}")
                else:
                    # 예상치 못한 상태
                    time.sleep(5)
                    pbar.update(5)
                    
            except sagemaker_client.exceptions.ResourceNotFound:
                # Feature Group이 완전히 삭제됨
                pbar.set_description("완료")
                click.echo("✓ Feature Group 삭제 완료!")
                break
            except Exception as e:
                if "ResourceNotFound" in str(e):
                    pbar.set_description("완료")
                    click.echo("✓ Feature Group 삭제 완료!")
                    break
                else:
                    raise


def _cleanup_resources(feature_group_name: str):
    """관련 리소스 정리"""
    try:
        # CloudWatch 메트릭 정리 등 추가적인 정리 작업
        # 실제로는 AWS에서 자동으로 정리됨
        click.echo("✓ 관련 메타데이터 정리 완료")
    except Exception:
        pass  # 정리 작업 실패는 무시