"""Main CLI module for SageMaker FeatureStore Online CLI"""

import click
from typing import Optional
from .config import Config
from .commands import list_cmd, get_cmd, put_cmd, bulk_get_cmd, bulk_put_cmd, clear_cmd, migrate_cmd, create_cmd, delete_cmd, export_cmd, analyze_cmd, add_features_cmd


@click.group()
@click.option('--profile', help='사용할 AWS 프로필')
@click.option('--region', help='사용할 AWS 리전')
@click.pass_context
def cli(ctx, profile: Optional[str], region: Optional[str]):
    """SageMaker FeatureStore CLI - 온라인/오프라인 피처 스토어 관리 도구"""
    ctx.ensure_object(dict)
    ctx.obj['config'] = Config(profile=profile, region=region)


@cli.command('list')
@click.option('--output-format', '-o', type=click.Choice(['table', 'json']), default='table',
              help='출력 형식')
@click.pass_context
def list_feature_groups(ctx, output_format: str):
    """모든 피처 그룹 목록 조회 (온라인/오프라인)
    
    \b
    예시:
      # 테이블 형태로 출력
      fs list
      
      # JSON 형태로 출력
      fs list --output-format json
    """
    config = ctx.obj['config']
    list_cmd.list_feature_groups(config, output_format)


@cli.command('get')
@click.argument('feature_group_name')
@click.argument('record_identifier_value')
@click.option('--feature-names', help='조회할 피처 이름들 (쉼표로 구분)')
@click.option('--output-format', '-o', type=click.Choice(['table', 'json']), default='json',
              help='출력 형식')
@click.pass_context
def get_record(ctx, feature_group_name: str, record_identifier_value: str, 
               feature_names: Optional[str], output_format: str):
    """피처 그룹에서 단일 레코드 조회
    
    \b
    예시:
      # 기본 조회
      fs get my-feature-group record-id-123
      
      # 특정 피처만 조회
      fs get my-feature-group record-id-123 \\
        --feature-names "feature1,feature2,feature3"
      
      # 테이블 형태로 출력
      fs get my-feature-group record-id-123 --output-format table
    """
    config = ctx.obj['config']
    feature_list = feature_names.split(',') if feature_names else None
    get_cmd.get_record(config, feature_group_name, record_identifier_value, feature_list, output_format)


@cli.command('put')
@click.argument('feature_group_name')
@click.option('--record', required=True, help='저장할 레코드의 JSON 문자열')
@click.pass_context
def put_record(ctx, feature_group_name: str, record: str):
    """피처 그룹에 단일 레코드 저장
    
    \b
    예시:
      # 단일 레코드 저장
      fs put my-feature-group \\
        --record '{"feature1": "value1", "feature2": "value2", "record_id": "123"}'
    """
    config = ctx.obj['config']
    put_cmd.put_record(config, feature_group_name, record)


@cli.command('bulk-get')
@click.argument('feature_group_name')
@click.argument('input_file')
@click.option('--output-file', '-o', help='출력 파일 경로')
@click.option('--feature-names', help='조회할 피처 이름들 (쉼표로 구분)')
@click.option('--current-time', '-c', is_flag=True, help='조회된 결과의 Time 필드를 현재 시간으로 교체')
@click.pass_context
def bulk_get_records(ctx, feature_group_name: str, input_file: str, 
                    output_file: Optional[str], feature_names: Optional[str], current_time: bool):
    """입력 파일(JSON/CSV)을 사용하여 피처 그룹에서 대량 레코드 조회
    
    \b
    예시:
      # JSON 파일에서 레코드 ID 목록을 읽어 조회
      fs bulk-get my-feature-group input_ids.json
      
      # 결과를 파일로 저장
      fs bulk-get my-feature-group input_ids.json \\
        --output-file results.json
      
      # CSV 파일 사용
      fs bulk-get my-feature-group input_ids.csv \\
        --output-file results.csv
      
      # 특정 피처만 조회
      fs bulk-get my-feature-group input_ids.json \\
        --feature-names "feature1,feature2"
      
      # 현재 시간으로 Time 필드 교체
      fs bulk-get my-feature-group input_ids.json --current-time
    """
    config = ctx.obj['config']
    feature_list = feature_names.split(',') if feature_names else None
    bulk_get_cmd.bulk_get_records(config, feature_group_name, input_file, output_file, feature_list, current_time)


@cli.command('bulk-put')
@click.argument('feature_group_name')
@click.argument('input_file')
@click.option('--output-file', '-o', help='결과 로그를 저장할 파일 경로')
@click.option('--batch-size', default=100, help='배치 처리 크기 (기본값: 100, 최대 1000 권장)')
@click.pass_context
def bulk_put_records(ctx, feature_group_name: str, input_file: str, output_file: Optional[str], batch_size: int):
    """입력 파일(JSON/CSV)을 사용하여 피처 그룹에 대량 레코드 저장
    
    \b
    예시:
      # JSON 파일에서 레코드들을 읽어 업데이트
      fs bulk-put my-feature-group records.json
      
      # CSV 파일 사용
      fs bulk-put my-feature-group records.csv
      
      # 배치 크기 조정으로 성능 향상
      fs bulk-put my-feature-group records.csv --batch-size 500
      
      # 결과 로그를 파일로 저장
      fs bulk-put my-feature-group records.json \\
        --output-file logs.txt --batch-size 200
    """
    if batch_size <= 0 or batch_size > 1000:
        click.echo("배치 크기는 1-1000 사이여야 합니다.", err=True)
        raise click.Abort()
    
    config = ctx.obj['config']
    bulk_put_cmd.bulk_put_records(config, feature_group_name, input_file, output_file, batch_size)


@cli.command('clear')
@click.argument('feature_group_name')
@click.option('--online-only', is_flag=True, help='온라인 스토어만 삭제')
@click.option('--offline-only', is_flag=True, help='오프라인 스토어만 삭제')
@click.option('--force', is_flag=True, help='확인 없이 즉시 삭제')
@click.option('--backup-s3', help='삭제 전 S3 백업 경로')
@click.option('--dry-run', is_flag=True, help='실제 삭제 없이 계획만 확인')
@click.pass_context
def clear_feature_group(ctx, feature_group_name: str, online_only: bool, offline_only: bool, 
                       force: bool, backup_s3: Optional[str], dry_run: bool):
    """피처 그룹의 모든 데이터 삭제
    
    \b
    예시:
      # 모든 데이터 삭제 (확인 프롬프트 포함)
      fs clear my-feature-group
      
      # 온라인 스토어만 삭제
      fs clear my-feature-group --online-only
      
      # 오프라인 스토어만 삭제
      fs clear my-feature-group --offline-only
      
      # 백업 후 삭제
      fs clear my-feature-group \\
        --backup-s3 s3://my-backup/fg-backup/
      
      # 강제 삭제 (확인 없음)
      fs clear my-feature-group --force
      
      # 계획만 확인 (실제 삭제 없음)
      fs clear my-feature-group --dry-run
    """
    if online_only and offline_only:
        click.echo("--online-only와 --offline-only 옵션을 동시에 사용할 수 없습니다.", err=True)
        raise click.Abort()
    
    config = ctx.obj['config']
    clear_cmd.clear_feature_group(config, feature_group_name, online_only, offline_only, 
                                 force, backup_s3, dry_run)


@cli.command('migrate')
@click.argument('source_feature_group')
@click.argument('target_feature_group')
@click.option('--clear-target', is_flag=True, help='타겟 피처그룹의 기존 데이터 삭제')
@click.option('--batch-size', default=100, help='배치 처리 사이즈 (기본: 100)')
@click.option('--max-workers', default=4, help='동시 처리 워커 수 (기본: 4)')
@click.option('--dry-run', is_flag=True, help='실제 마이그레이션 없이 계획만 확인')
@click.option('--filter-query', help='마이그레이션할 데이터 필터링 (SQL WHERE 절)')
@click.pass_context
def migrate_feature_group(ctx, source_feature_group: str, target_feature_group: str, 
                         clear_target: bool, batch_size: int, max_workers: int, 
                         dry_run: bool, filter_query: Optional[str]):
    """피처 그룹 간 데이터 마이그레이션
    
    \b
    예시:
      # 기본 마이그레이션
      fs migrate source-fg target-fg
      
      # 타겟 데이터 삭제 후 마이그레이션
      fs migrate source-fg target-fg --clear-target
      
      # 배치 사이즈 조정
      fs migrate source-fg target-fg \\
        --batch-size 50 --max-workers 8
      
      # 계획만 확인 (실제 마이그레이션 없음)
      fs migrate source-fg target-fg --dry-run
      
      # 특정 조건의 데이터만 마이그레이션
      fs migrate source-fg target-fg \\
        --filter-query "WHERE created_date >= '2024-01-01'"
    """
    if batch_size <= 0:
        click.echo("배치 사이즈는 1 이상이어야 합니다.", err=True)
        raise click.Abort()
    
    if max_workers <= 0:
        click.echo("워커 수는 1 이상이어야 합니다.", err=True)
        raise click.Abort()
    
    config = ctx.obj['config']
    migrate_cmd.migrate_feature_group(
        config, source_feature_group, target_feature_group,
        clear_target=clear_target, batch_size=batch_size, 
        max_workers=max_workers, dry_run=dry_run, 
        filter_query=filter_query
    )


# create, delete, export, analyze 명령어 등록
cli.add_command(create_cmd.create)
cli.add_command(delete_cmd.delete)
cli.add_command(export_cmd.export)

@cli.command('analyze')
@click.argument('feature_group_name', required=False)
@click.option('--bucket', help='S3 버킷 이름')
@click.option('--prefix', help='S3 프리픽스 경로')
@click.option('--export', help='결과를 CSV 파일로 내보낼 경로')
@click.option('--output-format', '-o', type=click.Choice(['table', 'json']), default='table',
              help='출력 형식')
@click.pass_context
def analyze_feature_store(ctx, feature_group_name: Optional[str], bucket: Optional[str], 
                         prefix: Optional[str], export: Optional[str], output_format: str):
    """피처 스토어 오프라인 스토어(S3) 용량 및 비용 분석
    
    ⚠️  주의: 이 명령어는 오프라인 스토어(S3)만 분석합니다. 
         온라인 스토어(DynamoDB)는 분석하지 않습니다.
    
    \b
    예시:
      # 특정 피처 그룹의 오프라인 스토어 분석
      fs analyze my-feature-group
      
      # S3 위치 직접 분석
      fs analyze --bucket my-bucket --prefix path/to/data
      
      # 결과를 CSV로 내보내기
      fs analyze my-feature-group --export analysis_report.csv
      
      # JSON 형태로 출력
      fs analyze my-feature-group --output-format json
    """
    if not feature_group_name and (not bucket or not prefix):
        click.echo("피처 그룹 이름 또는 --bucket과 --prefix를 모두 제공해야 합니다.", err=True)
        raise click.Abort()
    
    config = ctx.obj['config']
    analyze_cmd.analyze_feature_store(config, feature_group_name, bucket, prefix, export, output_format)


@cli.command('add-features')
@click.argument('feature_group_name')
@click.argument('features_file')
@click.option('--dry-run', is_flag=True, help='실제 변경 없이 계획만 확인')
@click.option('--wait/--no-wait', default=True, help='업데이트 완료까지 대기 여부 (기본값: True)')
@click.pass_context
def add_features_command(ctx, feature_group_name: str, features_file: str, dry_run: bool, wait: bool):
    """피처 그룹에 새로운 feature들을 추가합니다.
    
    \b
    예시:
      # 새로운 feature들 추가
      fs add-features my-feature-group new_features.json
      
      # 미리보기만 확인
      fs add-features my-feature-group new_features.json --dry-run
      
      # 백그라운드에서 업데이트
      fs add-features my-feature-group new_features.json --no-wait
    """
    add_features_cmd.add_features(feature_group_name, features_file, dry_run, wait)


@cli.command('schema')
@click.argument('feature_group_name')
@click.option('--output-format', '-o', type=click.Choice(['table', 'json']), default='table',
              help='출력 형식')
@click.pass_context
def show_schema(ctx, feature_group_name: str, output_format: str):
    """피처 그룹의 현재 스키마 조회
    
    \b
    예시:
      # 테이블 형태로 스키마 출력
      fs schema my-feature-group
      
      # JSON 형태로 스키마 출력
      fs schema my-feature-group --output-format json
    """
    add_features_cmd.show_schema(feature_group_name, output_format)


@cli.command('schema-template')
@click.option('--output', '-o', default='feature_template.json', help='출력 파일 경로')
def generate_template(output: str):
    """새로운 feature definition 템플릿 파일 생성
    
    \b
    예시:
      # 기본 템플릿 생성
      fs schema-template
      
      # 특정 파일명으로 템플릿 생성
      fs schema-template --output my_features.json
    """
    add_features_cmd.generate_feature_template(output)


@cli.command('add-feature')
@click.argument('feature_group_name')
@click.option('--feature', '-f', multiple=True, required=True,
              help='Feature 정의 (형식: name:type[:description] 또는 name:type:list:dimension[:description])')
@click.option('--dry-run', is_flag=True, help='실제 변경 없이 계획만 확인')
@click.option('--wait/--no-wait', default=True, help='업데이트 완료까지 대기 여부 (기본값: True)')
@click.pass_context
def add_feature_flags_command(ctx, feature_group_name: str, feature: tuple, dry_run: bool, wait: bool):
    """CLI 플래그로 피처 그룹에 feature들을 추가합니다.
    
    \b
    Feature 정의 형식 (벡터/집합은 Iceberg 만 지원):
      기본 형식: name:type[:description]
      벡터 형식: name:type:list:dimension[:description]
      집합 형식: name:type:set[:description]
    
    \b
    지원하는 타입:
      - String: 문자열 데이터
      - Integral: 정수형 데이터  
      - Fractional: 실수형 데이터
    
    \b
    예시:
      # 기본 feature 추가
      fs add-feature my-fg -f "age:Integral:사용자 나이" -f "name:String:사용자 이름"
      
      # 벡터 feature 추가  
      fs add-feature my-fg -f "embeddings:String:list:256:텍스트 임베딩"
      
      # 집합 feature 추가
      fs add-feature my-fg -f "tags:String:set:사용자 태그"
      
      # 미리보기만 확인
      fs add-feature my-fg -f "new_field:String" --dry-run
    """
    add_features_cmd.add_features_from_flags(feature_group_name, list(feature), dry_run, wait)


@cli.command('add-feature-json')
@click.argument('feature_group_name')
@click.option('--json', '-j', multiple=True, required=True,
              help='JSON 형태의 feature 정의')
@click.option('--dry-run', is_flag=True, help='실제 변경 없이 계획만 확인')
@click.option('--wait/--no-wait', default=True, help='업데이트 완료까지 대기 여부 (기본값: True)')
@click.pass_context  
def add_feature_json_command(ctx, feature_group_name: str, json: tuple, dry_run: bool, wait: bool):
    """JSON 문자열로 피처 그룹에 feature들을 추가합니다.
    
    \b
    JSON 형식:
      {"FeatureName": "name", "FeatureType": "String", "Description": "설명"}
    
    \b
    벡터 feature JSON 예시:
      {
        "FeatureName": "embeddings",
        "FeatureType": "String", 
        "CollectionType": "List",
        "CollectionConfig": {"VectorConfig": {"Dimension": 128}},
        "Description": "임베딩 벡터"
      }
    
    \b
    예시:
      # 기본 feature 추가
      fs add-feature-json my-fg \\
        -j '{"FeatureName": "score", "FeatureType": "Fractional"}' \\
        -j '{"FeatureName": "category", "FeatureType": "String"}'
      
      # 벡터 feature 추가
      fs add-feature-json my-fg \\
        -j '{"FeatureName": "vector", "FeatureType": "String", "CollectionType": "List", "CollectionConfig": {"VectorConfig": {"Dimension": 256}}}'
      
      # 미리보기만 확인  
      fs add-feature-json my-fg -j '{"FeatureName": "test", "FeatureType": "String"}' --dry-run
    """
    add_features_cmd.add_features_from_json_strings(feature_group_name, list(json), dry_run, wait)


if __name__ == '__main__':
    cli()