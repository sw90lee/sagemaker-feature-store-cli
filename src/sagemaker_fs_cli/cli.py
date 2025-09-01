"""Main CLI module for SageMaker FeatureStore Online CLI"""

import click
from typing import Optional
from .config import Config
from .commands import list_cmd, get_cmd, put_cmd, bulk_get_cmd, bulk_put_cmd, clear_cmd, migrate_cmd, create_cmd, delete_cmd, export_cmd, analyze_cmd, add_features_cmd, batch_update_cmd


@click.group()
@click.option('--profile', help='사용할 AWS 프로필')
@click.option('--region', help='사용할 AWS 리전')
@click.pass_context
def cli(ctx, profile: Optional[str], region: Optional[str]):
    """SageMaker FeatureStore CLI - 오프라인 피처 스토어 전용 관리 도구"""
    ctx.ensure_object(dict)
    ctx.obj['config'] = Config(profile=profile, region=region)


@cli.command('list')
@click.option('--output-format', '-o', type=click.Choice(['table', 'json']), default='table',
              help='출력 형식')
@click.pass_context
def list_feature_groups(ctx, output_format: str):
    """모든 피처 그룹 목록 조회 (오프라인 스토어)
    
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
    """오프라인 스토어(Athena)에서 단일 레코드 조회
    
    \b
    예시:
      # 오프라인 스토어에서 기본 조회
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
    """오프라인 스토어(S3)에 단일 레코드 저장
    
    \b
    예시:
      # 오프라인 스토어에 단일 레코드 저장
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
    """입력 파일(JSON/CSV)을 사용하여 오프라인 스토어(Athena)에서 대량 레코드 조회
    
    \b
    예시:
      # 오프라인 스토어에서 JSON 파일의 레코드 ID 목록으로 조회
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
    """입력 파일(JSON/CSV)을 사용하여 오프라인 스토어(S3)에 대량 레코드 저장
    
    \b
    예시:
      # 오프라인 스토어에 JSON 파일의 레코드들을 업로드
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
@click.option('--deduplicate-only', is_flag=True, help='삭제 대신 중복된 record_id만 제거 (EventTime 기준 최신만 유지)')
@click.pass_context
def clear_feature_group(ctx, feature_group_name: str, online_only: bool, offline_only: bool, 
                       force: bool, backup_s3: Optional[str], dry_run: bool, deduplicate_only: bool):
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
      
      # 중복된 record_id만 제거 (삭제하지 않음)
      fs clear my-feature-group --deduplicate-only
      
      # 중복 제거 미리보기
      fs clear my-feature-group --deduplicate-only --dry-run
    """
    if online_only and offline_only:
        click.echo("--online-only와 --offline-only 옵션을 동시에 사용할 수 없습니다.", err=True)
        raise click.Abort()
    
    config = ctx.obj['config']
    clear_cmd.clear_feature_group(config, feature_group_name, online_only, offline_only, 
                                 force, backup_s3, dry_run, deduplicate_only)


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
@click.argument('features_file', required=False)
@click.option('--feature', '-f', multiple=True,
              help='Feature 정의 (형식: name:type[:description] 또는 name:type:list:dimension[:description])')
@click.option('--json', '-j', multiple=True,
              help='JSON 형태의 feature 정의')
@click.option('--dry-run', is_flag=True, help='실제 변경 없이 계획만 확인')
@click.option('--wait/--no-wait', default=True, help='업데이트 완료까지 대기 여부 (기본값: True)')
@click.pass_context
def add_features_command(ctx, feature_group_name: str, features_file: str, feature: tuple, json: tuple, dry_run: bool, wait: bool):
    """피처 그룹에 새로운 feature들을 추가합니다 (통합 명령어).
    
    세 가지 방식을 지원합니다:
    
    \b
    1. JSON 파일 방식:
       fs add-features my-fg features.json
    
    \b
    2. CLI 플래그 방식:
       fs add-features my-fg -f "name:String:설명" -f "score:Fractional"
    
    \b
    3. JSON 문자열 방식:
       fs add-features my-fg -j '{"FeatureName": "name", "FeatureType": "String"}'
    
    \b
    Feature 정의 형식 (벡터/집합은 Iceberg 만 지원):
      기본 형식: name:type[:description]
      벡터 형식: name:type:list:dimension[:description]  
      집합 형식: name:type:set[:description]
    
    \b
    예시:
      # JSON 파일로 추가
      fs add-features my-fg new_features.json
      
      # CLI 플래그로 추가
      fs add-features my-fg -f "user_score:Fractional:사용자 점수" -f "status:String"
      
      # JSON 문자열로 추가
      fs add-features my-fg -j '{"FeatureName": "conversion_rate", "FeatureType": "Fractional"}'
      
      # 미리보기만 확인
      fs add-features my-fg features.json --dry-run
      
      # 백그라운드 실행
      fs add-features my-fg -f "new_field:String" --no-wait
    """
    # 입력 방식 검증
    input_methods = sum([
        bool(features_file),
        bool(feature),
        bool(json)
    ])
    
    if input_methods == 0:
        click.echo("❌ Feature 정의가 필요합니다. 다음 중 하나를 선택하세요:", err=True)
        click.echo("  - JSON 파일: fs add-features my-fg features.json", err=True)
        click.echo("  - CLI 플래그: fs add-features my-fg -f 'name:type:설명'", err=True)
        click.echo("  - JSON 문자열: fs add-features my-fg -j '{\"FeatureName\": \"name\", \"FeatureType\": \"String\"}'", err=True)
        raise click.Abort()
    
    if input_methods > 1:
        click.echo("❌ 한 번에 하나의 입력 방식만 사용할 수 있습니다.", err=True)
        click.echo("  사용된 방식:", err=True)
        if features_file:
            click.echo(f"    - JSON 파일: {features_file}", err=True)
        if feature:
            click.echo(f"    - CLI 플래그: {len(feature)}개", err=True)
        if json:
            click.echo(f"    - JSON 문자열: {len(json)}개", err=True)
        raise click.Abort()
    
    # 입력 방식에 따라 적절한 함수 호출
    if features_file:
        # JSON 파일 방식
        add_features_cmd.add_features(feature_group_name, features_file, dry_run, wait)
    elif feature:
        # CLI 플래그 방식
        add_features_cmd.add_features_from_flags(feature_group_name, list(feature), dry_run, wait)
    elif json:
        # JSON 문자열 방식
        add_features_cmd.add_features_from_json_strings(feature_group_name, list(json), dry_run, wait)


@cli.command('schema')
@click.argument('feature_group_name', required=False)
@click.option('--output-format', '-o', type=click.Choice(['table', 'json']), default='table',
              help='출력 형식 (스키마 조회용)')
@click.option('--template', is_flag=True, help='Feature definition 템플릿 파일 생성')
@click.option('--template-output', default='feature_template.json', help='템플릿 출력 파일 경로')
@click.pass_context
def schema_command(ctx, feature_group_name: str, output_format: str, template: bool, template_output: str):
    """피처 그룹의 스키마 조회 또는 템플릿 생성 (통합 명령어).
    
    두 가지 모드를 지원합니다:
    
    \b
    1. 스키마 조회 모드:
       fs schema <feature-group-name> [옵션]
    
    \b  
    2. 템플릿 생성 모드:
       fs schema --template [옵션]
    
    \b
    예시:
      # 스키마 조회 (테이블 형태)
      fs schema my-feature-group
      
      # 스키마 조회 (JSON 형태)
      fs schema my-feature-group --output-format json
      
      # 템플릿 생성 (기본 파일명)
      fs schema --template
      
      # 템플릿 생성 (사용자 정의 파일명)
      fs schema --template --template-output my_features.json
    """
    # 입력 모드 검증
    if template and feature_group_name:
        click.echo("❌ 스키마 조회와 템플릿 생성을 동시에 할 수 없습니다.", err=True)
        click.echo("  - 스키마 조회: fs schema <feature-group-name>", err=True)
        click.echo("  - 템플릿 생성: fs schema --template", err=True)
        raise click.Abort()
    
    if not template and not feature_group_name:
        click.echo("❌ Feature Group 이름이 필요하거나 --template 옵션을 사용하세요.", err=True)
        click.echo("  - 스키마 조회: fs schema <feature-group-name>", err=True)
        click.echo("  - 템플릿 생성: fs schema --template", err=True)
        raise click.Abort()
    
    # 모드에 따라 적절한 함수 호출
    if template:
        # 템플릿 생성 모드
        add_features_cmd.generate_feature_template(template_output)
    else:
        # 스키마 조회 모드
        add_features_cmd.show_schema(feature_group_name, output_format)


@cli.command('batch-update')
@click.argument('feature_group_name')
@click.option('--column', required=True, help='업데이트할 컬럼명')
@click.option('--old-value', help='변경할 기존 값 (단일 값 변경용)')
@click.option('--new-value', help='새로운 값 (단일 값 변경용)')
@click.option('--mapping-file', help='매핑 파일 경로 (.json 또는 .csv)')
@click.option('--conditional-mapping', help='조건부 매핑 JSON 문자열')
@click.option('--transform-function', type=click.Choice(['regex_replace', 'prefix_suffix', 'uppercase', 'lowercase', 'copy_from_column', 'extract_time_prefix']),
              help='변환 함수 타입')
@click.option('--regex-pattern', help='정규식 패턴 (transform-function=regex_replace 시 필요)')
@click.option('--regex-replacement', default='', help='정규식 치환 문자열 (기본값: 빈 문자열)')
@click.option('--prefix', default='', help='접두사 (transform-function=prefix_suffix 시)')
@click.option('--suffix', default='', help='접미사 (transform-function=prefix_suffix 시)')
@click.option('--source-column', help='복사할 원본 컬럼명 (transform-function=copy_from_column 시 필요)')
@click.option('--prefix-pattern', default=r'(\d{4}-\d{2}-\d{2})', help='시간 추출용 정규식 패턴 (extract_time_prefix 시 사용)')
@click.option('--time-format', default='auto', help='시간 형식 (extract_time_prefix 시 사용, 기본값: auto)')
@click.option('--to-iso/--no-to-iso', default=True, help='추출된 시간을 ISO 형식으로 변환 (extract_time_prefix 시 사용)')
@click.option('--dry-run', is_flag=True, default=True, help='실제 실행하지 않고 테스트만 수행 (기본값: True)')
@click.option('--no-dry-run', is_flag=True, help='실제로 데이터를 업데이트')
@click.option('--skip-validation', is_flag=True, help='Athena 검증 건너뛰기')
@click.option('--filter-column', help='추가 필터 컬럼명')
@click.option('--filter-value', help='추가 필터 값')
@click.option('--cleanup-backups', is_flag=True, help='백업 파일 자동 정리')
@click.option('--batch-size', default=1000, help='배치 크기 (기본값: 1000)')
@click.option('--deduplicate/--no-deduplicate', default=True, help='중복 record_id 제거 (EventTime 기준 최신만 유지, 기본값: True)')
@click.pass_context
def batch_update_feature_store(ctx, feature_group_name: str, column: str,
                              old_value: Optional[str], new_value: Optional[str],
                              mapping_file: Optional[str], conditional_mapping: Optional[str],
                              transform_function: Optional[str], regex_pattern: Optional[str], 
                              regex_replacement: str, prefix: str, suffix: str, 
                              source_column: Optional[str], prefix_pattern: str,
                              time_format: str, to_iso: bool,
                              dry_run: bool, no_dry_run: bool, skip_validation: bool,
                              filter_column: Optional[str], filter_value: Optional[str],
                              cleanup_backups: bool, batch_size: int, deduplicate: bool):
    """피처 그룹의 오프라인 스토어 데이터를 대량으로 업데이트
    
    다양한 업데이트 방식을 지원합니다:
    
    \b
    1. 단일 값 변경:
       fs batch-update my-fg --column status --old-value "old" --new-value "new"
    
    \b
    2. 매핑 파일 사용:
       fs batch-update my-fg --column status --mapping-file mapping.json
    
    \b
    3. 조건부 매핑:
       fs batch-update my-fg --column status --conditional-mapping '{"category": {"A": {"old1": "new1"}}}'
    
    \b
    예시:
      # 단일 값 변경 (테스트)
      fs batch-update my-fg --column RB_Result --old-value "ABNORMAL" --new-value "NORMAL"
      
      # 실제 변경 실행
      fs batch-update my-fg --column RB_Result --old-value "ABNORMAL" --new-value "NORMAL" --no-dry-run
      
      # 매핑 파일로 여러 값 변경
      fs batch-update my-fg --column status --mapping-file value_mapping.json --no-dry-run
      
      # 조건부 매핑
      fs batch-update my-fg --column result --conditional-mapping '{"category": {"A": {"old": "new"}}}' --no-dry-run
      
      # 필터 조건 적용
      fs batch-update my-fg --column status --old-value "old" --new-value "new" \\
        --filter-column region --filter-value "us-east-1" --no-dry-run
    
    ⚠️ 주의사항:
      - 기본적으로 --dry-run 모드로 실행됩니다
      - 실제 변경을 위해서는 --no-dry-run 플래그를 사용하세요
      - 변경 전 자동으로 백업이 생성됩니다
      - 대용량 데이터의 경우 시간이 오래 걸릴 수 있습니다
      - 기본적으로 중복된 record_id의 경우 EventTime 기준 최신 레코드만 업데이트됩니다
    """
    # dry-run 로직 처리
    if no_dry_run:
        dry_run = False
    
    # 입력 검증
    input_methods = sum([
        bool(old_value and new_value),
        bool(mapping_file),
        bool(conditional_mapping),
        bool(transform_function)
    ])
    
    if input_methods == 0:
        click.echo("❌ 업데이트 방식을 선택해야 합니다:", err=True)
        click.echo("  - 단일 값: --old-value 'old' --new-value 'new'", err=True)
        click.echo("  - 매핑 파일: --mapping-file mapping.json", err=True)
        click.echo("  - 조건부 매핑: --conditional-mapping '{...}'", err=True)
        click.echo("  - 변환 함수: --transform-function regex_replace", err=True)
        raise click.Abort()
    
    if input_methods > 1:
        click.echo("❌ 한 번에 하나의 업데이트 방식만 사용할 수 있습니다.", err=True)
        raise click.Abort()
    
    if old_value and not new_value:
        click.echo("❌ --old-value를 사용할 때는 --new-value도 필요합니다.", err=True)
        raise click.Abort()
    
    if new_value and not old_value:
        click.echo("❌ --new-value를 사용할 때는 --old-value도 필요합니다.", err=True)
        raise click.Abort()
    
    if batch_size <= 0 or batch_size > 10000:
        click.echo("❌ 배치 크기는 1-10000 사이여야 합니다.", err=True)
        raise click.Abort()
    
    # 변환 함수별 추가 검증
    if transform_function:
        if transform_function == 'regex_replace' and not regex_pattern:
            click.echo("❌ regex_replace 변환에는 --regex-pattern이 필요합니다.", err=True)
            raise click.Abort()
        
        if transform_function == 'copy_from_column' and not source_column:
            click.echo("❌ copy_from_column 변환에는 --source-column이 필요합니다.", err=True)
            raise click.Abort()
    
    # 변환 함수 옵션 준비
    transform_options = {}
    if transform_function:
        if transform_function == 'regex_replace':
            transform_options = {
                'pattern': regex_pattern,
                'replacement': regex_replacement
            }
        elif transform_function == 'prefix_suffix':
            transform_options = {
                'prefix': prefix,
                'suffix': suffix
            }
        elif transform_function == 'copy_from_column':
            transform_options = {
                'source_column': source_column
            }
        elif transform_function == 'extract_time_prefix':
            transform_options = {
                'time_format': time_format,
                'prefix_pattern': prefix_pattern,
                'to_iso': to_iso,
                'source_column': source_column
            }
    
    config = ctx.obj['config']
    batch_update_cmd.batch_update(
        config=config,
        feature_group_name=feature_group_name,
        column_name=column,
        old_value=old_value,
        new_value=new_value,
        mapping_file=mapping_file,
        conditional_mapping=conditional_mapping,
        transform_type=transform_function,
        transform_options=transform_options,
        dry_run=dry_run,
        skip_validation=skip_validation,
        filter_column=filter_column,
        filter_value=filter_value,
        cleanup_backups=cleanup_backups,
        batch_size=batch_size,
        deduplicate=deduplicate
    )


if __name__ == '__main__':
    cli()