"""
Feature Store Export 명령어 구현
"""
import json
import csv
import gzip
import time
import os
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import urlparse

import boto3
import click
from tqdm import tqdm

from ..config import Config


@click.command()
@click.argument('feature_group_name')
@click.argument('output_file')
@click.option('--format', '-f', type=click.Choice(['csv', 'json', 'parquet']), default='csv', 
              help='출력 형식 (기본값: csv)')
@click.option('--limit', type=int, help='내보낼 레코드 수 제한')
@click.option('--where', help='SQL WHERE 조건절 추가')
@click.option('--columns', help='내보낼 컬럼 선택 (쉼표로 구분)')
@click.option('--order-by', help='정렬 기준 컬럼 지정')
@click.option('--compress/--no-compress', default=False, help='압축 여부 (기본값: False)')
@click.option('--chunk-size', default=10000, help='배치 처리 크기 (기본값: 10000)')
@click.option('--s3-output-location', help='Athena 쿼리 결과 임시 저장 S3 위치')
@click.option('--database', default='sagemaker_featurestore', help='Athena 데이터베이스 이름 (기본값: sagemaker_featurestore)')
@click.option('--online-compatible', is_flag=True, help='Online store bulk-put 호환 형식으로 변환')
@click.option('--column-mapping', help='컬럼명 매핑 지정 (예: "event_time:EventTime,customer_id:record_id")')
@click.option('--add-event-time', is_flag=True, help='EventTime 필드 자동 추가/변환')
@click.option('--record-identifier', help='레코드 식별자 필드명 지정')
@click.option('--dry-run', is_flag=True, help='실제 내보내기 없이 쿼리 및 예상 결과만 표시')
@click.pass_context
def export(
    ctx,
    feature_group_name: str,
    output_file: str,
    format: str,
    limit: Optional[int],
    where: Optional[str],
    columns: Optional[str],
    order_by: Optional[str],
    compress: bool,
    chunk_size: int,
    s3_output_location: Optional[str],
    database: str,
    online_compatible: bool,
    column_mapping: Optional[str],
    add_event_time: bool,
    record_identifier: Optional[str],
    dry_run: bool
):
    """Offline Store에서 Feature Group 데이터를 파일로 내보내기합니다.
    
    FEATURE_GROUP_NAME: 내보낼 Feature Group의 이름
    OUTPUT_FILE: 저장할 파일 경로
    
    \b
    예시:
      # 기본 내보내기 (CSV 형식)
      fs export my-feature-group data.csv
      
      # JSON 형식으로 내보내기
      fs export my-feature-group data.json --format json
      
      # 특정 컬럼만 내보내기
      fs export my-feature-group data.csv \\
        --columns "customer_id,age,balance"
      
      # 조건부 내보내기
      fs export my-feature-group recent_data.csv \\
        --where "event_time >= '2024-01-01'"
      
      # 최대 1000건만 내보내기
      fs export my-feature-group sample_data.csv --limit 1000
      
      # 압축된 파일로 내보내기
      fs export my-feature-group data.csv.gz --compress
      
      # Online Store 호환 형식으로 내보내기
      fs export my-feature-group online_data.json \\
        --online-compatible
      
      # 컬럼명 매핑하여 내보내기
      fs export my-feature-group mapped_data.csv \\
        --column-mapping "event_time:EventTime,customer_id:record_id"
      
      # 내보내기 계획만 확인
      fs export my-feature-group data.csv --dry-run
    """
    try:
        config = ctx.obj['config']
        
        if dry_run:
            click.echo("🔍 내보내기 계획 확인 (Dry Run)")
        else:
            click.echo("📥 Feature Store 데이터 내보내기 시작...")
        
        # 옵션 검증
        _validate_options(feature_group_name, output_file, format, chunk_size, 
                         column_mapping, config)
        
        # Feature Group 및 Offline Store 확인
        fg_details = _validate_feature_group(config, feature_group_name)
        
        # Athena 테이블 이름 찾기
        table_name = _find_athena_table_name(config, database, feature_group_name)
        if not table_name:
            raise click.ClickException(f"Feature Group '{feature_group_name}'에 대응하는 Athena 테이블을 찾을 수 없습니다.")
        
        click.echo(f"✓ Feature Group 검증 완료: {feature_group_name}")
        click.echo(f"✓ Athena 테이블 확인: {database}.{table_name}")
        
        # SQL 쿼리 생성
        query = _build_query(database, table_name, columns, where, order_by, limit)
        click.echo("✓ 쿼리 생성 완료")
        
        if dry_run:
            _display_dry_run_info(feature_group_name, fg_details, query, output_file, format)
            return
        
        # S3 출력 위치 설정
        if not s3_output_location:
            s3_output_location = _get_default_s3_output_location(config)
        
        # Athena 쿼리 실행
        result_location = _execute_athena_query(config, query, s3_output_location)
        click.echo(f"✓ 쿼리 실행 완료")
        
        # 결과 다운로드 및 변환
        _process_query_results(
            config=config,
            result_location=result_location,
            output_file=output_file,
            format=format,
            compress=compress,
            online_compatible=online_compatible,
            column_mapping=column_mapping,
            add_event_time=add_event_time,
            record_identifier=record_identifier
        )
        
        # 결과 요약 표시
        _display_export_summary(output_file, format, compress)
        
        click.echo("✅ 데이터 내보내기가 완료되었습니다!")
        
    except Exception as e:
        raise click.ClickException(f"데이터 내보내기 중 오류 발생: {str(e)}")


def _validate_options(
    feature_group_name: str,
    output_file: str,
    format: str,
    chunk_size: int,
    column_mapping: Optional[str],
    config: Config
):
    """옵션 검증"""
    
    # 청크 크기 검증
    if chunk_size <= 0:
        raise click.ClickException("청크 크기는 1 이상이어야 합니다.")
    
    # 출력 파일 디렉토리 생성
    output_dir = os.path.dirname(os.path.abspath(output_file))
    if output_dir and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
        except OSError as e:
            raise click.ClickException(f"출력 디렉토리 생성 실패: {str(e)}")
    
    # 컬럼 매핑 형식 검증
    if column_mapping:
        try:
            _parse_column_mapping(column_mapping)
        except Exception as e:
            raise click.ClickException(f"컬럼 매핑 형식 오류: {str(e)}")


def _validate_feature_group(config: Config, feature_group_name: str) -> Dict[str, Any]:
    """Feature Group 및 Offline Store 검증"""
    try:
        sagemaker_client = config.session.client('sagemaker')
        
        # 먼저 모든 Feature Group 목록을 확인
        try:
            list_response = sagemaker_client.list_feature_groups(MaxResults=100)
            available_fgs = [fg['FeatureGroupName'] for fg in list_response.get('FeatureGroupSummaries', [])]
            
            if not available_fgs:
                raise click.ClickException(f"계정에 Feature Group이 없습니다. 먼저 Feature Group을 생성해주세요.")
            
            # 유사한 이름 찾기
            similar_names = [fg for fg in available_fgs if feature_group_name.lower() in fg.lower() or fg.lower() in feature_group_name.lower()]
            
        except Exception as e:
            click.echo(f"⚠️ Feature Group 목록 조회 중 오류: {str(e)}")
            similar_names = []
        
        response = sagemaker_client.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        
        # Offline Store 활성화 확인
        if not response.get('OfflineStoreConfig'):
            raise click.ClickException(f"Feature Group '{feature_group_name}'에 Offline Store가 활성화되어 있지 않습니다.")
        
        return response
        
    except sagemaker_client.exceptions.ResourceNotFound:
        error_msg = f"Feature Group '{feature_group_name}'을 찾을 수 없습니다."
        
        if similar_names:
            error_msg += f"\n\n유사한 이름의 Feature Group들:"
            for fg_name in similar_names[:5]:  # 최대 5개만 표시
                error_msg += f"\n  - {fg_name}"
        elif available_fgs:
            error_msg += f"\n\n사용 가능한 Feature Group들:"
            for fg_name in available_fgs[:5]:  # 최대 5개만 표시
                error_msg += f"\n  - {fg_name}"
        
        raise click.ClickException(error_msg)
        
    except Exception as e:
        if "ResourceNotFound" in str(e):
            raise click.ClickException(f"Feature Group '{feature_group_name}'을 찾을 수 없습니다.")
        else:
            raise click.ClickException(f"Feature Group 검증 중 오류: {str(e)}")


def _find_athena_table_name(config: Config, database: str, feature_group_name: str) -> Optional[str]:
    """Feature Group에 대응하는 Athena 테이블 이름 찾기"""
    try:
        athena_client = config.session.client('athena')
        
        # 가능한 테이블 이름 패턴들
        account_id = config.session.client('sts').get_caller_identity()['Account']
        table_name_patterns = [
            feature_group_name.replace('-', '_'),
            feature_group_name.lower().replace('-', '_'),
            f"{feature_group_name.replace('-', '_')}_{account_id}",
            f"{feature_group_name.lower().replace('-', '_')}_{account_id}"
        ]
        
        # 실제 테이블 목록 조회
        response = athena_client.list_table_metadata(
            CatalogName='AwsDataCatalog',
            DatabaseName=database
        )
        
        existing_tables = [table['Name'] for table in response.get('TableMetadataList', [])]
        
        # 디버깅: 사용 가능한 테이블 목록 표시
        click.echo(f"🔍 사용 가능한 Athena 테이블 목록:")
        for table_name in existing_tables:
            click.echo(f"  - {table_name}")
        
        click.echo(f"🔍 시도할 패턴들:")
        for pattern in table_name_patterns:
            click.echo(f"  - {pattern}")
        
        # 패턴 매칭으로 테이블 찾기 (대소문자 구분 없이)
        for pattern in table_name_patterns:
            for table_name in existing_tables:
                if pattern.lower() == table_name.lower():
                    click.echo(f"✓ 매칭된 테이블: {table_name}")
                    return table_name
        
        # 부분 매칭도 시도해보기
        click.echo(f"🔍 부분 매칭 시도...")
        feature_group_base = feature_group_name.replace('-', '_').lower()
        for table_name in existing_tables:
            if feature_group_base in table_name.lower():
                click.echo(f"✓ 부분 매칭된 테이블: {table_name}")
                return table_name
        
        return None
        
    except Exception as e:
        click.echo(f"⚠️ Athena 테이블 조회 중 오류: {str(e)}")
        return None


def _build_query(
    database: str, 
    table_name: str, 
    columns: Optional[str], 
    where: Optional[str], 
    order_by: Optional[str], 
    limit: Optional[int]
) -> str:
    """SQL 쿼리 동적 생성"""
    
    # SELECT 절
    if columns:
        column_list = [col.strip() for col in columns.split(',')]
        select_clause = ', '.join(column_list)
    else:
        select_clause = '*'
    
    # 기본 쿼리
    query = f'SELECT {select_clause} FROM "{database}"."{table_name}"'
    
    # WHERE 절
    if where:
        # WHERE 키워드가 이미 포함되어 있는지 확인
        where_clause = where.strip()
        if not where_clause.upper().startswith('WHERE'):
            where_clause = f"WHERE {where_clause}"
        query += f" {where_clause}"
    
    # ORDER BY 절
    if order_by:
        order_clause = order_by.strip()
        if not order_clause.upper().startswith('ORDER BY'):
            order_clause = f"ORDER BY {order_clause}"
        query += f" {order_clause}"
    
    # LIMIT 절
    if limit:
        query += f" LIMIT {limit}"
    
    return query


def _get_default_s3_output_location(config: Config) -> str:
    """기본 S3 출력 위치 설정"""
    try:
        # SageMaker 기본 버킷 사용
        account_id = config.session.client('sts').get_caller_identity()['Account']
        region = config.session.region_name
        bucket_name = f"sagemaker-{region}-{account_id}"
        
        # 버킷 존재 확인
        s3_client = config.session.client('s3')
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            return f"s3://{bucket_name}/athena-results/"
        except:
            pass
        
        # 대체 버킷 찾기
        response = s3_client.list_buckets()
        for bucket in response['Buckets']:
            if 'sagemaker' in bucket['Name'].lower():
                return f"s3://{bucket['Name']}/athena-results/"
        
        # 마지막 대안
        if response['Buckets']:
            return f"s3://{response['Buckets'][0]['Name']}/athena-results/"
        
        raise click.ClickException("사용 가능한 S3 버킷을 찾을 수 없습니다. --s3-output-location을 지정해주세요.")
        
    except Exception as e:
        raise click.ClickException(f"S3 출력 위치 설정 실패: {str(e)}")


def _execute_athena_query(config: Config, query: str, s3_output_location: str) -> str:
    """Athena 쿼리 실행"""
    try:
        athena_client = config.session.client('athena')
        
        # 쿼리 실행
        response = athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={'OutputLocation': s3_output_location},
            WorkGroup='primary'
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # 쿼리 완료까지 대기
        with tqdm(desc="Athena 쿼리 실행 중", unit="초") as pbar:
            while True:
                result = athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                status = result['QueryExecution']['Status']['State']
                pbar.set_description(f"상태: {status}")
                
                if status == 'SUCCEEDED':
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    reason = result['QueryExecution']['Status'].get('StateChangeReason', '알 수 없는 오류')
                    raise click.ClickException(f"Athena 쿼리 실패: {reason}")
                
                time.sleep(2)
                pbar.update(2)
        
        # 결과 위치 반환
        result_location = result['QueryExecution']['ResultConfiguration']['OutputLocation']
        return result_location
        
    except Exception as e:
        if isinstance(e, click.ClickException):
            raise
        else:
            raise click.ClickException(f"Athena 쿼리 실행 중 오류: {str(e)}")


def _process_query_results(
    config: Config,
    result_location: str,
    output_file: str,
    format: str,
    compress: bool,
    online_compatible: bool,
    column_mapping: Optional[str],
    add_event_time: bool,
    record_identifier: Optional[str]
):
    """쿼리 결과를 다운로드하고 변환"""
    
    # S3에서 결과 파일 다운로드
    temp_file = _download_from_s3(config, result_location)
    
    try:
        # 데이터 처리 및 변환
        _convert_and_save_data(
            temp_file=temp_file,
            output_file=output_file,
            format=format,
            compress=compress,
            online_compatible=online_compatible,
            column_mapping=column_mapping,
            add_event_time=add_event_time,
            record_identifier=record_identifier
        )
    finally:
        # 임시 파일 정리
        if os.path.exists(temp_file):
            os.remove(temp_file)


def _download_from_s3(config: Config, s3_url: str) -> str:
    """S3에서 파일 다운로드"""
    try:
        s3_client = config.session.client('s3')
        parsed_url = urlparse(s3_url)
        bucket_name = parsed_url.netloc
        key = parsed_url.path.lstrip('/')
        
        # 임시 파일명
        temp_file = f"/tmp/athena_result_{int(time.time())}.csv"
        
        # 다운로드
        with tqdm(desc="S3에서 결과 다운로드 중", unit="B", unit_scale=True) as pbar:
            def progress_callback(bytes_transferred):
                pbar.update(bytes_transferred)
            
            s3_client.download_file(
                bucket_name, key, temp_file,
                Callback=progress_callback
            )
        
        return temp_file
        
    except Exception as e:
        raise click.ClickException(f"S3 다운로드 실패: {str(e)}")


def _convert_and_save_data(
    temp_file: str,
    output_file: str,
    format: str,
    compress: bool,
    online_compatible: bool,
    column_mapping: Optional[str],
    add_event_time: bool,
    record_identifier: Optional[str]
):
    """데이터 변환 및 저장"""
    
    # CSV 데이터 읽기
    with open(temp_file, 'r', encoding='utf-8') as f:
        csv_reader = csv.DictReader(f)
        rows = list(csv_reader)
    
    if not rows:
        click.echo("⚠️ 내보낼 데이터가 없습니다.")
        return
    
    # Online 호환성 처리
    if online_compatible or column_mapping or add_event_time:
        rows = _apply_online_compatibility(rows, column_mapping, add_event_time, record_identifier)
    
    # 형식별 저장
    if format == 'json':
        _save_as_json(rows, output_file, compress)
    elif format == 'csv':
        _save_as_csv(rows, output_file, compress)
    elif format == 'parquet':
        _save_as_parquet(rows, output_file, compress)


def _apply_online_compatibility(
    rows: List[Dict],
    column_mapping: Optional[str],
    add_event_time: bool,
    record_identifier: Optional[str]
) -> List[Dict]:
    """Online store 호환성 변환 적용"""
    
    # 컬럼 매핑 파싱
    mapping = {}
    if column_mapping:
        mapping = _parse_column_mapping(column_mapping)
    
    # 기본 매핑 규칙 추가
    default_mappings = {
        'event_time': 'EventTime',
        'eventtime': 'EventTime',
        'timestamp': 'EventTime',
        'created_at': 'EventTime'
    }
    
    for old_name, new_name in default_mappings.items():
        if old_name not in mapping:
            mapping[old_name] = new_name
    
    processed_rows = []
    current_timestamp = str(int(time.time()))
    
    for row in rows:
        new_row = {}
        
        # 컬럼 매핑 적용
        for old_key, value in row.items():
            new_key = mapping.get(old_key, old_key)
            
            # 모든 값을 문자열로 변환 (Online store 요구사항)
            if value is None:
                new_row[new_key] = ""
            else:
                new_row[new_key] = str(value)
        
        # EventTime 자동 추가/변환
        if add_event_time and 'EventTime' not in new_row:
            new_row['EventTime'] = current_timestamp
        
        processed_rows.append(new_row)
    
    return processed_rows


def _parse_column_mapping(column_mapping: str) -> Dict[str, str]:
    """컬럼 매핑 문자열 파싱"""
    mapping = {}
    
    for pair in column_mapping.split(','):
        if ':' not in pair:
            raise ValueError(f"잘못된 매핑 형식: '{pair}'. 'old_name:new_name' 형식이어야 합니다.")
        
        old_name, new_name = pair.split(':', 1)
        mapping[old_name.strip()] = new_name.strip()
    
    return mapping


def _save_as_json(rows: List[Dict], output_file: str, compress: bool):
    """JSON 형식으로 저장"""
    
    def write_json(f):
        for row in tqdm(rows, desc="JSON 저장 중"):
            json.dump(row, f, ensure_ascii=False)
            f.write('\n')
    
    if compress:
        with gzip.open(output_file, 'wt', encoding='utf-8') as f:
            write_json(f)
    else:
        with open(output_file, 'w', encoding='utf-8') as f:
            write_json(f)


def _save_as_csv(rows: List[Dict], output_file: str, compress: bool):
    """CSV 형식으로 저장"""
    if not rows:
        return
    
    fieldnames = rows[0].keys()
    
    def write_csv(f):
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in tqdm(rows, desc="CSV 저장 중"):
            writer.writerow(row)
    
    if compress:
        with gzip.open(output_file, 'wt', encoding='utf-8', newline='') as f:
            write_csv(f)
    else:
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            write_csv(f)


def _save_as_parquet(rows: List[Dict], output_file: str, compress: bool):
    """Parquet 형식으로 저장 (pandas 필요)"""
    try:
        import pandas as pd
        
        df = pd.DataFrame(rows)
        
        compression = 'gzip' if compress else None
        df.to_parquet(output_file, compression=compression, index=False)
        
    except ImportError:
        raise click.ClickException("Parquet 형식을 사용하려면 pandas와 pyarrow를 설치해야 합니다: pip install pandas pyarrow")


def _display_dry_run_info(
    feature_group_name: str,
    fg_details: Dict[str, Any],
    query: str,
    output_file: str,
    format: str
):
    """Dry-run 정보 표시"""
    
    click.echo(f"\nFeature Group: {feature_group_name}")
    
    # Offline Store 정보
    offline_config = fg_details.get('OfflineStoreConfig', {})
    s3_uri = offline_config.get('S3StorageConfig', {}).get('S3Uri', 'N/A')
    table_format = offline_config.get('TableFormat', 'N/A')
    click.echo(f"  - Offline Store: {s3_uri}")
    click.echo(f"  - 테이블 형식: {table_format}")
    click.echo(f"  - 출력 파일: {output_file}")
    
    click.echo(f"\n실행할 쿼리:")
    click.echo(f"  {query}")
    
    click.echo(f"\n예상 결과:")
    click.echo(f"  - 출력 형식: {format}")
    click.echo(f"  - 예상 파일 크기: 데이터에 따라 다름")
    click.echo(f"  - Athena 쿼리 비용: 스캔된 데이터에 따라 다름")
    
    click.echo(f"\n실제 내보내기를 실행하려면 --dry-run 옵션을 제거하세요.")


def _display_export_summary(output_file: str, format: str, compress: bool):
    """내보내기 요약 표시"""
    try:
        file_size = os.path.getsize(output_file)
        file_size_mb = file_size / (1024 * 1024)
        
        click.echo(f"\n📋 내보내기 요약:")
        click.echo(f"  - 출력 파일: {output_file}")
        click.echo(f"  - 파일 형식: {format.upper()}")
        click.echo(f"  - 압축 여부: {'예' if compress else '아니오'}")
        click.echo(f"  - 파일 크기: {file_size_mb:.2f}MB")
        
    except Exception as e:
        click.echo(f"⚠️ 요약 정보 표시 중 오류: {str(e)}")