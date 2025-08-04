"""Main CLI module for SageMaker FeatureStore Online CLI"""

import click
from typing import Optional
from .config import Config
from .commands import list_cmd, get_cmd, put_cmd, bulk_get_cmd, bulk_put_cmd


@click.group()
@click.option('--profile', help='사용할 AWS 프로필')
@click.option('--region', help='사용할 AWS 리전')
@click.pass_context
def cli(ctx, profile: Optional[str], region: Optional[str]):
    """SageMaker FeatureStore Online CLI - 온라인 피처 스토어 관리 도구"""
    ctx.ensure_object(dict)
    ctx.obj['config'] = Config(profile=profile, region=region)


@cli.command('list')
@click.option('--output-format', '-o', type=click.Choice(['table', 'json']), default='table',
              help='출력 형식')
@click.pass_context
def list_feature_groups(ctx, output_format: str):
    """모든 온라인 피처 그룹 목록 조회"""
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
    """피처 그룹에서 단일 레코드 조회"""
    config = ctx.obj['config']
    feature_list = feature_names.split(',') if feature_names else None
    get_cmd.get_record(config, feature_group_name, record_identifier_value, feature_list, output_format)


@cli.command('put')
@click.argument('feature_group_name')
@click.option('--record', required=True, help='저장할 레코드의 JSON 문자열')
@click.pass_context
def put_record(ctx, feature_group_name: str, record: str):
    """피처 그룹에 단일 레코드 저장"""
    config = ctx.obj['config']
    put_cmd.put_record(config, feature_group_name, record)


@cli.command('bulk-get')
@click.argument('feature_group_name')
@click.argument('input_file')
@click.option('--output-file', '-o', help='출력 파일 경로')
@click.option('--feature-names', help='조회할 피처 이름들 (쉼표로 구분)')
@click.pass_context
def bulk_get_records(ctx, feature_group_name: str, input_file: str, 
                    output_file: Optional[str], feature_names: Optional[str]):
    """입력 파일(JSON/CSV)을 사용하여 피처 그룹에서 대량 레코드 조회"""
    config = ctx.obj['config']
    feature_list = feature_names.split(',') if feature_names else None
    bulk_get_cmd.bulk_get_records(config, feature_group_name, input_file, output_file, feature_list)


@cli.command('bulk-put')
@click.argument('feature_group_name')
@click.argument('input_file')
@click.option('--output-file', '-o', help='결과 로그를 저장할 파일 경로')
@click.pass_context
def bulk_put_records(ctx, feature_group_name: str, input_file: str, output_file: Optional[str]):
    """입력 파일(JSON/CSV)을 사용하여 피처 그룹에 대량 레코드 저장"""
    config = ctx.obj['config']
    bulk_put_cmd.bulk_put_records(config, feature_group_name, input_file, output_file)


if __name__ == '__main__':
    cli()