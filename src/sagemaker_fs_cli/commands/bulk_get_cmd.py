"""Bulk get command implementation"""

import click
import os
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from ..config import Config
from ..utils.file_handler import FileHandler
from ..utils.formatter import OutputFormatter


def get_single_record(config: Config, feature_group_name: str, record_id: str, 
                     feature_names: Optional[List[str]]) -> Dict[str, Any]:
    """Get a single record (helper function for bulk operations)"""
    try:
        request_params = {
            'FeatureGroupName': feature_group_name,
            'RecordIdentifierValueAsString': str(record_id)
        }
        
        if feature_names:
            request_params['FeatureNames'] = feature_names
        
        response = config.featurestore_runtime.get_record(**request_params)
        
        if not response.get('Record'):
            return {'record_id': record_id, 'error': 'Record not found'}
        
        # Format the record
        record_data = {'record_id': record_id}
        for feature in response['Record']:
            feature_name = feature['FeatureName']
            feature_value = feature['ValueAsString']
            record_data[feature_name] = feature_value
        
        return record_data
        
    except ClientError as e:
        return {'record_id': record_id, 'error': str(e)}
    except Exception as e:
        return {'record_id': record_id, 'error': f'Unexpected error: {str(e)}'}


def bulk_get_records(config: Config, feature_group_name: str, input_file: str, 
                    output_file: Optional[str], feature_names: Optional[List[str]]) -> None:
    """Bulk get records from feature group using input file"""
    try:
        # Validate input file exists
        if not os.path.exists(input_file):
            click.echo(f"입력 파일 '{input_file}'을 찾을 수 없습니다", err=True)
            raise click.Abort()
        
        # Read input file
        try:
            input_data = FileHandler.read_file(input_file)
        except Exception as e:
            click.echo(f"입력 파일 읽기 오류: {e}", err=True)
            raise click.Abort()
        
        if not input_data:
            click.echo("입력 파일이 비어있습니다", err=True)
            raise click.Abort()
        
        # Extract record identifiers
        record_ids = []
        for record in input_data:
            if isinstance(record, dict):
                # Try to find record identifier in various common field names
                record_id = (record.get('record_id') or 
                           record.get('id') or 
                           record.get('RecordIdentifier') or 
                           record.get('record_identifier'))
                if record_id is not None:
                    record_ids.append(str(record_id))
                else:
                    # If no identifier field found, use the first value
                    if record:
                        first_key = next(iter(record))
                        record_ids.append(str(record[first_key]))
            else:
                record_ids.append(str(record))
        
        if not record_ids:
            click.echo("입력 파일에서 레코드 식별자를 찾을 수 없습니다", err=True)
            raise click.Abort()
        
        click.echo(f"{len(record_ids)}개 레코드 처리 중...")
        
        # Perform bulk get operations with threading for better performance
        results = []
        max_workers = min(10, len(record_ids))  # Limit concurrent requests
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_record_id = {
                executor.submit(get_single_record, config, feature_group_name, 
                               record_id, feature_names): record_id 
                for record_id in record_ids
            }
            
            # Collect results
            completed = 0
            for future in as_completed(future_to_record_id):
                result = future.result()
                results.append(result)
                completed += 1
                
                if completed % 10 == 0 or completed == len(record_ids):
                    click.echo(f"완료: {completed}/{len(record_ids)}")
        
        # Sort results to maintain order
        results.sort(key=lambda x: record_ids.index(x.get('record_id', '')))
        
        # Filter out errors for successful records (but keep track of errors)
        successful_records = [r for r in results if 'error' not in r]
        error_records = [r for r in results if 'error' in r]
        
        if error_records:
            click.echo(f"경고: {len(error_records)}개 레코드 조회 실패", err=True)
            for error_record in error_records[:5]:  # Show first 5 errors
                click.echo(f"  - {error_record['record_id']}: {error_record['error']}", err=True)
            if len(error_records) > 5:
                click.echo(f"  ... 그리고 {len(error_records) - 5}개 더 많은 오류", err=True)
        
        if not successful_records:
            click.echo("성공적으로 조회된 레코드가 없습니다", err=True)
            raise click.Abort()
        
        # Output results
        if output_file:
            try:
                FileHandler.write_file(successful_records, output_file)
                click.echo(f"결과가 '{output_file}'에 저장되었습니다")
                click.echo(f"{len(successful_records)}개 레코드가 성공적으로 조회되었습니다")
            except Exception as e:
                click.echo(f"출력 파일 쓰기 오류: {e}", err=True)
                raise click.Abort()
        else:
            # Print to stdout
            output = OutputFormatter.format_json(successful_records)
            click.echo(output)
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFound':
            click.echo(f"피처 그룹 '{feature_group_name}'을 찾을 수 없습니다", err=True)
        else:
            click.echo(f"AWS 오류: {e}", err=True)
        raise click.Abort()
    except Exception as e:
        click.echo(f"예상치 못한 오류: {e}", err=True)
        raise click.Abort()