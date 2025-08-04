"""Bulk put command implementation"""

import click
import os
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from ..config import Config
from ..utils.file_handler import FileHandler


def put_single_record(config: Config, feature_group_name: str, record_data: Dict[str, Any], 
                     feature_definitions: Dict[str, Dict]) -> Dict[str, Any]:
    """Put a single record (helper function for bulk operations)"""
    try:
        # Add EventTime if not provided
        if 'EventTime' not in record_data:
            record_data['EventTime'] = str(int(time.time()))
        
        # Format record for SageMaker FeatureStore
        formatted_record = []
        record_id = None
        
        for feature_name, value in record_data.items():
            if feature_name not in feature_definitions:
                continue
            
            formatted_record.append({
                'FeatureName': feature_name,
                'ValueAsString': str(value)
            })
            
            # Try to identify record identifier
            if not record_id and feature_name.lower() in ['record_id', 'id', 'record_identifier']:
                record_id = str(value)
        
        if not record_id:
            # Use first feature value as record_id for tracking
            record_id = str(next(iter(record_data.values()))) if record_data else 'unknown'
        
        if not formatted_record:
            return {'record_id': record_id, 'error': 'No valid features found'}
        
        # Put the record
        response = config.featurestore_runtime.put_record(
            FeatureGroupName=feature_group_name,
            Record=formatted_record
        )
        
        return {
            'record_id': record_id,
            'status': 'success',
            'request_id': response.get('ResponseMetadata', {}).get('RequestId')
        }
        
    except ClientError as e:
        return {'record_id': record_id or 'unknown', 'error': str(e)}
    except Exception as e:
        return {'record_id': record_id or 'unknown', 'error': f'Unexpected error: {str(e)}'}


def bulk_put_records(config: Config, feature_group_name: str, input_file: str, output_file: Optional[str] = None) -> None:
    """Bulk put records to feature group using input file"""
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
        
        # Get feature group details to understand the schema
        fg_details = config.sagemaker.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        
        # Check if online store is enabled
        if not fg_details.get('OnlineStoreConfig'):
            click.echo(f"피처 그룹 '{feature_group_name}'에 온라인 스토어가 활성화되어 있지 않습니다", err=True)
            raise click.Abort()
        
        # Prepare feature definitions
        feature_definitions = {fd['FeatureName']: fd for fd in fg_details['FeatureDefinitions']}
        
        # Validate input data
        valid_records = []
        for i, record in enumerate(input_data):
            if not isinstance(record, dict):
                click.echo(f"경고: 레코드 {i+1}이 딕셔너리가 아닙니다. 건너똙니다", err=True)
                continue
            if not record:
                click.echo(f"경고: 레코드 {i+1}이 비어있습니다. 건너똙니다", err=True)
                continue
            valid_records.append(record)
        
        if not valid_records:
            click.echo("입력 파일에서 유효한 레코드를 찾을 수 없습니다", err=True)
            raise click.Abort()
        
        click.echo(f"{len(valid_records)}개 레코드 처리 중...")
        
        # Perform bulk put operations with threading for better performance
        results = []
        max_workers = min(10, len(valid_records))  # Limit concurrent requests
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_record = {
                executor.submit(put_single_record, config, feature_group_name, 
                               record, feature_definitions): record 
                for record in valid_records
            }
            
            # Collect results
            completed = 0
            for future in as_completed(future_to_record):
                result = future.result()
                results.append(result)
                completed += 1
                
                if completed % 10 == 0 or completed == len(valid_records):
                    click.echo(f"완료: {completed}/{len(valid_records)}")
        
        # Analyze results
        successful_records = [r for r in results if r.get('status') == 'success']
        error_records = [r for r in results if 'error' in r]
        
        # Report results
        click.echo(f"\n대량 입력 작업 완료:")
        click.echo(f"  - 성공: {len(successful_records)}")
        click.echo(f"  - 실패: {len(error_records)}")
        
        if error_records:
            click.echo(f"\n처음 {min(5, len(error_records))}개 오류:")
            for error_record in error_records[:5]:
                click.echo(f"  - Record {error_record.get('record_id', 'unknown')}: {error_record['error']}")
            
            if len(error_records) > 5:
                click.echo(f"  ... 그리고 {len(error_records) - 5}개 더 많은 오류")
        
        if successful_records:
            click.echo(f"\n피처 그룹 '{feature_group_name}'에 {len(successful_records)}개 레코드가 성공적으로 저장되었습니다")
        else:
            click.echo("성공적으로 저장된 레코드가 없습니다", err=True)
            raise click.Abort()
        
        # Save results to output file if specified
        if output_file:
            try:
                result_summary = {
                    'feature_group_name': feature_group_name,
                    'input_file': input_file,
                    'total_records': len(valid_records),
                    'successful_records': len(successful_records),
                    'failed_records': len(error_records),
                    'success_details': successful_records,
                    'error_details': error_records
                }
                
                FileHandler.write_file([result_summary], output_file)
                click.echo(f"결과가 '{output_file}'에 저장되었습니다")
                
            except Exception as e:
                click.echo(f"출력 파일 쓰기 오류: {e}", err=True)
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFound':
            click.echo(f"피처 그룹 '{feature_group_name}'을 찾을 수 없습니다", err=True)
        elif error_code == 'ValidationException':
            click.echo(f"유효성 검사 오류: {e.response['Error']['Message']}", err=True)
        else:
            click.echo(f"AWS 오류: {e}", err=True)
        raise click.Abort()
    except Exception as e:
        click.echo(f"예상치 못한 오류: {e}", err=True)
        raise click.Abort()