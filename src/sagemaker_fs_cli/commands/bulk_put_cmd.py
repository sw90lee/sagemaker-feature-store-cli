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


def _put_single_formatted_record(config: Config, feature_group_name: str, formatted_record: List[Dict]) -> Dict[str, Any]:
    """Put a single formatted record (helper for batch processing)"""
    response = config.featurestore_runtime.put_record(
        FeatureGroupName=feature_group_name,
        Record=formatted_record
    )
    return response


def bulk_put_records(config: Config, feature_group_name: str, input_file: str, 
                    output_file: Optional[str] = None, batch_size: int = 100) -> None:
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
        
        click.echo(f"{len(valid_records)}개 레코드를 배치 크기 {batch_size}로 처리 중...")
        
        # Process records in batches using batch_put_record
        all_results = []
        total_batches = (len(valid_records) + batch_size - 1) // batch_size
        
        for batch_idx in range(0, len(valid_records), batch_size):
            batch_records = valid_records[batch_idx:batch_idx + batch_size]
            batch_num = (batch_idx // batch_size) + 1
            
            click.echo(f"배치 {batch_num}/{total_batches} 처리 중... ({len(batch_records)}개 레코드)")
            
            # Format records for batch_put_record
            formatted_records = []
            for record in batch_records:
                formatted_record = []
                for feature_name, value in record.items():
                    if feature_name in feature_definitions:
                        formatted_record.append({
                            'FeatureName': feature_name,
                            'ValueAsString': str(value)
                        })
                
                if formatted_record:  # Only add if we have valid features
                    formatted_records.append(formatted_record)
            
            if not formatted_records:
                click.echo(f"배치 {batch_num}에서 유효한 레코드가 없습니다. 건너뜀")
                continue
            
            # Execute batch put using ThreadPool for parallel processing
            try:
                batch_results = []
                max_workers = min(20, len(formatted_records))  # Increased from 10 to 20
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit all tasks for this batch
                    future_to_idx = {
                        executor.submit(_put_single_formatted_record, config, feature_group_name, record): (idx, batch_idx + idx)
                        for idx, record in enumerate(formatted_records)
                    }
                    
                    # Collect results
                    for future in as_completed(future_to_idx):
                        batch_idx_info, global_idx = future_to_idx[future]
                        try:
                            result = future.result()
                            batch_results.append({
                                'record_id': f'batch_{batch_num}_record_{batch_idx_info+1}',
                                'global_index': global_idx,
                                'status': 'success'
                            })
                        except Exception as e:
                            batch_results.append({
                                'record_id': f'batch_{batch_num}_record_{batch_idx_info+1}',
                                'global_index': global_idx,
                                'error': str(e)
                            })
                
                all_results.extend(batch_results)
                
                # Progress update
                completed_records = min(batch_idx + batch_size, len(valid_records))
                click.echo(f"완료: {completed_records}/{len(valid_records)}")
                
            except Exception as e:
                error_msg = f"배치 {batch_num} 처리 중 오류: {str(e)}"
                click.echo(error_msg, err=True)
                # Add error results for all records in this batch
                for idx in range(len(batch_records)):
                    all_results.append({
                        'record_id': f'batch_{batch_num}_record_{idx+1}',
                        'error': error_msg
                    })
        
        # Analyze results
        successful_records = [r for r in all_results if r.get('status') == 'success']
        error_records = [r for r in all_results if 'error' in r]
        
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
                # Get successful input records for bulk-get style output
                successful_input_records = []
                for result in all_results:
                    if result.get('status') == 'success' and 'global_index' in result:
                        # Use the global index to get the corresponding input record
                        global_idx = result['global_index']
                        if global_idx < len(valid_records):
                            successful_input_records.append(valid_records[global_idx])
                
                # Create comprehensive result with both summary and data
                result_output = {
                    'summary': {
                        'feature_group_name': feature_group_name,
                        'input_file': input_file,
                        'total_records': len(valid_records),
                        'successful_records': len(successful_records),
                        'failed_records': len(error_records),
                        'success_details': successful_records,
                        'error_details': error_records
                    },
                    'data': successful_input_records
                }
                
                FileHandler.write_file([result_output], output_file)
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