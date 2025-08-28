"""Bulk put command implementation for offline store"""

import click
import os
import time
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from urllib.parse import urlparse
from ..config import Config
from ..utils.file_handler import FileHandler


def bulk_put_records(config: Config, feature_group_name: str, input_file: str, 
                    output_file: Optional[str] = None, batch_size: int = 100) -> None:
    """Bulk put records to offline store (S3) using input file"""
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
        
        # Check if offline store is enabled
        if not fg_details.get('OfflineStoreConfig'):
            click.echo(f"피처 그룹 '{feature_group_name}'에 오프라인 스토어가 활성화되어 있지 않습니다", err=True)
            raise click.Abort()
        
        # Prepare feature definitions
        feature_definitions = {fd['FeatureName']: fd for fd in fg_details['FeatureDefinitions']}
        
        # Validate input data
        valid_records = []
        for i, record in enumerate(input_data):
            if not isinstance(record, dict):
                click.echo(f"경고: 레코드 {i+1}이 딕셔너리가 아닙니다. 건너뜀", err=True)
                continue
            if not record:
                click.echo(f"경고: 레코드 {i+1}이 비어있습니다. 건너뜀", err=True)
                continue
            valid_records.append(record)
        
        if not valid_records:
            click.echo("입력 파일에서 유효한 레코드를 찾을 수 없습니다", err=True)
            raise click.Abort()
        
        click.echo(f"{len(valid_records)}개 레코드를 배치 크기 {batch_size}로 S3에 업로드 중...")
        
        # Process records in batches and upload to S3
        all_results = []
        total_batches = (len(valid_records) + batch_size - 1) // batch_size
        
        # Get S3 configuration
        offline_config = fg_details['OfflineStoreConfig']
        s3_uri = offline_config['S3StorageConfig']['S3Uri']
        parsed_uri = urlparse(s3_uri)
        bucket_name = parsed_uri.netloc
        prefix = parsed_uri.path.lstrip('/')
        
        for batch_idx in range(0, len(valid_records), batch_size):
            batch_records = valid_records[batch_idx:batch_idx + batch_size]
            batch_num = (batch_idx // batch_size) + 1
            
            click.echo(f"배치 {batch_num}/{total_batches} 처리 중... ({len(batch_records)}개 레코드)")
            
            # Upload batch to S3
            try:
                batch_results = _upload_batch_to_s3(config, feature_group_name, batch_records, 
                                                   bucket_name, prefix, batch_num, feature_definitions)
                all_results.extend(batch_results)
                
                # Progress update
                completed_records = min(batch_idx + batch_size, len(valid_records))
                click.echo(f"완료: {completed_records}/{len(valid_records)}")
                
            except Exception as e:
                error_msg = f"배치 {batch_num} S3 업로드 중 오류: {str(e)}"
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
        click.echo(f"\n대량 업로드 작업 완료:")
        click.echo(f"  - 성공: {len(successful_records)}")
        click.echo(f"  - 실패: {len(error_records)}")
        
        if error_records:
            click.echo(f"\n처음 {min(5, len(error_records))}개 오류:")
            for error_record in error_records[:5]:
                click.echo(f"  - Record {error_record.get('record_id', 'unknown')}: {error_record['error']}")
            
            if len(error_records) > 5:
                click.echo(f"  ... 그리고 {len(error_records) - 5}개 더 많은 오류")
        
        if successful_records:
            click.echo(f"\n피처 그룹 '{feature_group_name}'의 오프라인 스토어(S3)에 {len(successful_records)}개 레코드가 성공적으로 업로드되었습니다")
        else:
            click.echo("성공적으로 업로드된 레코드가 없습니다", err=True)
            raise click.Abort()
        
        # Save results to output file if specified
        if output_file:
            try:
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
                    'data': valid_records[:len(successful_records)]  # Only successful records
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


def _upload_batch_to_s3(config: Config, feature_group_name: str, batch_records: List[Dict], 
                        bucket_name: str, prefix: str, batch_num: int, feature_definitions: Dict) -> List[Dict[str, Any]]:
    """Upload a batch of records to S3 in offline store format"""
    s3_client = config.session.client('s3')
    batch_results = []
    
    # Create a batch file
    timestamp = datetime.now().strftime('%Y/%m/%d/%H')
    batch_key = f"{prefix.rstrip('/')}/year={timestamp.split('/')[0]}/month={timestamp.split('/')[1]}/day={timestamp.split('/')[2]}/hour={timestamp.split('/')[3]}/batch_{batch_num}_{int(time.time())}.jsonl"
    
    # Prepare batch data as JSONL (JSON Lines)
    batch_data = []
    for idx, record in enumerate(batch_records):
        # Filter only valid features
        filtered_record = {k: str(v) for k, v in record.items() if k in feature_definitions}
        
        if filtered_record:  # Only add if we have valid features
            # Add EventTime if not present
            if 'EventTime' not in filtered_record:
                filtered_record['EventTime'] = str(int(time.time()))
            
            batch_data.append(json.dumps(filtered_record, ensure_ascii=False))
            batch_results.append({
                'record_id': f'batch_{batch_num}_record_{idx+1}',
                'status': 'success'
            })
        else:
            batch_results.append({
                'record_id': f'batch_{batch_num}_record_{idx+1}',
                'error': 'No valid features found'
            })
    
    if batch_data:
        # Upload to S3
        batch_content = '\n'.join(batch_data)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=batch_key,
            Body=batch_content.encode('utf-8'),
            ContentType='application/x-ndjson'
        )
        
        click.echo(f"S3 업로드 완료: s3://{bucket_name}/{batch_key}")
    
    return batch_results