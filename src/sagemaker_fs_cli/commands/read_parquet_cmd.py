"""Parquet file reading command for SageMaker FeatureStore CLI"""

import json
import os
import sys
from typing import Optional, Dict, Any, List
import click
import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError

from ..config import Config


def read_s3_parquet(config: Config, s3_path: str, save_path: Optional[str] = None) -> None:
    """Read parquet file from S3 path"""
    try:
        # Parse S3 path
        if not s3_path.startswith('s3://'):
            click.echo("❌ S3 경로는 s3://로 시작해야 합니다.", err=True)
            return
        
        # Remove s3:// prefix
        path_parts = s3_path[5:].split('/', 1)
        if len(path_parts) < 2:
            click.echo("❌ 올바른 S3 경로 형식이 아닙니다. 예: s3://bucket-name/path/file.parquet", err=True)
            return
        
        bucket_name = path_parts[0]
        object_key = path_parts[1]
        
        click.echo(f"📖 S3에서 Parquet 파일을 읽는 중: {s3_path}")
        
        # Create S3 client
        if config.profile:
            session = boto3.Session(profile_name=config.profile)
            s3_client = session.client('s3', region_name=config.region)
        else:
            s3_client = boto3.client('s3', region_name=config.region)
        
        # Download and read parquet file
        try:
            df = pd.read_parquet(s3_path)
        except Exception as e:
            click.echo(f"❌ Parquet 파일 읽기 실패: {str(e)}", err=True)
            return
        
        # Generate file analysis
        analysis = analyze_parquet_data(df, s3_path)
        
        # Output results
        if save_path:
            save_analysis_to_file(analysis, save_path)
            click.echo(f"✅ 분석 결과가 {save_path}에 저장되었습니다.")
        else:
            click.echo(json.dumps(analysis, indent=2, ensure_ascii=False, default=str))
            
    except NoCredentialsError:
        click.echo("❌ AWS 자격 증명을 찾을 수 없습니다.", err=True)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            click.echo(f"❌ 버킷을 찾을 수 없습니다: {bucket_name}", err=True)
        elif error_code == 'NoSuchKey':
            click.echo(f"❌ 파일을 찾을 수 없습니다: {object_key}", err=True)
        elif error_code == 'AccessDenied':
            click.echo("❌ 접근이 거부되었습니다. 권한을 확인해주세요.", err=True)
        else:
            click.echo(f"❌ S3 오류: {e.response['Error']['Message']}", err=True)
    except Exception as e:
        click.echo(f"❌ 알 수 없는 오류가 발생했습니다: {str(e)}", err=True)


def read_local_parquet(file_path: str, save_path: Optional[str] = None) -> None:
    """Read parquet file from local path"""
    try:
        if not os.path.exists(file_path):
            click.echo(f"❌ 파일을 찾을 수 없습니다: {file_path}", err=True)
            return
        
        if not file_path.lower().endswith('.parquet'):
            click.echo("❌ Parquet 파일(.parquet)만 지원됩니다.", err=True)
            return
        
        click.echo(f"📖 로컬 Parquet 파일을 읽는 중: {file_path}")
        
        # Read parquet file
        try:
            df = pd.read_parquet(file_path)
        except Exception as e:
            click.echo(f"❌ Parquet 파일 읽기 실패: {str(e)}", err=True)
            return
        
        # Generate file analysis
        analysis = analyze_parquet_data(df, file_path)
        
        # Output results
        if save_path:
            save_analysis_to_file(analysis, save_path)
            click.echo(f"✅ 분석 결과가 {save_path}에 저장되었습니다.")
        else:
            click.echo(json.dumps(analysis, indent=2, ensure_ascii=False, default=str))
            
    except Exception as e:
        click.echo(f"❌ 파일 읽기 실패: {str(e)}", err=True)


def analyze_parquet_data(df: pd.DataFrame, file_path: str) -> Dict[str, Any]:
    """Analyze parquet data and return detailed information"""
    analysis = {
        "file_info": {
            "file_path": file_path,
            "file_size_mb": round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns)
        },
        "schema": {},
        "data_overview": {},
        "record_id_info": {},
        "sample_data": {}
    }
    
    # Schema analysis
    for column in df.columns:
        dtype = str(df[column].dtype)
        null_count = int(df[column].isnull().sum())
        null_percentage = round((null_count / len(df)) * 100, 2)
        unique_count = int(df[column].nunique())
        
        analysis["schema"][column] = {
            "data_type": dtype,
            "null_count": null_count,
            "null_percentage": null_percentage,
            "unique_values": unique_count,
            "non_null_count": int(len(df) - null_count)
        }
        
        # Add sample values for each column
        non_null_values = df[column].dropna()
        if len(non_null_values) > 0:
            sample_values = non_null_values.head(3).tolist()
            # Convert datetime objects to strings for JSON serialization
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                sample_values = [str(val) for val in sample_values]
            analysis["schema"][column]["sample_values"] = sample_values
    
    # Data overview
    analysis["data_overview"] = {
        "total_memory_usage_mb": round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
        "has_duplicates": bool(df.duplicated().any()),
        "duplicate_count": int(df.duplicated().sum()),
        "columns_with_nulls": [col for col in df.columns if df[col].isnull().any()],
        "all_numeric_columns": [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])],
        "all_string_columns": [col for col in df.columns if pd.api.types.is_string_dtype(df[col]) or df[col].dtype == 'object'],
        "datetime_columns": [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    }
    
    # Record ID analysis - look for common record identifier columns
    record_id_candidates = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['id', 'key', 'identifier', 'record']):
            record_id_candidates.append(col)
    
    analysis["record_id_info"] = {
        "record_id_candidates": record_id_candidates,
        "details": {}
    }
    
    # Analyze potential record ID columns
    for col in record_id_candidates:
        unique_ratio = df[col].nunique() / len(df)
        analysis["record_id_info"]["details"][col] = {
            "unique_count": int(df[col].nunique()),
            "total_count": len(df),
            "unique_ratio": round(unique_ratio, 4),
            "is_likely_primary_key": unique_ratio == 1.0 and df[col].isnull().sum() == 0,
            "has_nulls": bool(df[col].isnull().any()),
            "sample_values": [str(val) for val in df[col].dropna().head(5).tolist()]
        }
    
    # Sample data (first 3 rows)
    try:
        sample_df = df.head(3).copy()
        # Convert datetime objects to strings for JSON serialization
        for col in sample_df.columns:
            if pd.api.types.is_datetime64_any_dtype(sample_df[col]):
                sample_df[col] = sample_df[col].astype(str)
        analysis["sample_data"] = {
            "first_3_records": sample_df.to_dict('records')
        }
    except Exception:
        analysis["sample_data"] = {
            "first_3_records": "샘플 데이터를 가져올 수 없습니다."
        }
    
    # Additional statistics for numeric columns
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        analysis["numeric_statistics"] = {}
        for col in numeric_cols:
            stats = df[col].describe()
            analysis["numeric_statistics"][col] = {
                "mean": round(float(stats['mean']), 4) if pd.notna(stats['mean']) else None,
                "std": round(float(stats['std']), 4) if pd.notna(stats['std']) else None,
                "min": float(stats['min']) if pd.notna(stats['min']) else None,
                "max": float(stats['max']) if pd.notna(stats['max']) else None,
                "median": round(float(stats['50%']), 4) if pd.notna(stats['50%']) else None
            }
    
    return analysis


def save_analysis_to_file(analysis: Dict[str, Any], file_path: str) -> None:
    """Save analysis results to file"""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False, default=str)
    except Exception as e:
        click.echo(f"❌ 파일 저장 실패: {str(e)}", err=True)
        raise


def read_parquet_file(config: Config, file_path: str, save_path: Optional[str] = None) -> None:
    """Main function to read parquet file (S3 or local)"""
    if file_path.startswith('s3://'):
        read_s3_parquet(config, file_path, save_path)
    else:
        read_local_parquet(file_path, save_path)