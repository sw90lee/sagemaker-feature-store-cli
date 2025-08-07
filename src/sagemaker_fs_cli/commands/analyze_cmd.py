"""Analyze command implementation"""

import click
import pandas as pd
from datetime import datetime, timezone
from collections import defaultdict
from typing import Optional, Dict, Any
from botocore.exceptions import ClientError
from ..config import Config
from ..utils.formatter import OutputFormatter


class FeatureStoreAnalyzer:
    def __init__(self, config: Config):
        self.config = config
        self.s3 = config.s3
        self.sagemaker = config.sagemaker
        
    def get_feature_group_s3_location(self, feature_group_name: str):
        """Get S3 location for a specific feature group"""
        try:
            response = self.sagemaker.describe_feature_group(
                FeatureGroupName=feature_group_name
            )
            
            offline_config = response.get('OfflineStoreConfig', {})
            s3_uri = offline_config.get('S3StorageConfig', {}).get('S3Uri', '')
            
            if s3_uri:
                if s3_uri.startswith('s3://'):
                    s3_uri = s3_uri[5:]
                parts = s3_uri.split('/', 1)
                bucket = parts[0]
                prefix = parts[1] if len(parts) > 1 else ''
                return bucket, prefix
            
            return None, None
            
        except ClientError as e:
            click.echo(f"피처 그룹 {feature_group_name} 정보를 가져올 수 없습니다: {e}", err=True)
            return None, None
    
    def calculate_storage_cost(self, size_gb: float, storage_class: str = 'STANDARD') -> float:
        """Estimate monthly storage cost based on AWS S3 pricing"""
        pricing = {
            'STANDARD': 0.023,
            'STANDARD_IA': 0.0125,
            'GLACIER': 0.004,
            'DEEP_ARCHIVE': 0.00099
        }
        
        cost_per_gb = pricing.get(storage_class, pricing['STANDARD'])
        return size_gb * cost_per_gb
    
    def analyze_feature_store_storage(self, bucket: str, prefix: str, feature_group_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Analyze Feature Store storage with detailed statistics"""
        click.echo(f"\n{'='*60}")
        click.echo(f"Feature Store 분석: s3://{bucket}/{prefix}")
        if feature_group_name:
            click.echo(f"피처 그룹: {feature_group_name}")
        click.echo(f"{'='*60}\n")
        
        objects = []
        file_types = defaultdict(lambda: {'count': 0, 'size': 0})
        
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if '.' in key:
                            ext = key.rsplit('.', 1)[1].lower()
                            file_types[ext]['count'] += 1
                            file_types[ext]['size'] += obj['Size']
                        
                        objects.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'storage_class': obj.get('StorageClass', 'STANDARD')
                        })
        
        except ClientError as e:
            click.echo(f"S3 액세스 오류: {e}", err=True)
            return None
        
        if not objects:
            click.echo("지정된 위치에서 파일을 찾을 수 없습니다.")
            return None
        
        # Create DataFrame
        df = pd.DataFrame(objects)
        df['date'] = pd.to_datetime(df['last_modified']).dt.date
        df['month'] = pd.to_datetime(df['last_modified']).dt.to_period('M')
        
        # Calculate statistics
        total_size_bytes = df['size'].sum()
        total_size_mb = total_size_bytes / (1024 * 1024)
        total_size_gb = total_size_mb / 1024
        total_size_tb = total_size_gb / 1024
        total_files = len(df)
        
        # Overall statistics
        click.echo("📊 전체 통계")
        click.echo(f"{'─'*40}")
        click.echo(f"총 파일 수: {total_files:,}")
        click.echo(f"총 용량:")
        click.echo(f"  - Bytes: {total_size_bytes:,}")
        click.echo(f"  - MB: {total_size_mb:,.2f}")
        click.echo(f"  - GB: {total_size_gb:,.2f}")
        if total_size_tb >= 0.01:
            click.echo(f"  - TB: {total_size_tb:,.2f}")
        
        # File type breakdown
        click.echo(f"\n📁 파일 유형별 분석")
        click.echo(f"{'─'*40}")
        click.echo(f"{'유형':<15} {'개수':>10} {'크기(MB)':>15} {'비율':>15}")
        click.echo(f"{'─'*40}")
        
        for ext, stats in sorted(file_types.items(), key=lambda x: x[1]['size'], reverse=True):
            size_mb = stats['size'] / (1024 * 1024)
            percentage = (stats['size'] / total_size_bytes) * 100
            click.echo(f"{ext:<15} {stats['count']:>10,} {size_mb:>15,.2f} {percentage:>14.1f}%")
        
        # Storage class analysis
        storage_class_stats = df.groupby('storage_class').agg({
            'size': ['sum', 'count']
        }).reset_index()
        storage_class_stats.columns = ['storage_class', 'total_size', 'file_count']
        
        click.echo(f"\n💾 스토리지 클래스별 분석")
        click.echo(f"{'─'*40}")
        click.echo(f"{'클래스':<20} {'파일수':>10} {'크기(GB)':>15}")
        click.echo(f"{'─'*40}")
        
        for _, row in storage_class_stats.iterrows():
            size_gb = row['total_size'] / (1024**3)
            click.echo(f"{row['storage_class']:<20} {row['file_count']:>10,} {size_gb:>15,.2f}")
        
        # Monthly growth analysis
        monthly_stats = df.groupby('month').agg({
            'size': ['sum', 'count']
        }).reset_index()
        monthly_stats.columns = ['month', 'total_size', 'file_count']
        monthly_stats['size_gb'] = monthly_stats['total_size'] / (1024**3)
        monthly_stats['cumulative_size_gb'] = monthly_stats['size_gb'].cumsum()
        
        click.echo(f"\n📈 월별 증가 분석")
        click.echo(f"{'─'*60}")
        click.echo(f"{'월':<12} {'파일수':>10} {'크기(GB)':>12} {'누적(GB)':>18}")
        click.echo(f"{'─'*60}")
        
        for _, row in monthly_stats.iterrows():
            month_str = str(row['month'])
            click.echo(f"{month_str:<12} {row['file_count']:>10,} {row['size_gb']:>12,.2f} {row['cumulative_size_gb']:>18,.2f}")
        
        # Recent activity
        recent_files = df.nlargest(10, 'last_modified')[['key', 'size', 'last_modified']]
        click.echo(f"\n🕐 최근 활동 (최근 10개 파일)")
        click.echo(f"{'─'*80}")
        click.echo(f"{'파일명':<50} {'크기(MB)':>15} {'수정시간':>15}")
        click.echo(f"{'─'*80}")
        
        for _, row in recent_files.iterrows():
            file_name = row['key'].split('/')[-1]
            if len(file_name) > 47:
                file_name = file_name[:44] + '...'
            size_mb = row['size'] / (1024 * 1024)
            mod_time = row['last_modified'].strftime('%Y-%m-%d %H:%M')
            click.echo(f"{file_name:<50} {size_mb:>15,.2f} {mod_time:>15}")
        
        # Cost estimation
        click.echo(f"\n💰 월간 예상 스토리지 비용")
        click.echo(f"{'─'*40}")
        
        total_monthly_cost = 0
        for storage_class in df['storage_class'].unique():
            class_size_gb = df[df['storage_class'] == storage_class]['size'].sum() / (1024**3)
            monthly_cost = self.calculate_storage_cost(class_size_gb, storage_class)
            total_monthly_cost += monthly_cost
            click.echo(f"{storage_class}: ${monthly_cost:,.2f}")
        
        click.echo(f"{'─'*40}")
        click.echo(f"총 월간 비용: ${total_monthly_cost:,.2f}")
        click.echo(f"연간 비용 예상: ${total_monthly_cost * 12:,.2f}")
        
        # Return summary data
        return {
            'bucket': bucket,
            'prefix': prefix,
            'feature_group': feature_group_name,
            'total_files': total_files,
            'total_size_bytes': total_size_bytes,
            'total_size_gb': total_size_gb,
            'file_types': dict(file_types),
            'monthly_stats': monthly_stats.to_dict('records'),
            'estimated_monthly_cost': total_monthly_cost,
            'analysis_timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    def export_to_csv(self, data: Dict[str, Any], output_file: str):
        """Export analysis results to CSV"""
        summary_data = {
            'Metric': [
                'Feature Group',
                'S3 Location',
                'Total Files',
                'Total Size (GB)',
                'Estimated Monthly Cost (USD)',
                'Analysis Date'
            ],
            'Value': [
                data.get('feature_group', 'N/A'),
                f"s3://{data['bucket']}/{data['prefix']}",
                f"{data['total_files']:,}",
                f"{data['total_size_gb']:.2f}",
                f"${data['estimated_monthly_cost']:.2f}",
                data['analysis_timestamp']
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(output_file, index=False)
        
        # Monthly stats
        if data.get('monthly_stats'):
            monthly_df = pd.DataFrame(data['monthly_stats'])
            monthly_file = output_file.replace('.csv', '_monthly.csv')
            monthly_df.to_csv(monthly_file, index=False)
        
        click.echo(f"\n📄 결과를 다음 파일로 내보냈습니다: {output_file}")


def analyze_feature_store(config: Config, feature_group_name: Optional[str], 
                         bucket: Optional[str], prefix: Optional[str], 
                         export_file: Optional[str], output_format: str) -> None:
    """Analyze Feature Store storage usage and costs"""
    try:
        analyzer = FeatureStoreAnalyzer(config)
        
        # Determine bucket and prefix
        if feature_group_name:
            bucket, prefix = analyzer.get_feature_group_s3_location(feature_group_name)
            if not bucket:
                click.echo(f"피처 그룹 '{feature_group_name}'의 S3 위치를 찾을 수 없습니다", err=True)
                raise click.Abort()
        elif not bucket or not prefix:
            click.echo("--feature-group 또는 --bucket과 --prefix를 모두 제공해야 합니다", err=True)
            raise click.Abort()
        
        # Analyze storage
        results = analyzer.analyze_feature_store_storage(bucket, prefix, feature_group_name)
        
        if not results:
            raise click.Abort()
        
        # Export if requested
        if export_file:
            analyzer.export_to_csv(results, export_file)
        
        # JSON output if requested
        if output_format == 'json':
            click.echo(f"\n📋 JSON 결과:")
            output = OutputFormatter.format_json(results)
            click.echo(output)
        
    except ClientError as e:
        click.echo(f"AWS 오류: {e}", err=True)
        raise click.Abort()
    except Exception as e:
        click.echo(f"예상치 못한 오류: {e}", err=True)
        raise click.Abort()