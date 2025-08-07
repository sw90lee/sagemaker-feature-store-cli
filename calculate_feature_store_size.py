#!/usr/bin/env python3
"""
피처스토어 용량 분석 스크립트 (이슈 #22)

관련 이슈들:
- #17: 피처스토어 운영 개선 및 최적화 계획 수립 (상위 Epic)
- #22: 피처스토어 용량 분석 (현재 이슈) - CLOSED
- #23: 피처 데이터 조회 및 접근성 개선 - Phase2에서 캐싱 및 인덱싱 최적화 가능
- #24: 라벨링 변경 반영 프로세스 최적화 - Phase2에서 비동기 처리 및 성능 개선 가능
- #25: 피처스토어 메타데이터 관리 체계 구축 - Phase2에서 메타데이터 API 및 변경 로그 시스템 구현 가능
- #26: 피처 데이터 라이프사이클 관리 자동화 - Phase2에서 자동화 시스템 및 대시보드 구현 가능

Phase2 가능성 평가:
✅ #23 (데이터 조회 최적화): MVP 접근 시 낮은 복잡도 - 온라인/오프라인 하이브리드로 1주 내 구현 가능
   고도화: 스마트 캐싱, 고급 인덱싱, 사용 패턴 기반 최적화 등은 추후 단계에서 확장
✅ #24 (라벨링 프로세스 최적화): MVP 접근 시 낮은 복잡도 - 온라인 피처스토어 직접 연결로 3-5일 내 구현 가능 (#23과 연계)
   고도화: 고급 모니터링, 충돌 해결, 복잡한 비동기 처리 등은 추후 단계에서 확장
✅ #25 (메타데이터 관리): MVP 접근 시 낮은 복잡도 - 기존 테이블 확장으로 2-3일 내 구현 가능
   고도화: 별도 테이블 설계, 고급 검색/필터링, 대시보드 등은 추후 단계에서 확장
✅ #26 (라이프사이클 자동화): MVP 접근 시 낮은 복잡도 - 피처그룹 생성 시 TTL 설정으로 기본 요건 충족 가능
   고도화: 사용 패턴 분석, 중요도 분류, 대시보드 등은 추후 단계에서 확장

전체 권장사항: Phase2에서 모든 이슈 구현 가능 (모두 MVP 접근법 적용). 
MVP 우선순위: #26(TTL설정) → #25(테이블 확장) → #23,#24(온라인 피처스토어 연계, 동시 구현) 순서 권장.
특히 #23,#24는 하나의 온라인 피처스토어 아키텍처로 두 문제를 동시 해결하여 시너지 극대화 가능.
"""
import boto3
import argparse
from datetime import datetime, timezone
import pandas as pd
import json
from collections import defaultdict
import sys

class FeatureStoreAnalyzer:
    def __init__(self, region='us-east-1'):
        self.s3 = boto3.client('s3', region_name=region)
        self.sagemaker = boto3.client('sagemaker', region_name=region)
        self.region = region
        
    def list_feature_groups(self):
        """List all feature groups in the account"""
        feature_groups = []
        paginator = self.sagemaker.get_paginator('list_feature_groups')
        
        for page in paginator.paginate():
            for fg in page['FeatureGroupSummaries']:
                feature_groups.append({
                    'name': fg['FeatureGroupName'],
                    'creation_time': fg['CreationTime'],
                    'status': fg['FeatureGroupStatus']
                })
        
        return feature_groups
    
    def get_feature_group_s3_location(self, feature_group_name):
        """Get S3 location for a specific feature group"""
        try:
            response = self.sagemaker.describe_feature_group(
                FeatureGroupName=feature_group_name
            )
            
            # Extract offline store config
            offline_config = response.get('OfflineStoreConfig', {})
            s3_uri = offline_config.get('S3StorageConfig', {}).get('S3Uri', '')
            
            if s3_uri:
                # Parse S3 URI to get bucket and prefix
                if s3_uri.startswith('s3://'):
                    s3_uri = s3_uri[5:]
                parts = s3_uri.split('/', 1)
                bucket = parts[0]
                prefix = parts[1] if len(parts) > 1 else ''
                return bucket, prefix
            
            return None, None
            
        except Exception as e:
            print(f"Error getting feature group info for {feature_group_name}: {str(e)}")
            return None, None
    
    def calculate_storage_cost(self, size_gb, storage_class='STANDARD'):
        """Estimate monthly storage cost based on AWS S3 pricing"""
        # Pricing per GB per month (approximate, varies by region)
        pricing = {
            'STANDARD': 0.023,
            'STANDARD_IA': 0.0125,
            'GLACIER': 0.004,
            'DEEP_ARCHIVE': 0.00099
        }
        
        cost_per_gb = pricing.get(storage_class, pricing['STANDARD'])
        return size_gb * cost_per_gb
    
    def analyze_feature_store_storage(self, bucket, prefix, feature_group_name=None):
        """
        Analyze Feature Store storage with detailed statistics
        """
        print(f"\n{'='*60}")
        print(f"Analyzing Feature Store: s3://{bucket}/{prefix}")
        if feature_group_name:
            print(f"Feature Group: {feature_group_name}")
        print(f"{'='*60}\n")
        
        objects = []
        file_types = defaultdict(lambda: {'count': 0, 'size': 0})
        
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Analyze file types
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
        
        except Exception as e:
            print(f"Error accessing S3: {str(e)}")
            return None
        
        if not objects:
            print("No files found in the specified location.")
            return None
        
        # Create DataFrame
        df = pd.DataFrame(objects)
        
        # Extract date/time information
        df['date'] = pd.to_datetime(df['last_modified']).dt.date
        df['hour'] = pd.to_datetime(df['last_modified']).dt.hour
        df['month'] = pd.to_datetime(df['last_modified']).dt.to_period('M')
        
        # Calculate statistics
        total_size_bytes = df['size'].sum()
        total_size_mb = total_size_bytes / (1024 * 1024)
        total_size_gb = total_size_mb / 1024
        total_size_tb = total_size_gb / 1024
        total_files = len(df)
        
        # Overall statistics
        print("📊 OVERALL STATISTICS")
        print(f"{'─'*40}")
        print(f"Total Files: {total_files:,}")
        print(f"Total Size:")
        print(f"  - Bytes: {total_size_bytes:,}")
        print(f"  - MB: {total_size_mb:,.2f}")
        print(f"  - GB: {total_size_gb:,.2f}")
        if total_size_tb >= 0.01:
            print(f"  - TB: {total_size_tb:,.2f}")
        
        # File type breakdown
        print(f"\n📁 FILE TYPE BREAKDOWN")
        print(f"{'─'*40}")
        print(f"{'Type':<15} {'Count':>10} {'Size (MB)':>15} {'% of Total':>15}")
        print(f"{'─'*40}")
        
        for ext, stats in sorted(file_types.items(), key=lambda x: x[1]['size'], reverse=True):
            size_mb = stats['size'] / (1024 * 1024)
            percentage = (stats['size'] / total_size_bytes) * 100
            print(f"{ext:<15} {stats['count']:>10,} {size_mb:>15,.2f} {percentage:>14.1f}%")
        
        # Storage class analysis
        storage_class_stats = df.groupby('storage_class').agg({
            'size': ['sum', 'count']
        }).reset_index()
        storage_class_stats.columns = ['storage_class', 'total_size', 'file_count']
        
        print(f"\n💾 STORAGE CLASS DISTRIBUTION")
        print(f"{'─'*40}")
        print(f"{'Class':<20} {'Files':>10} {'Size (GB)':>15}")
        print(f"{'─'*40}")
        
        for _, row in storage_class_stats.iterrows():
            size_gb = row['total_size'] / (1024**3)
            print(f"{row['storage_class']:<20} {row['file_count']:>10,} {size_gb:>15,.2f}")
        
        # Monthly growth analysis
        monthly_stats = df.groupby('month').agg({
            'size': ['sum', 'count'],
            'last_modified': 'max'
        }).reset_index()
        monthly_stats.columns = ['month', 'total_size', 'file_count', 'latest_update']
        monthly_stats['size_gb'] = monthly_stats['total_size'] / (1024**3)
        monthly_stats['cumulative_size_gb'] = monthly_stats['size_gb'].cumsum()
        
        print(f"\n📈 MONTHLY GROWTH ANALYSIS")
        print(f"{'─'*60}")
        print(f"{'Month':<12} {'Files':>10} {'Size (GB)':>12} {'Cumulative (GB)':>18}")
        print(f"{'─'*60}")
        
        for _, row in monthly_stats.iterrows():
            month_str = str(row['month'])
            print(f"{month_str:<12} {row['file_count']:>10,} {row['size_gb']:>12,.2f} {row['cumulative_size_gb']:>18,.2f}")
        
        # Recent activity
        recent_files = df.nlargest(10, 'last_modified')[['key', 'size', 'last_modified']]
        print(f"\n🕐 RECENT ACTIVITY (Last 10 files)")
        print(f"{'─'*80}")
        print(f"{'File':<50} {'Size (MB)':>15} {'Modified':>15}")
        print(f"{'─'*80}")
        
        for _, row in recent_files.iterrows():
            file_name = row['key'].split('/')[-1]
            if len(file_name) > 47:
                file_name = file_name[:44] + '...'
            size_mb = row['size'] / (1024 * 1024)
            mod_time = row['last_modified'].strftime('%Y-%m-%d %H:%M')
            print(f"{file_name:<50} {size_mb:>15,.2f} {mod_time:>15}")
        
        # Cost estimation
        print(f"\n💰 ESTIMATED MONTHLY STORAGE COSTS")
        print(f"{'─'*40}")
        
        for storage_class in df['storage_class'].unique():
            class_size_gb = df[df['storage_class'] == storage_class]['size'].sum() / (1024**3)
            monthly_cost = self.calculate_storage_cost(class_size_gb, storage_class)
            print(f"{storage_class}: ${monthly_cost:,.2f}")
        
        total_monthly_cost = sum(
            self.calculate_storage_cost(
                df[df['storage_class'] == sc]['size'].sum() / (1024**3), 
                sc
            ) for sc in df['storage_class'].unique()
        )
        print(f"{'─'*40}")
        print(f"Total Monthly Cost: ${total_monthly_cost:,.2f}")
        print(f"Annual Cost Projection: ${total_monthly_cost * 12:,.2f}")
        
        # Return summary data for export
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
    
    def export_to_csv(self, data, output_file):
        """Export analysis results to CSV"""
        # Create summary DataFrame
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
        
        # Export to CSV
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            # Summary sheet
            summary_df.to_csv(output_file.replace('.xlsx', '_summary.csv'), index=False)
            
            # Monthly stats
            if data.get('monthly_stats'):
                monthly_df = pd.DataFrame(data['monthly_stats'])
                monthly_df.to_csv(output_file.replace('.xlsx', '_monthly.csv'), index=False)
        
        print(f"\n📄 Results exported to: {output_file.replace('.xlsx', '_*.csv')}")

def main():
    parser = argparse.ArgumentParser(
        description='Analyze SageMaker Feature Store storage usage and costs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze specific S3 location
  python calculate_feature_store_size.py --bucket my-bucket --prefix path/to/feature-store
  
  # Analyze specific feature group
  python calculate_feature_store_size.py --feature-group my-feature-group
  
  # List all feature groups
  python calculate_feature_store_size.py --list-feature-groups
  
  # Export results to CSV
  python calculate_feature_store_size.py --bucket my-bucket --prefix path --export report.csv
        """
    )
    
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--prefix', help='S3 prefix path to Feature Store data')
    parser.add_argument('--feature-group', help='Feature Group name to analyze')
    parser.add_argument('--list-feature-groups', action='store_true', 
                       help='List all feature groups in the account')
    parser.add_argument('--region', default='us-east-1', help='AWS region (default: us-east-1)')
    parser.add_argument('--export', help='Export results to CSV file')
    
    args = parser.parse_args()
    
    # Initialize analyzer
    analyzer = FeatureStoreAnalyzer(region=args.region)
    
    # List feature groups if requested
    if args.list_feature_groups:
        print("\n📋 FEATURE GROUPS IN ACCOUNT")
        print(f"{'─'*80}")
        print(f"{'Name':<40} {'Status':<15} {'Created':<25}")
        print(f"{'─'*80}")
        
        feature_groups = analyzer.list_feature_groups()
        for fg in feature_groups:
            created = fg['creation_time'].strftime('%Y-%m-%d %H:%M:%S')
            print(f"{fg['name']:<40} {fg['status']:<15} {created:<25}")
        
        print(f"\nTotal Feature Groups: {len(feature_groups)}")
        return
    
    # Determine bucket and prefix
    if args.feature_group:
        # Get S3 location from feature group
        bucket, prefix = analyzer.get_feature_group_s3_location(args.feature_group)
        if not bucket:
            print(f"Error: Could not find S3 location for feature group '{args.feature_group}'")
            sys.exit(1)
        feature_group_name = args.feature_group
    elif args.bucket and args.prefix:
        bucket = args.bucket
        prefix = args.prefix
        feature_group_name = None
    else:
        parser.error("Please provide either --feature-group or both --bucket and --prefix")
    
    # Analyze storage
    results = analyzer.analyze_feature_store_storage(bucket, prefix, feature_group_name)
    
    # Export if requested
    if results and args.export:
        analyzer.export_to_csv(results, args.export)

if __name__ == '__main__':
    main()