"""Output formatting utilities"""

import json
from typing import List, Dict, Any
from tabulate import tabulate


class OutputFormatter:
    @staticmethod
    def format_table(data: List[Dict[str, Any]], headers: List[str] = None) -> str:
        """Format data as a table"""
        if not data:
            return "표시할 데이터가 없습니다"
        
        if headers is None:
            headers = list(data[0].keys()) if data else []
        
        rows = []
        for item in data:
            row = [str(item.get(header, '')) for header in headers]
            rows.append(row)
        
        return tabulate(rows, headers=headers, tablefmt='grid')
    
    @staticmethod
    def format_json(data: Any, indent: int = 2) -> str:
        """Format data as JSON"""
        return json.dumps(data, indent=indent, ensure_ascii=False, default=str)
    
    @staticmethod
    def format_feature_groups(feature_groups: List[Dict[str, Any]]) -> str:
        """Format feature groups list with detailed information"""
        headers = [
            'FeatureGroupName', 
            'Status', 
            'IngestMode',
            'StorageType',
            'TTL',
            'EventTimeFeature',
            'RecordIdFeature',
            'TableFormat',
            'AthenaTable',
            'CreationTime'
        ]
        formatted_data = []
        
        for fg in feature_groups:
            formatted_data.append({
                'FeatureGroupName': fg.get('FeatureGroupName', ''),
                'Status': fg.get('FeatureGroupStatus', ''),
                'IngestMode': fg.get('IngestMode', 'Unknown'),
                'StorageType': fg.get('StorageType', 'Standard'),
                'TTL': fg.get('TTLValue', 'N/A'),
                'EventTimeFeature': fg.get('EventTimeFeatureName', 'N/A'),
                'RecordIdFeature': fg.get('RecordIdentifierFeatureName', 'N/A'),
                'TableFormat': fg.get('TableFormat', 'N/A'),
                'AthenaTable': fg.get('AthenaTable', 'N/A'),
                'CreationTime': fg.get('CreationTime', '')
            })
        
        return OutputFormatter.format_table(formatted_data, headers)