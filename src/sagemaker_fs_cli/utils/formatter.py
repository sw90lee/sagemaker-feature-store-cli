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
        """Format feature groups list"""
        headers = ['FeatureGroupName', 'FeatureGroupStatus', 'OnlineStoreConfig', 'CreationTime']
        formatted_data = []
        
        for fg in feature_groups:
            formatted_data.append({
                'FeatureGroupName': fg.get('FeatureGroupName', ''),
                'FeatureGroupStatus': fg.get('FeatureGroupStatus', ''),
                'OnlineStoreConfig': '활성화됨' if fg.get('OnlineStoreConfig') else '비활성화됨',
                'CreationTime': fg.get('CreationTime', '')
            })
        
        return OutputFormatter.format_table(formatted_data, headers)