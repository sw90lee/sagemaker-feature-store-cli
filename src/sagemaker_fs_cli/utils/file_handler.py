"""File handling utilities for JSON and CSV operations"""

import json
import csv
import pandas as pd
from typing import List, Dict, Any, Union
from pathlib import Path


class FileHandler:
    @staticmethod
    def read_json(file_path: str) -> List[Dict[str, Any]]:
        """Read JSON file and return list of records"""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            else:
                raise ValueError("JSON 파일은 딕셔너리 또는 딕셔너리 목록을 포함해야 합니다")
    
    @staticmethod
    def read_csv(file_path: str) -> List[Dict[str, Any]]:
        """Read CSV file and return list of records"""
        df = pd.read_csv(file_path)
        return df.to_dict('records')
    
    @staticmethod
    def write_json(data: List[Dict[str, Any]], file_path: str) -> None:
        """Write data to JSON file"""
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    
    @staticmethod
    def write_csv(data: List[Dict[str, Any]], file_path: str) -> None:
        """Write data to CSV file"""
        if not data:
            return
        
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
    
    @staticmethod
    def detect_file_type(file_path: str) -> str:
        """Detect file type based on extension"""
        suffix = Path(file_path).suffix.lower()
        if suffix == '.json':
            return 'json'
        elif suffix == '.csv':
            return 'csv'
        else:
            raise ValueError(f"지원하지 않는 파일 형식: {suffix}. .json과 .csv만 지원됩니다.")
    
    @staticmethod
    def read_file(file_path: str) -> List[Dict[str, Any]]:
        """Read file based on its type"""
        file_type = FileHandler.detect_file_type(file_path)
        if file_type == 'json':
            return FileHandler.read_json(file_path)
        elif file_type == 'csv':
            return FileHandler.read_csv(file_path)
    
    @staticmethod
    def write_file(data: List[Dict[str, Any]], file_path: str) -> None:
        """Write data to file based on file type"""
        file_type = FileHandler.detect_file_type(file_path)
        if file_type == 'json':
            FileHandler.write_json(data, file_path)
        elif file_type == 'csv':
            FileHandler.write_csv(data, file_path)