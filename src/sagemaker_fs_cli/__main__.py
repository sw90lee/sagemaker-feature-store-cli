"""Entry point for the SageMaker FeatureStore CLI when run as a module"""

import sys
import os

# PyInstaller 환경에서 모듈 경로 설정
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    # PyInstaller로 패키징된 환경
    sys.path.insert(0, sys._MEIPASS)

try:
    from sagemaker_fs_cli.cli import cli
except ImportError:
    # 개발 환경에서는 상대 import 사용
    from .cli import cli

if __name__ == '__main__':
    cli()