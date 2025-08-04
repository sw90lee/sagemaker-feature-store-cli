"""Standalone entry point for PyInstaller builds"""

import sys
import os

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# Import and run CLI
from sagemaker_fs_cli.cli import cli

if __name__ == '__main__':
    cli()