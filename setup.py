"""Setup configuration for SageMaker FeatureStore Online CLI"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8') if (this_directory / "README.md").exists() else ""

# Read requirements
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="sagemaker-featurestore-cli",
    version="1.0.1",
    author="Your Name",
    author_email="your.email@example.com",
    description="CLI tool for managing SageMaker FeatureStore Online operations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/sagemaker-featurestore-cli",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Systems Administration",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "fs=sagemaker_fs_cli.cli:cli",  # Main short command
            "sagemaker-fs=sagemaker_fs_cli.cli:cli",
            "sm-fs=sagemaker_fs_cli.cli:cli",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)