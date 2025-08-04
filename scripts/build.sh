#!/bin/bash

# SageMaker FeatureStore CLI Build Script for Linux/Unix
set -e

echo "SageMaker FeatureStore CLI Build Script"
echo "========================================"

# Check if we're in the right directory
if [ ! -d "src/sagemaker_fs_cli" ]; then
    echo "Error: Must run from project root directory"
    exit 1
fi

# Create virtual environment for build
echo "Creating build environment..."
python3 -m venv build_env
source build_env/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install pyinstaller
pip install -r requirements.txt

# Build executable
echo "Building executable..."
pyinstaller --onefile --name fs --console --clean src/sagemaker_fs_cli/__main__.py

# Rename executable with platform info
PLATFORM=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

if [ "$ARCH" = "x86_64" ]; then
    ARCH="x64"
fi

mv dist/fs "dist/fs-${PLATFORM}-${ARCH}"

echo "Build completed successfully!"
echo "Executable: dist/fs-${PLATFORM}-${ARCH}"

# Clean up
deactivate
rm -rf build_env build *.spec

echo "Cleaned up build artifacts"