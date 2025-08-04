@echo off
REM SageMaker FeatureStore CLI Build Script for Windows

echo SageMaker FeatureStore CLI Build Script
echo ========================================

REM Check if we're in the right directory
if not exist "src\sagemaker_fs_cli" (
    echo Error: Must run from project root directory
    exit /b 1
)

REM Create virtual environment for build
echo Creating build environment...
python -m venv build_env
call build_env\Scripts\activate.bat

REM Install dependencies
echo Installing dependencies...
python -m pip install --upgrade pip
pip install pyinstaller
pip install -r requirements.txt

REM Build executable
echo Building executable...
pyinstaller --onefile --name fs --console --clean src\sagemaker_fs_cli\__main__.py

REM Rename executable with platform info
set PLATFORM=windows
set ARCH=x64

move dist\fs.exe dist\fs-%PLATFORM%-%ARCH%.exe

echo Build completed successfully!
echo Executable: dist\fs-%PLATFORM%-%ARCH%.exe

REM Clean up
call deactivate
rmdir /s /q build_env
rmdir /s /q build
del *.spec

echo Cleaned up build artifacts