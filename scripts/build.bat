@echo off
REM SageMaker FeatureStore CLI Build Script for Windows

echo SageMaker FeatureStore CLI Build Script
echo ========================================

REM Change to script's parent directory (project root)
cd /d "%~dp0.."

REM Check if we're in the right directory
if not exist "src\sagemaker_fs_cli" (
    echo Error: Cannot find src\sagemaker_fs_cli directory
    echo Current directory: %CD%
    echo Please ensure you're running this script from the project directory
    pause
    exit /b 1
)

REM Create virtual environment for build
echo Creating build environment...
python -m venv build_env
if %ERRORLEVEL% neq 0 (
    echo Error: Failed to create virtual environment
    pause
    exit /b 1
)

call build_env\Scripts\activate.bat
if %ERRORLEVEL% neq 0 (
    echo Error: Failed to activate virtual environment
    pause
    exit /b 1
)

REM Install dependencies
echo Installing dependencies...
python -m pip install --upgrade pip
if %ERRORLEVEL% neq 0 (
    echo Error: Failed to upgrade pip
    pause
    exit /b 1
)

pip install pyinstaller
if %ERRORLEVEL% neq 0 (
    echo Error: Failed to install pyinstaller
    pause
    exit /b 1
)

pip install -r requirements.txt
if %ERRORLEVEL% neq 0 (
    echo Error: Failed to install requirements
    pause
    exit /b 1
)

REM Build executable
echo Building executable...
pyinstaller --onefile --name fs --console --clean src\sagemaker_fs_cli\__main__.py
if %ERRORLEVEL% neq 0 (
    echo Error: Failed to build executable
    pause
    exit /b 1
)

REM Rename executable with platform info
set PLATFORM=windows
set ARCH=x64

if exist dist\fs.exe (
    move dist\fs.exe dist\fs-%PLATFORM%-%ARCH%.exe
    if %ERRORLEVEL% neq 0 (
        echo Warning: Failed to rename executable
    )
) else (
    echo Error: Executable not found in dist directory
    pause
    exit /b 1
)

echo.
echo Build completed successfully!
echo Executable: dist\fs-%PLATFORM%-%ARCH%.exe
echo.

REM Clean up
echo Cleaning up build artifacts...
call deactivate
if exist build_env rmdir /s /q build_env
if exist build rmdir /s /q build
if exist *.spec del *.spec

echo Cleaned up build artifacts
echo.
echo Build process completed!
pause