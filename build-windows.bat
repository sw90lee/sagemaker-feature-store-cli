@echo off
REM Simple Windows build script - run from anywhere

echo SageMaker FeatureStore CLI - Windows Build
echo ===========================================

REM Get the directory where this batch file is located
set "SCRIPT_DIR=%~dp0"

REM Change to the script directory (project root)
cd /d "%SCRIPT_DIR%"

REM Check if we're in the right place
if not exist "src\sagemaker_fs_cli" (
    echo Error: Cannot find project structure
    echo Looking for: %CD%\src\sagemaker_fs_cli
    echo Please make sure this script is in the project root directory
    pause
    exit /b 1
)

REM Call the main build script
call scripts\build.bat

echo.
echo Done! You can now run the executable:
echo %CD%\dist\fs-windows-x64.exe
pause