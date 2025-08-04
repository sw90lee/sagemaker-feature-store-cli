#!/usr/bin/env python3
"""Build script for creating executable releases"""

import os
import sys
import platform
import subprocess
import shutil
from pathlib import Path


def run_command(cmd, description=""):
    """Run a command and handle errors"""
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Error: {description} failed")
        print(f"stdout: {result.stdout}")
        print(f"stderr: {result.stderr}")
        sys.exit(1)
    
    print(f"Success: {description}")
    return result


def install_dependencies():
    """Install build dependencies"""
    print("Installing build dependencies...")
    
    # Install PyInstaller
    run_command([sys.executable, "-m", "pip", "install", "pyinstaller"], 
                "Installing PyInstaller")
    
    # Install project dependencies
    run_command([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                "Installing project dependencies")


def build_executable():
    """Build the executable using PyInstaller"""
    print("Building executable...")
    
    # Determine the platform
    system = platform.system().lower()
    arch = platform.machine().lower()
    
    if arch == 'x86_64':
        arch = 'x64'
    elif arch == 'amd64':
        arch = 'x64'
    
    # Build with PyInstaller
    build_cmd = [
        "pyinstaller",
        "--onefile",
        "--name", "fs",
        "--console",
        "--clean",
        "src/sagemaker_fs_cli/main.py"
    ]
    
    run_command(build_cmd, "Building executable with PyInstaller")
    
    # Move the executable to dist directory with platform-specific name
    dist_dir = Path("dist")
    executable_name = "fs"
    if system == "windows":
        executable_name += ".exe"
    
    original_exe = dist_dir / executable_name
    platform_exe = dist_dir / f"fs-{system}-{arch}{'.exe' if system == 'windows' else ''}"
    
    if original_exe.exists():
        if platform_exe.exists():
            platform_exe.unlink()
        original_exe.rename(platform_exe)
        print(f"Created: {platform_exe}")
    else:
        print(f"Error: Executable not found at {original_exe}")
        sys.exit(1)


def clean_build_files():
    """Clean up build artifacts"""
    print("Cleaning up build files...")
    
    # Remove build directories
    for dir_name in ["build", "__pycache__"]:
        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)
            print(f"Removed: {dir_name}")
    
    # Remove spec files
    for spec_file in Path(".").glob("*.spec"):
        if spec_file.name != "build.spec":  # Keep our custom spec file
            spec_file.unlink()
            print(f"Removed: {spec_file}")


def main():
    """Main build function"""
    print("SageMaker FeatureStore CLI Build Script")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not Path("src/sagemaker_fs_cli").exists():
        print("Error: Must run from project root directory")
        sys.exit(1)
    
    try:
        # Install dependencies
        install_dependencies()
        
        # Build executable
        build_executable()
        
        # Clean up
        clean_build_files()
        
        print("\n" + "=" * 50)
        print("Build completed successfully!")
        print("Executable can be found in the 'dist' directory")
        
    except KeyboardInterrupt:
        print("\nBuild interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nBuild failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()