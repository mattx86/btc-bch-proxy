@echo off
REM Initialize btc-bch-proxy: create venv, install dependencies, create config

echo ========================================
echo  BTC-BCH Proxy Initialization
echo ========================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    exit /b 1
)

REM Create virtual environment
if exist venv (
    echo Virtual environment already exists at venv\
) else (
    echo Creating virtual environment...
    python -m venv venv
    if errorlevel 1 (
        echo ERROR: Failed to create virtual environment
        exit /b 1
    )
    echo Created virtual environment: venv\
)

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Install dependencies
echo Installing dependencies...
pip install -e . >nul 2>&1
if errorlevel 1 (
    echo WARNING: Failed to install in development mode, trying direct install...
    pip install pydantic pyyaml loguru click
)
echo Dependencies installed.

REM Create config file
if exist config.yaml (
    echo config.yaml already exists, skipping...
) else (
    echo Creating config.yaml...
    btc-bch-proxy init --no-venv
)

REM Deactivate virtual environment
echo Deactivating virtual environment...
call deactivate

echo.
echo ========================================
echo  Initialization Complete!
echo ========================================
echo.
echo Next steps:
echo   1. Edit config.yaml with your pool settings
echo   2. Run: start.bat
echo.
