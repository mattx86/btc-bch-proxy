@echo off
REM Validate crypto-stratum-proxy configuration

REM Check if venv exists
if not exist venv\Scripts\activate.bat (
    echo ERROR: Virtual environment not found. Run init.bat first.
    exit /b 1
)

REM Check if config exists
if not exist config.yaml (
    echo ERROR: config.yaml not found. Run init.bat first.
    exit /b 1
)

REM Activate virtual environment and validate config
call venv\Scripts\activate.bat
crypto-stratum-proxy validate config.yaml
call deactivate
