@echo off
REM Check crypto-stratum-proxy status

REM Check if venv exists
if not exist venv\Scripts\activate.bat (
    echo ERROR: Virtual environment not found. Run init.bat first.
    exit /b 1
)

REM Activate virtual environment and check status
call venv\Scripts\activate.bat
crypto-stratum-proxy status
call deactivate
