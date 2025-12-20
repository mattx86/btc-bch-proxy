@echo off
REM Stop crypto-stratum-proxy

echo Stopping Crypto Stratum Proxy...

REM Check if venv exists
if not exist venv\Scripts\activate.bat (
    echo ERROR: Virtual environment not found. Run init.bat first.
    exit /b 1
)

REM Activate virtual environment and stop proxy
call venv\Scripts\activate.bat
crypto-stratum-proxy stop
call deactivate
