@echo off
REM Start btc-bch-proxy

echo Starting BTC-BCH Proxy...

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

REM Activate virtual environment and start proxy
call venv\Scripts\activate.bat
btc-bch-proxy start -c config.yaml %*
call deactivate
