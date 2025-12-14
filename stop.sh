#!/bin/bash
# Stop btc-bch-proxy

echo "Stopping BTC-BCH Proxy..."

# Check if venv exists
if [ ! -f "venv/bin/activate" ]; then
    echo "ERROR: Virtual environment not found. Run ./init.sh first."
    exit 1
fi

# Activate virtual environment and stop proxy
source venv/bin/activate
btc-bch-proxy stop
deactivate
