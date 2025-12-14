#!/bin/bash
# Start btc-bch-proxy

echo "Starting BTC-BCH Proxy..."

# Check if venv exists
if [ ! -f "venv/bin/activate" ]; then
    echo "ERROR: Virtual environment not found. Run ./init.sh first."
    exit 1
fi

# Check if config exists
if [ ! -f "config.yaml" ]; then
    echo "ERROR: config.yaml not found. Run ./init.sh first."
    exit 1
fi

# Activate virtual environment and start proxy
source venv/bin/activate
btc-bch-proxy start -c config.yaml "$@"
deactivate
