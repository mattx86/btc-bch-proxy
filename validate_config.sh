#!/bin/bash
# Validate crypto-stratum-proxy configuration

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

# Activate virtual environment and validate config
source venv/bin/activate
crypto-stratum-proxy validate config.yaml
deactivate
