#!/bin/bash
# Check crypto-stratum-proxy status

# Check if venv exists
if [ ! -f "venv/bin/activate" ]; then
    echo "ERROR: Virtual environment not found. Run ./init.sh first."
    exit 1
fi

# Activate virtual environment and check status
source venv/bin/activate
crypto-stratum-proxy status
deactivate
