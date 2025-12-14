#!/bin/bash
# Initialize btc-bch-proxy: create venv, install dependencies, create config

echo "========================================"
echo " BTC-BCH Proxy Initialization"
echo "========================================"
echo

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    if ! command -v python &> /dev/null; then
        echo "ERROR: Python is not installed or not in PATH"
        exit 1
    fi
    PYTHON=python
else
    PYTHON=python3
fi

# Create virtual environment
if [ -d "venv" ]; then
    echo "Virtual environment already exists at venv/"
else
    echo "Creating virtual environment..."
    $PYTHON -m venv venv
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to create virtual environment"
        exit 1
    fi
    echo "Created virtual environment: venv/"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -e . > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "WARNING: Failed to install in development mode, trying direct install..."
    pip install pydantic pyyaml loguru click
fi
echo "Dependencies installed."

# Create config file
if [ -f "config.yaml" ]; then
    echo "config.yaml already exists, skipping..."
else
    echo "Creating config.yaml..."
    btc-bch-proxy init --no-venv
fi

# Deactivate virtual environment
echo "Deactivating virtual environment..."
deactivate

echo
echo "========================================"
echo " Initialization Complete!"
echo "========================================"
echo
echo "Next steps:"
echo "  1. Edit config.yaml with your pool settings"
echo "  2. Run: ./start.sh"
echo
