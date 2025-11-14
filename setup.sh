#!/bin/bash
# Setup script for Restaurant ELT Pipeline

echo "=========================================="
echo "Restaurant ELT Pipeline - Setup"
echo "=========================================="
echo ""

# Check Python version
python_version=$(python --version 2>&1 | awk '{print $2}')
echo "Python version: $python_version"

# Create virtual environment
echo ""
echo "Creating virtual environment..."
python -m venv venv

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo ""
echo "Installing dependencies..."
pip install -r requirements.txt

# Copy .env.example to .env if not exists
if [ ! -f .env ]; then
    echo ""
    echo "Creating .env file from .env.example..."
    cp .env.example .env
    echo "⚠️  Please update .env with your Azure SAS URL if needed"
fi

# Create necessary directories
echo ""
echo "Creating directories..."
mkdir -p data/csv
mkdir -p data/outputs
mkdir -p data/dagster_storage

echo ""
echo "=========================================="
echo "✅ Setup complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Activate virtual environment:"
echo "     source venv/bin/activate"
echo ""
echo "  2. Verify setup:"
echo "     python verify_setup.py"
echo ""
echo "  3. Run pipeline:"
echo "     python run_pipeline.py"
echo ""
