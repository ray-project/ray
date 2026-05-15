#!/bin/bash
# Quick setup script for Riva Models Demo
# Run this before the presentation to verify everything works

set -e

echo "🚀 Riva Models Demo - Quick Setup"
echo "=================================="
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.8+"
    exit 1
fi
echo "✅ Python 3 found: $(python3 --version)"

# Check pip
if ! command -v pip3 &> /dev/null && ! command -v pip &> /dev/null; then
    echo "❌ pip not found. Please install pip"
    exit 1
fi
echo "✅ pip found"

# Install dependencies
echo ""
echo "📦 Installing dependencies..."
pip3 install -q -r requirements.txt
echo "✅ Dependencies installed"

# Check API key
echo ""
if [ -z "$INFERENCE_API_KEY" ]; then
    echo "⚠️  WARNING: INFERENCE_API_KEY not set"
    echo ""
    echo "To set your API key, run:"
    echo "  export INFERENCE_API_KEY='your-key-here'"
    echo ""
    read -p "Do you want to test the app anyway? (y/n) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo "✅ INFERENCE_API_KEY is set"
    
    # Run API tests
    echo ""
    echo "🧪 Testing API connectivity..."
    python3 test_api.py
fi

# Ready to go
echo ""
echo "=================================="
echo "✅ Setup complete!"
echo ""
echo "To start the demo:"
echo "  python3 app.py"
echo ""
echo "Then open your browser to:"
echo "  http://localhost:5000"
echo ""
echo "📖 See PRESENTATION_GUIDE.md for demo talking points"
echo "=================================="
