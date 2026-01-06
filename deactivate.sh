#!/bin/bash
# Deactivate virtual environment
echo "🔄 Deactivating virtual environment..."
deactivate 2>/dev/null || echo "No active virtual environment"
echo "✅ Virtual environment deactivated!"
