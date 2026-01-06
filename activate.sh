#!/bin/bash
# Activate UV virtual environment
echo "🔄 Activating UV virtual environment..."
source .venv/bin/activate
echo "✅ Virtual environment activated!"
echo "📍 Python: $(which python)"
echo "📍 Pip: $(which pip)"
echo "📍 UV: $(which uv)"
echo ""
echo "🚀 Ready to run TikTok Toxicity Detection!"
echo "   • python src/ingestion/json_to_kafka.py"
echo "   • ./scripts/start_streaming.sh"
echo "   • ./scripts/run_batch.sh"
echo ""
echo "💡 Deactivate with: deactivate"
