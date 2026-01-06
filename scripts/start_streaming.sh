#!/bin/bash
# ===================================================================
# Start Speed Layer - Spark Structured Streaming
# ===================================================================

set -e  # Exit on error

echo "=========================================="
echo "Starting Speed Layer - Streaming Job"
echo "=========================================="

# Set project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "Project root: $PROJECT_ROOT"

# Check if config exists
if [ ! -f "config/config.yaml" ]; then
    echo "ERROR: config/config.yaml not found!"
    exit 1
fi

# Set Python path
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Spark packages
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3"

echo ""
echo "Starting Spark Streaming job..."
echo "Checkpoint: $PROJECT_ROOT/checkpoints/speed"
echo ""

# Run spark-submit with individual --conf options
spark-submit \
    --master local[*] \
    --packages "$PACKAGES" \
    --conf spark.sql.streaming.checkpointLocation="$PROJECT_ROOT/checkpoints/speed" \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    --conf spark.sql.streaming.schemaInference=true \
    --driver-memory 2g \
    --executor-memory 2g \
    src/speed_layer/streaming_toxicity.py

echo ""
echo "=========================================="
echo "Speed Layer stopped"
echo "=========================================="