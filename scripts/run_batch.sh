#!/bin/bash
# ===================================================================
# Run Batch Layer - Full Recompute
# Chạy hàng ngày bằng cron: 0 2 * * * /path/to/run_batch.sh
# ===================================================================

set -e  # Exit on error

echo "=========================================="
echo "Starting Batch Layer - Full Recompute"
echo "Date: $(date)"
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

# Spark configurations
SPARK_CONFIGS=(
    "--conf" "spark.sql.shuffle.partitions=8"
    "--conf" "spark.sql.adaptive.enabled=true"
    "--conf" "spark.sql.adaptive.coalescePartitions.enabled=true"
    "--conf" "spark.driver.extraJavaOptions=-Djava.net.preferIPv4Stack=true"
    "--conf" "spark.executor.extraJavaOptions=-Djava.net.preferIPv4Stack=true"
    "--conf" "spark.driver.host=127.0.0.1"
)

echo ""
echo "Starting Spark Batch job..."
echo ""

# Run spark-submit
spark-submit \
    --master local[*] \
    --packages "$PACKAGES" \
    "${SPARK_CONFIGS[@]}" \
    --driver-memory 4g \
    --executor-memory 4g \
    src/batch_layer/batch_full_recompute.py

# Refresh materialized views
echo ""
echo "Refreshing PostgreSQL materialized views..."
docker-compose exec -T -e PGPASSWORD=tiktok_pass postgres psql -U tiktok_user -d tiktok_toxicity -c "SELECT refresh_serving_views();"

echo ""
echo "=========================================="
echo "Batch Layer completed successfully"
echo "Date: $(date)"
echo "=========================================="

