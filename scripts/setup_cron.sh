#!/bin/bash
# ===================================================================
# Setup Cron Job cho Batch Layer
# ===================================================================

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_PATH="$PROJECT_ROOT/scripts/run_batch.sh"
LOG_PATH="$PROJECT_ROOT/logs/batch_layer.log"

# Tạo thư mục logs
mkdir -p "$PROJECT_ROOT/logs"

echo "Setting up cron job for Batch Layer..."
echo "Script: $SCRIPT_PATH"
echo "Log: $LOG_PATH"

# Cron schedule: Chạy hàng ngày lúc 2h sáng
CRON_SCHEDULE="0 2 * * *"

# Tạo cron entry
CRON_ENTRY="$CRON_SCHEDULE $SCRIPT_PATH >> $LOG_PATH 2>&1"

# Check if cron entry already exists
if crontab -l 2>/dev/null | grep -q "$SCRIPT_PATH"; then
    echo "Cron job already exists!"
    echo "Current cron jobs:"
    crontab -l | grep "$SCRIPT_PATH"
else
    # Add to crontab
    (crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -
    echo "✓ Cron job added successfully!"
    echo "Schedule: Daily at 2:00 AM"
fi

echo ""
echo "To view all cron jobs: crontab -l"
echo "To remove this cron job: crontab -e"

