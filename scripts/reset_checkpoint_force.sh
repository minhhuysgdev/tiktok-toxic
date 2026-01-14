#!/bin/bash
# ===================================================================
# Force Reset Spark Streaming Checkpoint (không cần confirm)
# ===================================================================

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECKPOINT_DIR="$PROJECT_ROOT/checkpoints/speed"

echo "🔄 Resetting checkpoint..."

# Backup và xóa checkpoint
if [ -d "$CHECKPOINT_DIR" ] && [ "$(ls -A $CHECKPOINT_DIR 2>/dev/null)" ]; then
    BACKUP_DIR="$CHECKPOINT_DIR.backup.$(date +%Y%m%d_%H%M%S)"
    mv "$CHECKPOINT_DIR" "$BACKUP_DIR" 2>/dev/null || rm -rf "$CHECKPOINT_DIR"
fi

# Tạo lại checkpoint directory trống
mkdir -p "$CHECKPOINT_DIR"

echo "✓ Checkpoint đã được reset!"
echo "  Lần chạy tiếp theo sẽ đọc từ đầu Kafka topic"
