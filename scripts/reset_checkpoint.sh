#!/bin/bash
# ===================================================================
# Reset Spark Streaming Checkpoint
# Xóa checkpoint để đọc lại từ đầu Kafka topic
# ===================================================================

set -e

echo "=========================================="
echo "Resetting Spark Streaming Checkpoint"
echo "=========================================="

# Set project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

CHECKPOINT_DIR="$PROJECT_ROOT/checkpoints/speed"

echo "Project root: $PROJECT_ROOT"
echo "Checkpoint directory: $CHECKPOINT_DIR"

# Kiểm tra xem checkpoint có tồn tại không
if [ ! -d "$CHECKPOINT_DIR" ]; then
    echo "⚠️  Checkpoint directory không tồn tại: $CHECKPOINT_DIR"
    exit 0
fi

# Xác nhận trước khi xóa
echo ""
echo "⚠️  CẢNH BÁO: Bạn sắp xóa checkpoint directory!"
echo "   Điều này sẽ làm Spark đọc lại từ đầu Kafka topic."
echo ""
read -p "Bạn có chắc chắn muốn tiếp tục? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "❌ Đã hủy. Checkpoint không bị xóa."
    exit 0
fi

# Backup checkpoint trước khi xóa (optional)
BACKUP_DIR="$CHECKPOINT_DIR.backup.$(date +%Y%m%d_%H%M%S)"
if [ -d "$CHECKPOINT_DIR" ] && [ "$(ls -A $CHECKPOINT_DIR)" ]; then
    echo ""
    echo "📦 Đang backup checkpoint đến: $BACKUP_DIR"
    mv "$CHECKPOINT_DIR" "$BACKUP_DIR"
    echo "✓ Backup completed"
else
    echo ""
    echo "📁 Checkpoint directory trống hoặc không tồn tại"
fi

# Tạo lại checkpoint directory trống
mkdir -p "$CHECKPOINT_DIR"
echo "✓ Đã tạo lại checkpoint directory"

echo ""
echo "=========================================="
echo "✓ Reset checkpoint thành công!"
echo "=========================================="
echo ""
echo "Lần chạy tiếp theo sẽ đọc từ đầu Kafka topic (earliest offset)"
echo ""
