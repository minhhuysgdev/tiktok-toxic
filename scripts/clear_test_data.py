#!/usr/bin/env python3
"""
Script để clear dữ liệu test và reset Kafka
"""
import sys
import yaml
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.db_utils import PostgresHelper

def clear_postgres_data():
    """Xóa dữ liệu test trong PostgreSQL"""
    print("=" * 60)
    print("Clearing PostgreSQL Test Data")
    print("=" * 60)
    print()
    
    try:
        # Load config
        project_root = Path(__file__).parent.parent
        config_path = project_root / "config" / "config.yaml"
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        postgres_config = config['postgres']
        db_helper = PostgresHelper(postgres_config)
        db_helper.connect()
        
        # Xóa dữ liệu trong speed_comments
        print("🗑️  Đang xóa dữ liệu trong speed_comments...")
        delete_comments = "DELETE FROM speed_comments WHERE video_id = '7353876663521840401'"
        db_helper.execute_query(delete_comments)
        print("✓ Đã xóa dữ liệu trong speed_comments")
        
        # Xóa dữ liệu trong speed_video_stats
        print("🗑️  Đang xóa dữ liệu trong speed_video_stats...")
        delete_stats = "DELETE FROM speed_video_stats WHERE video_id = '7353876663521840401'"
        db_helper.execute_query(delete_stats)
        print("✓ Đã xóa dữ liệu trong speed_video_stats")
        
        # Hoặc xóa tất cả (nếu muốn) - có thể truyền argument
        import sys
        if len(sys.argv) > 1 and sys.argv[1] == '--all':
            print("🗑️  Đang xóa TẤT CẢ dữ liệu...")
            db_helper.execute_query("TRUNCATE TABLE speed_comments RESTART IDENTITY")
            db_helper.execute_query("TRUNCATE TABLE speed_video_stats")
            print("✓ Đã xóa TẤT CẢ dữ liệu")
        else:
            print("⚠️  Chỉ xóa dữ liệu test (video_id = 7353876663521840401)")
            print("   Để xóa tất cả, chạy: python scripts/clear_test_data.py --all")
        
        db_helper.close()
        print()
        
    except Exception as e:
        print(f"❌ Lỗi khi xóa dữ liệu PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def reset_kafka_topic():
    """Reset Kafka topic để đọc lại từ đầu"""
    print("=" * 60)
    print("Resetting Kafka Topic")
    print("=" * 60)
    print()
    
    try:
        import subprocess
        
        # Kiểm tra Kafka có đang chạy không
        print("🔍 Kiểm tra Kafka...")
        result = subprocess.run(
            ["docker", "exec", "kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if "tiktok-raw" not in result.stdout:
            print("⚠️  Topic tiktok-raw không tồn tại")
            return False
        
        print("✓ Topic tiktok-raw tồn tại")
        print()
        
        # Xóa topic và tạo lại
        print("🗑️  Đang xóa topic tiktok-raw...")
        subprocess.run(
            ["docker", "exec", "kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", "--delete", "--topic", "tiktok-raw"],
            capture_output=True,
            timeout=10
        )
        print("✓ Đã xóa topic")
        
        print("🆕 Đang tạo lại topic tiktok-raw...")
        subprocess.run(
            ["docker", "exec", "kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", 
             "--create", "--topic", "tiktok-raw", "--partitions", "1", "--replication-factor", "1"],
            capture_output=True,
            timeout=10
        )
        print("✓ Đã tạo lại topic")
        print()
        
        print("💡 Topic đã được reset - bạn có thể produce messages mới")
        print()
        
    except subprocess.TimeoutExpired:
        print("❌ Timeout khi kết nối Kafka")
        return False
    except FileNotFoundError:
        print("❌ Docker không được tìm thấy hoặc Kafka container không chạy")
        print("   Hãy chạy: docker-compose up -d")
        return False
    except Exception as e:
        print(f"❌ Lỗi khi reset Kafka: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def reset_checkpoint():
    """Reset Spark checkpoint"""
    print("=" * 60)
    print("Resetting Spark Checkpoint")
    print("=" * 60)
    print()
    
    try:
        import subprocess
        
        checkpoint_dir = Path(__file__).parent.parent / "checkpoints" / "speed"
        
        if checkpoint_dir.exists():
            print(f"🗑️  Đang xóa checkpoint tại: {checkpoint_dir}")
            import shutil
            shutil.rmtree(checkpoint_dir)
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            print("✓ Đã reset checkpoint")
        else:
            print("⚠️  Checkpoint directory không tồn tại")
        
        print()
        
    except Exception as e:
        print(f"❌ Lỗi khi reset checkpoint: {e}")
        return False
    
    return True

def main():
    """Main function"""
    print()
    print("=" * 60)
    print("Clear Test Data & Reset Kafka")
    print("=" * 60)
    print()
    
    # 1. Clear PostgreSQL
    if not clear_postgres_data():
        print("❌ Không thể clear PostgreSQL data")
        return
    
    # 2. Reset checkpoint
    reset_checkpoint()
    
    # 3. Reset Kafka
    import sys
    if len(sys.argv) > 1 and '--reset-kafka' in sys.argv:
        if not reset_kafka_topic():
            print("⚠️  Không thể reset Kafka topic (có thể Kafka không chạy)")
    else:
        print("⚠️  Bỏ qua reset Kafka topic")
        print("   Để reset Kafka, chạy: python scripts/clear_test_data.py --reset-kafka")
        print("   Hoặc reset thủ công:")
        print("   docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic tiktok-raw")
        print("   docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic tiktok-raw --partitions 1 --replication-factor 1")
    
    print()
    print("=" * 60)
    print("✓ Hoàn thành!")
    print("=" * 60)
    print()
    print("Bây giờ bạn có thể:")
    print("  1. Produce messages mới vào Kafka")
    print("  2. Chạy streaming để đọc từ đầu")
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Đã dừng")
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
