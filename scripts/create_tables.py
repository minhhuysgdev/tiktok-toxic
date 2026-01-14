#!/usr/bin/env python3
"""
Script để tạo tất cả các bảng cần thiết trong PostgreSQL
"""
import sys
import yaml
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.db_utils import create_tables_schema

def main():
    """Tạo tất cả các bảng trong PostgreSQL"""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # Load config
    project_root = Path(__file__).parent.parent
    config_path = project_root / "config" / "config.yaml"
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        postgres_config = config['postgres']
        
        print("=" * 60)
        print("Creating PostgreSQL Tables")
        print("=" * 60)
        print(f"Database: {postgres_config['database']}")
        print(f"Host: {postgres_config['host']}:{postgres_config['port']}")
        print()
        
        # Test connection first
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=postgres_config['host'],
                port=postgres_config['port'],
                database=postgres_config['database'],
                user=postgres_config['user'],
                password=postgres_config['password']
            )
            conn.close()
            print("✓ PostgreSQL connection successful")
        except Exception as conn_err:
            print(f"❌ Không thể kết nối PostgreSQL: {conn_err}")
            print("Hãy đảm bảo PostgreSQL đang chạy:")
            print("  docker-compose up -d postgres")
            sys.exit(1)
        
        print()
        create_tables_schema(postgres_config)
        print()
        print("=" * 60)
        print("✓ Tất cả các bảng đã được tạo thành công!")
        print("=" * 60)
        print()
        print("Các bảng đã tạo:")
        print("  - speed_comments (comments với toxicity detection)")
        print("  - speed_video_stats (stats theo window)")
        print("  - speed_alerts (alerts cho toxic videos)")
        print("  - batch_video_stats (batch layer stats)")
        print("  - batch_hashtag_stats (batch layer hashtag stats)")
        print("  - batch_user_ranking (batch layer user ranking)")
        print()
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file config: {config_path}")
        sys.exit(1)
    except KeyError as e:
        print(f"❌ Thiếu config key: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Lỗi khi tạo bảng: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
