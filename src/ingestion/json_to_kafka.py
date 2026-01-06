"""
Kafka Producer - Đọc file JSONL và đẩy vào Kafka topic
"""
import json
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Dict, Any, Tuple, Optional

import yaml
from kafka import KafkaProducer
from kafka.errors import KafkaError

from ..utils.s3_utils import create_s3_helper

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TikTokKafkaProducer:
    """
    Producer đọc file JSONL từ thư mục raw và đẩy vào Kafka
    """
    
    def __init__(self, config_path: str = None):
        """Khởi tạo producer với config"""
        if config_path is None:
            # Find project root from script location (src/ingestion/json_to_kafka.py)
            script_dir = Path(__file__).resolve().parent  # src/ingestion/ (absolute)
            project_root = script_dir.parent.parent  # project root (absolute)
            config_path = project_root / "config" / "config.yaml"

        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Kafka config
        kafka_config = self.config['kafka']
        self.topic = kafka_config['topic_raw']
        
        # Ingestion config
        ingestion_config = self.config['ingestion']
        self.poll_interval = ingestion_config['poll_interval']
        self.use_s3 = ingestion_config.get('use_s3', False)

        # Khởi tạo S3 helper nếu được cấu hình
        self.s3_helper = None
        if self.use_s3 and 's3' in self.config:
            self.s3_helper = create_s3_helper(self.config['s3'])
            if self.s3_helper:
                logger.info("✓ S3 datasource enabled")
            else:
                logger.warning("S3 configuration failed, falling back to local filesystem")
                self.use_s3 = False

        # Local directories (dùng cho cả local và S3 temp files)
        script_dir = Path(__file__).resolve().parent  # src/ingestion/
        project_root = script_dir.parent.parent  # project root
        self.raw_data_dir = project_root / ingestion_config['raw_data_dir'].lstrip('./')
        self.processed_dir = project_root / ingestion_config['processed_dir'].lstrip('./')
        self.failed_dir = project_root / ingestion_config['failed_dir'].lstrip('./')

        # Tạo thư mục local
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.failed_dir.mkdir(parents=True, exist_ok=True)

        # Temporary directory cho S3 downloads
        self.temp_dir = Path(tempfile.mkdtemp(prefix="tiktok_ingestion_"))
        
        # Khởi tạo Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            compression_type=kafka_config['compression_type'],
            acks='all',  # Đảm bảo message được ghi thành công
            retries=3,
            max_in_flight_requests_per_connection=1  # Đảm bảo thứ tự
        )
        
        logger.info(f"✓ Kafka Producer initialized - Topic: {self.topic}")
        logger.info(f"✓ Monitoring directory: {self.raw_data_dir.absolute()}")
    
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """Kiểm tra tính hợp lệ của record"""
        required_fields = ['video_id']
        return all(field in record for field in required_fields)
    
    def process_file(self, file_path: Path) -> Tuple[int, int]:
        """
        Xử lý một file JSONL
        Returns: (success_count, error_count)
        """
        success_count = 0
        error_count = 0
        
        logger.info(f"Processing file: {file_path.name}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        # Parse JSON
                        record = json.loads(line)
                        
                        # Validate
                        if not self.validate_record(record):
                            logger.warning(f"Invalid record at line {line_num}: missing video_id")
                            error_count += 1
                            continue
                        
                        # Send to Kafka
                        video_id = str(record['video_id'])
                        future = self.producer.send(
                            self.topic,
                            key=video_id,
                            value=record
                        )
                        
                        # Đợi confirmation (blocking)
                        future.get(timeout=10)
                        success_count += 1
                        
                        if success_count % 100 == 0:
                            logger.info(f"Sent {success_count} records...")
                    
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error at line {line_num}: {e}")
                        error_count += 1
                    
                    except KafkaError as e:
                        logger.error(f"Kafka error at line {line_num}: {e}")
                        error_count += 1
                    
                    except Exception as e:
                        logger.error(f"Unexpected error at line {line_num}: {e}")
                        error_count += 1
            
            # Flush để đảm bảo tất cả messages được gửi
            self.producer.flush()
            
            logger.info(f"✓ File {file_path.name}: {success_count} success, {error_count} errors")
            return success_count, error_count
        
        except Exception as e:
            logger.error(f"✗ Failed to process file {file_path.name}: {e}")
            return success_count, error_count
    
    def move_file(self, file_path: Path, destination_dir: Path):
        """Di chuyển file sau khi xử lý"""
        try:
            dest_path = destination_dir / file_path.name
            # Nếu file đã tồn tại, thêm timestamp
            if dest_path.exists():
                timestamp = int(time.time())
                dest_path = destination_dir / f"{file_path.stem}_{timestamp}{file_path.suffix}"
            
            shutil.move(str(file_path), str(dest_path))
            logger.info(f"Moved {file_path.name} to {destination_dir.name}/")
        except Exception as e:
            logger.error(f"Failed to move file {file_path.name}: {e}")
    
    def scan_and_process(self):
        """Quét và xử lý các file mới từ datasource"""
        if self.use_s3 and self.s3_helper:
            self._scan_and_process_s3()
        else:
            self._scan_and_process_local()

    def _scan_and_process_local(self):
        """Quét và xử lý files từ local filesystem"""
        files = list(self.raw_data_dir.glob("*.jsonl"))

        if not files:
            logger.debug("No new files found")
            return

        logger.info(f"Found {len(files)} local file(s) to process")

        for file_path in files:
            success_count, error_count = self.process_file(file_path)

            # Di chuyển file dựa trên kết quả
            if error_count == 0 and success_count > 0:
                self.move_file(file_path, self.processed_dir)
            elif success_count > 0:
                # Có lỗi nhưng vẫn có records thành công
                self.move_file(file_path, self.processed_dir)
            else:
                # Toàn bộ file lỗi
                self.move_file(file_path, self.failed_dir)

    def _scan_and_process_s3(self):
        """Quét và xử lý files từ S3"""
        # Lấy danh sách files mới từ S3
        new_files = self.s3_helper.get_new_files([])  # Có thể mở rộng để track processed files

        if not new_files:
            logger.debug("No new files found in S3")
            return

        logger.info(f"Found {len(new_files)} S3 file(s) to process")

        for s3_key in new_files:
            # Download file từ S3
            local_file = self.s3_helper.process_file_from_s3(s3_key, self.temp_dir)
            if not local_file:
                logger.error(f"Failed to download {s3_key}, skipping")
                continue

            try:
                # Xử lý file
                success_count, error_count = self.process_file(local_file)

                # Move file trong S3 dựa trên kết quả
                success = error_count == 0 and success_count > 0
                self.s3_helper.move_processed_file(s3_key, success)

            finally:
                # Cleanup temp file
                if local_file.exists():
                    local_file.unlink()
    
    def run(self):
        """Chạy producer liên tục"""
        logger.info("=" * 60)
        logger.info("TikTok Kafka Producer Started")
        logger.info("=" * 60)
        
        try:
            while True:
                self.scan_and_process()
                time.sleep(self.poll_interval)
        
        except KeyboardInterrupt:
            logger.info("\n✓ Shutting down gracefully...")
        
        finally:
            self.producer.close()
            logger.info("✓ Producer closed")

            # Cleanup temp directory
            if hasattr(self, 'temp_dir') and self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                logger.info("✓ Temp directory cleaned up")
    
    def close(self):
        """Đóng producer"""
        if self.producer:
            self.producer.close()


def main():
    """Entry point"""
    producer = TikTokKafkaProducer()
    producer.run()


if __name__ == "__main__":
    main()

