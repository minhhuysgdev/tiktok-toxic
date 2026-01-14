"""
Kafka Producer - Đọc file JSONL và đẩy vào Kafka topic
"""
import json
import logging
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List

import yaml
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Cấu hình logging trước
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent directory to path để có thể import từ src
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import S3 helper với error handling
try:
    from src.utils.s3_utils import create_s3_helper
except ImportError as e:
    logger.error(f"Failed to import S3 utils: {e}")
    logger.error("Make sure you're running from project root or PYTHONPATH is set correctly")
    create_s3_helper = None


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
        print(f"DEBUG: Bootstrap Servers: {kafka_config['bootstrap_servers']}")
        print(f"DEBUG: Topic: {self.topic}")
        
        # Ingestion config
        ingestion_config = self.config['ingestion']
        self.poll_interval = ingestion_config['poll_interval']
        self.use_s3 = ingestion_config.get('use_s3', False)
        self.debug_mode = ingestion_config.get('debug_mode', False)

        # Khởi tạo S3 helper nếu được cấu hình
        self.s3_helper = None
        if self.use_s3 and 's3' in self.config:
            if create_s3_helper is None:
                logger.error("S3 helper not available (import failed), falling back to local filesystem")
                self.use_s3 = False
            else:
                logger.info("Attempting to initialize S3 helper...")
                self.s3_helper = create_s3_helper(self.config['s3'])
                if self.s3_helper:
                    logger.info("✓ S3 datasource enabled")
                    logger.info(f"  Bucket: {self.config['s3'].get('bucket_name')}")
                    logger.info(f"  Prefix: {self.config['s3'].get('raw_data_prefix', 'raw/')}")
                else:
                    logger.warning("S3 configuration failed, falling back to local filesystem")
                    self.use_s3 = False
        elif self.use_s3:
            logger.warning("use_s3 is True but S3 config not found in config.yaml")
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
        
        # Khởi tạo Kafka Producer với tối ưu cho throughput
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            api_version=(0, 10, 1),
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1  # Ensure ordering
        )
        
        # Track sending errors
        self.send_errors = []
        
        logger.info(f"✓ Kafka Producer initialized - Topic: {self.topic}")
        logger.info(f"✓ Monitoring directory: {self.raw_data_dir.absolute()}")
    
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """Kiểm tra tính hợp lệ của record"""
        required_fields = ['video_id']
        return all(field in record for field in required_fields)
    
    def _send_batch(self, batch: List[Tuple[str, Dict[str, Any]]]):
        """
        Gửi batch messages không blocking
        """
        futures = []
        for video_id, record in batch:
            try:
                # Producer đã có value_serializer và key_serializer
                future = self.producer.send(
                    self.topic,
                    key=video_id,  # key_serializer sẽ tự động encode
                    value=record   # value_serializer sẽ tự động serialize
                )
                futures.append((future, video_id))
            except Exception as e:
                error_msg = str(e) if e else "Unknown error"
                error_type = type(e).__name__
                logger.error(f"Error sending message for video_id={video_id}: [{error_type}] {error_msg}")
                import traceback
                logger.debug(f"Traceback: {traceback.format_exc()}")
                self.send_errors.append((video_id, error_msg))
        
        # Check results async (không block)
        for future, video_id in futures:
            try:
                result = future.get(timeout=30)  # Timeout 30s để đảm bảo message được gửi
                # Success - không cần log (tránh spam logs)
            except Exception as e:
                error_msg = str(e) if e else "Unknown error"
                error_type = type(e).__name__
                # Chỉ log warning nếu thực sự có lỗi (không phải timeout do network delay)
                if "timeout" not in error_msg.lower():
                    logger.warning(f"Message send confirmation failed for video_id={video_id}: [{error_type}] {error_msg}")
                    self.send_errors.append((video_id, error_msg))
                else:
                    # Timeout có thể do network delay nhưng message vẫn được gửi
                    logger.debug(f"Message send timeout for video_id={video_id} (may still be sent)")
    
    def process_file(self, file_path: Path) -> Tuple[int, int]:
        """
        Xử lý một file JSON/JSONL (mảng hoặc từng dòng)
        Returns: (success_count, error_count)
        """
        success_count = 0
        error_count = 0

        logger.info(f"Processing file: {file_path.name}")
        
        # Debug mode: Dừng trước khi process file
        if self.debug_mode:
            logger.info(f"  File size: {file_path.stat().st_size / 1024:.1f} KB")
            input(f"  [DEBUG] Press Enter to process {file_path.name}...")

        # Initialize batch for this file
        batch_size = 100
        batch = []

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                if file_path.suffix == '.json':
                    try:
                        records = json.load(f)
                        if not isinstance(records, list):
                            logger.error("Root of JSON file is not an array")
                            return 0, 1
                        
                        total_records = len(records)
                        logger.info(f"  File contains {total_records} total records")
                        
                        for line_num, record in enumerate(records, 1):
                            if not self.validate_record(record):
                                logger.warning(f"Invalid record at index {line_num}: missing video_id")
                                error_count += 1
                                continue
                            
                            video_id = str(record['video_id'])
                            batch.append((video_id, record))
                            
                            # Send batch khi đủ size
                            if len(batch) >= batch_size:
                                self._send_batch(batch)
                                success_count += len(batch)
                                batch = []
                                if success_count % 1000 == 0:
                                    logger.info(f"Sent {success_count} records...")
                        
                        # Send remaining batch
                        if batch:
                            self._send_batch(batch)
                            success_count += len(batch)
                            batch = []
                        
                        logger.info(f"  Summary: {success_count} sent, {error_count} errors, {total_records - success_count - error_count} skipped")
                    except Exception as ex:
                        logger.error(f"JSON array decode error: {ex}")
                        error_count += 1
                else:
                    # JSONL: count total lines first for logging
                    f.seek(0)
                    total_lines = sum(1 for line in f if line.strip())
                    f.seek(0)
                    logger.info(f"  File contains {total_lines} total lines (JSONL)")
                    processed_lines = 0
                    
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        processed_lines += 1
                        try:
                            record = json.loads(line)
                            if not self.validate_record(record):
                                logger.warning(f"Invalid record at line {line_num}: missing video_id")
                                error_count += 1
                                continue
                            
                            video_id = str(record['video_id'])
                            batch.append((video_id, record))
                            
                            # Send batch khi đủ size
                            if len(batch) >= batch_size:
                                self._send_batch(batch)
                                success_count += len(batch)
                                batch = []
                                if success_count % 1000 == 0:
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
                    
                    # Send remaining batch for JSONL
                    if batch:
                        self._send_batch(batch)
                        success_count += len(batch)
                        batch = []
                    
                    skipped = processed_lines - success_count - error_count
                    logger.info(f"  Summary: {success_count} sent, {error_count} errors, {skipped} skipped (out of {total_lines} total lines)")
            
            # Flush remaining messages
            self.producer.flush(timeout=30)
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
        # Quét cả file .json và .jsonl
        files = list(self.raw_data_dir.glob("*.json")) + list(self.raw_data_dir.glob("*.jsonl"))
        if not files:
            logger.debug("No new files found in local directory")
            return

        logger.info(f"Found {len(files)} local file(s) to process")
        for f in files[:5]:  # Show first 5 files
            logger.info(f"  - {f.name}")
        if len(files) > 5:
            logger.info(f"  ... and {len(files) - 5} more files")
        
        # Debug mode: Dừng lại để kiểm tra
        if self.debug_mode and files:
            logger.info("=" * 60)
            logger.info("DEBUG MODE: Pausing for inspection...")
            logger.info(f"Found {len(files)} files to process:")
            for i, f in enumerate(files[:10], 1):
                logger.info(f"  {i}. {f.name} ({f.stat().st_size / 1024:.1f} KB)")
            if len(files) > 10:
                logger.info(f"  ... and {len(files) - 10} more")
            logger.info("=" * 60)
            input("Press Enter to continue processing... (or Ctrl+C to stop)")

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
        try:
            # Debug: List tất cả files trong raw/ prefix
            raw_files_json = self.s3_helper.list_files(self.s3_helper.raw_data_prefix, '.json')
            raw_files_jsonl = self.s3_helper.list_files(self.s3_helper.raw_data_prefix, '.jsonl')
            all_raw_files = raw_files_json + raw_files_jsonl
            
            logger.info(f"S3 scan: Found {len(raw_files_json)} .json files, {len(raw_files_jsonl)} .jsonl files in raw/")
            if all_raw_files:
                logger.info(f"Sample S3 files: {all_raw_files[:3]}")
            
            # Lấy processed files từ S3 để filter
            processed_files_json = self.s3_helper.list_files(self.s3_helper.processed_data_prefix, '.json')
            processed_files_jsonl = self.s3_helper.list_files(self.s3_helper.processed_data_prefix, '.jsonl')
            all_processed = processed_files_json + processed_files_jsonl
            
            logger.info(f"S3 scan: Found {len(all_processed)} files in processed/")
            
            # Filter: chỉ lấy files chưa được processed
            processed_names = {Path(f).name for f in all_processed}
            new_files = [f for f in all_raw_files if Path(f).name not in processed_names]
            
            logger.info(f"S3 scan: {len(new_files)} new files to process (after filtering processed)")
            
            # Debug mode: Dừng lại để kiểm tra
            if self.debug_mode and new_files:
                logger.info("=" * 60)
                logger.info("DEBUG MODE: Pausing for inspection...")
                logger.info(f"Found {len(new_files)} files to process:")
                for i, f in enumerate(new_files[:10], 1):
                    logger.info(f"  {i}. {f}")
                if len(new_files) > 10:
                    logger.info(f"  ... and {len(new_files) - 10} more")
                logger.info("=" * 60)
                input("Press Enter to continue processing... (or Ctrl+C to stop)")
            
        except Exception as e:
            logger.error(f"Error scanning S3: {e}", exc_info=True)
            logger.info("Falling back to local filesystem...")
            self._scan_and_process_local()
            return

        if not new_files:
            logger.info(f"No new files found in S3 (bucket: {self.config['s3'].get('bucket_name')}, prefix: {self.config['s3'].get('raw_data_prefix', 'raw/')})")
            logger.info("Checking local filesystem for files...")
            # Fallback to local nếu S3 không có files
            self._scan_and_process_local()
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
        logger.info(f"Data Source: {'S3' if self.use_s3 else 'Local Filesystem'}")
        if self.use_s3:
            logger.info(f"  S3 Bucket: {self.config['s3'].get('bucket_name')}")
            logger.info(f"  S3 Prefix: {self.config['s3'].get('raw_data_prefix', 'raw/')}")
        else:
            logger.info(f"  Local Directory: {self.raw_data_dir.absolute()}")
        logger.info(f"Kafka Topic: {self.topic}")
        logger.info(f"Poll Interval: {self.poll_interval} seconds")
        logger.info(f"Debug Mode: {'ON' if self.debug_mode else 'OFF'}")
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

