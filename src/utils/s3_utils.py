"""
S3 Utilities - AWS S3 Helper Functions for data ingestion
"""
import logging
import os
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Helper:
    """
    Helper class để làm việc với AWS S3
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Khởi tạo S3 client

        Args:
            config: Dict chứa s3 config (endpoint_url, access_key_id, secret_access_key, region, bucket_name)
        """
        self.config = config
        self.bucket_name = config['bucket_name']
        self.raw_data_prefix = config.get('raw_data_prefix', 'raw/')
        self.processed_data_prefix = config.get('processed_data_prefix', 'processed/')
        self.failed_data_prefix = config.get('failed_data_prefix', 'failed/')
        self.archive_prefix = config.get('archive_prefix', 'archive/')

        # Khởi tạo S3 client
        self.s3_client = boto3.client(
            's3',
            endpoint_url=config.get('endpoint_url'),
            aws_access_key_id=config['access_key_id'],
            aws_secret_access_key=config['secret_access_key'],
            region_name=config['region']
        )

        logger.info(f"✓ S3 client initialized - Bucket: {self.bucket_name}")

    def test_connection(self) -> bool:
        """Test kết nối S3"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info("✓ S3 connection successful")
            return True
        except ClientError as e:
            logger.error(f"Failed to connect to S3: {e}")
            return False

    def list_files(self, prefix: str, suffix: str = None) -> List[str]:
        """
        Liệt kê files trong bucket với prefix và suffix nhất định

        Args:
            prefix: Prefix của object keys
            suffix: Suffix của object keys (vd: '.jsonl')

        Returns:
            List of object keys
        """
        try:
            objects = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if suffix is None or key.endswith(suffix):
                            objects.append(key)

            return objects
        except ClientError as e:
            logger.error(f"Failed to list files in {prefix}: {e}")
            return []

    def download_file(self, s3_key: str, local_path: Path) -> bool:
        """
        Download file từ S3 về local

        Args:
            s3_key: S3 object key
            local_path: Đường dẫn local để lưu file

        Returns:
            True nếu thành công, False nếu thất bại
        """
        try:
            # Tạo thư mục cha nếu chưa có
            local_path.parent.mkdir(parents=True, exist_ok=True)

            self.s3_client.download_file(self.bucket_name, s3_key, str(local_path))
            logger.info(f"✓ Downloaded {s3_key} to {local_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to download {s3_key}: {e}")
            return False

    def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """
        Upload file từ local lên S3

        Args:
            local_path: Đường dẫn file local
            s3_key: S3 object key đích

        Returns:
            True nếu thành công, False nếu thất bại
        """
        try:
            self.s3_client.upload_file(str(local_path), self.bucket_name, s3_key)
            logger.info(f"✓ Uploaded {local_path} to {s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {local_path} to {s3_key}: {e}")
            return False

    def move_file(self, source_key: str, dest_key: str) -> bool:
        """
        Move/rename file trong S3 (copy rồi delete)

        Args:
            source_key: S3 object key nguồn
            dest_key: S3 object key đích

        Returns:
            True nếu thành công, False nếu thất bại
        """
        try:
            # Copy object
            copy_source = {'Bucket': self.bucket_name, 'Key': source_key}
            self.s3_client.copy_object(CopySource=copy_source, Bucket=self.bucket_name, Key=dest_key)

            # Delete source object
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=source_key)

            logger.info(f"✓ Moved {source_key} to {dest_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to move {source_key} to {dest_key}: {e}")
            return False

    def delete_file(self, s3_key: str) -> bool:
        """
        Xóa file khỏi S3

        Args:
            s3_key: S3 object key cần xóa

        Returns:
            True nếu thành công, False nếu thất bại
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"✓ Deleted {s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete {s3_key}: {e}")
            return False

    def get_new_files(self, processed_files: List[str]) -> List[str]:
        """
        Lấy danh sách files mới từ raw data prefix

        Args:
            processed_files: List các files đã xử lý

        Returns:
            List of new S3 keys
        """
        all_raw_files = self.list_files(self.raw_data_prefix, '.jsonl')
        processed_keys = {f"{self.processed_data_prefix}{Path(key).name}" for key in processed_files}
        processed_keys.update({f"{self.failed_data_prefix}{Path(key).name}" for key in processed_files})

        new_files = [key for key in all_raw_files if key not in processed_keys]
        return new_files

    def process_file_from_s3(self, s3_key: str, local_dir: Path) -> Optional[Path]:
        """
        Download và xử lý file từ S3

        Args:
            s3_key: S3 object key
            local_dir: Thư mục local tạm thời

        Returns:
            Path to downloaded file, None nếu thất bại
        """
        local_file = local_dir / Path(s3_key).name

        if self.download_file(s3_key, local_file):
            return local_file
        return None

    def move_processed_file(self, original_s3_key: str, success: bool = True):
        """
        Move file đã xử lý sang thư mục tương ứng

        Args:
            original_s3_key: S3 key gốc
            success: True nếu xử lý thành công, False nếu thất bại
        """
        filename = Path(original_s3_key).name

        if success:
            dest_key = f"{self.processed_data_prefix}{filename}"
        else:
            dest_key = f"{self.failed_data_prefix}{filename}"

        self.move_file(original_s3_key, dest_key)


def create_s3_helper(config: Dict[str, Any]) -> Optional[S3Helper]:
    """
    Factory function để tạo S3Helper

    Args:
        config: S3 config

    Returns:
        S3Helper instance hoặc None nếu cấu hình không đầy đủ
    """
    required_keys = ['access_key_id', 'secret_access_key', 'region', 'bucket_name']

    if not all(key in config for key in required_keys):
        logger.warning("S3 config is incomplete, S3 features will be disabled")
        return None

    try:
        helper = S3Helper(config)
        if helper.test_connection():
            return helper
        else:
            logger.error("S3 connection test failed")
            return None
    except Exception as e:
        logger.error(f"Failed to create S3 helper: {e}")
        return None
