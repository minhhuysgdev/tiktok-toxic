"""
Batch Layer - Spark Batch Job để recompute toàn bộ dữ liệu lịch sử
Chạy hàng ngày bằng cron
"""
import logging
import sys
from pathlib import Path
from datetime import datetime

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, explode, count, sum as spark_sum,
    when, current_timestamp, collect_list, size, flatten,
    row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType
)

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.models.toxicity_detector import create_spark_udf
from src.utils.db_utils import write_to_postgres_jdbc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchToxicityProcessor:
    """
    Spark Batch processor để xử lý toàn bộ dữ liệu lịch sử
    """
    
    def __init__(self, config_path: str = None):
        """Khởi tạo với config"""
        if config_path is None:
            # Find project root from script location
            script_dir = Path(__file__).resolve().parent  # src/batch_layer/ (absolute)
            project_root = script_dir.parent.parent  # project root (absolute)
            config_path = project_root / "config" / "config.yaml"

        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.kafka_config = self.config['kafka']
        self.postgres_config = self.config['postgres']
        self.batch_config = self.config['batch_layer']
        self.model_config = self.config['model']

        # Convert relative paths to absolute paths from project root
        script_dir = Path(__file__).resolve().parent  # src/batch_layer/
        project_root = script_dir.parent.parent  # project root
        if 'archive_dir' in self.batch_config:
            self.batch_config['archive_dir'] = str(project_root / self.batch_config['archive_dir'].lstrip('./'))
        if 'checkpoint_dir' in self.batch_config:
            self.batch_config['checkpoint_dir'] = str(project_root / self.batch_config['checkpoint_dir'].lstrip('./'))
        
        # Khởi tạo Spark Session
        self.spark = self._create_spark_session()
        
        # Tạo toxicity detection UDF
        self.toxicity_udf = create_spark_udf(
            model_name=self.model_config['name'],
            device=self.model_config['device']
        )
        
        logger.info("✓ BatchToxicityProcessor initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Tạo Spark Session"""
        spark = SparkSession.builder \
            .appName("TikTok-BatchLayer-Toxicity") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                    "org.postgresql:postgresql:42.7.3") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✓ Spark Session created")
        return spark
    
    def _define_schema(self) -> StructType:
        """Định nghĩa schema cho JSON"""
        comment_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("text", StringType(), True),
            StructField("created_at", StringType(), True)
        ])
        
        schema = StructType([
            StructField("video_id", StringType(), True),
            StructField("caption", StringType(), True),
            StructField("comments", ArrayType(comment_schema), True),
            StructField("hashtags", ArrayType(StringType()), True),
            StructField("created_at", StringType(), True)
        ])
        
        return schema
    
    def read_from_kafka(self):
        """
        Đọc toàn bộ dữ liệu từ Kafka (từ earliest offset)
        """
        logger.info(f"Reading from Kafka topic: {self.kafka_config['topic_raw']}")
        
        df = self.spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config['bootstrap_servers']) \
            .option("subscribe", self.kafka_config['topic_raw']) \
            .option("kafka.request.timeout.ms", "120000") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        # Parse JSON
        schema = self._define_schema()
        
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        count_records = parsed_df.count()
        logger.info(f"✓ Read {count_records} records from Kafka")
        
        return parsed_df
    
    def read_from_archive(self):
        """
        Đọc dữ liệu từ data lake/archive (Parquet/JSON)
        """
        archive_dir = self.batch_config['archive_dir']
        logger.info(f"Reading from archive: {archive_dir}")
        
        try:
            # Try Parquet first
            df = self.spark.read.parquet(archive_dir)
            logger.info(f"✓ Read {df.count()} records from Parquet archive")
            return df
        except:
            try:
                # Fallback to JSON
                schema = self._define_schema()
                df = self.spark.read.json(archive_dir, schema=schema)
                logger.info(f"✓ Read {df.count()} records from JSON archive")
                return df
            except Exception as e:
                logger.warning(f"No archive data found: {e}")
                return None
    
    def process_comments(self, df):
        """
        Xử lý comments: explode và detect toxicity
        """
        # Explode comments
        exploded_df = df.select(
            col("video_id"),
            col("hashtags"),
            explode(col("comments")).alias("comment")
        ).select(
            "video_id",
            "hashtags",
            col("comment.user_id").alias("user_id"),
            col("comment.text").alias("comment_text")
        )
        
        # Filter empty comments
        exploded_df = exploded_df.filter(
            (col("comment_text").isNotNull()) & 
            (col("comment_text") != "")
        )
        
        # Detect toxicity
        logger.info("Applying toxicity detection...")
        
        toxicity_df = exploded_df.withColumn(
            "toxicity_label",
            self.toxicity_udf(col("comment_text"))
        )
        
        # Add flags
        toxicity_df = toxicity_df.withColumn(
            "is_toxic",
            when(col("toxicity_label").isin(["HATE", "OFFENSIVE"]), 1).otherwise(0)
        ).withColumn(
            "is_hate",
            when(col("toxicity_label") == "HATE", 1).otherwise(0)
        ).withColumn(
            "is_offensive",
            when(col("toxicity_label") == "OFFENSIVE", 1).otherwise(0)
        ).withColumn(
            "is_clean",
            when(col("toxicity_label") == "CLEAN", 1).otherwise(0)
        )
        
        return toxicity_df
    
    def compute_video_stats(self, df):
        """
        Tính toán thống kê theo video_id
        """
        logger.info("Computing video statistics...")
        
        video_stats = df.groupBy("video_id").agg(
            count("*").alias("total_comments"),
            spark_sum("is_toxic").alias("toxic_comments"),
            spark_sum("is_hate").alias("hate_comments"),
            spark_sum("is_offensive").alias("offensive_comments"),
            spark_sum("is_clean").alias("clean_comments")
        )
        
        # Calculate toxic ratio
        video_stats = video_stats.withColumn(
            "toxic_ratio",
            (col("toxic_comments") / col("total_comments")).cast("float")
        ).withColumn(
            "computed_at",
            current_timestamp()
        )
        
        logger.info(f"✓ Computed stats for {video_stats.count()} videos")
        return video_stats
    
    def compute_hashtag_stats(self, df):
        """
        Tính toán thống kê theo hashtag
        """
        logger.info("Computing hashtag statistics...")
        
        # Explode hashtags
        hashtag_df = df.select(
            col("video_id"),
            col("is_toxic"),
            explode(col("hashtags")).alias("hashtag")
        ).filter(col("hashtag").isNotNull())
        
        # Aggregate by hashtag
        hashtag_stats = hashtag_df.groupBy("hashtag").agg(
            count("video_id").alias("total_videos"),
            count("*").alias("total_comments"),
            spark_sum("is_toxic").alias("toxic_comments")
        )
        
        # Calculate toxic ratio
        hashtag_stats = hashtag_stats.withColumn(
            "toxic_ratio",
            (col("toxic_comments") / col("total_comments")).cast("float")
        ).withColumn(
            "computed_at",
            current_timestamp()
        )
        
        # Sort by toxic ratio
        hashtag_stats = hashtag_stats.orderBy(col("toxic_ratio").desc())
        
        logger.info(f"✓ Computed stats for {hashtag_stats.count()} hashtags")
        return hashtag_stats
    
    def compute_user_ranking(self, df):
        """
        Tính toán ranking người dùng toxic nhất
        """
        logger.info("Computing user ranking...")
        
        user_stats = df.groupBy("user_id").agg(
            count("*").alias("total_comments"),
            spark_sum("is_toxic").alias("toxic_comments")
        )
        
        # Calculate toxic ratio
        user_stats = user_stats.withColumn(
            "toxic_ratio",
            (col("toxic_comments") / col("total_comments")).cast("float")
        ).withColumn(
            "computed_at",
            current_timestamp()
        )
        
        # Filter users with at least 10 comments
        user_stats = user_stats.filter(col("total_comments") >= 10)
        
        # Sort by toxic ratio
        user_stats = user_stats.orderBy(col("toxic_ratio").desc())
        
        logger.info(f"✓ Computed ranking for {user_stats.count()} users")
        return user_stats
    
    def compute_user_groups(self, df):
        """
        Nhóm người dùng có chung hành vi toxic theo hashtag
        """
        logger.info("Computing toxic user groups...")
        
        # 1. Lấy comments toxic và explode hashtags
        toxic_user_hashtags = df.filter(col("is_toxic") == 1) \
            .select(col("user_id"), explode(col("hashtags")).alias("hashtag"))
            
        if toxic_user_hashtags.count() == 0:
            logger.warning("No toxic comments found for user grouping")
            return None
            
        # 2. Tìm hashtag toxic nhất của mỗi user
        user_top_hashtag = toxic_user_hashtags.groupBy("user_id", "hashtag") \
            .agg(count("*").alias("hashtag_count"))
            
        # Window function để lấy top 1 hashtag cho mỗi user
        window_spec = Window.partitionBy("user_id").orderBy(col("hashtag_count").desc())
        
        user_groups = user_top_hashtag.withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .select(
                col("user_id"),
                col("hashtag").alias("group_topic"),
                col("hashtag_count").alias("toxic_comment_count")
            )
            
        user_groups = user_groups.withColumn("computed_at", current_timestamp())
        
        logger.info(f"✓ Created {user_groups.count()} user-group mappings")
        return user_groups
    
    def write_to_postgres(self, df, table_name: str, mode: str = "overwrite"):
        """
        Ghi DataFrame vào PostgreSQL
        """
        jdbc_url = self.postgres_config['jdbc_url']
        properties = {
            "user": self.postgres_config['user'],
            "password": self.postgres_config['password'],
            "driver": "org.postgresql.Driver"
        }
        
        try:
            write_to_postgres_jdbc(df, table_name, jdbc_url, properties, mode)
            logger.info(f"✓ Written to {table_name} (mode: {mode})")
        except Exception as e:
            logger.error(f"Failed to write to {table_name}: {e}")
            raise
    
    def run(self):
        """
        Chạy batch processing pipeline
        """
        logger.info("=" * 60)
        logger.info("Starting Batch Layer - Full Recompute")
        logger.info(f"Timestamp: {datetime.now()}")
        logger.info("=" * 60)
        
        try:
            # 1. Read data (try archive first, fallback to Kafka)
            df = self.read_from_archive()
            if df is None or df.count() == 0:
                df = self.read_from_kafka()
            
            if df is None or df.count() == 0:
                logger.warning("No data to process!")
                return
            
            # Cache for reuse
            df.cache()
            
            # 2. Process comments and detect toxicity
            toxicity_df = self.process_comments(df)
            toxicity_df.cache()
            
            # 3. Compute video stats
            video_stats = self.compute_video_stats(toxicity_df)
            self.write_to_postgres(video_stats, "batch_video_stats", mode="overwrite")
            
            # 4. Compute hashtag stats
            hashtag_stats = self.compute_hashtag_stats(toxicity_df)
            self.write_to_postgres(hashtag_stats, "batch_hashtag_stats", mode="overwrite")
            
            # 5. Compute user ranking
            user_ranking = self.compute_user_ranking(toxicity_df)
            self.write_to_postgres(user_ranking, "batch_user_ranking", mode="overwrite")
            
            # 6. Compute user groups
            user_groups = self.compute_user_groups(toxicity_df)
            if user_groups:
                self.write_to_postgres(user_groups, "batch_user_groups", mode="overwrite")
            
            logger.info("=" * 60)
            logger.info("✓ Batch processing completed successfully")
            logger.info("=" * 60)
        
        except Exception as e:
            logger.error(f"Batch processing error: {e}", exc_info=True)
            raise
        
        finally:
            self.spark.stop()
            logger.info("✓ Spark session stopped")


def main():
    """Entry point"""
    processor = BatchToxicityProcessor()
    processor.run()


if __name__ == "__main__":
    main()

