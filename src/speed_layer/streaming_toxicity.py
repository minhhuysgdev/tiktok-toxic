"""
Speed Layer - Spark Structured Streaming cho Real-time Toxicity Detection
"""
import logging
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, explode, window, count, sum as spark_sum,
    when, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, TimestampType
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


class StreamingToxicityProcessor:
    """
    Spark Structured Streaming processor cho realtime toxicity detection
    """
    
    def __init__(self, config_path: str = None):
        """Khởi tạo với config"""
        if config_path is None:
            # Find project root from script location
            script_dir = Path(__file__).resolve().parent  # src/speed_layer/ (absolute)
            project_root = script_dir.parent.parent  # project root (absolute)
            config_path = project_root / "config" / "config.yaml"

        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.kafka_config = self.config['kafka']
        self.postgres_config = self.config['postgres']
        self.speed_config = self.config['speed_layer']
        self.model_config = self.config['model']

        # Convert relative paths to absolute paths from project root
        script_dir = Path(__file__).resolve().parent  # src/speed_layer/
        project_root = script_dir.parent.parent  # project root
        if 'checkpoint_dir' in self.speed_config:
            self.speed_config['checkpoint_dir'] = str(project_root / self.speed_config['checkpoint_dir'].lstrip('./'))
        
        # Khởi tạo Spark Session
        self.spark = self._create_spark_session()
        
        # Tạo toxicity detection UDF
        self.toxicity_udf = create_spark_udf(
            model_name=self.model_config['name'],
            device=self.model_config['device']
        )
        
        logger.info("✓ StreamingToxicityProcessor initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Tạo Spark Session với config phù hợp"""
        spark = SparkSession.builder \
            .appName("TikTok-SpeedLayer-Toxicity") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                    "org.postgresql:postgresql:42.7.3") \
            .config("spark.sql.streaming.checkpointLocation", 
                    self.speed_config['checkpoint_dir']) \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✓ Spark Session created")
        return spark
    
    def _define_schema(self) -> StructType:
        """
        Định nghĩa schema cho JSON từ Kafka
        """
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
        """Đọc streaming data từ Kafka"""
        logger.info(f"Reading from Kafka topic: {self.kafka_config['topic_raw']}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config['bootstrap_servers']) \
            .option("subscribe", self.kafka_config['topic_raw']) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON
        schema = self._define_schema()
        
        parsed_df = df.select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), schema).alias("data")
        ).select(
            "kafka_timestamp",
            "data.*"
        )
        
        logger.info("✓ Kafka stream configured")
        return parsed_df
    
    def process_comments(self, df):
        """
        Xử lý comments: explode và detect toxicity
        """
        # Explode comments
        exploded_df = df.select(
            col("video_id"),
            col("kafka_timestamp"),
            explode(col("comments")).alias("comment")
        ).select(
            "video_id",
            "kafka_timestamp",
            col("comment.user_id").alias("user_id"),
            col("comment.text").alias("comment_text")
        )
        
        # Filter empty comments
        exploded_df = exploded_df.filter(
            (col("comment_text").isNotNull()) & 
            (col("comment_text") != "")
        )
        
        # Detect toxicity using UDF
        logger.info("Applying toxicity detection UDF...")
        
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
    
    def aggregate_by_window(self, df):
        """
        Aggregate theo window và video_id
        """
        window_duration = self.speed_config['window_duration']
        watermark_delay = self.speed_config['watermark_delay']
        
        # Add watermark
        df = df.withWatermark("kafka_timestamp", watermark_delay)
        
        # Aggregate
        aggregated_df = df.groupBy(
            window(col("kafka_timestamp"), window_duration),
            col("video_id")
        ).agg(
            count("*").alias("total_comments"),
            spark_sum("is_toxic").alias("toxic_comments"),
            spark_sum("is_hate").alias("hate_comments"),
            spark_sum("is_offensive").alias("offensive_comments"),
            spark_sum("is_clean").alias("clean_comments")
        )
        
        # Calculate toxic ratio
        aggregated_df = aggregated_df.withColumn(
            "toxic_ratio",
            (col("toxic_comments") / col("total_comments")).cast("float")
        ).withColumn(
            "window_start",
            col("window.start")
        ).withColumn(
            "window_end",
            col("window.end")
        ).select(
            "video_id",
            "window_start",
            "window_end",
            "total_comments",
            "toxic_comments",
            "hate_comments",
            "offensive_comments",
            "clean_comments",
            "toxic_ratio"
        )
        
        return aggregated_df
    
    def write_to_postgres(self, df, table_name: str):
        """
        Ghi streaming data vào PostgreSQL
        """
        jdbc_url = self.postgres_config['jdbc_url']
        properties = {
            "user": self.postgres_config['user'],
            "password": self.postgres_config['password'],
            "driver": "org.postgresql.Driver"
        }
        
        checkpoint_dir = f"{self.speed_config['checkpoint_dir']}/{table_name}"
        
        def write_batch(batch_df, batch_id):
            """Write each micro-batch to PostgreSQL"""
            try:
                if batch_df.count() > 0:
                    batch_df.write \
                        .format("jdbc") \
                        .option("url", jdbc_url) \
                        .option("dbtable", table_name) \
                        .option("user", properties["user"]) \
                        .option("password", properties["password"]) \
                        .option("driver", properties["driver"]) \
                        .mode("append") \
                        .save()
                    
                    logger.info(f"✓ Batch {batch_id} written to {table_name}: {batch_df.count()} rows")
            except Exception as e:
                logger.error(f"Failed to write batch {batch_id}: {e}")
                raise
        
        query = df.writeStream \
            .outputMode("append") \
            .foreachBatch(write_batch) \
            .option("checkpointLocation", checkpoint_dir) \
            .start()
        
        return query
    
    def create_alerts(self, df):
        """
        Tạo alerts cho videos có toxic ratio cao
        """
        threshold = self.speed_config['toxic_threshold']
        
        alerts_df = df.filter(col("toxic_ratio") > threshold) \
            .select(
                "video_id",
                "window_end",
                "toxic_ratio",
                "total_comments",
                "toxic_comments"
            ) \
            .withColumn("created_at", current_timestamp())
        
        return alerts_df
    
    def run(self):
        """
        Chạy streaming pipeline
        """
        logger.info("=" * 60)
        logger.info("Starting Speed Layer - Streaming Toxicity Detection")
        logger.info("=" * 60)
        
        try:
            # 1. Read from Kafka
            raw_stream = self.read_from_kafka()
            
            # 2. Process comments and detect toxicity
            toxicity_stream = self.process_comments(raw_stream)
            
            # 3. Aggregate by window
            stats_stream = self.aggregate_by_window(toxicity_stream)
            
            # 4. Write stats to PostgreSQL
            stats_query = self.write_to_postgres(stats_stream, "speed_video_stats")
            
            # 5. Create and write alerts
            alerts_stream = self.create_alerts(stats_stream)
            alerts_query = self.write_to_postgres(alerts_stream, "speed_alerts")
            
            logger.info("✓ Streaming queries started")
            logger.info("Waiting for data... (Press Ctrl+C to stop)")
            
            # Wait for termination
            stats_query.awaitTermination()
            alerts_query.awaitTermination()
        
        except KeyboardInterrupt:
            logger.info("\n✓ Shutting down gracefully...")
        
        except Exception as e:
            logger.error(f"Streaming error: {e}", exc_info=True)
            raise
        
        finally:
            self.spark.stop()
            logger.info("✓ Spark session stopped")


def main():
    """Entry point"""
    processor = StreamingToxicityProcessor()
    processor.run()


if __name__ == "__main__":
    main()

