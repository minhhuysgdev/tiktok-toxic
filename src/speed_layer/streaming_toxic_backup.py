"""
Speed Layer - Spark Structured Streaming cho Real-time Toxicity Detection
"""
import logging
import sys
import warnings
from functools import partial
from pathlib import Path

# Suppress HuggingFace deprecation warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="huggingface_hub")

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, explode, when, current_timestamp, udf
)
from pyspark.sql.types import StringType
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


def get_main_hashtag(hashtags_list, main_hashtags=None):
    """Xác định main hashtag từ danh sách hashtags"""
    if not hashtags_list or not main_hashtags:
        return None
    
    def normalize_tag(tag):
        if not tag:
            return ""
        return tag.lower().strip().replace("#", "")
    
    normalized_hashtags = [normalize_tag(h) for h in hashtags_list]
    
    for main_tag in main_hashtags:
        normalized_main = normalize_tag(main_tag)
        if normalized_main in normalized_hashtags:
            return main_tag
    
    return None


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
        
        # Tạo toxicity detection UDF (sử dụng pandas UDF để xử lý theo batch)
        self.toxicity_udf = create_spark_udf(
            model_name=self.model_config['name'],
            device=self.model_config['device'],
            batch_size=self.model_config.get('batch_size', 32)
        )

        # Tạo UDF cho main hashtag (cached để tránh re-initialization)
        main_hashtags_list = list(self.speed_config.get('main_hashtags', []))
        get_main_hashtag_partial = partial(get_main_hashtag, main_hashtags=main_hashtags_list)
        self.get_main_hashtag_udf = udf(get_main_hashtag_partial, StringType()).asNondeterministic()
        
        logger.info("✓ StreamingToxicityProcessor initialized")
    
    def _preload_model(self):
        """Pre-load model trên executor để tránh bottleneck load lần đầu"""
        import time
        logger.info("🔄 Pre-loading model trên executor...")
        start_time = time.time()

        try:
            # Tạo một DataFrame test nhỏ để trigger việc load model
            test_data = [("Test comment để pre-load model", "Another test comment")]
            test_df = self.spark.createDataFrame(test_data, ["comment_text"])

            # Chạy UDF để trigger việc load model trên executor
            logger.info("🤖 Loading model (có thể mất 10-30s lần đầu)...")
            result_df = test_df.withColumn("toxicity_label", self.toxicity_udf(col("comment_text")))
            results = result_df.collect()  # Trigger execution

            load_time = time.time() - start_time
            logger.info(f"✓ Model đã được pre-load trên executor ({load_time:.2f}s)")
            logger.info(f"   Test results: {results}")

        except Exception as e:
            load_time = time.time() - start_time
            logger.warning(f"⚠️  Không thể pre-load model sau {load_time:.2f}s: {e}")
            logger.info("   Model sẽ được load khi có data đầu tiên")
    
    def _create_spark_session(self) -> SparkSession:
        """Tạo Spark Session với config tối ưu cho performance cao"""
        spark = SparkSession.builder \
            .appName("TikTok-SpeedLayer-Toxicity") \
            .master("local[1]") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                    "org.postgresql:postgresql:42.7.3") \
            .config("spark.sql.streaming.checkpointLocation",
                    self.speed_config['checkpoint_dir']) \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.driver.maxResultSize", "2g") \
            .config("spark.executor.cores", "1") \
            .config("spark.default.parallelism", "1") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.kryoserializer.buffer.max", "1024m") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        logger.info("✓ Spark Session created (tối ưu cho performance)")
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
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config['bootstrap_servers']) \
            .option("subscribe", self.kafka_config['topic_raw']) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON
        schema = self._define_schema()
        return df.select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("kafka_timestamp", "data.*")
    
    def process_comments(self, df):
        """Xử lý comments: explode và detect toxicity"""
        import time

        # 1. Explode comments
        logger.info("🔍 Đang explode comments...")
        start_explode = time.time()
        exploded_df = df.select(
            col("video_id"), col("kafka_timestamp"), col("hashtags"),
            explode(col("comments")).alias("comment")
        ).select(
            "video_id", "kafka_timestamp", "hashtags",
            col("comment.user_id").alias("user_id"),
            col("comment.text").alias("comment_text")
        )
        explode_time = time.time() - start_explode
        logger.info(f"✓ Explode xong ({explode_time:.3f}s)")

        # 2. Filter valid comments (có text)
        logger.info("🔍 Đang filter comments hợp lệ...")
        start_filter = time.time()
        filtered_df = exploded_df.filter(col("comment_text").isNotNull() & (col("comment_text") != ""))
        filter_time = time.time() - start_filter
        logger.info(f"✓ Filter xong ({filter_time:.3f}s) - Số lượng sẽ được tính trong batch processing")

        # 3. Detect toxicity (bottleneck chính)
        logger.info("🤖 Đang detect toxicity (có thể mất 5-20s lần đầu)...")
        start_detect = time.time()
        result = filtered_df.withColumn("toxicity_label", self.toxicity_udf(col("comment_text"))) \
            .withColumn("is_toxic", when(col("toxicity_label").isin(["HATE", "OFFENSIVE"]), 1).otherwise(0)) \
            .withColumn("is_hate", when(col("toxicity_label") == "HATE", 1).otherwise(0)) \
            .withColumn("is_offensive", when(col("toxicity_label") == "OFFENSIVE", 1).otherwise(0)) \
            .withColumn("is_clean", when(col("toxicity_label") == "CLEAN", 1).otherwise(0))
        detect_time = time.time() - start_detect
        logger.info(f"✓ Detect toxicity xong ({detect_time:.2f}s)")

        return result
    
    def prepare_comments_for_db(self, df):
        """Chuẩn bị comments để ghi vào PostgreSQL"""
        return df.select(
            "video_id", "user_id", "comment_text", "toxicity_label",
            "is_toxic", "is_hate", "is_offensive", "is_clean",
            "hashtags", "kafka_timestamp"
        ).withColumn("main_hashtag", self.get_main_hashtag_udf(col("hashtags"))) \
         .withColumn("processed_at", current_timestamp())
    
    
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
            """Ghi batch vào PostgreSQL"""
            import time
            batch_start = time.time()

            try:
                logger.info(f"🔍 Batch {batch_id} - Bắt đầu xử lý...")

                # Check if empty
                sample_check = time.time()
                if not batch_df.take(1):
                    logger.info(f"⚠️  Batch {batch_id} is empty, skipping")
                    return

                row_count = batch_df.count()
                check_time = time.time() - sample_check
                logger.info(f"📦 Batch {batch_id}: {row_count} comments -> {table_name} (check: {check_time:.3f}s)")

                # Prepare data for insert
                prepare_start = time.time()
                logger.info("💾 Đang chuẩn bị data để insert...")
                db_df = self.prepare_comments_for_db(batch_df)
                prepare_time = time.time() - prepare_start
                logger.info(f"✓ Chuẩn bị xong ({prepare_time:.3f}s)")

                # Insert to PostgreSQL
                logger.info("📝 Đang insert vào PostgreSQL...")
                insert_start = time.time()
                db_df.write.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", properties["user"]) \
                    .option("password", properties["password"]) \
                    .option("driver", properties["driver"]) \
                    .option("batchsize", "1000") \
                    .option("rewriteBatchedStatements", "true") \
                    .option("numPartitions", "1") \
                    .option("isolationLevel", "NONE") \
                    .option("truncate", "false") \
                    .mode("append") \
                    .save()

                insert_time = time.time() - insert_start
                total_time = time.time() - batch_start
                logger.info(f"✓ Đã lưu {row_count} comments ({insert_time:.2f}s insert, {total_time:.2f}s total)")

                # Performance metrics
                if row_count > 0:
                    per_comment = total_time / row_count * 1000
                    logger.info(f"   📊 Performance: {per_comment:.1f}ms/comment, {row_count/total_time:.1f} comments/sec")

            except Exception as e:
                logger.error(f"❌ Lỗi batch {batch_id}: {e}", exc_info=True)
        
        # Lấy trigger time từ config (mặc định 1 giây để xử lý nhanh)
        trigger_time = self.speed_config.get('trigger_interval', '1 second')
        
        query = df.writeStream \
            .outputMode("append") \
            .trigger(processingTime=trigger_time) \
            .foreachBatch(write_batch) \
            .option("checkpointLocation", checkpoint_dir) \
            .start()
        
        return query
    
    def run(self):
        """Chạy streaming pipeline - Đơn giản: chỉ detect và lưu comments"""
        logger.info("🚀 Starting Spark Streaming...")

        # Pre-load model để tránh bottleneck load lần đầu
        self._preload_model()

        try:
            # Read Kafka -> Process -> Write Comments to DB
            raw_stream = self.read_from_kafka()
            toxicity_stream = self.process_comments(raw_stream)
            
            # Chỉ lưu comments vào DB (đơn giản)
            comments_query = self.write_to_postgres(
                self.prepare_comments_for_db(toxicity_stream), 
                "speed_comments"
            )
            
            logger.info("✓ Streaming started - Đang đợi dữ liệu từ Kafka...")
            logger.info("   Comments sẽ được detect toxicity và lưu vào speed_comments")
            comments_query.awaitTermination()
        
        except KeyboardInterrupt:
            logger.info("\n✓ Shutting down...")
            try:
                if 'comments_query' in locals():
                    comments_query.stop()
            except Exception as e:
                logger.warning(f"Error stopping query: {e}")
        
        except Exception as e:
            logger.error(f"Streaming error: {e}", exc_info=True)
            try:
                if 'comments_query' in locals():
                    comments_query.stop()
            except:
                pass
            raise
        
        finally:
            # Đợi một chút để query dừng hoàn toàn
            import time
            time.sleep(1)
            try:
                self.spark.stop()
                logger.info("✓ Spark session stopped")
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {e}")

            # Log final performance summary
            logger.info("=" * 60)
            logger.info("🏁 Streaming session ended")
            logger.info("=" * 60)


def main():
    """Entry point"""
    processor = StreamingToxicityProcessor()
    processor.run()


if __name__ == "__main__":
    main()

