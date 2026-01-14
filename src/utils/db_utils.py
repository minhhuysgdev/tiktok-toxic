"""
Database Utilities - JDBC/PostgreSQL Helper Functions
"""
import logging
from typing import Dict, Any, List

import psycopg2
from psycopg2.extras import execute_batch
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class PostgresHelper:
    """
    Helper class để làm việc với PostgreSQL
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Khởi tạo PostgreSQL connection
        
        Args:
            config: Dict chứa postgres config (host, port, database, user, password)
        """
        self.config = config
        self.connection = None
    
    def connect(self):
        """Tạo connection đến PostgreSQL"""
        if self.connection is None or self.connection.closed:
            try:
                self.connection = psycopg2.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    database=self.config['database'],
                    user=self.config['user'],
                    password=self.config['password']
                )
                logger.info("✓ Connected to PostgreSQL")
            except Exception as e:
                logger.error(f"Failed to connect to PostgreSQL: {e}")
                raise
        return self.connection
    
    def close(self):
        """Đóng connection"""
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("✓ PostgreSQL connection closed")
    
    def execute_query(self, query: str, params: tuple = None):
        """
        Thực thi một query
        
        Args:
            query: SQL query
            params: Parameters cho query
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute(query, params)
            conn.commit()
            logger.info(f"✓ Query executed successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            cursor.close()
    
    def execute_many(self, query: str, data: List[tuple]):
        """
        Thực thi batch insert/update
        
        Args:
            query: SQL query với placeholders
            data: List of tuples chứa data
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            execute_batch(cursor, query, data, page_size=1000)
            conn.commit()
            logger.info(f"✓ Batch insert/update completed: {len(data)} rows")
        except Exception as e:
            conn.rollback()
            logger.error(f"Batch execution failed: {e}")
            raise
        finally:
            cursor.close()
    
    def create_table_if_not_exists(self, table_name: str, schema: str):
        """
        Tạo table nếu chưa tồn tại
        
        Args:
            table_name: Tên table
            schema: Schema definition
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        );
        """
        self.execute_query(query)
        logger.info(f"✓ Table {table_name} created/verified")
    
    def truncate_table(self, table_name: str):
        """Xóa toàn bộ dữ liệu trong table"""
        query = f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"
        self.execute_query(query)
        logger.info(f"✓ Table {table_name} truncated")


def write_to_postgres_jdbc(
    df: DataFrame,
    table_name: str,
    jdbc_url: str,
    properties: Dict[str, str],
    mode: str = "append"
):
    """
    Ghi DataFrame vào PostgreSQL qua JDBC (dùng trong Spark)
    
    Args:
        df: Spark DataFrame
        table_name: Tên table
        jdbc_url: JDBC URL
        properties: JDBC properties (user, password, driver)
        mode: Write mode (append/overwrite)
    """
    try:
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", properties.get("user")) \
            .option("password", properties.get("password")) \
            .option("driver", properties.get("driver", "org.postgresql.Driver")) \
            .mode(mode) \
            .save()
        
        logger.info(f"✓ DataFrame written to {table_name} (mode: {mode})")
    
    except Exception as e:
        logger.error(f"Failed to write to PostgreSQL: {e}")
        raise


def write_stream_to_postgres(
    df: DataFrame,
    table_name: str,
    jdbc_url: str,
    properties: Dict[str, str],
    checkpoint_location: str,
    output_mode: str = "append"
):
    """
    Ghi Streaming DataFrame vào PostgreSQL
    
    Args:
        df: Streaming DataFrame
        table_name: Tên table
        jdbc_url: JDBC URL
        properties: JDBC properties
        checkpoint_location: Checkpoint directory
        output_mode: Output mode (append/update/complete)
    """
    def write_batch_to_postgres(batch_df, batch_id):
        """Function để ghi mỗi micro-batch"""
        try:
            if batch_df.count() > 0:
                batch_df.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", properties.get("user")) \
                    .option("password", properties.get("password")) \
                    .option("driver", properties.get("driver", "org.postgresql.Driver")) \
                    .mode("append") \
                    .save()
                
                logger.info(f"✓ Batch {batch_id} written to {table_name}: {batch_df.count()} rows")
        except Exception as e:
            logger.error(f"Failed to write batch {batch_id}: {e}")
            raise
    
    # Start streaming query
    query = df.writeStream \
        .outputMode(output_mode) \
        .foreachBatch(write_batch_to_postgres) \
        .option("checkpointLocation", checkpoint_location) \
        .start()
    
    return query


def upsert_to_postgres(
    df: DataFrame,
    table_name: str,
    key_columns: List[str],
    jdbc_url: str,
    properties: Dict[str, str]
):
    """
    Upsert DataFrame vào PostgreSQL (INSERT ... ON CONFLICT UPDATE)
    
    Args:
        df: Spark DataFrame
        table_name: Tên table
        key_columns: Columns làm unique key
        jdbc_url: JDBC URL
        properties: JDBC properties
    """
    # Tạo temp table
    temp_table = f"{table_name}_temp"
    
    # Write to temp table
    write_to_postgres_jdbc(df, temp_table, jdbc_url, properties, mode="overwrite")
    
    # Execute upsert
    postgres_config = {
        'host': jdbc_url.split('//')[1].split(':')[0],
        'port': int(jdbc_url.split(':')[3].split('/')[0]),
        'database': jdbc_url.split('/')[-1],
        'user': properties.get("user"),
        'password': properties.get("password")
    }
    
    helper = PostgresHelper(postgres_config)
    
    # Build upsert query
    columns = df.columns
    conflict_target = ', '.join(key_columns)
    update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in key_columns])
    
    upsert_query = f"""
    INSERT INTO {table_name} ({', '.join(columns)})
    SELECT {', '.join(columns)} FROM {temp_table}
    ON CONFLICT ({conflict_target}) 
    DO UPDATE SET {update_set};
    """
    
    helper.execute_query(upsert_query)
    helper.execute_query(f"DROP TABLE {temp_table};")
    helper.close()
    
    logger.info(f"✓ Upsert completed for {table_name}")


def create_tables_schema(postgres_config: Dict[str, Any]):
    """
    Tạo toàn bộ tables schema cần thiết
    
    Args:
        postgres_config: PostgreSQL config
    """
    helper = PostgresHelper(postgres_config)
    
    # Speed Layer Tables
    helper.create_table_if_not_exists(
        "speed_video_stats",
        """
        video_id VARCHAR(100),
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        total_comments INT,
        toxic_comments INT,
        hate_comments INT,
        offensive_comments INT,
        clean_comments INT,
        toxic_ratio FLOAT,
        hashtags TEXT[],
        main_hashtag VARCHAR(200),
        PRIMARY KEY (video_id, window_end)
        """
    )
    
    helper.create_table_if_not_exists(
        "speed_alerts",
        """
        id SERIAL PRIMARY KEY,
        video_id VARCHAR(100),
        window_end TIMESTAMP,
        toxic_ratio FLOAT,
        total_comments INT,
        toxic_comments INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """
    )
    
    helper.create_table_if_not_exists(
        "speed_comments",
        """
        id SERIAL PRIMARY KEY,
        video_id VARCHAR(100),
        user_id VARCHAR(100),
        comment_text TEXT,
        toxicity_label VARCHAR(20),
        is_toxic INT,
        is_hate INT,
        is_offensive INT,
        is_clean INT,
        hashtags TEXT[],
        main_hashtag VARCHAR(200),
        kafka_timestamp TIMESTAMP,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """
    )
    
    # Batch Layer Tables
    helper.create_table_if_not_exists(
        "batch_video_stats",
        """
        video_id VARCHAR(100) PRIMARY KEY,
        total_comments INT,
        toxic_comments INT,
        hate_comments INT,
        offensive_comments INT,
        clean_comments INT,
        toxic_ratio FLOAT,
        computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """
    )
    
    helper.create_table_if_not_exists(
        "batch_hashtag_stats",
        """
        hashtag VARCHAR(200) PRIMARY KEY,
        total_videos INT,
        total_comments INT,
        toxic_comments INT,
        toxic_ratio FLOAT,
        computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """
    )
    
    helper.create_table_if_not_exists(
        "batch_user_ranking",
        """
        user_id VARCHAR(100) PRIMARY KEY,
        total_comments INT,
        toxic_comments INT,
        toxic_ratio FLOAT,
        computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """
    )
    
    helper.close()
    logger.info("✓ All tables created/verified")


if __name__ == "__main__":
    # Test
    logging.basicConfig(level=logging.INFO)
    
    config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'tiktok_toxicity',
        'user': 'tiktok_user',
        'password': 'tiktok_pass'
    }
    
    try:
        create_tables_schema(config)
    except Exception as e:
        logger.error(f"Test failed: {e}")

