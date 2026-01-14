#!/usr/bin/env python3
"""
Đọc TẤT CẢ messages từ Kafka và lưu vào PostgreSQL
"""
import sys
import time
import json
import yaml
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from src.models.toxicity_detector import ToxicityDetector
from src.utils.db_utils import PostgresHelper

def get_main_hashtag(hashtags_list, main_hashtags=None):
    """
    Xác định main hashtag từ danh sách hashtags.
    
    Logic:
    1. So sánh hashtags từ dữ liệu với main_hashtags từ config (sau khi normalize)
    2. Normalize: bỏ dấu #, lowercase, trim spaces để so sánh
    3. Nếu khớp, trả về giá trị GỐC từ config (có dấu #) để lưu vào DB
    
    Args:
        hashtags_list: List hashtags từ dữ liệu (có thể có hoặc không có dấu #)
        main_hashtags: List main hashtags từ config (có dấu #)
    
    Returns:
        main_hashtag từ config nếu khớp, None nếu không khớp
    """
    if not hashtags_list or not main_hashtags:
        return None
    
    # Đảm bảo hashtags_list là list
    if not isinstance(hashtags_list, list):
        return None
    
    # Lọc bỏ các giá trị None hoặc rỗng
    hashtags_list = [h for h in hashtags_list if h]
    if not hashtags_list:
        return None
    
    def normalize_tag(tag):
        """
        Chuẩn hóa hashtag để so sánh:
        - Bỏ dấu # ở đầu (nếu có)
        - Lowercase
        - Trim spaces
        """
        if not tag:
            return ""
        # Chuyển sang string nếu không phải
        tag_str = str(tag).strip()
        if not tag_str:
            return ""
        # Bỏ dấu # ở đầu nếu có
        tag_str = tag_str.lstrip('#')
        # Lowercase và trim
        return tag_str.lower().strip()
    
    # Chuẩn hóa tất cả hashtags trong list
    normalized_hashtags = [normalize_tag(h) for h in hashtags_list]
    # Lọc bỏ các giá trị rỗng sau khi normalize
    normalized_hashtags = [h for h in normalized_hashtags if h]
    
    if not normalized_hashtags:
        return None
    
    # So sánh với main_hashtags từ config
    # QUAN TRỌNG: Trả về giá trị GỐC từ config (có dấu #) để lưu vào DB
    for main_tag in main_hashtags:
        if not main_tag:
            continue
        normalized_main = normalize_tag(main_tag)
        if normalized_main and normalized_main in normalized_hashtags:
            # Trả về giá trị gốc từ config (có dấu #)
            return main_tag
    
    return None

def process_all_kafka_messages():
    """Đọc và xử lý TẤT CẢ messages từ Kafka, lưu vào DB"""
    print("=" * 60)
    print("🚀 Processing All Kafka Messages to Database")
    print("=" * 60)
    print()

    # 1. Đọc TẤT CẢ messages từ Kafka
    print("📥 Đang đọc tất cả messages từ Kafka...")
    start_read = time.time()

    consumer = KafkaConsumer(
        "tiktok-raw",
        bootstrap_servers="127.0.0.1:9092",
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=10000  # Tăng timeout
    )

    all_messages = []
    message_count = 0

    try:
        for msg in consumer:
            try:
                data = json.loads(msg.value.decode('utf-8'))
                all_messages.append(data)
                message_count += 1

                # Hiển thị progress mỗi 10 messages
                if message_count % 10 == 0:
                    print(f"   📦 Đã đọc {message_count} messages...")

            except json.JSONDecodeError:
                continue  # Bỏ qua message không parse được

    except Exception as e:
        print(f"⚠️  Dừng đọc sau {message_count} messages: {e}")

    consumer.close()

    if not all_messages:
        print("❌ Không có message nào trong Kafka topic!")
        print("   Hãy chạy: python scripts/produce_test_message.py")
        return

    read_time = time.time() - start_read
    print(f"✓ Đọc {message_count} messages thành công ({read_time:.2f}s)")
    print()
    
    # 2. Tổng hợp tất cả comments từ tất cả messages
    print("📝 Đang tổng hợp comments từ tất cả messages...")
    start_parse = time.time()

    all_comments = []
    all_comment_texts = []
    video_count = 0

    for data in all_messages:
        video_id = data.get('video_id', 'unknown')
        comments = data.get('comments', [])
        hashtags = data.get('hashtags', [])

        if comments:
            video_count += 1
            for comment in comments:
                text = comment.get('text', '').strip()
                if text:  # Chỉ lấy comments có text
                    all_comments.append({
                        'video_id': video_id,
                        'user_id': comment.get('user_id', ''),
                        'text': text,
                        'hashtags': hashtags
                    })
                    all_comment_texts.append(text)

    parse_time = time.time() - start_parse
    print(f"✓ Tổng hợp xong ({parse_time:.3f}s)")
    print(f"   Videos: {video_count}")
    print(f"   Total comments: {len(all_comments)}")
    print()

    if not all_comment_texts:
        print("❌ Không có comment nào để xử lý!")
        return

    # 3. Load model (lần đầu sẽ chậm)
    print("🤖 Load model ViHateT5 (batch_size=32)...")
    start_model_load = time.time()

    detector = ToxicityDetector(
        model_name="tarudesu/ViHateT5-base-HSD",
        device="cpu",
        batch_size=32  # Giảm để nhẹ hơn
    )

    model_load_time = time.time() - start_model_load
    print(f"✓ Model loaded ({model_load_time:.2f}s)")
    print()
    
    # 4. Xử lý TẤT CẢ comments với batch prediction
    print("=" * 60)
    print(f"🔍 Detect toxicity cho {len(all_comment_texts)} comments...")
    print("=" * 60)
    start_batch = time.time()

    # Process theo batches nhỏ để tránh memory issues
    batch_size = 100  # Process 100 comments cùng lúc
    all_results = []

    for i in range(0, len(all_comment_texts), batch_size):
        batch_texts = all_comment_texts[i:i + batch_size]
        batch_results = detector.predict_batch(batch_texts)
        all_results.extend(batch_results)

        # Progress indicator
        processed = min(i + batch_size, len(all_comment_texts))
        print(f"   📊 Processed {processed}/{len(all_comment_texts)} comments...")

    batch_total_time = time.time() - start_batch
    print()

    # Hiển thị sample results
    print("📋 Sample Results:")
    for i, (comment, label) in enumerate(zip(all_comments[:10], all_results[:10]), 1):
        status = "🚨" if label in ["HATE", "OFFENSIVE"] else "✅"
        print(f"  {i}. {status} [{label:8s}] {comment['text'][:40]}...")

    if len(all_comments) > 10:
        print(f"  ... và {len(all_comments) - 10} comments khác")

    print()
    print(f"✓ Đã xử lý {len(all_results)} comments trong {batch_total_time:.2f}s")
    print(f"  Trung bình: {batch_total_time/len(all_results)*1000:.1f}ms/comment")
    print(f"  Tốc độ: {len(all_results)/batch_total_time:.1f} comments/sec")
    print()
    
    # Thống kê toxicity tổng thể
    print("=" * 60)
    print("📈 Thống kê Toxicity Tổng Thể")
    print("=" * 60)
    toxic_count = sum(1 for r in all_results if r in ["HATE", "OFFENSIVE"])
    clean_count = sum(1 for r in all_results if r == "CLEAN")
    hate_count = sum(1 for r in all_results if r == "HATE")
    offensive_count = sum(1 for r in all_results if r == "OFFENSIVE")

    print(f"📊 Tổng kết:")
    print(f"   Messages: {message_count}")
    print(f"   Videos: {video_count}")
    print(f"   Comments: {len(all_results)}")
    print()
    print(f"🎯 Phân tích Toxicity:")
    print(f"   CLEAN: {clean_count} ({clean_count/len(all_results)*100:.1f}%)")
    print(f"   OFFENSIVE: {offensive_count} ({offensive_count/len(all_results)*100:.1f}%)")
    print(f"   HATE: {hate_count} ({hate_count/len(all_results)*100:.1f}%)")
    print(f"   TOXIC (tổng): {toxic_count} ({toxic_count/len(all_results)*100:.1f}%)")
    print()
    
    # 5. Lưu vào PostgreSQL
    print("=" * 60)
    print("💾 Đang lưu vào PostgreSQL...")
    print("=" * 60)
    start_db = time.time()
    db_time = 0
    
    try:
        # Load config
        project_root = Path(__file__).parent.parent
        config_path = project_root / "config" / "config.yaml"
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        postgres_config = config['postgres']
        db_helper = PostgresHelper(postgres_config)
        db_helper.connect()
        
        # Đảm bảo cột main_hashtag tồn tại trong cả 2 bảng (thêm nếu chưa có)
        try:
            conn = db_helper.connect()
            cursor = conn.cursor()
            
            # Kiểm tra và thêm vào speed_comments
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'speed_comments' AND column_name = 'main_hashtag'
            """)
            result = cursor.fetchone()
            if not result:
                print("⚠️  Cột 'main_hashtag' chưa tồn tại trong speed_comments, đang thêm...")
                cursor.execute("ALTER TABLE speed_comments ADD COLUMN main_hashtag VARCHAR(200);")
                conn.commit()
                print("✓ Đã thêm cột 'main_hashtag' vào bảng speed_comments")
            
            # Kiểm tra và thêm vào speed_video_stats
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'speed_video_stats' AND column_name = 'main_hashtag'
            """)
            result = cursor.fetchone()
            if not result:
                print("⚠️  Cột 'main_hashtag' chưa tồn tại trong speed_video_stats, đang thêm...")
                cursor.execute("ALTER TABLE speed_video_stats ADD COLUMN main_hashtag VARCHAR(200);")
                conn.commit()
                print("✓ Đã thêm cột 'main_hashtag' vào bảng speed_video_stats")
            
            cursor.close()
        except Exception as e:
            conn.rollback()
            print(f"⚠️  Không thể thêm cột main_hashtag: {e}")
            print("   Có thể cột đã tồn tại hoặc có lỗi khác")
        
        # Lấy main hashtags từ config
        main_hashtags = config.get('speed_layer', {}).get('main_hashtags', [])
        if not main_hashtags:
            print("⚠️  Cảnh báo: Không tìm thấy main_hashtags trong config!")
        else:
            print(f"📋 Main hashtags từ config ({len(main_hashtags)} hashtags):")
            for i, tag in enumerate(main_hashtags, 1):
                print(f"   {i}. {tag}")
        
        # Debug: Hiển thị một số hashtags mẫu từ dữ liệu
        sample_hashtags = set()
        hashtags_by_video = {}
        for comment in all_comments[:20]:  # Xem nhiều hơn để có đủ mẫu
            video_id = comment.get('video_id')
            hashtags = comment.get('hashtags', [])
            if hashtags:
                sample_hashtags.update(hashtags)
                if video_id not in hashtags_by_video:
                    hashtags_by_video[video_id] = hashtags
        
        if sample_hashtags:
            print(f"\n📋 Sample hashtags từ dữ liệu ({len(sample_hashtags)} unique hashtags):")
            sample_list = list(sample_hashtags)[:15]
            for i, tag in enumerate(sample_list, 1):
                print(f"   {i}. {tag}")
            
            # Test matching với một vài ví dụ
            print(f"\n🔍 Test matching logic:")
            for video_id, hashtags in list(hashtags_by_video.items())[:3]:
                test_main = get_main_hashtag(hashtags, main_hashtags)
                print(f"   Video {video_id}: hashtags={hashtags} -> main_hashtag={test_main}")
        else:
            print("⚠️  Không tìm thấy hashtags trong dữ liệu!")
        
        # Chuẩn bị data để insert comments
        kafka_timestamp = datetime.now()  # Dùng thời gian hiện tại vì không có message timestamp
        
        insert_query = """
        INSERT INTO speed_comments 
        (video_id, user_id, comment_text, toxicity_label, is_toxic, is_hate, is_offensive, is_clean, hashtags, main_hashtag, kafka_timestamp, processed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        insert_data = []
        main_hashtag_count = 0
        debug_samples_shown = 0
        
        print(f"\n💾 Đang chuẩn bị insert {len(all_comments)} comments...")
        
        for comment, label in zip(all_comments, all_results):
            if not comment.get('text'):
                continue
            
            hashtags = comment.get('hashtags', [])
            main_hashtag = get_main_hashtag(hashtags, main_hashtags)
            
            # Debug: Hiển thị một vài ví dụ cụ thể
            if debug_samples_shown < 3:
                print(f"   🔍 Ví dụ {debug_samples_shown + 1}: hashtags={hashtags} -> main_hashtag='{main_hashtag}'")
                debug_samples_shown += 1
            
            if main_hashtag:
                main_hashtag_count += 1
                
            is_toxic = 1 if label in ["HATE", "OFFENSIVE"] else 0
            is_hate = 1 if label == "HATE" else 0
            is_offensive = 1 if label == "OFFENSIVE" else 0
            is_clean = 1 if label == "CLEAN" else 0
            
            insert_data.append((
                comment.get('video_id'),
                comment.get('user_id'),
                comment.get('text'),
                label,
                is_toxic,
                is_hate,
                is_offensive,
                is_clean,
                hashtags,
                main_hashtag,
                kafka_timestamp,
                datetime.now()
            ))
        
        # Batch insert comments
        db_helper.execute_many(insert_query, insert_data)
        print(f"✓ Đã lưu {len(insert_data)} comments vào speed_comments")
        print(f"   📊 Comments có main_hashtag: {main_hashtag_count}/{len(insert_data)} ({main_hashtag_count/len(insert_data)*100:.1f}%)")
        
        # 6. Tính toán và lưu stats theo từng video vào speed_video_stats
        print("📊 Đang tính toán stats và lưu vào speed_video_stats...")
        
        # Nhóm comments theo video_id
        video_stats = defaultdict(lambda: {'comments': [], 'results': [], 'hashtags': []})
        
        for comment, label in zip(all_comments, all_results):
            video_id = comment.get('video_id')
            video_stats[video_id]['comments'].append(comment)
            video_stats[video_id]['results'].append(label)
            if comment.get('hashtags'):
                video_stats[video_id]['hashtags'] = comment.get('hashtags')
        
        # Tạo window (dùng kafka_timestamp làm window_end, window_start = window_end - 1 minute)
        window_end = kafka_timestamp
        window_start = window_end - timedelta(minutes=1)
        
        # Insert stats cho từng video
        stats_query = """
        INSERT INTO speed_video_stats 
        (video_id, window_start, window_end, total_comments, toxic_comments, hate_comments, offensive_comments, clean_comments, toxic_ratio, hashtags, main_hashtag)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (video_id, window_end) 
        DO UPDATE SET
            total_comments = EXCLUDED.total_comments,
            toxic_comments = EXCLUDED.toxic_comments,
            hate_comments = EXCLUDED.hate_comments,
            offensive_comments = EXCLUDED.offensive_comments,
            clean_comments = EXCLUDED.clean_comments,
            toxic_ratio = EXCLUDED.toxic_ratio,
            hashtags = EXCLUDED.hashtags,
            main_hashtag = EXCLUDED.main_hashtag
        """
        
        stats_inserted = 0
        main_hashtag_stats_count = 0
        
        for video_id, stats in video_stats.items():
            results = stats['results']
            total_comments = len(results)
            toxic_comments = sum(1 for r in results if r in ["HATE", "OFFENSIVE"])
            hate_comments = sum(1 for r in results if r == "HATE")
            offensive_comments = sum(1 for r in results if r == "OFFENSIVE")
            clean_comments = sum(1 for r in results if r == "CLEAN")
            toxic_ratio = toxic_comments / total_comments if total_comments > 0 else 0.0
            hashtags = stats.get('hashtags', [])
            
            # Tính toán main_hashtag từ hashtags của video
            main_hashtag = get_main_hashtag(hashtags, main_hashtags)
            if main_hashtag:
                main_hashtag_stats_count += 1
            
            stats_data = (
                video_id,
                window_start,
                window_end,
                total_comments,
                toxic_comments,
                hate_comments,
                offensive_comments,
                clean_comments,
                toxic_ratio,
                hashtags,
                main_hashtag
            )
            
            db_helper.execute_query(stats_query, stats_data)
            stats_inserted += 1
        
        db_time = time.time() - start_db
        print(f"✓ Đã lưu stats cho {stats_inserted} videos vào speed_video_stats ({db_time:.2f}s)")
        print(f"   📊 Videos có main_hashtag: {main_hashtag_stats_count}/{stats_inserted} ({main_hashtag_stats_count/stats_inserted*100:.1f}%)")
        print(f"   Window: {window_start} -> {window_end}")
        print()
        
        db_helper.close()
        
    except Exception as e:
        print(f"❌ Lỗi khi lưu vào PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        print()
    
    print("=" * 60)
    print("🎉 Test hoàn thành!")
    print("=" * 60)
    print()

    total_time = read_time + model_load_time + batch_total_time
    print("📊 TỔNG KẾT PERFORMANCE:")
    print(f"  📥 Đọc {message_count} messages: {read_time:.2f}s")
    print(f"  🤖 Load model: {model_load_time:.2f}s")
    print(f"  🚀 Xử lý {len(all_results)} comments: {batch_total_time:.2f}s")
    print(f"  ⏱️  TỔNG THỜI GIAN: {total_time:.2f}s")
    print()
    print("💡 Hiệu suất:")
    print(f"   - Tốc độ đọc: {message_count/read_time:.1f} messages/sec")
    print(f"   - Tốc độ xử lý: {len(all_results)/batch_total_time:.1f} comments/sec")
    print(f"   - Trung bình: {batch_total_time/len(all_results)*1000:.1f}ms/comment")
    print()
    print("🎯 Kết quả phân tích:")
    print(f"   - Tổng comments: {len(all_results)}")
    print(f"   - Tỷ lệ toxic: {toxic_count/len(all_results)*100:.1f}%")
    print(f"   - Videos processed: {video_count}")
    print()

if __name__ == "__main__":
    try:
        process_all_kafka_messages()
    except KeyboardInterrupt:
        print("\n⚠️  Đã dừng processing")
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
