# 🚀 Quick Start Guide

Hướng dẫn nhanh để chạy TikTok Toxicity Detection Pipeline.

## ⚡ Chạy Nhanh (5 phút)

### Bước 1: Cài đặt Dependencies

```bash
# Install Python packages
pip install -r requirements.txt

# Start infrastructure (Kafka + PostgreSQL)
docker-compose up -d

# Wait for services to be ready (~30 seconds)
sleep 30
```

### Bước 2: Initialize Database

```bash
# Tạo tables và views
./scripts/init_db.sh
```

### Bước 3: Generate Sample Data

```bash
# Tạo dữ liệu test
python scripts/generate_sample_data.py

# Kiểm tra
ls -lh data/raw/
```

### Bước 4: Start Pipeline

Mở 2 terminal:

**Terminal 1 - Ingestion Layer (Kafka Producer):**
```bash
python src/ingestion/json_to_kafka.py
```

**Terminal 2 - Speed Layer (Streaming):**
```bash
./scripts/start_streaming.sh
```

### Bước 5: Query Results

```bash
# Connect to PostgreSQL
psql -h localhost -U tiktok_user -d tiktok_toxicity

# Query data
SELECT * FROM serving_video_stats LIMIT 10;
SELECT * FROM serving_top_toxic_videos LIMIT 5;
SELECT * FROM serving_recent_activity;
```

## 🔄 Workflow

```
1. Data Files → data/raw/*.jsonl
2. Producer → Kafka topic: tiktok-raw
3. Streaming → PostgreSQL: speed_*
4. Query → serving_* views
```

## 📊 Batch Layer (Optional)

```bash
# Chạy batch processing 1 lần
./scripts/run_batch.sh

# Setup cron (chạy hàng ngày lúc 2h sáng)
./scripts/setup_cron.sh
```

## 🧪 Testing Individual Components

### Test Model
```bash
python src/models/toxicity_detector.py
```

### Test Database Utils
```bash
python src/utils/db_utils.py
```

### Monitor Kafka
```bash
# View messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tiktok-raw \
  --from-beginning \
  --max-messages 10
```

## 🛑 Stop Services

```bash
# Stop Spark jobs (Ctrl+C in terminals)

# Stop infrastructure
docker-compose down

# Keep data
docker-compose down -v  # Remove volumes too
```

## 📈 Power BI Connection

```
Type: PostgreSQL
Server: localhost
Port: 5432
Database: tiktok_toxicity
Username: powerbi_reader
Password: powerbi_read123

Tables/Views to use:
- serving_video_stats
- serving_hashtag_stats
- serving_user_ranking
- serving_top_toxic_videos
- serving_recent_activity
```

## 🐛 Common Issues

### Lỗi: "Connection refused" (Kafka/PostgreSQL)

```bash
# Check services
docker-compose ps

# Restart if needed
docker-compose restart
```

### Lỗi: "Model not found"

Lần đầu chạy sẽ download model (~500MB). Cần internet connection.

### Lỗi: "Checkpoint already exists"

```bash
# Clear checkpoints
rm -rf checkpoints/speed/*
rm -rf checkpoints/batch/*
```

## 📚 Next Steps

1. Xem [README.md](README.md) để biết chi tiết đầy đủ
2. Chỉnh sửa [config/config.yaml](config/config.yaml) cho production
3. Setup monitoring và alerting
4. Connect Power BI để visualize

## 💡 Tips

- Speed Layer cần ~2GB RAM
- Batch Layer cần ~4GB RAM
- Model inference nhanh hơn với GPU (CUDA)
- Use Parquet format cho archive data (nhanh hơn JSON)

## ✅ Checklist

- [ ] Docker Desktop đang chạy
- [ ] Python 3.11+ installed
- [ ] Ít nhất 8GB RAM available
- [ ] Port 9092 (Kafka), 5432 (PostgreSQL) available
- [ ] Internet connection (download model lần đầu)

---

Có vấn đề? Xem [README.md](README.md) phần Troubleshooting hoặc tạo issue.

