# TikTok Toxicity Monitoring - Kiến Trúc Lambda (Không Airflow)

Dự án thu thập dữ liệu TikTok → phát hiện nội dung tiêu cực (toxicity/hate speech) → phân tích người dùng toxic và hashtag tranh cãi.

## Kiến Trúc Lambda (không Airflow)
- **Batch Layer**: Spark Batch job chạy hàng ngày (cron) → xử lý toàn bộ dữ liệu lịch sử → ghi vào PostgreSQL (batch views)
- **Speed Layer**: Spark Structured Streaming chạy liên tục → xử lý realtime/micro-batch → ghi vào PostgreSQL (speed views)
- **Serving Layer**: PostgreSQL Views merge batch + speed → Power BI connect trực tiếp
- **Ingestion**: Đọc file JSON/JSONL → push Kafka (hoặc trực tiếp file)

## Công nghệ
- Python 3.11+
- Apache Spark 3.5+ (Structured Streaming + Batch)
- Apache Kafka
- PostgreSQL
- Hugging Face model: tarudesu/ViHateT5-HSD (Vietnamese hate speech detection)
- Cron (hoặc Kubernetes CronJob) cho Batch Layer

Yêu cầu: Generate toàn bộ source code theo cấu trúc thư mục bên dưới.