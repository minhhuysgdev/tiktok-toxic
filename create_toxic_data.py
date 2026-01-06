#!/usr/bin/env python3
"""
Tạo data với nhiều toxic comments để test alerts (toxic_ratio > 0.7)
"""
import json
from datetime import datetime, timedelta
from pathlib import Path

def create_toxic_sample_data():
    """Tạo data với nhiều toxic comments để trigger alerts"""

    now = datetime.now()

    toxic_data = [
        {
            "video_id": "toxic_video_001",
            "caption": "Video gây tranh cãi",
            "comments": [
                {
                    "user_id": "user_toxic_001",
                    "text": "Đồ ngu ngốc hư hỏng",  # Toxic
                    "created_at": (now - timedelta(seconds=50)).strftime("%Y-%m-%dT%H:%M:%S")
                },
                {
                    "user_id": "user_toxic_002",
                    "text": "Thằng khốn nạn",  # Toxic
                    "created_at": (now - timedelta(seconds=40)).strftime("%Y-%m-%dT%H:%M:%S")
                },
                {
                    "user_id": "user_toxic_003",
                    "text": "Đồ óc chó ngu si",  # Toxic
                    "created_at": (now - timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%S")
                },
                {
                    "user_id": "user_toxic_004",
                    "text": "Video này tệ hại",  # Toxic
                    "created_at": (now - timedelta(seconds=20)).strftime("%Y-%m-%dT%H:%M:%S")
                },
                {
                    "user_id": "user_clean_001",
                    "text": "Tôi thích video này",  # Clean
                    "created_at": (now - timedelta(seconds=10)).strftime("%Y-%m-%dT%H:%M:%S")
                }
            ],
            "hashtags": ["controversial", "debate", "opinion"],
            "created_at": (now - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%S")
        },
        {
            "video_id": "toxic_video_002",
            "caption": "Nội dung gây sốc",
            "comments": [
                {
                    "user_id": "user_toxic_005",
                    "text": "Đồ rác rưởi",  # Toxic
                    "created_at": (now - timedelta(seconds=45)).strftime("%Y-%m-%dT%H:%M:%S")
                },
                {
                    "user_id": "user_toxic_006",
                    "text": "Thật kinh tởm",  # Toxic
                    "created_at": (now - timedelta(seconds=35)).strftime("%Y-%m-%dT%H:%M:%S")
                },
                {
                    "user_id": "user_toxic_007",
                    "text": "Đồ khốn kiếp",  # Toxic
                    "created_at": (now - timedelta(seconds=25)).strftime("%Y-%m-%dT%H:%M:%S")
                },
                {
                    "user_id": "user_toxic_008",
                    "text": "Tôi ghét cái này",  # Toxic
                    "created_at": (now - timedelta(seconds=15)).strftime("%Y-%m-%dT%H:%M:%S")
                },
                {
                    "user_id": "user_toxic_009",
                    "text": "Thật vô lý",  # Toxic
                    "created_at": (now - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%S")
                }
            ],
            "hashtags": ["shocking", "controversy", "debate"],
            "created_at": (now - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S")
        }
    ]

    # Tạo thư mục nếu chưa có
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Ghi file
    output_file = raw_dir / "toxic_test_data.jsonl"
    with open(output_file, 'w', encoding='utf-8') as f:
        for item in toxic_data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')

    print(f"✅ Đã tạo data toxic: {output_file}")
    print(f"📊 Timestamp hiện tại: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Video 1: 5 comments (4 toxic = 80% toxic_ratio)")
    print(f"📁 Video 2: 5 comments (5 toxic = 100% toxic_ratio)")
    print(f"🚨 Dự kiến: Cả 2 video sẽ trigger alerts (toxic_ratio > 0.7)")

    return str(output_file)

if __name__ == "__main__":
    create_toxic_sample_data()
