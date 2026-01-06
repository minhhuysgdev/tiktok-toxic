#!/usr/bin/env python3
"""
Tạo data mẫu với timestamp hiện tại để test Speed Layer
"""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

def create_new_sample_data():
    """Tạo data mẫu với timestamp hiện tại"""

    # Timestamp hiện tại
    now = datetime.now()

    sample_data = [
        {
            "video_id": "video_new_001",
            "caption": "Review đồ ăn sáng ngon",
            "comments": [
                {
                    "user_id": "user_new_001",
                    "text": "Hay quá bạn ơi!",
                    "created_at": (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S")
                },
                {
                    "user_id": "user_new_002",
                    "text": "Cảm ơn bạn đã chia sẻ",
                    "created_at": (now - timedelta(minutes=4)).strftime("%Y-%m-%dT%H:%M:%S")
                },
                {
                    "user_id": "user_new_003",
                    "text": "Đồ ngu ngốc",
                    "created_at": (now - timedelta(minutes=3)).strftime("%Y-%m-%dT%H:%M:%S")
                }
            ],
            "hashtags": ["food", "review", "breakfast"],
            "created_at": (now - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")
        },
        {
            "video_id": "video_new_002",
            "caption": "Dance challenge mới nhất",
            "comments": [
                {
                    "user_id": "user_new_004",
                    "text": "Đẹp quá đi thôi",
                    "created_at": (now - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%S")
                },
                {
                    "user_id": "user_new_005",
                    "text": "Thằng khốn nạn",
                    "created_at": (now - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S")
                },
                {
                    "user_id": "user_new_006",
                    "text": "Video rất hữu ích",
                    "created_at": now.strftime("%Y-%m-%dT%H:%M:%S")
                }
            ],
            "hashtags": ["dance", "tiktok", "challenge"],
            "created_at": (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S")
        }
    ]

    # Tạo thư mục nếu chưa có
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Ghi file
    output_file = raw_dir / "new_test_data.jsonl"
    with open(output_file, 'w', encoding='utf-8') as f:
        for item in sample_data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')

    print(f"✅ Đã tạo data mới: {output_file}")
    print(f"📊 Timestamp hiện tại: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 File có {len(sample_data)} records")

    return str(output_file)

if __name__ == "__main__":
    create_new_sample_data()
