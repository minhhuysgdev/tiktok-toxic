#!/usr/bin/env python3
"""
Generate sample TikTok data for testing
"""
import json
import random
from datetime import datetime, timedelta
from pathlib import Path


# Sample data
SAMPLE_CAPTIONS = [
    "Chia sẻ mẹo nấu ăn ngon",
    "Du lịch Đà Lạt cực đẹp",
    "Review sản phẩm mới",
    "Dance challenge #fyp",
    "Học tiếng Anh hiệu quả",
]

CLEAN_COMMENTS = [
    "Hay quá bạn ơi!",
    "Cảm ơn bạn đã chia sẻ",
    "Video rất hữu ích",
    "Mình đã thử và thành công",
    "Đẹp quá đi thôi",
    "Like và subscribe ủng hộ bạn",
]

OFFENSIVE_COMMENTS = [
    "Xem ngứa mắt quá",
    "Tệ thật đấy",
    "Chán không muốn xem nữa",
    "Làm gì mà dở thế",
]

HATE_COMMENTS = [
    "Đồ ngu ngốc",
    "Thằng khốn nạn",
    "Đồ óc chó",
    "Mày là thằng đần",
]

HASHTAGS = [
    ["fyp", "viral", "trending"],
    ["food", "cooking", "recipe"],
    ["travel", "vietnam", "dalat"],
    ["dance", "tiktok", "challenge"],
    ["education", "english", "learning"],
]

USER_IDS = [f"user_{i:04d}" for i in range(1, 101)]


def generate_comment(comment_type="clean"):
    """Generate a random comment"""
    if comment_type == "clean":
        text = random.choice(CLEAN_COMMENTS)
    elif comment_type == "offensive":
        text = random.choice(OFFENSIVE_COMMENTS)
    else:  # hate
        text = random.choice(HATE_COMMENTS)
    
    return {
        "user_id": random.choice(USER_IDS),
        "text": text,
        "created_at": (datetime.now() - timedelta(hours=random.randint(0, 24))).isoformat()
    }


def generate_video():
    """Generate a random video record"""
    video_id = f"video_{random.randint(10000, 99999)}"
    
    # Generate comments with different toxicity levels
    num_comments = random.randint(5, 30)
    comments = []
    
    for _ in range(num_comments):
        # 60% clean, 25% offensive, 15% hate
        rand = random.random()
        if rand < 0.60:
            comment_type = "clean"
        elif rand < 0.85:
            comment_type = "offensive"
        else:
            comment_type = "hate"
        
        comments.append(generate_comment(comment_type))
    
    return {
        "video_id": video_id,
        "caption": random.choice(SAMPLE_CAPTIONS),
        "comments": comments,
        "hashtags": random.choice(HASHTAGS),
        "created_at": (datetime.now() - timedelta(hours=random.randint(0, 48))).isoformat()
    }


def main():
    """Generate sample data files"""
    # Create data directory
    data_dir = Path("data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate multiple files
    num_files = 3
    videos_per_file = 50
    
    print("=" * 60)
    print("Generating Sample TikTok Data")
    print("=" * 60)
    
    for file_num in range(1, num_files + 1):
        filename = data_dir / f"tiktok_sample_{file_num}.jsonl"
        
        with open(filename, 'w', encoding='utf-8') as f:
            for i in range(videos_per_file):
                video = generate_video()
                f.write(json.dumps(video, ensure_ascii=False) + '\n')
        
        print(f"✓ Created {filename} with {videos_per_file} videos")
    
    print("\n" + "=" * 60)
    print(f"✓ Generated {num_files} files with {num_files * videos_per_file} total videos")
    print("=" * 60)
    
    # Print sample record
    print("\nSample record:")
    print(json.dumps(generate_video(), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

