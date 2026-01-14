#!/usr/bin/env python3
"""
Script để đẩy test message đúng format vào Kafka
"""
from kafka import KafkaProducer
import json
from datetime import datetime

def produce_test_message():
    """Đẩy test message với format đúng"""
    producer = KafkaProducer(
        bootstrap_servers='127.0.0.1:9092',
        api_version=(0, 10, 1),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Tạo test message đúng format
    test_message = {
        "video_id": "test_video_001",
        "caption": "Test video caption",
        "comments": [
            {
                "user_id": "user_001",
                "text": "Xin chào bạn khỏe không?",
                "created_at": datetime.now().isoformat() + "Z"
            },
            {
                "user_id": "user_002", 
                "text": "Đồ ngu ngốc!",
                "created_at": datetime.now().isoformat() + "Z"
            },
            {
                "user_id": "user_003",
                "text": "Video hay quá!",
                "created_at": datetime.now().isoformat() + "Z"
            }
        ],
        "hashtags": ["#test", "#demo", "#toxicity"],
        "created_at": datetime.now().isoformat() + "Z"
    }
    
    try:
        print("Sending test message to Kafka...")
        print(f"Message: {json.dumps(test_message, indent=2, ensure_ascii=False)}")
        
        future = producer.send('tiktok-raw', test_message)
        result = future.get(timeout=10)
        
        print(f"\n✓ Message sent successfully!")
        print(f"  Topic: {result.topic}")
        print(f"  Partition: {result.partition}")
        print(f"  Offset: {result.offset}")
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"✗ Failed to send message: {e}")
        producer.close()
        return False

if __name__ == "__main__":
    produce_test_message()
