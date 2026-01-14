from kafka import KafkaAdminClient, KafkaConsumer
import sys

def check_kafka():
    bootstrap_servers = "127.0.0.1:9092"
    print(f"Testing connection to {bootstrap_servers}...")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers, 
            request_timeout_ms=10000
        )
        topics = admin_client.list_topics()
        print(f"✓ Connected successfully!")
        print(f"✓ Available topics: {topics}")
        
        if "tiktok-raw" in topics:
            print("✓ Topic 'tiktok-raw' exists.")
        else:
            print("! Topic 'tiktok-raw' DOES NOT exist.")
            
    except Exception as e:
        print(f"✗ Failed to connect via AdminClient: {e}")
        return False
        
    try:
        consumer = KafkaConsumer(
            "tiktok-raw",
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='debug-checker',
            request_timeout_ms=15000
        )
        print("✓ Consumer created. Checking for messages...")
        msgs = consumer.poll(timeout_ms=2000)
        if msgs:
            print(f"✓ Found {sum(len(m) for m in msgs.values())} messages in batch.")
            for partition, messages in msgs.items():
                print(f"  - Sample: {messages[0].value[:100]}")
        else:
            print("! No messages found in 'tiktok-raw' (poll timed out).")
        
        consumer.close()
    except Exception as e:
        print(f"✗ Failed to create Consumer: {e}")
        return False
        
    return True

if __name__ == "__main__":
    success = check_kafka()
    sys.exit(0 if success else 1)
