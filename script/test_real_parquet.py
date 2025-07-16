#!/usr/bin/env python3
"""
Send real parquet file to ATD ingestion service
"""

from kafka import KafkaProducer
import json
import time
import os

def send_real_parquet_file():
    """Send a real parquet file for testing"""
    # Pick the first parquet file from the directory
    parquet_file = "/home/vinaypatil/infosys/20250703-06h2843.parquet"
    
    # Check if file exists
    if not os.path.exists(parquet_file):
        print(f"File not found: {parquet_file}")
        return False
    
    file_size = os.path.getsize(parquet_file)
    print(f"Found parquet file: {parquet_file} ({file_size} bytes)")
    
    # Create producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Create message for real parquet file
    message = {
        "filename": parquet_file,
        "format": "parquet",
        "size": file_size,
        "tenant": "nykaa",  # This will create atd_nykaa table
        "timestamp": time.time(),
        "message_id": f"test_real_{int(time.time())}"
    }
    
    print(f"Sending message: {message}")
    
    # Send message
    future = producer.send('atd_test_topic', message)
    record = future.get(timeout=10)
    
    print(f"Message sent successfully!")
    print(f"  Topic: {record.topic}")
    print(f"  Partition: {record.partition}")
    print(f"  Offset: {record.offset}")
    
    producer.close()
    return True

if __name__ == "__main__":
    send_real_parquet_file()