#!/usr/bin/env python3
"""
Script to emit parquet filenames to Kafka for ATD ingestion
"""

import argparse
import json
import os
import time
from kafka import KafkaProducer


def emit_file(filename, topic='bulk_ingestion_parquet', tenant='nykaa'):
    """Emit a single parquet file to Kafka topic"""
    
    # Validate file exists
    if not os.path.exists(filename):
        print(f"Error: File not found: {filename}")
        return False
    
    # Get file size
    file_size = os.path.getsize(filename)
    
    # Create producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Create message
    message = {
        'filename': filename,
        'format': 'parquet',
        'size': file_size,
        'tenant': tenant,
        'timestamp': time.time(),
        'message_id': f'emit_{int(time.time())}'
    }
    
    print(f"Emitting file: {filename}")
    print(f"Size: {file_size} bytes ({file_size/1024/1024:.2f} MB)")
    print(f"Topic: {topic}")
    print(f"Tenant: {tenant} (will use table: atd_{tenant})")
    
    try:
        # Send message
        future = producer.send(topic, message)
        record = future.get(timeout=10)
        
        print(f"✓ Message sent successfully!")
        print(f"  Partition: {record.partition}")
        print(f"  Offset: {record.offset}")
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"Error sending message: {e}")
        producer.close()
        return False


def main():
    parser = argparse.ArgumentParser(description='Emit parquet filename to Kafka for ATD ingestion')
    parser.add_argument('filename', help='Path to parquet file')
    parser.add_argument('--topic', default='bulk_ingestion_parquet', help='Kafka topic (default: bulk_ingestion_parquet)')
    parser.add_argument('--tenant', default='nykaa', help='Tenant name (default: nykaa)')
    
    args = parser.parse_args()
    
    success = emit_file(args.filename, args.topic, args.tenant)
    exit(0 if success else 1)


if __name__ == "__main__":
    main()