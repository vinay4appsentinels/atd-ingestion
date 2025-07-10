#!/usr/bin/env python3
"""
Script to emit all parquet files from a directory to Kafka for ATD ingestion
"""

import argparse
import json
import os
import time
import glob
from kafka import KafkaProducer


def emit_all_files(directory, topic='bulk_ingestion_parquet', tenant='nykaa', delay=1):
    """Emit all parquet files from directory to Kafka topic"""
    
    # Find all parquet files
    parquet_pattern = os.path.join(directory, "*.parquet")
    parquet_files = glob.glob(parquet_pattern)
    
    if not parquet_files:
        print(f"No parquet files found in {directory}")
        return False
    
    print(f"Found {len(parquet_files)} parquet files in {directory}")
    
    # Create producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    successful = 0
    failed = 0
    
    for i, parquet_file in enumerate(sorted(parquet_files), 1):
        try:
            file_size = os.path.getsize(parquet_file)
            filename = os.path.basename(parquet_file)
            
            # Create message
            message = {
                'filename': parquet_file,
                'format': 'parquet',
                'size': file_size,
                'tenant': tenant,
                'timestamp': time.time(),
                'message_id': f'bulk_{i}_{int(time.time())}'
            }
            
            print(f"[{i}/{len(parquet_files)}] Emitting: {filename} ({file_size/1024/1024:.2f} MB)")
            
            # Send message
            future = producer.send(topic, message)
            record = future.get(timeout=10)
            
            print(f"  ✓ Sent to partition {record.partition}, offset {record.offset}")
            successful += 1
            
            # Delay between messages
            if delay > 0 and i < len(parquet_files):
                time.sleep(delay)
                
        except Exception as e:
            print(f"  ✗ Error sending {filename}: {e}")
            failed += 1
    
    producer.close()
    
    print(f"\n=== Summary ===")
    print(f"Total files: {len(parquet_files)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Topic: {topic}")
    print(f"Tenant: {tenant} (will use table: atd_{tenant})")
    
    return failed == 0


def main():
    parser = argparse.ArgumentParser(description='Emit all parquet files from directory to Kafka for ATD ingestion')
    parser.add_argument('directory', help='Directory containing parquet files')
    parser.add_argument('topic', help='Kafka topic name')
    parser.add_argument('--tenant', default='nykaa', help='Tenant name (default: nykaa)')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay between messages in seconds (default: 1.0)')
    
    args = parser.parse_args()
    
    # Validate directory exists
    if not os.path.isdir(args.directory):
        print(f"Error: Directory not found: {args.directory}")
        exit(1)
    
    success = emit_all_files(args.directory, args.topic, args.tenant, args.delay)
    exit(0 if success else 1)


if __name__ == "__main__":
    main()