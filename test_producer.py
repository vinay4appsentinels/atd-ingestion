#!/usr/bin/env python3
"""
Test producer for ATD Ingestion Service
Sends test messages to Kafka topic for testing
"""

from kafka import KafkaProducer
import json
import time
import argparse


def send_test_messages(bootstrap_servers, topic, num_messages=5):
    """Send test messages to Kafka topic"""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"Sending {num_messages} test messages to topic '{topic}'...")
    
    for i in range(num_messages):
        message = {
            "filename": f"/tmp/test_data_{i}.parquet",
            "format": "parquet",
            "size": 125,
            "tenant": "test_tenant",
            "timestamp": time.time(),
            "message_id": i
        }
        
        future = producer.send(topic, message)
        record_metadata = future.get(timeout=10)
        
        print(f"Sent message {i}: topic={record_metadata.topic}, "
              f"partition={record_metadata.partition}, "
              f"offset={record_metadata.offset}")
        
        time.sleep(1)  # Add delay between messages
    
    producer.flush()
    producer.close()
    print("All messages sent successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test producer for ATD Ingestion")
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='localhost:9092',
        help='Kafka bootstrap servers'
    )
    parser.add_argument(
        '--topic',
        type=str,
        default='atd-topic',
        help='Kafka topic name'
    )
    parser.add_argument(
        '--num-messages',
        type=int,
        default=5,
        help='Number of test messages to send'
    )
    
    args = parser.parse_args()
    
    send_test_messages(
        args.bootstrap_servers.split(','),
        args.topic,
        args.num_messages
    )