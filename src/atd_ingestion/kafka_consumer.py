"""
Kafka consumer module for ATD Ingestion Service
"""

import json
import logging
from typing import Dict, Any, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from .config import Config


class KafkaMessageConsumer:
    """Manages Kafka consumer and message reception"""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.consumer: Optional[KafkaConsumer] = None
    
    def create_consumer(self) -> KafkaConsumer:
        """Create and configure Kafka consumer"""
        consumer_config = {
            'bootstrap_servers': self.config.kafka.bootstrap_servers,
            'group_id': self.config.kafka.group_id,
            'auto_offset_reset': self.config.kafka.auto_offset_reset,
            'enable_auto_commit': self.config.kafka.enable_auto_commit,
            'value_deserializer': lambda m: json.loads(m.decode('utf-8'))
        }
        
        # Add optional timeout configurations if present
        if hasattr(self.config.kafka, 'max_poll_interval_ms'):
            consumer_config['max_poll_interval_ms'] = self.config.kafka.max_poll_interval_ms
        if hasattr(self.config.kafka, 'session_timeout_ms'):
            consumer_config['session_timeout_ms'] = self.config.kafka.session_timeout_ms
            
        self.consumer = KafkaConsumer(
            self.config.kafka.topic,
            **consumer_config
        )
        
        self.logger.info(
            f"Connected to Kafka topic: {self.config.kafka.topic} "
            f"with consumer group: {self.config.kafka.group_id}"
        )
        return self.consumer
    
    def close(self):
        """Close the Kafka consumer"""
        if self.consumer:
            try:
                self.consumer.close()
                self.logger.info("Kafka consumer closed")
            except Exception as e:
                self.logger.error(f"Error closing Kafka consumer: {str(e)}")
    
    def poll_messages(self, timeout_ms: int = 1000) -> Dict:
        """Poll for messages from Kafka"""
        if not self.consumer:
            raise RuntimeError("Consumer not initialized")
        
        try:
            return self.consumer.poll(timeout_ms=timeout_ms)
        except KafkaError as e:
            self.logger.error(f"Kafka error during poll: {str(e)}")
            raise
    
    def commit(self):
        """Manually commit offsets"""
        if self.consumer and not self.config.kafka.enable_auto_commit:
            try:
                self.consumer.commit()
            except Exception as e:
                self.logger.error(f"Error committing offsets: {str(e)}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get consumer metrics"""
        if not self.consumer:
            return {}
        
        metrics = self.consumer.metrics()
        return {
            'records_consumed': metrics.get('records-consumed-total', 0),
            'bytes_consumed': metrics.get('bytes-consumed-total', 0),
            'fetch_latency_avg': metrics.get('fetch-latency-avg', 0),
            'records_per_request_avg': metrics.get('records-per-request-avg', 0)
        }