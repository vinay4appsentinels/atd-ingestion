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
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            
            # Core timeout configurations
            'session_timeout_ms': 10000,  # 10 seconds default
            'heartbeat_interval_ms': 3000,  # 3 seconds (must be < session_timeout_ms/3)
            'max_poll_interval_ms': 300000,  # 5 minutes default
            'request_timeout_ms': 30000,  # 30 seconds
            
            # Connection and retry settings
            'connections_max_idle_ms': 540000,  # 9 minutes
            'reconnect_backoff_ms': 50,
            'reconnect_backoff_max_ms': 1000,
            'retry_backoff_ms': 100,
            
            # Metadata settings
            'metadata_max_age_ms': 300000,  # 5 minutes
            'api_version_auto_timeout_ms': 5000,  # 5 seconds
            
            # Consumer position
            'auto_commit_interval_ms': 5000,  # 5 seconds
            
            # IMPORTANT: This ensures poll() returns even with no data
            'consumer_timeout_ms': -1,  # No consumer timeout (poll timeout is controlled by poll())
        }
        
        # Override with config values if present
        if hasattr(self.config.kafka, 'max_poll_interval_ms'):
            consumer_config['max_poll_interval_ms'] = self.config.kafka.max_poll_interval_ms
        if hasattr(self.config.kafka, 'session_timeout_ms'):
            consumer_config['session_timeout_ms'] = self.config.kafka.session_timeout_ms
            # Adjust heartbeat interval accordingly
            consumer_config['heartbeat_interval_ms'] = min(3000, consumer_config['session_timeout_ms'] // 3)
            
        self.logger.info(f"Creating Kafka consumer with config: {consumer_config}")
            
        # Create consumer and subscribe to topic
        self.consumer = KafkaConsumer(
            self.config.kafka.topic,
            **consumer_config
        )
        
        self.logger.info(
            f"Kafka consumer created and subscribed to topic: {self.config.kafka.topic} "
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
        """Poll for messages from Kafka
        
        Args:
            timeout_ms: Maximum time to wait for messages in milliseconds
            
        Returns:
            Dictionary of TopicPartition -> list of ConsumerRecords
            Returns empty dict {} if no messages are available within timeout
        """
        if not self.consumer:
            raise RuntimeError("Consumer not initialized")
        
        try:
            # Poll for messages - this MUST return within timeout_ms
            # even if no messages are available
            result = self.consumer.poll(timeout_ms=timeout_ms, max_records=500)
            
            # Log only if we got messages
            if result:
                total_records = sum(len(records) for records in result.values())
                self.logger.debug(f"Poll returned {total_records} records from {len(result)} partitions")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error during poll: {type(e).__name__}: {str(e)}")
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