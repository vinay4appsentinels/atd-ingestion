"""
Main service module for ATD Ingestion Service
"""

import logging
import signal
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Full
from typing import Optional

from .config import Config
from .kafka_consumer import KafkaMessageConsumer
from .worker import Worker, MessageProcessor
from .logging_setup import setup_logging


class ATDIngestionService:
    """Main service class that orchestrates Kafka consumption and message processing"""
    
    def __init__(self, config_path: str, topic_override: Optional[str] = None):
        # Load configuration
        self.config = Config.from_yaml(config_path)
        
        # Override topic if provided
        if topic_override:
            self.config.kafka.topic = topic_override
        
        # Setup logging
        self.logger = setup_logging(self.config)
        
        # Initialize components
        self.running = True
        self.kafka_consumer = KafkaMessageConsumer(self.config, self.logger)
        self.thread_pool: Optional[ThreadPoolExecutor] = None
        # Set queue size limit to prevent memory buildup (2x thread pool size)
        queue_size = self.config.thread_pool_size * 2
        self.processing_queue = Queue(maxsize=queue_size)
        self.workers = []
        self.stop_event = threading.Event()
        
        # Pre-filtering: Load tenant validator early
        self.tenant_validator = MessageProcessor(self.config, self.logger, 0)
        
        # Statistics
        self.stats = {
            'messages_received': 0,
            'messages_processed': 0,
            'messages_failed': 0,
            'start_time': time.time()
        }
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _initialize_tenant_tables(self):
        """Initialize tables for all allowed tenants on startup"""
        from .worker import MessageProcessor
        
        self.logger.info("INITIALIZING TENANT TABLES")
        self.logger.info("-" * 30)
        
        # Create a temporary MessageProcessor to access tenant management methods
        processor = MessageProcessor(self.config, self.logger, 0)
        allowed_tenants = processor._load_allowed_tenants()
        
        if not allowed_tenants:
            self.logger.info("No allowed tenants configured, skipping table initialization")
            return
        
        self.logger.info(f"Found {len(allowed_tenants)} allowed tenants: {', '.join(allowed_tenants)}")
        
        # First, update the cache of existing tables
        processor._update_tenant_table_cache()
        
        # Then create missing tables for allowed tenants
        missing_tenants = set(allowed_tenants) - processor.tenants_with_tables
        
        if not missing_tenants:
            self.logger.info("All allowed tenants already have tables")
        else:
            self.logger.info(f"Creating tables for {len(missing_tenants)} tenants: {', '.join(sorted(missing_tenants))}")
            
            success_count = 0
            for tenant in missing_tenants:
                self.logger.info(f"Creating table for tenant: {tenant}")
                try:
                    if processor._ensure_tenant_table_exists(tenant):
                        success_count += 1
                        self.logger.info(f"✓ Table created for tenant: {tenant}")
                    else:
                        self.logger.warning(f"✗ Failed to create table for tenant: {tenant}")
                except Exception as e:
                    self.logger.error(f"✗ Error creating table for tenant {tenant}: {str(e)}")
            
            self.logger.info(f"Table creation complete: {success_count}/{len(missing_tenants)} successful")
        
        self.logger.info(f"Total tenants with tables: {len(processor.tenants_with_tables)}")
        self.logger.info("-" * 30)
        
        # Return the cache for sharing with workers
        return processor.tenants_with_tables
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}. Shutting down...")
        self.stop()
    
    def start(self):
        """Start the service"""
        # Print startup banner
        self.logger.info("="*70)
        self.logger.info("ATD INGESTION SERVICE STARTING")
        self.logger.info(f"LOG LEVEL: {self.config.log_level}")
        self.logger.debug(f"Log file: {self.config.log_file}")        
        self.logger.info(f"Thread Pool Size: {self.config.thread_pool_size}")
        self.logger.info("="*70)
        
        # Print detailed configuration
        config_dict = self.config.to_dict()
        self.logger.info("Configuration Details:")
        self.logger.info("-"*50)
        
        # Kafka Configuration
        self.logger.info("KAFKA CONFIGURATION:")
        self.logger.info(f"  Topic: {config_dict['kafka']['topic']}")
        self.logger.info(f"  Bootstrap Servers: {config_dict['kafka']['bootstrap_servers']}")
        self.logger.info(f"  Consumer Group: {config_dict['kafka']['group_id']}")
        self.logger.info(f"  Auto Offset Reset: {config_dict['kafka']['auto_offset_reset']}")
        self.logger.info(f"  Auto Commit: {config_dict['kafka']['enable_auto_commit']}")
        self.logger.info(f"  Poll Timeout: {config_dict['kafka']['poll_timeout_ms']}ms ({config_dict['kafka']['poll_timeout_ms']/1000:.1f} seconds)")
        if 'max_poll_interval_ms' in config_dict['kafka']:
            self.logger.info(f"  Max Poll Interval: {config_dict['kafka']['max_poll_interval_ms']}ms ({config_dict['kafka']['max_poll_interval_ms']/60000:.1f} minutes)")
        if 'session_timeout_ms' in config_dict['kafka']:
            self.logger.info(f"  Session Timeout: {config_dict['kafka']['session_timeout_ms']}ms ({config_dict['kafka']['session_timeout_ms']/60000:.1f} minutes)")
        
        # ClickHouse Configuration
        self.logger.info("\nCLICKHOUSE CONFIGURATION:")
        self.logger.info(f"  Host: {config_dict['clickhouse']['host']}")
        self.logger.info(f"  Port: {config_dict['clickhouse']['port']}")
        self.logger.info(f"  Database: {config_dict['clickhouse']['database']}")
        
        # AS-CLI Configuration
        self.logger.info("\nAS-CLI CONFIGURATION:")
        self.logger.info(f"  Executable: {config_dict['as_cli']['executable']}")
        self.logger.info(f"  Timeout: {config_dict['as_cli']['timeout']}s")
        
        # Table Configuration
        self.logger.info("\nTABLE CONFIGURATION:")
        self.logger.info(f"  Auto Create Tables: {config_dict['table_config']['auto_create_tables']}")
        self.logger.info(f"  Table Prefix: {config_dict['table_config']['table_prefix']}")
        self.logger.info(f"  Default Table: {config_dict['table_config']['default_table']}")
        self.logger.info(f"  Batch Size: {config_dict['table_config']['batch_size']}")
        
        # Service Configuration
        self.logger.info("\nSERVICE CONFIGURATION:")
        self.logger.info(f"  Thread Pool Size: {config_dict['thread_pool_size']}")
        self.logger.info(f"  Log Level: {config_dict['log_level']} (Active: {logging.getLevelName(self.logger.level)})")
        self.logger.info(f"  Log File: {config_dict['log_file']}")
        self.logger.info(f"  Retry Enabled: {config_dict['service']['retry']['enabled']}")
        if config_dict['service']['retry']['enabled']:
            self.logger.info(f"  Max Retry Attempts: {config_dict['service']['retry']['max_attempts']}")
            self.logger.info(f"  Retry Backoff: {config_dict['service']['retry']['backoff_seconds']}s")
        
        self.logger.info("-"*50)
        
        # Initialize tenant tables on startup and get shared cache
        self.shared_tenant_cache = self._initialize_tenant_tables()
        
        try:
            # Create Kafka consumer
            self.logger.info("Creating Kafka consumer...")
            self.kafka_consumer.create_consumer()
            
            # Create thread pool and workers
            self.logger.info("Starting worker threads...")
            self._start_workers()
            
            # Service is ready
            self.logger.info("="*70)
            self.logger.info("ATD INGESTION SERVICE STARTED SUCCESSFULLY")
            self.logger.info(f"Listening on topic: {config_dict['kafka']['topic']}")
            self.logger.info(f"Ready to process messages with {config_dict['thread_pool_size']} workers")
            self.logger.info("="*70)
            
            # Main processing loop
            self._run_main_loop()
            
        except Exception as e:
            self.logger.error(f"Fatal error in service: {str(e)}", exc_info=True)
            raise
        finally:
            self.shutdown()
    
    def _start_workers(self):
        """Start worker threads"""
        self.thread_pool = ThreadPoolExecutor(max_workers=self.config.thread_pool_size)
        
        for i in range(self.config.thread_pool_size):
            worker = Worker(i, self.config, self.logger, self.shared_tenant_cache)
            self.workers.append(worker)
            self.thread_pool.submit(worker.run, self.processing_queue, self.stop_event)
        
        self.logger.info(f"Started {self.config.thread_pool_size} worker threads")
    
    def _run_main_loop(self):
        """Main service loop that consumes from Kafka and queues messages"""
        self.logger.info("Entering main processing loop")
        
        # Track time for periodic logging
        last_poll_log_time = time.time()
        poll_log_interval = 20  # Log every 20 seconds when no messages
        polls_without_messages = 0
        
        while self.running:
            try:
                # Poll for messages
                poll_start = time.time()
                self.logger.info(f"Main service: Starting poll #{polls_without_messages + 1}")
                messages = self.kafka_consumer.poll_messages(timeout_ms=self.config.kafka.poll_timeout_ms)
                poll_duration = time.time() - poll_start
                self.logger.info(f"Main service: Poll returned after {poll_duration:.3f}s with: {messages}")
                
                # Check if we actually got any records
                has_messages = False
                if messages:
                    for records in messages.values():
                        if records:
                            has_messages = True
                            break
                
                if has_messages:
                    # Process messages
                    for topic_partition, records in messages.items():
                        for record in records:
                            self.stats['messages_received'] += 1
                            self.logger.info(
                                f"Received message: offset={record.offset}, "
                                f"partition={record.partition}"
                            )
                            
                            # PRE-FILTER: Check tenant before queueing
                            if self._should_process_message(record):
                                try:
                                    # Add to processing queue with timeout to prevent blocking
                                    self.processing_queue.put(record, timeout=1.0)
                                except Full:
                                    self.logger.warning(
                                        f"Processing queue full, dropping message: "
                                        f"offset={record.offset}, partition={record.partition}"
                                    )
                                    self.stats['messages_failed'] += 1
                            else:
                                self.logger.info(
                                    f"Filtered out message for disabled tenant: "
                                    f"offset={record.offset}, partition={record.partition}"
                                )
                                self.stats['messages_failed'] += 1
                    
                    # Reset poll log timer when messages are received
                    last_poll_log_time = time.time()
                    polls_without_messages = 0
                else:
                    # No messages received
                    polls_without_messages += 1
                    current_time = time.time()
                    
                    # Debug: log status every 3 polls (approximately every minute with 20s polls)
                    if polls_without_messages % 3 == 0:
                        self.logger.info(f"DEBUG: No messages for {polls_without_messages} polls (poll took {poll_duration:.3f}s)")
                    
                    # Log every 5 polls (approximately 100 seconds with 20s polls)
                    if polls_without_messages == 5:
                        self.logger.info(
                            f"Main service: No messages from Kafka topic '{self.config.kafka.topic}', "
                            f"continuing to poll... (Queue size: {self.processing_queue.qsize()})"
                        )
                        polls_without_messages = 0
                
                # Log statistics periodically
                if self.stats['messages_received'] % 100 == 0 and self.stats['messages_received'] > 0:
                    self._log_statistics()
                
            except Exception as e:
                self.logger.error(f"Error in main loop: {str(e)}")
                if self.running:
                    time.sleep(5)  # Wait before retrying
    
    def stop(self):
        """Stop the service"""
        self.logger.info("Stopping ATD Ingestion Service")
        self.running = False
        self.stop_event.set()
    
    def shutdown(self):
        """Cleanup resources"""
        self.logger.info("Shutting down ATD Ingestion Service")
        
        # Stop workers
        if self.thread_pool:
            # Send poison pills to stop worker threads
            for _ in range(self.config.thread_pool_size):
                self.processing_queue.put(None)
            
            # Shutdown thread pool
            self.thread_pool.shutdown(wait=True)
            self.logger.info("Worker threads stopped")
        
        # Close Kafka consumer
        self.kafka_consumer.close()
        
        # Log final statistics
        self._log_statistics()
        
        self.logger.info("ATD Ingestion Service stopped")
    
    def _should_process_message(self, record) -> bool:
        """Pre-filter messages based on tenant allowlist before queueing"""
        try:
            # Extract tenant from message
            message_data = record.value
            if not isinstance(message_data, dict):
                return True  # Let worker handle malformed messages
            
            tenant = message_data.get('tenant')
            
            # If no tenant specified, allow processing
            if not tenant:
                return True
            
            # Check if tenant is allowed
            return self.tenant_validator._is_tenant_allowed(tenant)
            
        except Exception as e:
            self.logger.warning(f"Error pre-filtering message: {str(e)}")
            return True  # When in doubt, let worker handle it
    
    def _log_statistics(self):
        """Log service statistics"""
        uptime = time.time() - self.stats['start_time']
        self.logger.info(
            f"Statistics - Uptime: {uptime:.0f}s, "
            f"Messages received: {self.stats['messages_received']}, "
            f"Queue size: {self.processing_queue.qsize()}"
        )


def main():
    """Main entry point"""
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description="ATD Ingestion Service")
    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--topic',
        type=str,
        help='Kafka topic to consume from (overrides config file)'
    )
    
    args = parser.parse_args()
    
    # Check for topic override from environment variable
    topic_override = args.topic or os.getenv('KAFKA_TOPIC')
    
    try:
        service = ATDIngestionService(args.config, topic_override)
        service.start()
    except KeyboardInterrupt:
        print("\nShutdown requested via keyboard interrupt")
        sys.exit(0)
    except Exception as e:
        print(f"Service failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()