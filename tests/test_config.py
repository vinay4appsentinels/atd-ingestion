"""
Unit tests for configuration module
"""

import unittest
import tempfile
import yaml
from pathlib import Path

from src.atd_ingestion.config import Config, KafkaConfig, ClickHouseConfig


class TestConfig(unittest.TestCase):
    """Test configuration loading and validation"""
    
    def setUp(self):
        """Create a temporary config file for testing"""
        self.test_config = {
            'kafka': {
                'topic': 'test-topic',
                'bootstrap_servers': ['localhost:9092'],
                'group_id': 'test-group',
                'auto_offset_reset': 'earliest',
                'enable_auto_commit': False
            },
            'clickhouse': {
                'host': 'test-host',
                'port': 8123,
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_pass'
            },
            'as_cli': {
                'executable': 'test-cli',
                'timeout': 600,
                'additional_args': ['--test']
            },
            'auto_create_tables': False,
            'table_prefix': 'test',
            'default_table': 'test_logs',
            'batch_size': 5000,
            'thread_pool_size': 2,
            'log_level': 'DEBUG',
            'log_file': 'test.log',
            'service': {
                'retry': {
                    'enabled': True,
                    'max_attempts': 2,
                    'backoff_seconds': 10
                }
            }
        }
        
        # Create temporary config file
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
        yaml.dump(self.test_config, self.temp_file)
        self.temp_file.close()
    
    def tearDown(self):
        """Clean up temporary file"""
        Path(self.temp_file.name).unlink()
    
    def test_load_from_yaml(self):
        """Test loading configuration from YAML file"""
        config = Config.from_yaml(self.temp_file.name)
        
        # Test Kafka config
        self.assertEqual(config.kafka.topic, 'test-topic')
        self.assertEqual(config.kafka.bootstrap_servers, ['localhost:9092'])
        self.assertEqual(config.kafka.group_id, 'test-group')
        self.assertEqual(config.kafka.auto_offset_reset, 'earliest')
        self.assertFalse(config.kafka.enable_auto_commit)
        
        # Test ClickHouse config
        self.assertEqual(config.clickhouse.host, 'test-host')
        self.assertEqual(config.clickhouse.port, 8123)
        self.assertEqual(config.clickhouse.database, 'test_db')
        self.assertEqual(config.clickhouse.user, 'test_user')
        self.assertEqual(config.clickhouse.password, 'test_pass')
        
        # Test AS-CLI config
        self.assertEqual(config.as_cli.executable, 'test-cli')
        self.assertEqual(config.as_cli.timeout, 600)
        self.assertEqual(config.as_cli.additional_args, ['--test'])
        
        # Test table config
        self.assertFalse(config.table_config.auto_create_tables)
        self.assertEqual(config.table_config.table_prefix, 'test')
        self.assertEqual(config.table_config.default_table, 'test_logs')
        self.assertEqual(config.table_config.batch_size, 5000)
        
        # Test other config
        self.assertEqual(config.thread_pool_size, 2)
        self.assertEqual(config.log_level, 'DEBUG')
        self.assertEqual(config.log_file, 'test.log')
    
    def test_to_dict(self):
        """Test converting configuration to dictionary"""
        config = Config.from_yaml(self.temp_file.name)
        config_dict = config.to_dict()
        
        self.assertEqual(config_dict['kafka']['topic'], 'test-topic')
        self.assertEqual(config_dict['clickhouse']['password'], '***')  # Password should be masked
        self.assertEqual(config_dict['thread_pool_size'], 2)
    
    def test_defaults(self):
        """Test configuration defaults"""
        minimal_config = {
            'kafka': {},
            'clickhouse': {},
            'as_cli': {}
        }
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
        yaml.dump(minimal_config, temp_file)
        temp_file.close()
        
        try:
            config = Config.from_yaml(temp_file.name)
            
            # Check defaults
            self.assertEqual(config.kafka.topic, 'atd-topic')
            self.assertEqual(config.kafka.group_id, 'atd-ingestion-group')
            self.assertTrue(config.kafka.enable_auto_commit)
            self.assertEqual(config.thread_pool_size, 4)
            self.assertEqual(config.log_level, 'INFO')
            
        finally:
            Path(temp_file.name).unlink()


if __name__ == '__main__':
    unittest.main()