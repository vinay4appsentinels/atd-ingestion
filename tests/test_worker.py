"""
Unit tests for worker module
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import subprocess

from src.atd_ingestion.config import Config, ASCLIConfig, TableConfig
from src.atd_ingestion.worker import MessageProcessor, ProcessResult


class TestMessageProcessor(unittest.TestCase):
    """Test message processing functionality"""
    
    def setUp(self):
        """Set up test dependencies"""
        self.config = Mock(spec=Config)
        self.config.as_cli = ASCLIConfig(
            executable='as-cli',
            timeout=300,
            additional_args=['--verbose']
        )
        self.config.table_config = TableConfig(
            auto_create_tables=True,
            table_prefix='test',
            default_table='test_logs',
            batch_size=1000,
            schema_file=None
        )
        
        self.logger = Mock()
        self.processor = MessageProcessor(self.config, self.logger)
    
    def test_process_message_success(self):
        """Test successful message processing"""
        message = {
            'filename': '/path/to/test.parquet',
            'tenant': 'tenant1'
        }
        
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='Success',
                stderr=''
            )
            
            result = self.processor.process_message(message)
            
            self.assertTrue(result.success)
            self.assertEqual(result.file_path, '/path/to/test.parquet')
            self.assertEqual(result.tenant, 'tenant1')
            self.assertIsNone(result.error)
    
    def test_process_message_missing_filename(self):
        """Test processing message without filename"""
        message = {'tenant': 'tenant1'}
        
        result = self.processor.process_message(message)
        
        self.assertFalse(result.success)
        self.assertEqual(result.error, "Message missing filename field")
    
    def test_process_message_with_list_filename(self):
        """Test processing message with list of filenames"""
        message = {
            'filename': ['/path/file1.parquet', '/path/file2.parquet'],
            'tenant': 'tenant1'
        }
        
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='Success',
                stderr=''
            )
            
            result = self.processor.process_message(message)
            
            # Should process only the first file
            self.assertTrue(result.success)
            self.assertEqual(result.file_path, '/path/file1.parquet')
    
    def test_build_command_with_tenant(self):
        """Test command building with tenant"""
        cmd = self.processor._build_command('/path/file.parquet', 'tenant1')
        
        expected = [
            'as-cli', 'ingest', 'records',
            '--file', '/path/file.parquet',
            '--table-name', 'test_tenant1',
            '--create-table',
            '--batch-size', '1000',
            '--verbose'
        ]
        
        self.assertEqual(cmd, expected)
    
    def test_build_command_without_tenant(self):
        """Test command building without tenant"""
        cmd = self.processor._build_command('/path/file.parquet', None)
        
        self.assertIn('--table-name', cmd)
        self.assertIn('test_logs', cmd)
        self.assertNotIn('test_', cmd[cmd.index('--table-name') + 1])
    
    def test_process_file_timeout(self):
        """Test handling of subprocess timeout"""
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired('cmd', 300)
            
            result = self.processor._process_file('/path/file.parquet', 'tenant1')
            
            self.assertFalse(result.success)
            self.assertIn('Timeout', result.error)
    
    def test_process_file_command_failure(self):
        """Test handling of command failure"""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout='',
                stderr='Error: Table not found'
            )
            
            result = self.processor._process_file('/path/file.parquet', 'tenant1')
            
            self.assertFalse(result.success)
            self.assertIn('return code 1', result.error)
            self.assertEqual(result.stderr, 'Error: Table not found')


if __name__ == '__main__':
    unittest.main()