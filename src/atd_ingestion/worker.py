"""
Worker module for processing messages and executing as-cli commands
"""

import subprocess
import logging
import time
import queue
from typing import Dict, Any, Optional
from dataclasses import dataclass

from .config import Config


@dataclass
class ProcessResult:
    """Result of processing a file"""
    success: bool
    file_path: str
    tenant: Optional[str] = None
    duration: float = 0.0
    error: Optional[str] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None


class MessageProcessor:
    """Processes individual messages using as-cli"""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
    
    def process_message(self, message_data: Dict[str, Any]) -> ProcessResult:
        """Process a message containing file information"""
        start_time = time.time()
        
        try:
            # Extract filename and tenant from message
            filename = message_data.get('filename') or message_data.get('file_path')
            tenant = message_data.get('tenant')
            
            if not filename:
                return ProcessResult(
                    success=False,
                    file_path="unknown",
                    error="Message missing filename field",
                    duration=time.time() - start_time
                )
            
            # Process only single file per message
            if isinstance(filename, list):
                self.logger.warning(f"Message contains multiple files, processing only first: {filename[0]}")
                filename = filename[0]
            
            # Process the file
            result = self._process_file(filename, tenant)
            result.duration = time.time() - start_time
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            return ProcessResult(
                success=False,
                file_path=filename if 'filename' in locals() else "unknown",
                error=str(e),
                duration=time.time() - start_time
            )
    
    def _process_file(self, file_path: str, tenant: Optional[str] = None) -> ProcessResult:
        """Process a single file using as-cli"""
        try:
            # Build as-cli command
            cmd = self._build_command(file_path, tenant)
            
            self.logger.info(f"Executing: {' '.join(cmd)}")
            
            # Execute as-cli subprocess
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.config.as_cli.timeout
            )
            
            if result.returncode == 0:
                self.logger.info(f"Successfully ingested file: {file_path}")
                self.logger.debug(f"Output: {result.stdout}")
                return ProcessResult(
                    success=True,
                    file_path=file_path,
                    tenant=tenant,
                    stdout=result.stdout,
                    stderr=result.stderr
                )
            else:
                self.logger.error(f"Failed to ingest file: {file_path}")
                self.logger.error(f"Error: {result.stderr}")
                return ProcessResult(
                    success=False,
                    file_path=file_path,
                    tenant=tenant,
                    error=f"Command failed with return code {result.returncode}",
                    stdout=result.stdout,
                    stderr=result.stderr
                )
                
        except subprocess.TimeoutExpired:
            error_msg = f"Timeout while processing file: {file_path}"
            self.logger.error(error_msg)
            return ProcessResult(
                success=False,
                file_path=file_path,
                tenant=tenant,
                error=error_msg
            )
        except Exception as e:
            error_msg = f"Error processing file: {str(e)}"
            self.logger.error(error_msg)
            return ProcessResult(
                success=False,
                file_path=file_path,
                tenant=tenant,
                error=error_msg
            )
    
    def _build_command(self, file_path: str, tenant: Optional[str] = None) -> list:
        """Build the as-cli command"""
        cmd = [
            self.config.as_cli.executable,
            'ingest',
            'records',
            '--file', file_path
        ]
        
        # Add tenant-specific table if provided
        if tenant:
            table_name = f"{self.config.table_config.table_prefix}_{tenant}"
            cmd.extend(['--table-name', table_name])
        else:
            cmd.extend(['--table-name', self.config.table_config.default_table])
        
        # Add create table flag if configured
        if self.config.table_config.auto_create_tables:
            cmd.append('--create-table')
        
        # Add batch size if configured
        if self.config.table_config.batch_size:
            cmd.extend(['--batch-size', str(self.config.table_config.batch_size)])
        
        # Add schema file if configured
        if self.config.table_config.schema_file:
            cmd.extend(['--schema-file', self.config.table_config.schema_file])
        
        # Add any additional as-cli arguments from config
        if self.config.as_cli.additional_args:
            cmd.extend(self.config.as_cli.additional_args)
        
        return cmd


class Worker:
    """Worker thread that processes messages from the queue"""
    
    def __init__(self, worker_id: int, config: Config, logger: logging.Logger):
        self.worker_id = worker_id
        self.config = config
        self.logger = logger
        self.processor = MessageProcessor(config, logger)
        self.running = True
    
    def run(self, queue, stop_event):
        """Main worker loop"""
        self.logger.info(f"Worker {self.worker_id} started")
        
        while self.running and not stop_event.is_set():
            try:
                # Get message from queue with timeout (20 seconds)
                message = queue.get(timeout=20)
                
                if message is None:  # Poison pill to stop thread
                    break
                
                self.logger.info(f"Worker {self.worker_id} processing message: {message.value}")
                
                # Process with retry logic if enabled
                if self.config.service.retry_enabled:
                    result = self._process_with_retry(message.value)
                else:
                    result = self.processor.process_message(message.value)
                
                if result.success:
                    self.logger.info(
                        f"Worker {self.worker_id} successfully processed file: {result.file_path} "
                        f"in {result.duration:.2f}s"
                    )
                else:
                    self.logger.error(
                        f"Worker {self.worker_id} failed to process file: {result.file_path} "
                        f"- Error: {result.error}"
                    )
                
            except queue.Empty:
                # This is normal - no messages available
                self.logger.info(f"Worker {self.worker_id}: Nothing to process, continuing to poll for jobs")
                continue
            except Exception as e:
                if self.running:
                    self.logger.error(f"Worker {self.worker_id} error: {str(e)}")
        
        self.logger.info(f"Worker {self.worker_id} stopped")
    
    def stop(self):
        """Stop the worker"""
        self.running = False
    
    def _process_with_retry(self, message_data: Dict[str, Any]) -> ProcessResult:
        """Process message with retry logic"""
        last_result = None
        
        for attempt in range(1, self.config.service.max_attempts + 1):
            result = self.processor.process_message(message_data)
            last_result = result
            
            if result.success:
                return result
            
            if attempt < self.config.service.max_attempts:
                wait_time = self.config.service.backoff_seconds * attempt
                self.logger.warning(
                    f"Attempt {attempt} failed for file {result.file_path}. "
                    f"Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
        
        return last_result