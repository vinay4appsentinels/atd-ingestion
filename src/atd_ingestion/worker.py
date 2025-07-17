"""
Worker module for processing messages and executing as-cli commands
"""

import subprocess
import logging
import time
import os
from queue import Empty
from typing import Dict, Any, Optional, List
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
        self.allowed_tenants = self._load_allowed_tenants()
        self.tenants_with_tables = set()  # Cache of tenants that have tables
    
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
            
            # Validate tenant
            if tenant and not self._is_tenant_allowed(tenant):
                return ProcessResult(
                    success=False,
                    file_path=filename,
                    tenant=tenant,
                    error=f"Tenant '{tenant}' is not in the allowlist",
                    duration=time.time() - start_time
                )
            
            # Ensure tenant table exists (if tenant is provided and allowed)
            if tenant and not self._ensure_tenant_table_exists(tenant):
                self.logger.warning(f"Failed to ensure table exists for tenant '{tenant}', proceeding anyway")
                # Continue processing even if table creation fails
            
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
                self.logger.info(f"AS-CLI Output: {result.stdout}")
                if result.stderr:
                    self.logger.info(f"AS-CLI Stderr: {result.stderr}")
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
        
        # Add stateless database configuration options
        if hasattr(self.config, 'clickhouse'):
            if self.config.clickhouse.host:
                cmd.extend(['--db-host', self.config.clickhouse.host])
            if self.config.clickhouse.port:
                cmd.extend(['--db-port', str(self.config.clickhouse.port)])
            if self.config.clickhouse.database:
                cmd.extend(['--db-database', self.config.clickhouse.database])
            if hasattr(self.config.clickhouse, 'user') and self.config.clickhouse.user:
                cmd.extend(['--db-user', self.config.clickhouse.user])
            if hasattr(self.config.clickhouse, 'password') and self.config.clickhouse.password:
                cmd.extend(['--db-password', self.config.clickhouse.password])
        
        # Add any additional as-cli arguments from config
        if self.config.as_cli.additional_args:
            cmd.extend(self.config.as_cli.additional_args)
        
        return cmd
    
    def _load_allowed_tenants(self) -> List[str]:
        """Load allowed tenants from configuration or environment"""
        # Check environment variable first
        env_tenants = os.getenv('ALLOWED_TENANTS')
        if env_tenants:
            return [t.strip() for t in env_tenants.split(',')]
        
        # Check configuration
        if hasattr(self.config, 'allowed_tenants'):
            return self.config.allowed_tenants
        
        # Default allowed tenants
        return ['netskope_boomskope_npa','mcdonalds_default']
    
    def _is_tenant_allowed(self, tenant: str) -> bool:
        """Check if tenant is in the allowlist"""
        if not tenant:
            return True  # Allow messages without tenant
        
        # Normalize tenant name
        tenant = tenant.lower().strip()
        
        # Check against allowlist
        return tenant in self.allowed_tenants
    
    def _update_tenant_table_cache(self):
        """Update cache of tenants that have tables by querying ClickHouse"""
        try:
            self.logger.info("Updating tenant table cache...")
            
            # Use ClickHouse query to get all tables starting with 'atd_'
            import subprocess
            clickhouse_cmd = ['clickhouse-client', '--query', 
                             "SELECT name FROM system.tables WHERE database = 'default' AND name LIKE 'atd_%'"]
            
            # Add connection parameters
            if hasattr(self.config, 'clickhouse'):
                if self.config.clickhouse.host:
                    clickhouse_cmd.extend(['--host', self.config.clickhouse.host])
                if self.config.clickhouse.port:
                    clickhouse_cmd.extend(['--port', str(self.config.clickhouse.port)])
                if hasattr(self.config.clickhouse, 'user') and self.config.clickhouse.user:
                    clickhouse_cmd.extend(['--user', self.config.clickhouse.user])
                if hasattr(self.config.clickhouse, 'password') and self.config.clickhouse.password:
                    clickhouse_cmd.extend(['--password', self.config.clickhouse.password])
            
            # Execute ClickHouse query
            result = subprocess.run(
                clickhouse_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            # Parse output to find tenant tables
            self.tenants_with_tables.clear()
            if result.returncode == 0:
                # Each line contains a table name like 'atd_tenant_name'
                for line in result.stdout.strip().split('\n'):
                    if line.startswith('atd_'):
                        tenant_name = line[4:]  # Remove 'atd_' prefix
                        if tenant_name in self.allowed_tenants:
                            self.tenants_with_tables.add(tenant_name)
            
            self.logger.info(f"Found tables for {len(self.tenants_with_tables)} tenants: {', '.join(sorted(self.tenants_with_tables))}")
            
        except Exception as e:
            self.logger.error(f"Failed to update tenant table cache: {str(e)}")
    
    def _ensure_tenant_table_exists(self, tenant: str) -> bool:
        """Ensure table exists for tenant, create if not (uses cache to avoid unnecessary checks)"""
        try:
            # Check cache first - if tenant has table, no need to probe
            if tenant in self.tenants_with_tables:
                self.logger.debug(f"Table for tenant '{tenant}' exists (cached)")
                return True
            
            # Tenant not in cache, need to create table
            table_name = f"atd_{tenant}"
            self.logger.info(f"Table {table_name} not in cache, creating with 6-hour TTL")
                
            create_cmd = [
                'as-cli', 'ingest', 'tenant',
                '--create-table',
                '--tenant-name', tenant,
                '--ttl-hours', '6',
                '--schema-file', '/app/schema.json'  # Use schema file in Docker container
            ]
            
            # Add database connection overrides
            if hasattr(self.config, 'clickhouse'):
                if self.config.clickhouse.host:
                    create_cmd.extend(['--db-host', self.config.clickhouse.host])
                if self.config.clickhouse.port:
                    create_cmd.extend(['--db-port', str(self.config.clickhouse.port)])
                if self.config.clickhouse.database:
                    create_cmd.extend(['--db-database', self.config.clickhouse.database])
                if hasattr(self.config.clickhouse, 'user') and self.config.clickhouse.user:
                    create_cmd.extend(['--db-user', self.config.clickhouse.user])
                if hasattr(self.config.clickhouse, 'password') and self.config.clickhouse.password:
                    create_cmd.extend(['--db-password', self.config.clickhouse.password])
            
            # Create table
            create_result = subprocess.run(
                create_cmd,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if create_result.returncode == 0:
                self.logger.info(f"Successfully created table {table_name} with 6-hour TTL")
                # Add to cache so we don't check again
                self.tenants_with_tables.add(tenant)
                return True
            else:
                self.logger.error(f"Failed to create table {table_name}: {create_result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error ensuring table exists for tenant {tenant}: {str(e)}")
            return False


class Worker:
    """Worker thread that processes messages from the queue"""
    
    def __init__(self, worker_id: int, config: Config, logger: logging.Logger, shared_tenant_cache=None):
        self.worker_id = worker_id
        self.config = config
        self.logger = logger
        self.processor = MessageProcessor(config, logger)
        # Share the tenant cache across all workers
        if shared_tenant_cache is not None:
            self.processor.tenants_with_tables = shared_tenant_cache
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
                
            except Empty:
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