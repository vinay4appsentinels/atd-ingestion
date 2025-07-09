"""
Configuration management for ATD Ingestion Service
"""

import os
import yaml
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from pathlib import Path


@dataclass
class KafkaConfig:
    """Kafka configuration settings"""
    topic: str = "atd-topic"
    bootstrap_servers: List[str] = field(default_factory=lambda: ["localhost:9092"])
    group_id: str = "atd-ingestion-group"
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True


@dataclass
class ClickHouseConfig:
    """ClickHouse configuration settings"""
    host: str = "localhost"
    port: int = 18123
    database: str = "default"
    user: Optional[str] = None
    password: Optional[str] = None


@dataclass
class ASCLIConfig:
    """AS-CLI configuration settings"""
    executable: str = "as-cli"
    timeout: int = 300  # seconds
    additional_args: List[str] = field(default_factory=list)


@dataclass
class TableConfig:
    """Table management configuration"""
    auto_create_tables: bool = True
    table_prefix: str = "atd"
    default_table: str = "atd_logs"
    batch_size: int = 10000
    schema_file: Optional[str] = None


@dataclass
class ServiceConfig:
    """Service configuration"""
    retry_enabled: bool = True
    max_attempts: int = 3
    backoff_seconds: int = 5


@dataclass
class Config:
    """Main configuration class"""
    kafka: KafkaConfig
    clickhouse: ClickHouseConfig
    as_cli: ASCLIConfig
    table_config: TableConfig
    service: ServiceConfig
    thread_pool_size: int = 4
    log_level: str = "INFO"
    log_file: str = "logs/atd-ingestion.log"
    
    @classmethod
    def from_yaml(cls, config_path: str) -> "Config":
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        return cls(
            kafka=KafkaConfig(
                topic=data['kafka'].get('topic', 'atd-topic'),
                bootstrap_servers=data['kafka'].get('bootstrap_servers', ['localhost:9092']),
                group_id=data['kafka'].get('group_id', 'atd-ingestion-group'),
                auto_offset_reset=data['kafka'].get('auto_offset_reset', 'latest'),
                enable_auto_commit=data['kafka'].get('enable_auto_commit', True)
            ),
            clickhouse=ClickHouseConfig(
                host=data['clickhouse'].get('host', 'localhost'),
                port=data['clickhouse'].get('port', 18123),
                database=data['clickhouse'].get('database', 'default'),
                user=data['clickhouse'].get('user'),
                password=data['clickhouse'].get('password')
            ),
            as_cli=ASCLIConfig(
                executable=data['as_cli'].get('executable', 'as-cli'),
                timeout=data['as_cli'].get('timeout', 300),
                additional_args=data['as_cli'].get('additional_args', [])
            ),
            table_config=TableConfig(
                auto_create_tables=data.get('auto_create_tables', True),
                table_prefix=data.get('table_prefix', 'atd'),
                default_table=data.get('default_table', 'atd_logs'),
                batch_size=data.get('batch_size', 10000),
                schema_file=data.get('schema_file')
            ),
            service=ServiceConfig(
                retry_enabled=data.get('service', {}).get('retry', {}).get('enabled', True),
                max_attempts=data.get('service', {}).get('retry', {}).get('max_attempts', 3),
                backoff_seconds=data.get('service', {}).get('retry', {}).get('backoff_seconds', 5)
            ),
            thread_pool_size=data.get('thread_pool_size', 4),
            log_level=data.get('log_level', 'INFO'),
            log_file=data.get('log_file', 'logs/atd-ingestion.log')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'kafka': {
                'topic': self.kafka.topic,
                'bootstrap_servers': self.kafka.bootstrap_servers,
                'group_id': self.kafka.group_id,
                'auto_offset_reset': self.kafka.auto_offset_reset,
                'enable_auto_commit': self.kafka.enable_auto_commit
            },
            'clickhouse': {
                'host': self.clickhouse.host,
                'port': self.clickhouse.port,
                'database': self.clickhouse.database,
                'user': self.clickhouse.user,
                'password': '***' if self.clickhouse.password else None
            },
            'as_cli': {
                'executable': self.as_cli.executable,
                'timeout': self.as_cli.timeout,
                'additional_args': self.as_cli.additional_args
            },
            'table_config': {
                'auto_create_tables': self.table_config.auto_create_tables,
                'table_prefix': self.table_config.table_prefix,
                'default_table': self.table_config.default_table,
                'batch_size': self.table_config.batch_size,
                'schema_file': self.table_config.schema_file
            },
            'service': {
                'retry': {
                    'enabled': self.service.retry_enabled,
                    'max_attempts': self.service.max_attempts,
                    'backoff_seconds': self.service.backoff_seconds
                }
            },
            'thread_pool_size': self.thread_pool_size,
            'log_level': self.log_level,
            'log_file': self.log_file
        }