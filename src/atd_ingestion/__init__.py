"""
ATD Ingestion Service

A high-performance service for ingesting data from Kafka to ClickHouse
using the AppSentinels CLI with multi-process architecture.
"""

__version__ = "1.0.0"
__author__ = "AppSentinels"

from .service import ATDIngestionService
from .config import Config

__all__ = ["ATDIngestionService", "Config"]