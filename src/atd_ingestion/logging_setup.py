"""
Logging configuration for ATD Ingestion Service
"""

import logging
import logging.handlers
import os
from pathlib import Path

from .config import Config


def setup_logging(config: Config) -> logging.Logger:
    """Setup logging configuration"""
    logger = logging.getLogger('atd-ingestion')
    
    # Convert string log level to logging constant
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)
    logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create log directory if it doesn't exist
    log_file = Path(config.log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Console handler with color support
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = ColoredFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    
    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        config.log_file,
        maxBytes=50*1024*1024,  # 50MB
        backupCount=5
    )
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    
    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    # Force flush to ensure log level message appears
    for handler in logger.handlers:
        handler.flush()
    
    # Log the effective log level
    logger.info(f"Logging initialized with level: {logging.getLevelName(logger.level)}")
    
    return logger


class ColoredFormatter(logging.Formatter):
    """Custom formatter with color support for console output"""
    
    # Color codes
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        # Only add colors if outputting to a terminal
        if hasattr(record, 'levelname') and record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.RESET}"
        return super().format(record)