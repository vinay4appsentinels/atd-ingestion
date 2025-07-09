#!/usr/bin/env python3
"""
Setup script for ATD Ingestion Service
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

setup(
    name="atd-ingestion",
    version="1.0.0",
    description="High-performance Kafka to ClickHouse ingestion service using AppSentinels CLI",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="AppSentinels",
    author_email="support@appsentinels.com",
    url="https://github.com/appsentinels/atd-ingestion",
    
    # Package configuration
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    
    # Scripts
    scripts=["main.py"],
    
    # Dependencies
    install_requires=[
        "kafka-python>=2.0.2",
        "PyYAML>=6.0.1",
        "clickhouse-driver>=0.2.6",
    ],
    
    # Optional dependencies
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
        "test": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-mock>=3.10.0",
        ]
    },
    
    # Entry points
    entry_points={
        "console_scripts": [
            "atd-ingestion=atd_ingestion.service:main",
        ]
    },
    
    # Metadata
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: System :: Distributed Computing",
        "Topic :: Utilities",
    ],
    
    # Python version requirement
    python_requires=">=3.8",
    
    # Include additional files
    include_package_data=True,
    package_data={
        "atd_ingestion": ["py.typed"],
    },
    
    # Keywords
    keywords="kafka clickhouse ingestion data-pipeline multiprocessing",
    
    # Project URLs
    project_urls={
        "Documentation": "https://docs.appsentinels.com/atd-ingestion",
        "Source": "https://github.com/appsentinels/atd-ingestion",
        "Tracker": "https://github.com/appsentinels/atd-ingestion/issues",
    },
)