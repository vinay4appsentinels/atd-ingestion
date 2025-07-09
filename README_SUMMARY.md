# ATD Ingestion Service - Summary & Outline

## What It Does

The ATD Ingestion Service is a production-ready data pipeline that:

1. **Monitors Kafka** - Continuously listens to a Kafka topic (`atd-topic`) for file ingestion requests
2. **Processes Files** - Receives file paths from Kafka messages and ingests the data into ClickHouse
3. **Multi-tenant Support** - Routes data to tenant-specific tables automatically
4. **Parallel Processing** - Uses configurable thread pools for concurrent file processing
5. **Table Management** - Automatically creates ClickHouse tables per tenant as needed

## Architecture Overview

```
Kafka Topic (atd-topic) 
    ↓ 
ATD Ingestion Service
    ├── Kafka Consumer (monitors messages)
    ├── Thread Pool (parallel processing)
    └── as-cli subprocess (actual ingestion)
         ↓
ClickHouse Database (tenant tables)
```

## Key Components

### 1. **Main Service** (`atd_ingestion_service.py`)
- Kafka consumer implementation
- Thread pool management
- Message processing logic
- Graceful shutdown handling

### 2. **Configuration** (`config/`)
- `config.yaml` - Local deployment settings
- `config.docker.yaml` - Docker deployment settings
- Configurable parameters:
  - Kafka connection details
  - Thread pool size
  - Table naming conventions
  - as-cli settings

### 3. **Docker Support**
- `Dockerfile` - Container image definition
- `docker-compose.yml` - Full stack with Kafka
- `docker-compose.prod.yml` - Production deployment
- Includes health checks and volume mounts

### 4. **Testing** (`test_producer.py`)
- Mock Kafka producer for testing
- Sends sample messages to validate pipeline

### 5. **Service Management**
- `atd-ingestion.service` - Systemd service file
- Auto-restart on failure
- Security hardening

## Workflow

1. **Message Reception**
   - Kafka message arrives with file path and tenant info
   - Message format: `{"filename": "/path/file.parquet", "tenant": "tenant1"}`

2. **Queue Processing**
   - Message added to internal processing queue
   - Worker thread picks up message

3. **File Ingestion**
   - Constructs table name: `atd_tenant1`
   - Executes: `as-cli ingest records --file /path/file.parquet --table-name atd_tenant1 --create-table`
   - as-cli handles ClickHouse operations

4. **Result Handling**
   - Success: Log completion, process next message
   - Failure: Log error, optionally retry

## Key Features

- **Scalability**: Thread pool allows processing multiple files concurrently
- **Reliability**: Graceful shutdown, error handling, logging
- **Multi-tenancy**: Automatic table separation per tenant
- **Automation**: Tables created automatically on first use
- **Monitoring**: Comprehensive logging to file and console
- **Deployment**: Docker and systemd support for production use

## Configuration Options

- `thread_pool_size`: Number of concurrent workers (default: 4)
- `auto_create_tables`: Enable automatic table creation (default: true)
- `table_prefix`: Prefix for tenant tables (default: "atd")
- `batch_size`: Records per batch for ingestion (default: 10000)
- `timeout`: Maximum time for file processing (default: 300s)

## Use Cases

1. **Log Aggregation**: Collect logs from multiple sources via Kafka
2. **Multi-tenant Analytics**: Separate data per customer/tenant
3. **Real-time Data Pipeline**: Stream processing from files to ClickHouse
4. **Batch Processing**: Handle large file ingestions efficiently