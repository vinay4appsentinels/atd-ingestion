# Bulk Parquet Ingestion Plan

## Overview
Ingest 2,410 parquet files (~20.2 GB total) from `/home/vinaypatil/infosys` directory into ClickHouse using the ATD ingestion service.

## File Analysis
- **Total Files**: 2,410 parquet files
- **Total Size**: ~20.2 GB
- **Average File Size**: ~8.4 MB
- **File Pattern**: `YYYYMMDD-HHhMMSS.parquet` (e.g., `20250703-06h2843.parquet`)
- **Date Range**: July 3, 2025 (single day of data)

## Current ClickHouse State
- **Database**: Running in Docker container `clickhouse-server`
- **Existing Tables**: 
  - `api_logs_parquet` (10,665,908 rows) - **TARGET TABLE**
  - `api_catalog`, `my_table`, `test_table`, `uri_api_catalog`
- **Target Table Schema**: `api_logs_parquet` with 69 columns including HTTP request/response data, session info, and API metadata

## Ingestion Strategy

### Phase 1: Pre-Ingestion Checks
- [ ] Verify ClickHouse container is healthy
- [ ] Check current table row count baseline
- [ ] Test sample file ingestion (1-2 files)
- [ ] Verify table schema matches parquet file structure

### Phase 2: Batch Processing Setup
- [ ] Create batch producer script to emit file paths to Kafka
- [ ] Configure appropriate batch sizes (recommendation: 50-100 files per batch)
- [ ] Set up progress monitoring and logging

### Phase 3: Ingestion Execution
- [ ] Start ATD ingestion service with appropriate topic
- [ ] Run batch producer to process all files
- [ ] Monitor ingestion progress and error rates
- [ ] Handle any failed files

## Technical Implementation

### 1. Table Strategy
- **Target Table**: `api_logs_parquet` (existing table with 10M+ rows)
- **Action**: APPEND data (no table cleanup needed)
- **Tenant**: Use consistent tenant name for all files (e.g., `bulk_ingestion_20250703`)

### 2. Kafka Configuration
- **Topic**: `bulk_ingestion_parquet`
- **Consumer Group**: `atd_bulk_ingestion`
- **Batch Size**: 50-100 files per batch for optimal performance

### 3. Message Format
```json
{
  "filename": "/home/vinaypatil/infosys/20250703-06h2843.parquet",
  "format": "parquet",
  "size": 6207513,
  "tenant": "bulk_ingestion_20250703",
  "timestamp": 1720847100.0,
  "message_id": "bulk_001_20250703-06h2843"
}
```

### 4. Performance Considerations
- **Estimated Time**: ~2-4 hours (based on 10-20 files/minute processing rate)
- **Resource Usage**: Monitor CPU, memory, and disk I/O during ingestion
- **Parallel Processing**: Use 4 worker threads (current configuration)

## Implementation Steps

### Step 1: Create Batch Producer
```bash
# Create producer script for bulk ingestion
python3 create_bulk_producer.py \
  --directory /home/vinaypatil/infosys \
  --pattern "*.parquet" \
  --topic bulk_ingestion_parquet \
  --tenant bulk_ingestion_20250703
```

### Step 2: Start Ingestion Service
```bash
# Start service with bulk ingestion topic
KAFKA_TOPIC=bulk_ingestion_parquet docker compose up -d
```

### Step 3: Monitor Progress
```bash
# Monitor service logs
docker logs -f atd-ingestion-service

# Check ClickHouse row count periodically
docker exec clickhouse-server clickhouse-client --query "SELECT count() FROM api_logs_parquet"
```

## Risk Mitigation

### 1. Data Integrity
- **Backup**: Current table has 10M+ rows - consider backup before bulk ingestion
- **Validation**: Compare expected vs actual row count after ingestion
- **Rollback Plan**: Keep list of ingested files for potential rollback

### 2. Performance Impact
- **Rate Limiting**: Implement delays between batches if needed
- **Resource Monitoring**: Watch CPU, memory, and disk usage
- **Service Health**: Monitor ClickHouse container health

### 3. Error Handling
- **Failed Files**: Log and retry failed ingestions
- **Duplicate Prevention**: Check for existing data if re-running
- **Cleanup**: Handle any corrupt or incomplete files

## Success Criteria
- [ ] All 2,410 files successfully ingested
- [ ] No data corruption or loss
- [ ] Ingestion completes within 4 hours
- [ ] ClickHouse table row count increases by expected amount
- [ ] No service crashes or performance degradation

## Post-Ingestion Verification
```sql
-- Check total row count increase
SELECT count() FROM api_logs_parquet;

-- Verify data from ingested files
SELECT MsgHeader_MessageTime, count() 
FROM api_logs_parquet 
WHERE MsgHeader_MessageTime LIKE '%2025-07-03%' 
GROUP BY MsgHeader_MessageTime 
ORDER BY MsgHeader_MessageTime;

-- Check for any processing errors
SELECT * FROM api_logs_parquet 
WHERE error_flags IS NOT NULL AND error_flags != '';
```

## Rollback Plan
If ingestion fails or causes issues:
1. Stop the ingestion service
2. Identify successfully ingested files from logs
3. Remove ingested data using tenant identifier
4. Resume from last successful batch

---

**Next Steps**: Create batch producer script and run pre-ingestion tests