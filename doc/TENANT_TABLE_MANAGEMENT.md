# ATD Ingestion - Tenant Table Management Instructions

## Prerequisites
- Docker image: `docker-hub.appsentinels.ai/atd-ingestion:latest`
- ClickHouse running and accessible
- Tenant must be in allowlist: `netskope_boomskope_npa`

## Create Tenant Table

### 1. Run Docker Container
```bash
docker run -it --rm --network host \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --create-table --tenant-name netskope_boomskope_npa
```

### 2. With Custom Schema File
```bash
docker run -it --rm --network host \
  -v /path/to/schema.json:/app/custom_schema.json \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --create-table --tenant-name netskope_boomskope_npa --schema-file /app/custom_schema.json
```

### 3. Dry Run (Show SQL without executing)
```bash
docker run -it --rm --network host \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --create-table --tenant-name netskope_boomskope_npa --dry-run
```

### 4. Drop and Recreate Table
```bash
docker run -it --rm --network host \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --create-table --tenant-name netskope_boomskope_npa --drop-first
```

### 5. Create Table with Custom TTL
```bash
# Create table with 24-hour TTL (default is 12 hours)
docker run -it --rm --network host \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --create-table --tenant-name netskope_boomskope_npa --ttl-hours 24
```

### 6. Create Table with 1-hour TTL (fast cleanup)
```bash
docker run -it --rm --network host \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --create-table --tenant-name netskope_boomskope_npa --ttl-hours 1
```

## List Tenant Tables

### 1. List All Tenant Tables
```bash
docker run -it --rm --network host \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --list-tables --tenant-name netskope_boomskope_npa
```

### 2. Using Docker Compose
```bash
# Start the service
docker-compose up -d

# Exec into running container
docker exec -it atd-ingestion-service bash

# Inside container - create table
as-cli ingest tenant --create-table --tenant-name netskope_boomskope_npa

# Inside container - list tables
as-cli ingest tenant --list-tables --tenant-name netskope_boomskope_npa
```

## Delete Tenant Table

### 1. Delete Table Permanently
```bash
docker run -it --rm --network host \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --delete-table --tenant-name netskope_boomskope_npa
```

### 2. Delete Table (Dry Run)
```bash
# Preview deletion without executing
docker run -it --rm --network host \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --delete-table --tenant-name netskope_boomskope_npa --dry-run
```

### 3. Delete with Database Connection Override
```bash
docker run -it --rm --network host \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --delete-table --tenant-name netskope_boomskope_npa \
  --db-host clickhouse \
  --db-port 9000 \
  --db-user default \
  --db-password clickhouse
```

## Database Connection Options

### Custom ClickHouse Connection
```bash
docker run -it --rm --network host \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --create-table --tenant-name netskope_boomskope_npa \
  --db-host clickhouse.example.com \
  --db-port 8123 \
  --db-user myuser \
  --db-password mypassword \
  --db-database mydatabase
```

## Expected Output

### Successful Table Creation
```
✅ Successfully created table atd_netskope_boomskope_npa with 80 columns (TTL: 12 hours)
```

### Successful Table Creation with Custom TTL
```
✅ Successfully created table atd_netskope_boomskope_npa with 80 columns (TTL: 24 hours)
```

### Table Already Exists
```
✅ Table atd_netskope_boomskope_npa already exists
```

### List Tables Output
```
📋 Found 1 tenant tables:
- Table: atd_netskope_boomskope_npa
  Tenant: netskope_boomskope_npa
  Records: 0
```

### Dry Run Output
```
🔍 DRY RUN - Would create table atd_netskope_boomskope_npa
SQL: CREATE TABLE IF NOT EXISTS `atd_netskope_boomskope_npa` (...)
```

### Successful Table Deletion
```
✅ Successfully deleted table atd_netskope_boomskope_npa (removed 1543 records)
```

### Delete Table Dry Run
```
🔍 DRY RUN - Would delete table atd_netskope_boomskope_npa with 1543 records
SQL: DROP TABLE `atd_netskope_boomskope_npa`
```

### Table Not Found for Deletion
```
❌ Table atd_netskope_boomskope_npa does not exist
```

## Table Structure

The created table will have:
- **Table Name**: `atd_netskope_boomskope_npa`
- **Engine**: MergeTree
- **Ordering**: `(MsgHeader.MessageTime, Id)`
- **Partitioning**: Monthly by `MsgHeader.MessageTime`
- **TTL**: 12 hours retention (configurable with `--ttl-hours`)
- **Columns**: Based on schema.json (80 columns)

### Schema File Structure

The `schema.json` file defines the ClickHouse table structure with field mappings:

```json
{
  "MsgHeader.MessageTime": "DateTime",
  "MsgHeader.Sequence": "Int64", 
  "MsgHeader.TenantId": "String",
  "MsgHeader.InternalOPCID": "String",
  "SrcInfo.Source": "Int64",
  "SrcInfo.RemoteAddress": "Float64",
  "Id": "String",
  "DownstreamL3L4.SourceIP": "String",
  "DownstreamL3L4.DestIP": "String",
  "DownstreamL3L4.L4Protocol": "String",
  "DownstreamL3L4.SourcePort": "Int64",
  "UpstreamL3L4.SourceIP": "String",
  "HTTPReq.Method": "String",
  "HTTPReq.URL": "String",
  "HTTPReq.Body": "String",
  "HTTPResp.StatusCode": "Int64",
  "_ip": "String"
}
```

**Key Schema Details:**
- **Total Columns**: 80 fields covering message headers, network data, HTTP traffic, and security information
- **Field Naming**: Uses dot notation (e.g., `MsgHeader.MessageTime`) which gets converted to underscores in ClickHouse (`MsgHeader_MessageTime`)
- **Data Types**: Standard ClickHouse types (DateTime, String, Int64, Float64)
- **Required Fields**: `MsgHeader.MessageTime` and `Id` are used for table ordering
- **Custom Schema**: You can provide your own schema file with `--schema-file` parameter

**Schema File Location:**
- **Container Default**: `/app/schema.json`
- **Custom Schema**: Mount with `-v /path/to/custom-schema.json:/app/custom_schema.json`

## Troubleshooting

### Tenant Not Allowed
```
❌ Tenant 'other_tenant' is not in the allowlist
```
**Solution**: Use `netskope_boomskope_npa` or set `ALLOWED_TENANTS` environment variable

### ClickHouse Connection Failed
```
❌ Database connection failed: Connection refused
```
**Solution**: Check ClickHouse is running and accessible on the specified host/port

### Schema File Not Found
```
❌ Schema file not found: /app/schema.json
```
**Solution**: Ensure schema.json exists in the container or mount custom schema file

## Environment Variables

Override tenant allowlist:
```bash
export ALLOWED_TENANTS="netskope_boomskope_npa,other_tenant"
docker run -it --rm --network host \
  -e ALLOWED_TENANTS="netskope_boomskope_npa,other_tenant" \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --create-table --tenant-name other_tenant
```

## Complete Example Workflow

### 1. Create Table
```bash
docker run -it --rm --network host \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --create-table --tenant-name netskope_boomskope_npa
```

### 2. Verify Table Creation
```bash
docker run -it --rm --network host \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest tenant --list-tables --tenant-name netskope_boomskope_npa
```

### 3. View Table Structure (Optional)
```bash
docker run -it --rm --network host \
  docker-hub.appsentinels.ai/atd-ingestion:latest \
  as-cli ingest view --table-name atd_netskope_boomskope_npa --limit 0
```

## CLI Command Reference

### Tenant Commands
```bash
# Create table
as-cli ingest tenant --create-table --tenant-name <tenant>

# List tables
as-cli ingest tenant --list-tables --tenant-name <tenant>

# Dry run
as-cli ingest tenant --create-table --tenant-name <tenant> --dry-run

# Drop and recreate
as-cli ingest tenant --create-table --tenant-name <tenant> --drop-first

# Custom schema
as-cli ingest tenant --create-table --tenant-name <tenant> --schema-file <path>
```

### Database Connection Options
```bash
--db-host <host>          # ClickHouse host
--db-port <port>          # ClickHouse port
--db-user <user>          # ClickHouse user
--db-password <password>  # ClickHouse password
--db-database <database>  # ClickHouse database
```

## Notes

- Only one tenant is currently allowed: `netskope_boomskope_npa`
- Tables are automatically created with optimized ClickHouse settings
- Schema is loaded from `/app/schema.json` in the container
- Use `--dry-run` to preview SQL before execution
- Tables use 90-day TTL for automatic data cleanup


as-cli ingest tenant --create-table --tenant-name netskope_boomskope_npa --schema-file /app/schema.json --db-port 19000 --db-host clickhouse-server --db-user default --db-password clickhouse