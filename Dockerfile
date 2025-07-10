FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install appsentinels-cli and as-cli-ingest plugin
# First install the base CLI
RUN pip install appsentinels-cli>=2.2.0

# Install additional dependencies needed by as-cli-ingest
RUN pip install \
    pandas>=2.0.0 \
    clickhouse-connect>=0.6.0 \
    pyarrow>=12.0.0 \
    python-dateutil

# Install as-cli-ingest plugin from local submodule in development mode
COPY ./as-cli-ingest /app/as-cli-ingest
RUN pip install -e /app/as-cli-ingest

# Verify as-cli installation and plugin loading
RUN as-cli --version
RUN as-cli plugin list

# Create non-root user
RUN useradd -m -u 1000 atduser && \
    mkdir -p /app/config && \
    chown -R atduser:atduser /app

# Copy application files
COPY --chown=atduser:atduser setup.py .
COPY --chown=atduser:atduser main.py .
COPY --chown=atduser:atduser src/ ./src/
COPY --chown=atduser:atduser config/ ./config/
COPY --chown=atduser:atduser test_producer.py .
COPY --chown=atduser:atduser schema.json .

# Install the atd-ingestion package in development mode
RUN pip install -e .

# Switch to non-root user
USER atduser

# Create volume mount points
VOLUME ["/app/config", "/data"]

# Default environment variables
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO
ENV KAFKA_TOPIC=atd_test_topic

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import socket; s = socket.socket(); s.connect(('localhost', 9092)); s.close()" || exit 1

# Run the service
CMD ["python", "main.py", "--config", "/app/config/config.docker.yaml"]