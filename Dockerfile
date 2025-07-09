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

# Install as-cli-ingest plugin
# Option 1: From PyPI (if published)
# RUN pip install as-cli-ingest

# Option 2: From local directory (for development)
# COPY ./as-cli-ingest /tmp/as-cli-ingest
# RUN pip install /tmp/as-cli-ingest && rm -rf /tmp/as-cli-ingest

# Option 3: From GitHub
RUN pip install git+https://github.com/appsentinels/as-cli-ingest.git

# Verify as-cli installation
RUN as-cli --version

# Create non-root user
RUN useradd -m -u 1000 atduser && \
    mkdir -p /app/logs /app/config && \
    chown -R atduser:atduser /app

# Copy application files
COPY --chown=atduser:atduser setup.py .
COPY --chown=atduser:atduser main.py .
COPY --chown=atduser:atduser src/ ./src/
COPY --chown=atduser:atduser config/ ./config/
COPY --chown=atduser:atduser test_producer.py .

# Switch to non-root user
USER atduser

# Create volume mount points
VOLUME ["/app/logs", "/app/config", "/data"]

# Default environment variables
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import socket; s = socket.socket(); s.connect(('localhost', 9092)); s.close()" || exit 1

# Run the service
CMD ["python", "main.py", "--config", "/app/config/config.yaml"]