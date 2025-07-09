.PHONY: help build test lint clean run docker-build docker-run install dev-install

# Default target
help:
	@echo "Available targets:"
	@echo "  install       - Install the package"
	@echo "  dev-install   - Install with development dependencies"
	@echo "  test          - Run unit tests"
	@echo "  lint          - Run linting checks"
	@echo "  format        - Format code with black"
	@echo "  clean         - Remove build artifacts"
	@echo "  run           - Run the service locally"
	@echo "  docker-build  - Build Docker image"
	@echo "  docker-run    - Run Docker container"
	@echo "  docker-prod   - Build production Docker image"

# Install the package
install:
	pip install -e .

# Install with development dependencies
dev-install:
	pip install -e ".[dev,test]"

# Run tests
test:
	python -m pytest tests/ -v --cov=src/atd_ingestion --cov-report=term-missing

# Run linting
lint:
	flake8 src/ tests/
	mypy src/

# Format code
format:
	black src/ tests/

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Run the service locally
run:
	python main.py --config config/config.yaml

# Build Docker image
docker-build:
	docker build -t atd-ingestion:latest .

# Run Docker container
docker-run:
	docker run -d --name atd-ingestion \
		-v $(PWD)/config:/app/config:ro \
		-v $(PWD)/logs:/app/logs \
		-v $(PWD)/data:/data:ro \
		atd-ingestion:latest

# Build production Docker image
docker-prod:
	docker build -f Dockerfile.prod -t atd-ingestion:prod .

# Run docker-compose
compose-up:
	docker-compose up -d

# Stop docker-compose
compose-down:
	docker-compose down

# View logs
logs:
	tail -f logs/atd-ingestion.log

# Run the test producer
test-producer:
	python test_producer.py --num-messages 5