# Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./ ./ha_event_forwarder/

# Create directory for config files
RUN mkdir -p /config

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    CONFIG_PATH=/config/config.yml \
    FLASK_HOST=0.0.0.0 \
    FLASK_PORT=5000

# Expose the Flask port
EXPOSE 5000

# Run the application
CMD ["python", "-m", "ha_event_forwarder"]