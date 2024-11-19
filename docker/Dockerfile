FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /app/src/
COPY scripts/ /app/scripts/

# Set Python path
ENV PYTHONPATH=/app

# Run crawler
CMD ["python", "scripts/run_crawler.py"]
