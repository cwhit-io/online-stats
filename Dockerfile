# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Copy environment file (if it exists)
COPY .env* ./

# Create directories for data and output
RUN mkdir -p data output

# Set Python path
ENV PYTHONPATH=/app

# Default command
CMD ["python", "--version"]