FROM python:3.10-slim

WORKDIR /opt/airflow

# Install system dependencies required for ML libraries
RUN apt-get update && apt-get install -y \
    libgomp1 \
    libopenblas-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy and install requirements
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend source code
COPY backend/ ./backend
COPY data/ ./data

# Set Python path to include the current directory
ENV PYTHONPATH=/opt/airflow

# Expose port and define entrypoint
EXPOSE 8000