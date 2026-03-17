FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (e.g., if TA-Lib is needed, we would install it here)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Set PYTHONPATH so that imports work correctly
ENV PYTHONPATH "${PYTHONPATH}:/app"
