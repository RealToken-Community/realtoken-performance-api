FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (optional but common)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    supervisor \
 && rm -rf /var/lib/apt/lists/*

# Install Python deps first (better layer cache)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the application
COPY . /app

# Create logs directory (your app writes to /app/logs)
RUN mkdir -p /app/logs

# Copy supervisord config
COPY supervisord.conf /app/supervisord.conf

# Entrypoint
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

CMD ["/app/entrypoint.sh"]