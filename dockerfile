FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    supervisor \
 && rm -rf /var/lib/apt/lists/*


COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --no-compile -r requirements.txt

# Copy the application
COPY api ./api
COPY core ./core
COPY config ./config
COPY job ./job
COPY data ./data
COPY Ressources ./Ressources
COPY supervisord.conf .
COPY entrypoint.sh .

# Create logs directory
RUN mkdir -p /app/logs

RUN chmod +x /app/entrypoint.sh && mkdir -p /app/logs

CMD ["/app/entrypoint.sh"]