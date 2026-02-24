#!/bin/sh
set -e

# Create data_tmp directory if not exists
mkdir -p /app/data_tmp

# Create file if not exists (empty)
touch /app/data_tmp/realtokens_data.json
touch /app/data_tmp/realtokens_history.json

# Start supervisord as PID 1
exec supervisord -c /app/supervisord.conf