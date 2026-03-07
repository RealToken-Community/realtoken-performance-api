#!/bin/sh
set -e

# Create file if not exists (empty)
touch /app/data/realtokens_data.json
touch /app/data/realtokens_history.json
touch /app/data/rent_files_parquet/list_rent_files_collected.json

# Start supervisord as PID 1
exec supervisord -c /app/supervisord.conf