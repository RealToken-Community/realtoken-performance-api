#!/bin/sh
set -e

# Create file if not exists (empty)
touch /app/data/realtokens_data.json
touch /app/data/realtokens_history.json
touch /app/data/rent_files_parquet/list_rent_files_collected.json

# Create Google service account file
if [ -n "$GOOGLE_SERVICE_ACCOUNT_JSON_BASE64" ]; then
  echo "$GOOGLE_SERVICE_ACCOUNT_JSON_BASE64" | base64 -d > /app/google-drive-service-account.json
  chmod 600 /app/google-drive-service-account.json
else
  echo "WARNING: GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 is not set"
fi

# Start supervisord as PID 1
exec supervisord -c /app/supervisord.conf