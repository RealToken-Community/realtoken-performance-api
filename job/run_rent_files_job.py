import os
import logging
from config.settings import RENT_FILES_FOLDER_ID
from core.services.send_telegram_alert import send_telegram_alert
from pathlib import Path
from job.rent_files import (
    build_google_credentials,
    list_all_files_flat,
    load_processed_ids,
    get_new_files,
    download_drive_file,
    extract_year_week,
    upsert_weekly_rent_csv_to_parquet,
    save_processed_id,
)

logger = logging.getLogger(__name__)


def run_rent_files_job():
    """
    Execute the rent files ingestion pipeline.

    Workflow:
    1. Authenticate with Google Drive.
    2. List all files available in the configured rent folder.
    3. Identify files that have not yet been processed.
    4. For each new file:
        - Download the CSV from Google Drive.
        - Extract the corresponding year and week from the filename/content.
        - Convert and upsert the weekly CSV data into the quarterly Parquet dataset.
    5. If processing fails:
        - Move the CSV file to the `errors` directory.
        - Log the error and send a Telegram alert.
    6. If processing succeeds:
        - Delete the downloaded CSV file.
        - Record the file ID as processed to avoid reprocessing.

    This job is designed to be idempotent: already processed files are skipped
    based on stored Google Drive file IDs.
    """
    
    logger.info("starting rent files job")

    try:
        # Build Google Drive API credentials
        credentials = build_google_credentials()

        # Retrieve all files in the configured Drive folder
        all_files = list_all_files_flat(RENT_FILES_FOLDER_ID, credentials)
    except Exception:
        logger.exception("Failed to list files from Google Drive")
        send_telegram_alert("Realtoken-Performance-api: Failed to list files from Google Drive")

    # Load the set of already processed Google Drive file IDs
    files_already_processed = load_processed_ids()

    # Determine which files still need to be processed
    new_files_to_be_processed = get_new_files(all_files, files_already_processed)
    logger.info(f"Checking Google drive: {len(new_files_to_be_processed)} new rent files to be downloaded")

    for file in new_files_to_be_processed:

        # Download the CSV file locally
        path_csv_file_downloaded = download_drive_file(file, credentials)

        try:
            # Extract year/week metadata from the CSV file
            year, week = extract_year_week(path_csv_file_downloaded)

            # Convert CSV data and upsert it into the quarterly parquet dataset
            path_parquet_file = upsert_weekly_rent_csv_to_parquet(
                path_csv_file_downloaded,
                year,
                week,
            )

        except Exception as e:
            # On failure: move the CSV to the error directory for inspection
            dst = Path("data/rent_files_csv/errors") / Path(path_csv_file_downloaded).name
            os.replace(path_csv_file_downloaded, dst)

            logger.error(f"Failed processing {path_csv_file_downloaded}: {e}")

            send_telegram_alert(
                f"Realtoken-Performance-api: Failed processing: {path_csv_file_downloaded}"
            )

            continue

        # On success: remove the CSV file
        Path(path_csv_file_downloaded).unlink()

        logger.info(
            f"CSV file {path_csv_file_downloaded} has been downloaded and converted to parquet file"
        )

        # Persist the processed file ID to avoid reprocessing
        save_processed_id(file.id)

    logger.info("rent files job completed")