import io
import time
import logging
import os
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from job.rent_files.list_all_files_flat import DriveFile

logger = logging.getLogger(__name__)


def download_drive_file(
    file: DriveFile,
    credentials,
    download_dir: str | Path = "./data/rent_files_csv",
) -> str:
    """
    Download a single Google Drive file using a Service Account (credentials)
    and save it locally.

    Returns
    -------
    str
        The relative path (as a string) to the downloaded file.
    """

    download_path = Path(download_dir)
    download_path.mkdir(parents=True, exist_ok=True)

    local_file_path = download_path / file.name

    # Build Drive service client (authenticated)
    service = build("drive", "v3", credentials=credentials, cache_discovery=False)

    try:
        # Small delay to reduce the risk of aggressive-rate issues (usually not needed with SA).
        time.sleep(0.1)

        request = service.files().get_media(fileId=file.id)

        with io.FileIO(local_file_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)  # 1MB chunks
            done = False
            while not done:
                _status, done = downloader.next_chunk()

        logger.info("Downloaded Drive file %s (%s)", file.name, file.id)
        return os.path.relpath(local_file_path, Path.cwd())

    except HttpError as e:
        # Minimal, useful error log
        logger.error("Failed to download Drive file %s (%s): %s", file.name, file.id, e)
        raise

    except Exception as e:
        logger.error("Failed to download Drive file %s (%s): %s", file.name, file.id, e)
        raise