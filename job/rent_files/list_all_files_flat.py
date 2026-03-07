from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


@dataclass(frozen=True)
class DriveFile:
    id: str
    name: str
    mime_type: str


class GoogleDriveAPIError(RuntimeError):
    pass


def _drive_files_list(
    service,
    q: str,
    *,
    page_token: Optional[str] = None,
    page_size: int = 1000,
    include_all_drives: bool = False,
) -> Tuple[List[Dict], Optional[str]]:
    """
    Low-level wrapper around Drive v3 `files.list`.
    Uses authenticated Google credentials.
    """
    try:
        request = service.files().list(
            q=q,
            pageSize=page_size,
            pageToken=page_token,
            fields="nextPageToken, files(id,name,mimeType)",
            includeItemsFromAllDrives=include_all_drives,
            supportsAllDrives=include_all_drives,
        )

        data = request.execute()

    except HttpError as e:
        raise GoogleDriveAPIError(f"Drive API error: {e}") from e

    files = data.get("files", []) or []
    next_token = data.get("nextPageToken")
    return files, next_token


def list_all_files_flat(
    folder_id: str,
    credentials,
    *,
    include_folders_in_output: bool = False,
    include_all_drives: bool = False,
) -> List[DriveFile]:
    """
    Return a flat list of all files contained in `folder_id`,
    including those in nested subfolders.
    """

    service = build("drive", "v3", credentials=credentials, cache_discovery=False)

    FOLDER_MIME = "application/vnd.google-apps.folder"

    stack: List[str] = [folder_id]
    seen_folders: set[str] = set()

    out: List[DriveFile] = []

    while stack:
        current_folder = stack.pop()

        if current_folder in seen_folders:
            continue

        seen_folders.add(current_folder)

        q = f"'{current_folder}' in parents and trashed = false"

        page_token: Optional[str] = None

        while True:
            items, page_token = _drive_files_list(
                service,
                q,
                page_token=page_token,
                include_all_drives=include_all_drives,
            )

            for it in items:
                file_id = it["id"]
                name = it.get("name", "")
                mime = it.get("mimeType", "")

                if mime == FOLDER_MIME:
                    stack.append(file_id)

                    if include_folders_in_output:
                        out.append(
                            DriveFile(
                                id=file_id,
                                name=name,
                                mime_type=mime,
                            )
                        )

                else:
                    out.append(
                        DriveFile(
                            id=file_id,
                            name=name,
                            mime_type=mime,
                        )
                    )

            if not page_token:
                break

    return out