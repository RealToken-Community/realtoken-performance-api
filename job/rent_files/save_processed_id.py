from pathlib import Path
import json


def save_processed_id(
    file_drive_id: str,
    json_path: str | Path = "data/rent_files_parquet/list_rent_files_collected.json",
) -> None:
    """
    Save one processed Google Drive file ID into the JSON file.

    JSON format:
        [
            "1A2B3C",
            "4D5E6F"
        ]

    Behavior:
    - Creates the file if it does not exist.
    - If the file is empty or contains only whitespace, starts from an empty list.
    - If the file contains null, starts from an empty list.
    - If the JSON structure is invalid, raises a ValueError.
    - Avoids duplicates.
    """

    path = Path(json_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    processed_ids: set[str] = set()

    if path.exists():
        content = path.read_text(encoding="utf-8")

        if content.strip():
            try:
                data = json.loads(content)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON format in {path}: {e}") from e

            if data is None:
                processed_ids = set()
            elif not isinstance(data, list) or any(not isinstance(x, str) for x in data):
                raise ValueError(
                    f"Invalid JSON structure in {path}. "
                    f"Expected a list of string IDs."
                )
            else:
                processed_ids = set(data)

    processed_ids.add(file_drive_id)

    path.write_text(
        json.dumps(sorted(processed_ids), indent=4),
        encoding="utf-8",
    )