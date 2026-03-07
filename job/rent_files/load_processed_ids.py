from pathlib import Path
from typing import Set
import json


def load_processed_ids(json_path: str | Path = 'data/rent_files_parquet/list_rent_files_collected.json') -> Set[str]:
    """
    Load the set of already processed Google Drive file IDs from a JSON file.

    Expected JSON format:
        A simple list of strings, for example:
        [
            "1A2B3C",
            "4D5E6F"
        ]

    Behavior:
    - If the file does not exist, returns an empty set.
    - If the file exists but is empty (0 byte or only whitespace), returns an empty set.
    - If the file contains null, returns an empty set.
    - If the JSON structure is not a list of strings, raises a ValueError.
    """

    path = Path(json_path)

    # If the file does not exist yet, nothing has been processed
    if not path.exists():
        return set()

    content = path.read_text(encoding="utf-8")

    # If file is empty or contains only whitespace
    if not content.strip():
        return set()

    # Parse JSON content
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format in {path}: {e}") from e

    # If file contains explicit null
    if data is None:
        return set()

    # Validate structure: must be a list of strings
    if not isinstance(data, list) or any(not isinstance(x, str) for x in data):
        raise ValueError(
            f"Invalid JSON structure in {path}. "
            f"Expected a list of string IDs."
        )

    return set(data)