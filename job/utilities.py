from __future__ import annotations
import requests, time
import json
import os
from typing import Any, Dict, List, Optional
from core.services.send_telegram_alert import send_telegram_alert
import logging
logger = logging.getLogger(__name__)


def fetch_json(url: str, timeout: int = 20) -> Optional[Any]:
    """Fetch JSON with basic cache-busting to avoid stale CDN responses."""
    try:
        headers = {
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Accept": "application/json",
            "User-Agent": "RealtokenUpdateAlertsBot/1.0",
        }
        params = {"_": str(int(time.time()))}  # cache-buster
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.warning("Failed to fetch JSON from %s: %s", url, e)
        send_telegram_alert(f"realtoken update alert bot: Failed to fetch JSON from {url}: {e}")
        return None
    
def list_to_dict_by_uuid(items: Optional[List[Dict[str, Any]]]) -> Optional[Dict[str, Dict[str, Any]]]:
    """
    Convert a list of dictionaries into a dictionary keyed by the 'uuid' value.

    Args:
        items: A list of dictionaries, each expected to contain a 'uuid' key.
               If None, returns None.

    Returns:
        - None if input is None.
        - Otherwise, a dictionary where:
            * Keys are the 'uuid' values from the input dictionaries.
            * Values are the corresponding full dictionaries from the list.
        Entries without a 'uuid' key or with a falsy 'uuid' value are ignored.
    """
    if items is None:
        return None

    result: Dict[str, Dict[str, Any]] = {}
    for item in items:
        uuid = item.get("uuid").lower()
        if not uuid:
            continue
        result[uuid] = item
    return result

def sort_realtoken_history_in_place(
    realtoken_history_data: Dict[str, Dict[str, Any]]
) -> None:
    """
    Ensure that for each uuid, the 'history' list is sorted
    chronologically by 'date' (YYYYMMDD).

    Sorting is done in-place.
    """
    for token_data in realtoken_history_data.values():
        history = token_data.get("history")
        if not history:
            continue

        history.sort(key=lambda x: x.get("date", ""))


def load_json(path: str) -> Any:
    """
    Load a JSON file from the given path and return its content.

    If the file is empty, return an empty dict {}.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    # If file is empty → return {}
    if os.path.getsize(path) == 0:
        return {}

    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            # If file contains invalid JSON but is not empty
            raise
    
import json
from typing import Any


def save_json(data: Any, path: str) -> None:
    """
    Save data to a JSON file at the given path.

    Args:
        data: Python object to serialize (dict, list, etc.).
        path: Path to the JSON file.

    Raises:
        TypeError: If the data is not JSON serializable.
        OSError: If the file cannot be written.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)