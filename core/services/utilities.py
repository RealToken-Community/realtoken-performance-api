from __future__ import annotations
import psycopg2
from psycopg2.extensions import connection as PGConnection
import requests, time
from datetime import datetime, timezone
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



def get_token_price_at_timestamp(
    realtoken_history_data: Dict[str, Dict[str, Any]],
    uuid: str,
    timestamp: datetime,
) -> Optional[float]:
    """
    Return the token price effective at the given timestamp.

    Rules:
    - History entries are applied chronologically (by YYYYMMDD).
    - The latest tokenPrice with entry_date <= timestamp is returned.
    - Fallback: if timestamp is older than the first history date, return the
      first tokenPrice available in history (even if its date is after timestamp).
    - Returns None if no tokenPrice exists anywhere in history.

    Args:
        realtoken_history_data: dict keyed by uuid
        uuid: token uuid
        timestamp: datetime (UTC recommended)

    Returns:
        tokenPrice as float or None
    """
    token_data = realtoken_history_data.get(uuid.lower())
    if not token_data:
        return None

    history = token_data.get("history", [])
    if not history:
        return None

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    # History is already sorted chronologically (YYYYMMDD) at data load time
    hist_sorted = history

    # Parse first date
    first_date_str = hist_sorted[0].get("date")
    if not first_date_str:
        return None  # or raise, depending on your tolerance

    first_date = datetime.strptime(first_date_str, "%Y%m%d").replace(tzinfo=timezone.utc)

    # Fallback if timestamp is before the first known date:
    # return the first tokenPrice that exists in the history
    if timestamp < first_date:
        for entry in hist_sorted:
            values = entry.get("values", {})
            if "tokenPrice" in values:
                return float(values["tokenPrice"])
        return None  # no tokenPrice anywhere

    # Walk forward:
    # - keep the latest tokenPrice <= timestamp
    # - if none exists, fallback to the first tokenPrice after timestamp
    current_price: Optional[float] = None
    first_price_after: Optional[float] = None
    
    for entry in hist_sorted:
        date_str = entry.get("date")
        if not date_str:
            continue
    
        entry_date = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
        values = entry.get("values", {})
    
        if entry_date <= timestamp:
            if "tokenPrice" in values:
                current_price = float(values["tokenPrice"])
        else:
            if first_price_after is None and "tokenPrice" in values:
                first_price_after = float(values["tokenPrice"])
            break
    
    return current_price if current_price is not None else first_price_after


def get_token_issuance_timestamp(
    realtoken_history_data: Dict[str, Dict[str, Any]],
    token_uuid: str,
) -> Optional[datetime]:
    """
    Return the oldest timestamp found in the history of a given token.

    This is used as a proxy for the token issuance date, based on the earliest
    available history entry.

    Args:
        realtoken_history_data: Dict keyed by token uuid (lowercase),
                                each value containing a "history" list.
        token_uuid: Token uuid to search for.

    Returns:
        A datetime corresponding to the oldest history date,
        or None if the token or history is missing.
    """
    token = realtoken_history_data.get(token_uuid.lower())
    if not token:
        return None

    history = token.get("history", [])
    if not history:
        return None

    dates = []
    for entry in history:
        raw_date = entry.get("date")
        if not raw_date:
            continue

        try:
            # date format: YYYYMMDD
            dates.append(datetime.strptime(raw_date, "%Y%m%d"))
        except ValueError:
            continue

    if not dates:
        return None

    return min(dates)


def get_pg_connection(pg_host, pg_port, pg_db, pg_user, pg_password) -> PGConnection:
    """
    Create and return a PostgreSQL connection.
    """
    return psycopg2.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_db,
        user=pg_user,
        password=pg_password,
        connect_timeout=10,
    )

def test_postgres_connection(POSTGRES_DATA) -> bool:
    try:
        conn = get_pg_connection(*POSTGRES_DATA)
        conn.close()
        return True
    except Exception as e:
        send_telegram_alert(f"roi calculator api: Postgres DB connection failed")
        logger.exception(f"Postgres connection failed")
        time.sleep(120)
        return False
