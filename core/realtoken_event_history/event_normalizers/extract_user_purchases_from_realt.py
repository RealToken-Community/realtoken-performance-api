from typing import Any, Dict, List
from datetime import datetime, timezone
from core.services.utilities import get_token_price_at_timestamp

def extract_user_purchases_from_realt(
    in_transfers: List[Dict[str, Any]],
    realtoken_data: Dict[str, Dict[str, Any]],
    realtoken_history_data: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Return inbound transfer events where the user bought tokens from RealT,
    and enrich each returned event with `price_per_token`.

    A transfer is considered a purchase from RealT if the transfer `source`
    address matches any `owner` address found in `realtoken_data`.

    The `price_per_token` is resolved via:
        get_token_price_at_timestamp(realtoken_history_data, uuid, timestamp)

    Args:
        in_transfers: List of inbound ERC-20 transfer events.
        realtoken_data: RealToken metadata indexed by token address, containing an `owner` field.
        realtoken_history_data: Token history data used to resolve token price at a timestamp.

    Returns:
        List of inbound transfer events representing purchases from RealT,
        each with an additional `price_per_token` field.
    """

    # Collect RealT owner addresses (case-insensitive)
    realt_owners = {
        meta["owner"].lower()
        for meta in realtoken_data.values()
        if isinstance(meta, dict) and meta.get("owner")
    }

    out: List[Dict[str, Any]] = []

    for ev in in_transfers:
        src = ev.get("source")
        if not src or src.lower() not in realt_owners:
            continue

        token = ev.get("token") or {}
        uuid = token.get("address")  # token contract address (uuid)
        ts_raw = ev.get("timestamp")     # usually a unix timestamp string from The Graph
        ts_dt = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc)

        price = None
        if uuid and ts_dt is not None:
            try:
                price = get_token_price_at_timestamp(realtoken_history_data, uuid, ts_dt)
            except Exception:
                price = None

        ev_copy = dict(ev)
        ev_copy["price_per_token"] = price
        out.append(ev_copy)

    return out

