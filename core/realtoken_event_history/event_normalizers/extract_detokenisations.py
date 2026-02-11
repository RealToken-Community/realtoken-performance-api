from typing import Any, Dict, List
from datetime import datetime, timezone
from core.services.utilities import get_token_price_at_timestamp


def extract_detokenisations(
    out_transfers: List[Dict[str, Any]],
    realtoken_history_data: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Return outbound transfer events where tokens have been detokenised 
    (i.e. tokens were sent to the zero address), and enrich each returned
    event with `price_per_token`.

    A transfer is considered a detokenisation if the transfer `destination`
    address is the zero address (0x000...000).

    The `price_per_token` is resolved via:
        get_token_price_at_timestamp(realtoken_history_data, uuid, timestamp)

    Args:
        out_transfers: List of outbound ERC-20 transfer events.
        realtoken_history_data: Token history data used to resolve token price at a timestamp.

    Returns:
        List of outbound transfer events representing detokenisations,
        each with an additional `price_per_token` field.
    """
    ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

    out: List[Dict[str, Any]] = []

    for ev in out_transfers:
        dst = ev.get("destination")
        if not dst or dst.lower() != ZERO_ADDRESS:
            continue

        token = ev.get("token") or {}
        uuid = token.get("address")       # token contract address (uuid)
        ts_raw = ev.get("timestamp")      # usually a unix timestamp string from The Graph
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
