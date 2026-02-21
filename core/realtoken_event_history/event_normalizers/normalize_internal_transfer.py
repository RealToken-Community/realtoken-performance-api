from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Sequence, Tuple
from core.realtoken_event_history.model import RealtokenEventType, RealtokenEvent

import logging
logger = logging.getLogger(__name__)


def normalize_internal_transfer(
    transfers_payload: Dict[str, Any],
    user_wallets: Sequence[str],
) -> Tuple[RealtokenEvent, ...]:
    """
    Normalize ONLY internal RealToken transfers (source and destination are both user wallets)
    from the dict returned by fetch_realtoken_transfers().

    Expected payload shape:
      {
        "data": {
          "outTransfers": [...],
          "inTransfers":  [...]
        },
        "meta": {...},
        ...
      }

    Returns:
        Tuple of normalized RealtokenEvent objects (immutable).

    Notes:
    - Internal transfers may appear twice (once in outTransfers and once in inTransfers).
      Deduplication must be handled later (e.g. by RealtokenEventHistory.add()).
    - price_per_token is None for transfers (not applicable).
    """
    wallets_lc = {w.lower() for w in user_wallets if w}

    data = (transfers_payload or {}).get("data") or {}
    out_transfers = data.get("outTransfers") or []
    in_transfers = data.get("inTransfers") or []

    normalized_events: list[RealtokenEvent] = []

    for raw in list(out_transfers) + list(in_transfers):
        src = (raw.get("source") or "").lower()
        dst = (raw.get("destination") or "").lower()

        # Only internal transfers: user wallet -> user wallet
        if src not in wallets_lc or dst not in wallets_lc:
            continue

        # --- timestamp (unix -> UTC datetime) ---
        ts = datetime.fromtimestamp(int(raw["timestamp"]), tz=timezone.utc)

        event = RealtokenEvent(
            token_address=raw["token"]["address"],
            amount=Decimal(raw["amount"]),
            source=raw["source"],
            destination=raw["destination"],
            timestamp=ts,
            transaction_hash=raw["transaction"]["id"],
            log_index=int(raw["log_index"]),
            event_type=RealtokenEventType.TRANSFER,
            price_per_token=None,
        )

        normalized_events.append(event)

    return tuple(normalized_events)