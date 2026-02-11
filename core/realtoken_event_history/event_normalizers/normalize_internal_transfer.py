from __future__ import annotations
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any, Dict, Sequence
from core.realtoken_event_history.model import RealtokenEventHistory, RealtokenEventType, RealtokenEvent


import logging
logger = logging.getLogger(__name__)

def normalize_internal_transfer(
    transfers_payload: Dict[str, Any],
    history: RealtokenEventHistory,
    user_wallets: Sequence[str],
) -> None:
    """
    Normalize ONLY internal RealToken transfers (source and destination are both user wallets)
    from the dict returned by fetch_realtoken_transfers(), and add them to RealtokenEventHistory.

    Expected payload shape (from fetch_realtoken_transfers):
      {
        "data": {
          "outTransfers": [...],
          "inTransfers":  [...]
        },
        "meta": {...},
        ...
      }

    Notes:
    - Internal transfers may appear twice (once in outTransfers and once in inTransfers).
      This is OK because history.add() deduplicates by (transaction_hash, log_index).
    - price_per_token is set to 0 for transfers (unknown / not applicable).
    """
    wallets_lc = {w.lower() for w in user_wallets if w}

    data = (transfers_payload or {}).get("data") or {}
    out_transfers = data.get("outTransfers") or []
    in_transfers = data.get("inTransfers") or []

    # Iterate over both; dedup is handled by history.add()
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

        history.add(event)