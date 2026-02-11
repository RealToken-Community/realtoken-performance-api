from __future__ import annotations
from decimal import Decimal
from datetime import datetime, timezone
from typing import Iterable
from core.realtoken_event_history.model import RealtokenEventHistory, RealtokenEventType, RealtokenEvent

import logging
logger = logging.getLogger(__name__)

def normalize_realt_purchases(
    realt_purchases: Iterable[dict],
    realtoken_event_history: RealtokenEventHistory,
) -> None:
    """
    Normalize RealT primary market purchase events and add them to the event history.

    Each raw event is expected to come from The Graph and represents
    a direct purchase from RealT.
    """
    for raw in realt_purchases:
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
            event_type=RealtokenEventType.BUY_FROM_REALT,
            price_per_token=raw["price_per_token"],
        )

        realtoken_event_history.add(event)