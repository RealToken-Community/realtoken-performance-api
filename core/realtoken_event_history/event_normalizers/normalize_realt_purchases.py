from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable, Tuple
from core.realtoken_event_history.model import RealtokenEventType, RealtokenEvent

import logging
logger = logging.getLogger(__name__)


def normalize_realt_purchases(
    realt_purchases: Iterable[dict],
) -> Tuple[RealtokenEvent, ...]:
    """
    Normalize RealT primary market purchase events.

    Each raw event is expected to come from The Graph and represents
    a direct purchase from RealT.

    Returns:
        Tuple of normalized RealtokenEvent objects (immutable).
    """
    normalized_events: list[RealtokenEvent] = []

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

        normalized_events.append(event)

    return tuple(normalized_events)