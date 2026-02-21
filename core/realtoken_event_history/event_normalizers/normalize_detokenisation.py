from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable, Tuple
from core.realtoken_event_history.model import RealtokenEventType, RealtokenEvent

import logging
logger = logging.getLogger(__name__)

def normalize_detokenisation(
    detokenisation_events: Iterable[dict],
) -> Tuple[RealtokenEvent, ...]:
    """
    Normalize RealT detokenisation events.

    Each raw event is expected to come from The Graph and represents
    a detokenisation.

    Returns:
        Tuple of normalized RealtokenEvent objects (immutable).
    """
    normalized_events: list[RealtokenEvent] = []

    for raw in detokenisation_events:
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
            event_type=RealtokenEventType.DETOKENISATION,
            price_per_token=raw["price_per_token"],
        )

        normalized_events.append(event)

    return tuple(normalized_events)