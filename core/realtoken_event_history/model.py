"""
This module defines the core data structures used to standardize and aggregate
all Realtoken-related transactions and events for a user.

It provides:
- An immutable, normalized event model representing atomic on-chain facts
  (buys, sells, liquidations, transfers, etc.).
- A centralized event history that collects, deduplicates, and organizes these
  events by token.

This structured event history serves as the single source of truth for all
subsequent performance, profitability, and analytics computations exposed by
the API.
"""

from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
from collections import defaultdict
from typing import Optional
from eth_utils import to_checksum_address, is_address
from enum import Enum
import re

_TX_HASH_REGEX = re.compile(r"^0x[a-fA-F0-9]{64}$")


class RealtokenEventType(str, Enum):
    BUY_FROM_REALT = "buy_from_realt"
    BUY_YAM = "buy_yam"
    SELL_YAM = "sell_yam"
    BUY_SWAPCAT = "buy_swapcat"
    SELL_SWAPCAT = "sell_swapcat"
    TRANSFER = "internal_transfer"
    LIQUIDATION = "liquidation"
    LIQUIDATED = "liquidated"
    DETOKENISATION = "detokenisation"

# =========================
# Event (immutable)
# =========================

@dataclass(frozen=True)
class RealtokenEvent:
    """
    Represents a single Realtoken-related event.

    This is an immutable, atomic fact that should never be modified once created.
    """
    token_address: str
    amount: Decimal
    source: str
    destination: str
    timestamp: datetime
    transaction_hash: str
    log_index: int
    event_type: RealtokenEventType
    price_per_token: Optional[Decimal]

    def __post_init__(self) -> None:
        # ---- token_address ----
        if not is_address(self.token_address):
            raise ValueError(f"Invalid token_address: {self.token_address}")
        object.__setattr__(self, "token_address", to_checksum_address(self.token_address))
        
        # ---- amount ----
        if self.amount <= 0:
            raise ValueError("amount must be strictly positive")

        # ---- addresses ----
        if not is_address(self.source):
            raise ValueError(f"Invalid EVM address (source): {self.source}")
        if not is_address(self.destination):
            raise ValueError(f"Invalid EVM address (destination): {self.destination}")

        # Enforce checksum format
        object.__setattr__(self, "source", to_checksum_address(self.source))
        object.__setattr__(self, "destination", to_checksum_address(self.destination))

        # ---- transaction hash ----
        if not _TX_HASH_REGEX.match(self.transaction_hash):
            raise ValueError(
                f"Invalid transaction hash format: {self.transaction_hash}"
            )
        object.__setattr__(self, "transaction_hash", self.transaction_hash.lower())

        # ---- log index ----
        if self.log_index < 0:
            raise ValueError("log_index must be >= 0")

        # ---- price ----
        if self.event_type == RealtokenEventType.TRANSFER:
            if self.price_per_token is not None:
                raise ValueError("price_per_token must be None for TRANSFER events")
        else:
            if self.price_per_token is None:
                raise ValueError(f"price_per_token is required for {self.event_type} events")
            if self.price_per_token < 0:
                raise ValueError("price_per_token cannot be negative")

        # ---- timestamp ----
        if not isinstance(self.timestamp, datetime):
            raise TypeError("timestamp must be a datetime instance")
        if self.timestamp.tzinfo is None:
            raise ValueError("timestamp must be UTC")

        # ---- event_type ----
        if not self.event_type:
            raise ValueError("event_type must be a non-empty string")
        
    @property
    def total_price(self) -> Optional[Decimal]:
        """
        Total value of the event (amount * price_per_token).

        Returns None for when price_per_token is undefined (for even_type TRANSFER).
        """
        if self.price_per_token is None:
            return None
        return Decimal(self.amount) * Decimal(self.price_per_token)
    
    def to_dict(self) -> dict:
        return {
            "token_address": self.token_address,
            "amount": str(self.amount),  # Decimal → str
            "source": self.source,
            "destination": self.destination,
            "timestamp": self.timestamp.isoformat(),  # datetime → ISO string
            "transaction_hash": self.transaction_hash,
            "log_index": self.log_index,
            "event_type": self.event_type.value,  # Enum → str
            "price_per_token": None if self.price_per_token is None else str(self.price_per_token),
            "total_price": None if self.total_price is None else str(self.total_price),
        }
    
    def __str__(self) -> str:
        price = (
            f"{self.price_per_token:.6f}"
            if self.price_per_token is not None
            else "N/A"
        )
        total = (
            f"{self.total_price:.6f}"
            if self.total_price is not None
            else "N/A"
        )
    
        token_short = f"{self.token_address[:6]}…{self.token_address[-4:]}"
    
        return (
            f"{self.event_type.value.upper():<14} "
            f"{self.amount:>10} @ {price:<12} "
            f"(total={total:<14}) "
            f"{self.timestamp.isoformat()} | "
            f"token={token_short} "
        )

    
    def __repr__(self) -> str:
        return (
            f"RealtokenEvent("
            f"event_type={self.event_type.value!r}, "
            f"amount={self.amount}, "
            f"price_per_token={self.price_per_token}, "
            f"total_price={self.total_price}, "
            f"token_address={self.token_address!r}, "
            f"source={self.source!r}, "
            f"destination={self.destination!r}, "
            f"timestamp={self.timestamp.isoformat()!r}, "
            f"transaction_hash={self.transaction_hash!r}, "
            f"log_index={self.log_index}"
            f")"
        )



# =========================
# History
# =========================

class RealtokenEventHistory:
    """
    Collection of all Realtoken events (buys, sells, transfers, fees, etc.),
    grouped by token UUID.
    """

    def __init__(self) -> None:
        self._events_by_token: dict[str, list[RealtokenEvent]] = defaultdict(list)
        self._seen_events: set[tuple[str, int]] = set()

    def add(self, event: RealtokenEvent) -> None:
        """
        Add a single event to the history.
    
        An event is uniquely identified by (transaction_hash, log_index).
        Duplicate events are silently ignored.
        """
        event_id = (event.transaction_hash, event.log_index)
    
        if event_id in self._seen_events:
            return  # already added
    
        self._seen_events.add(event_id)
        self._events_by_token[event.token_address].append(event)

    def events_for(self, token_address: str) -> list[RealtokenEvent]:
        """Return all events for a given token UUID."""
        return list(self._events_by_token.get(token_address, []))

    def tokens(self) -> list[str]:
        """Return all token UUIDs present in the history."""
        return list(self._events_by_token.keys())
    
    def sort_events_by_timestamp(self) -> None:
        """
        Sort events for every token from oldest to latest,
        based on the `timestamp` field.
    
        Sorting is done in-place.
        """
        for events in self._events_by_token.values():
            events.sort(key=lambda e: e.timestamp)
    
    def as_dict(self) -> dict[str, list[RealtokenEvent]]:
        """
        Return a shallow copy of the event history.
        """
        return {
            token: list(events)
            for token, events in self._events_by_token.items()
        }
    
    def as_dict_serialized(self) -> dict[str, list[dict]]:
        """
        Return a JSON-serializable representation of the event history.
        """
        return {
            token: [event.to_dict() for event in events]
            for token, events in self._events_by_token.items()
        }
    
    def events_for_tx(self, transaction_hash: str) -> list[RealtokenEvent]:
        """
        Return all events related to a given transaction hash.
    
        Events are returned ordered by log_index.
        """
        tx = transaction_hash.lower()
    
        if not _TX_HASH_REGEX.match(tx):
            raise ValueError(f"Invalid transaction hash format: {transaction_hash}")
    
        matches: list[RealtokenEvent] = []
    
        for events in self._events_by_token.values():
            for event in events:
                if event.transaction_hash == tx:
                    matches.append(event)
    
        matches.sort(key=lambda e: e.log_index)
        return matches
    
    def count_events(self) -> int:
        """
        Return the total number of events stored in the history,
        across all tokens.
        """
        return sum(len(events) for events in self._events_by_token.values())
