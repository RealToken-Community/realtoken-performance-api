from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, Iterable, Mapping, Optional


@dataclass(frozen=True)
class BalanceSnapshot:
    """
    Portfolio token balances captured at a specific point in time.

    Attributes:
        as_of: Datetime when the snapshot was captured (should be timezone-aware).
        balances_by_token: Mapping token_address -> balance amount (already aggregated across wallets/sources).
    """
    as_of: datetime
    balances_by_token: Mapping[str, Decimal]


class BalanceSnapshotSeries:
    """
    A collection of BalanceSnapshot objects.

    Purpose:
      - retrieve a snapshot by its exact datetime (for weekly snapshots, etc.)
      - retrieve the most recent snapshot (used as the "now" snapshot in practice)

    Notes:
      - This class enforces that there cannot be two snapshots with the same `as_of`.
    """

    def __init__(self, snapshots: Iterable[BalanceSnapshot] = ()) -> None:
        self._snapshots_by_as_of: Dict[datetime, BalanceSnapshot] = {}
        self._latest: Optional[BalanceSnapshot] = None

        for s in snapshots:
            self.add(s)

    def add(self, snapshot: BalanceSnapshot) -> None:
        """
        Add a snapshot to the series.

        Raises:
            ValueError: if a snapshot with the same `as_of` already exists.
        """
        if snapshot.as_of in self._snapshots_by_as_of:
            raise ValueError(f"Duplicate snapshot for datetime {snapshot.as_of}")

        self._snapshots_by_as_of[snapshot.as_of] = snapshot

        if self._latest is None or snapshot.as_of > self._latest.as_of:
            self._latest = snapshot

    def get(self, as_of: datetime) -> Optional[BalanceSnapshot]:
        """
        Return the snapshot for the exact datetime, or None if not found.
        """
        return self._snapshots_by_as_of.get(as_of)

    def latest(self) -> Optional[BalanceSnapshot]:
        """
        Return the most recent snapshot in the series.
        This is how you should retrieve the "now" snapshot (do not search by datetime.now()).
        """
        return self._latest
