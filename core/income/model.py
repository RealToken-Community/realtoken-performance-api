from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple, Union
from eth_utils import to_checksum_address


TokenAddress = str
WalletAddress = str


@dataclass(slots=True)
class WeeklyDistribution:
    """
    Represents a single weekly distribution (ISO year + ISO week).

    revenues structure:
    {
        "<token_address>": {
            "<wallet_address>": amount,
            ...
        },
        ...
    }
    """

    year: int
    week: int
    wallets: List[WalletAddress]
    revenues: Dict[TokenAddress, Dict[WalletAddress, float]]
    paid_in_currency: str  # e.g. "USD", "EUR"

    def __post_init__(self) -> None:
        # Validate ISO week
        if not (1 <= int(self.week) <= 53):
            raise ValueError(f"Invalid ISO week: {self.week}")

        # Normalize wallets
        self.wallets = list(dict.fromkeys(w.strip().lower() for w in self.wallets))

        # Normalize revenue structure
        normalized: Dict[str, Dict[str, float]] = {}

        for token, by_wallet in self.revenues.items():
            t = to_checksum_address(token.strip())
            normalized[t] = {}

            for wallet, amount in by_wallet.items():
                w = wallet.strip().lower()
                normalized[t][w] = normalized[t].get(w, 0.0) + float(amount)

                if w not in self.wallets:
                    self.wallets.append(w)

        self.revenues = normalized

        # Normalize currency
        self.paid_in_currency = self.paid_in_currency.upper()

    # ------------------------------------------------
    # Date logic
    # ------------------------------------------------

    @property
    def week_start_utc(self) -> datetime:
        """
        Monday 00:00:00 UTC of ISO (year, week).
        """
        dt = datetime.fromisocalendar(self.year, self.week, 1)
        return dt.replace(tzinfo=timezone.utc, hour=0, minute=0, second=0, microsecond=0)

    # ------------------------------------------------
    # Revenue logic
    # ------------------------------------------------

    @property
    def total_revenue(self) -> float:
        """
        Total revenue across all tokens and wallets.
        """
        return float(
            sum(amount for m in self.revenues.values() for amount in m.values())
        )

    @property
    def total_by_token(self) -> Dict[TokenAddress, float]:
        """
        Total revenue per token (sum across wallets).
        """
        return {
            token: float(sum(by_wallet.values()))
            for token, by_wallet in self.revenues.items()
        }
    
    def __str__(self) -> str:
        """
        Human-readable representation for printing.

        Example:

        2024-W03
            0xabc...   123.45
            0xdef...    98.11
        """

        # Compute total per wallet
        wallet_totals: Dict[str, float] = {}

        for by_wallet in self.revenues.values():
            for wallet, amount in by_wallet.items():
                wallet_totals[wallet] = wallet_totals.get(wallet, 0.0) + amount

        # Sort wallets for stable display
        wallets_sorted = sorted(wallet_totals.items())

        wallet_width = max((len(w) for w, _ in wallets_sorted), default=10)

        lines = []

        # Week header (fixed width for alignment)
        lines.append(f"{self.year}-W{self.week:02d}")

        for wallet, amount in wallets_sorted:
            lines.append(f"    {wallet:<{wallet_width}}  {amount:12.6f}")

        return "\n".join(lines)

    __repr__ = __str__
    


Key = Tuple[int, int]  # (year, week)


@dataclass(slots=True)
class WeeklyDistributionSeries:
    """
    Collection of WeeklyDistribution indexed by (year, ISO week),
    focused on aggregation across weeks.
    """

    _items: Dict[Key, WeeklyDistribution] = field(default_factory=dict)

    def __init__(
        self,
        distributions: Optional[Union[WeeklyDistribution, Iterable[WeeklyDistribution]]] = None,
    ) -> None:
        self._items = {}
        if distributions is not None:
            self.add(distributions)

    # ----------------------------
    # Core accessors
    # ----------------------------

    def get(self, year: int, week: int) -> Optional[WeeklyDistribution]:
        return self._items.get((year, week))

    @property
    def distributions(self) -> List[WeeklyDistribution]:
        """Chronologically ordered distributions."""
        return [self._items[k] for k in sorted(self._items.keys())]

    # ----------------------------
    # Mutations
    # ----------------------------

    def add(self, x: Union[WeeklyDistribution, Iterable[WeeklyDistribution]]) -> None:
        """Add one or many WeeklyDistribution objects."""
        if isinstance(x, WeeklyDistribution):
            self._add_one(x)
            return
        for d in x:
            self._add_one(d)

    def _add_one(self, d: WeeklyDistribution) -> None:
        key: Key = (d.year, d.week)

        if key not in self._items:
            self._items[key] = d
            return

        existing = self._items[key]

        # Reject if wallets differ (order-insensitive)
        w_existing = set(w.strip().lower() for w in existing.wallets)
        w_new = set(w.strip().lower() for w in d.wallets)
        if w_existing != w_new:
            raise ValueError(
                f"Conflicting WeeklyDistribution for {key}: wallets differ. "
                f"existing={sorted(w_existing)} new={sorted(w_new)}"
            )

        # Same (year, week) and same wallets -> reject duplicate to avoid silent overwrite
        raise ValueError(f"Duplicate WeeklyDistribution for {key} already exists.")

    # ----------------------------
    # Aggregations across weeks
    # ----------------------------

    @property
    def total_revenue(self) -> float:
        """Total revenue across all weeks, all tokens, all wallets."""
        return float(sum(d.total_revenue for d in self._items.values()))

    def total_revenue_for_token(self, token: str) -> float:
        """Total revenue for a given token across all weeks (sum across wallets)."""
        t = to_checksum_address(token.strip())
        total = 0.0
        for d in self._items.values():
            # WeeklyDistribution.total_by_token is a property: token -> float
            total += float(d.total_by_token.get(t, 0.0))
        return float(total)

    @property
    def total_by_token(self) -> Dict[str, float]:
        """
        Total revenue per token across all weeks.
        Returns: {token: total_amount}
        """
        agg: Dict[str, float] = {}
        for d in self._items.values():
            for token, amount in d.total_by_token.items():
                agg[token] = agg.get(token, 0.0) + float(amount)
        return agg