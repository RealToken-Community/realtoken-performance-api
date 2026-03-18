from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Tuple
from eth_utils import to_checksum_address


TokenAddress = str
WalletAddress = str
Key = Tuple[int, int]  # (year, week)


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

    # Precomputed fields
    _total_revenue: float = field(init=False, repr=False)
    _total_by_token: Dict[TokenAddress, float] = field(init=False, repr=False)
    _week_start_utc: datetime = field(init=False, repr=False)

    def __post_init__(self) -> None:
        # Validate ISO week
        if not (1 <= int(self.week) <= 53):
            raise ValueError(f"Invalid ISO week: {self.week}")

        # Normalize wallets
        normalized_wallets = list(dict.fromkeys(w.strip().lower() for w in self.wallets))

        # Normalize revenue structure
        normalized_revenues: Dict[TokenAddress, Dict[WalletAddress, float]] = {}

        for token, by_wallet in self.revenues.items():
            t = to_checksum_address(token.strip())
            normalized_revenues[t] = {}

            for wallet, amount in by_wallet.items():
                w = wallet.strip().lower()
                normalized_revenues[t][w] = normalized_revenues[t].get(w, 0.0) + float(amount)

                if w not in normalized_wallets:
                    normalized_wallets.append(w)

        self.wallets = normalized_wallets
        self.revenues = normalized_revenues
        self.paid_in_currency = self.paid_in_currency.upper()

        # Precompute week start
        dt = datetime.fromisocalendar(self.year, self.week, 1)
        self._week_start_utc = dt.replace(
            tzinfo=timezone.utc,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        # Precompute totals
        total_by_token: Dict[TokenAddress, float] = {}
        total_revenue = 0.0

        for token, by_wallet in self.revenues.items():
            token_total = float(sum(by_wallet.values()))
            total_by_token[token] = token_total
            total_revenue += token_total

        self._total_by_token = total_by_token
        self._total_revenue = float(total_revenue)

    @property
    def week_start_utc(self) -> datetime:
        """
        Monday 00:00:00 UTC of ISO (year, week).
        """
        return self._week_start_utc

    @property
    def total_revenue(self) -> float:
        """
        Total revenue across all tokens and wallets.
        """
        return self._total_revenue

    @property
    def total_by_token(self) -> Dict[TokenAddress, float]:
        """
        Total revenue per token (sum across wallets).
        """
        return self._total_by_token

    def __str__(self) -> str:
        """
        Human-readable representation for printing.

        Example:

        2024-W03
            0xabc...   123.45
            0xdef...    98.11
        """
        wallet_totals: Dict[str, float] = {}

        for by_wallet in self.revenues.values():
            for wallet, amount in by_wallet.items():
                wallet_totals[wallet] = wallet_totals.get(wallet, 0.0) + amount

        wallets_sorted = sorted(wallet_totals.items())
        wallet_width = max((len(w) for w, _ in wallets_sorted), default=10)

        lines = [f"{self.year}-W{self.week:02d}"]

        for wallet, amount in wallets_sorted:
            lines.append(f"    {wallet:<{wallet_width}}  {amount:12.6f}")

        return "\n".join(lines)

    __repr__ = __str__


@dataclass(slots=True)
class WeeklyDistributionSeries:
    """
    Immutable-like collection of WeeklyDistribution indexed by (year, ISO week),
    focused on aggregation across weeks.

    The full content is built at initialization time, validated once,
    sorted once, and all major aggregations are precomputed.
    """

    _items: Dict[Key, WeeklyDistribution] = field(init=False, repr=False)
    _distributions: List[WeeklyDistribution] = field(init=False, repr=False)
    _total_revenue: float = field(init=False, repr=False)
    _total_by_token: Dict[TokenAddress, float] = field(init=False, repr=False)

    def __init__(
        self,
        distributions: Optional[Iterable[WeeklyDistribution]] = None,
    ) -> None:
        items: Dict[Key, WeeklyDistribution] = {}

        for d in distributions or []:
            key: Key = (d.year, d.week)

            if key not in items:
                items[key] = d
                continue

            existing = items[key]

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

        self._items = items
        self._distributions = [items[k] for k in sorted(items.keys())]

        total_revenue = 0.0
        total_by_token: Dict[TokenAddress, float] = {}

        for d in self._distributions:
            total_revenue += d.total_revenue

            for token, amount in d.total_by_token.items():
                total_by_token[token] = total_by_token.get(token, 0.0) + float(amount)

        self._total_revenue = float(total_revenue)
        self._total_by_token = total_by_token

    def get(self, year: int, week: int) -> Optional[WeeklyDistribution]:
        return self._items.get((year, week))

    @property
    def distributions(self) -> List[WeeklyDistribution]:
        """
        Chronologically ordered distributions.
        """
        return self._distributions

    @property
    def total_revenue(self) -> float:
        """
        Total revenue across all weeks, all tokens, all wallets.
        """
        return self._total_revenue

    def total_revenue_for_token(self, token: str) -> float:
        """
        Total revenue for a given token across all weeks (sum across wallets).
        """
        t = to_checksum_address(token.strip())
        return float(self._total_by_token.get(t, 0.0))

    @property
    def total_by_token(self) -> Dict[TokenAddress, float]:
        """
        Total revenue per token across all weeks.
        Returns: {token: total_amount}
        """
        return self._total_by_token

    def cash_flow_amount_and_date_for_token(self, token: str) -> List[Tuple[Decimal, datetime]]:
        """
        Return a chronologically ordered list of cash flow pairs for one token:
        (revenue amount for the week, week start date in UTC).
        """
        t = to_checksum_address(token.strip())

        pairs: List[Tuple[Decimal, datetime]] = []

        for d in self._distributions:
            amount = d.total_by_token.get(t, 0.0)
            if amount != 0.0:
                pairs.append((Decimal(str(amount)), d.week_start_utc))

        return pairs

    def __len__(self) -> int:
        return len(self._distributions)

    def __iter__(self):
        return iter(self._distributions)

    def __repr__(self) -> str:
        return f"WeeklyDistributionSeries(count={len(self._distributions)})"