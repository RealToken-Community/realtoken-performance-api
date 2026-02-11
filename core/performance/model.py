from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional, Tuple, Iterable


EventId = Tuple[str, int]  # (transaction_hash, log_index)


@dataclass(frozen=True)
class Realization:
    """
    Represents a single realized PnL result derived from one OUT movement
    (SELL, DETOKENISATION, ...), using the Weighted Average Cost method.

    What is "Weighted Average Cost" (WAC)?
    - It is the average acquisition cost per token currently held, weighted by quantities.
    - With the WAC method:
        * The weighted average cost changes only when you BUY (IN movement).
        * When you SELL (OUT movement), the sale's cost basis is computed using the
          weighted average cost currently in effect (i.e. the historical acquisition cost allocated to the sold quantity)
        * The sale does not directly change the weighted average cost; it reduces both
          quantity held and total cost proportionally at the current weighted average cost (WAC = total_cost / quantity_held).

    For this Realization:
    - `weighted_avg_cost_used` is the weighted average cost per token applied to compute
      the cost basis removed by this OUT movement.
    - `cost_basis_out = amount_out * weighted_avg_cost_used`.
    """

    # ---- traceability ----
    event_id: EventId                   # Unique identifier of the OUT blockchain event (tx_hash, log_index)
    timestamp: datetime                 # Timestamp of the OUT movement

    # ---- OUT movement ----
    amount_out: Decimal                 # Quantity disposed by this OUT movement
    out_price_per_token: Decimal        # Effective price per token for the OUT movement
    out_total_price: Decimal            # Total value of the OUT movement (amount_out * out_price_per_token)

    # ---- WAC context used for this OUT ----
    weighted_avg_cost_used: Decimal     # Weighted average cost per token used for this OUT movement
    cost_basis_out: Decimal             # Cost basis removed by this OUT (amount_out * weighted_avg_cost_used)
    acquisitions_used_count: int        # Number of IN tx included in the current WAC calculation (debug/info)

    avg_holding_days: float             # Average holding age (in days) of the position at the time of this OUT

    # ---------------------------------------------------------------------
    # Derived metrics
    # ---------------------------------------------------------------------

    @property
    def pnl_amount(self) -> Decimal:
        """Absolute realized PnL for this OUT movement."""
        return self.out_total_price - self.cost_basis_out

    @property
    def pnl_pct(self) -> Optional[Decimal]:
        """
        Realized PnL percentage relative to the cost basis removed by this OUT.
        """
        if self.cost_basis_out == 0:
            return None
        return (self.pnl_amount / self.cost_basis_out) * Decimal("100")

    @property
    def annualized_return_pct(self) -> Optional[Decimal]:
        """
        Annualized return (%) based on avg_holding_days.

        Linear annualization:
            pnl_decimal * (365 / holding_days) * 100
        """
        if self.cost_basis_out == 0 or self.avg_holding_days <= 0:
            return None

        pnl_decimal = self.pnl_amount / self.cost_basis_out
        return pnl_decimal * (Decimal("365") / Decimal(str(self.avg_holding_days))) * Decimal("100")
    
    def __str__(self) -> str:
        def fmt_decimal(v: Optional[Decimal], pct: bool = False) -> str:
            if v is None:
                return "—"
            return f"{v:.2f}%" if pct else f"{v:.2f}"

        lines = [
            "Realization",
            "-" * 40,
            f"Event ID              : {self.event_id}",
            f"Timestamp             : {self.timestamp.isoformat()}",
            "",
            "OUT movement",
            f"  Amount out           : {fmt_decimal(self.amount_out)}",
            f"  Price / token        : {fmt_decimal(self.out_price_per_token)}",
            f"  Total OUT value      : {fmt_decimal(self.out_total_price)}",
            "",
            "Cost basis (WAC)",
            f"  WAC used             : {fmt_decimal(self.weighted_avg_cost_used)}",
            f"  Cost basis OUT       : {fmt_decimal(self.cost_basis_out)}",
            f"  IN tx count          : {self.acquisitions_used_count}",
            "",
            "Holding",
            f"  Avg holding days     : {self.avg_holding_days:.2f}",
            "",
            "Performance",
            f"  PnL amount           : {fmt_decimal(self.pnl_amount)}",
            f"  PnL %                : {fmt_decimal(self.pnl_pct, pct=True)}",
            f"  Annualized return %  : {fmt_decimal(self.annualized_return_pct, pct=True)}",
        ]

        return "\n".join(lines)
    
    def __repr__(self) -> str:
        return (
            f"Realization("
            f"event_id={self.event_id}, "
            f"amount_out={self.amount_out}, "
            f"cost_basis_out={self.cost_basis_out}, "
            f"out_total_price={self.out_total_price}, "
            f"pnl={self.pnl_amount}, "
            f"pnl_pct={self.pnl_pct}, "
            f"avg_holding_days={self.avg_holding_days}"
            f")"
        )
    


class RealizedPnLIndicator:
    """
    Final realized performance indicator built from a list of Realization.

    This object represents a finalized business result:
    - it is created from realized transactions
    - all aggregate figures are computed once at construction time
    - only the return and the annualized return are exposed as derived metrics
    """

    def __init__(self, realizations: Iterable["Realization"]) -> None:
        realizations = tuple(realizations)

        self.realization_count: int = len(realizations)

        total_cost_basis_out = Decimal("0")
        total_out_value = Decimal("0")
        weighted_holding_days_sum = Decimal("0")
        holding_weight_sum = Decimal("0")

        for r in realizations:
            total_cost_basis_out += r.cost_basis_out
            total_out_value += r.out_total_price

            if r.cost_basis_out > 0 and r.avg_holding_days > 0:
                w = r.cost_basis_out
                holding_weight_sum += w
                weighted_holding_days_sum += w * Decimal(str(r.avg_holding_days))

        self.total_cost_basis_out: Decimal = total_cost_basis_out
        self.total_out_value: Decimal = total_out_value
        self.realized_pnl: Decimal = total_out_value - total_cost_basis_out

        self.avg_holding_days: Optional[float] = (
            float(weighted_holding_days_sum / holding_weight_sum)
            if holding_weight_sum > 0
            else None
        )

    # ------------------------------------------------------------------
    # Derived business metrics, computed from the aggregated values above
    # ------------------------------------------------------------------

    @property
    def return_pct(self) -> Optional[Decimal]:
        """
        Realized return (%) = realized_pnl / total_cost_basis_out * 100
        """
        if self.total_cost_basis_out == 0:
            return None
        return (self.realized_pnl / self.total_cost_basis_out) * Decimal("100")

    @property
    def annualized_return_pct(self) -> Optional[Decimal]:
        """
        Linear annualization:
            pnl_decimal * (365 / avg_holding_days) * 100
        """
        if (
            self.total_cost_basis_out == 0
            or self.avg_holding_days is None
            or self.avg_holding_days <= 0
        ):
            return None

        pnl_decimal = self.realized_pnl / self.total_cost_basis_out
        return pnl_decimal * (Decimal("365") / Decimal(str(self.avg_holding_days))) * Decimal("100")
    
    # ------------------------------------------------------------------
    # Representation methods
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"RealizedPnLIndicator("
            f"realization_count={self.realization_count}, "
            f"total_cost_basis_out={self.total_cost_basis_out}, "
            f"total_out_value={self.total_out_value}, "
            f"realized_pnl={self.realized_pnl}, "
            f"avg_holding_days={self.avg_holding_days}, "
            f"return_pct={self.return_pct}, "
            f"annualized_return_pct={self.annualized_return_pct}"
            f")"
        )

    def __str__(self) -> str:
        def fmt_money(x: Decimal) -> str:
            return f"{x:,.2f}"

        def fmt_pct(x: Optional[Decimal]) -> str:
            return f"{x:.2f}%" if x is not None else "N/A"

        def fmt_days(x: Optional[float]) -> str:
            return f"{x:.1f} days" if x is not None else "N/A"

        return (
            "Realized PnL Summary\n"
            "---------------------\n"
            f"Annualized return:   {fmt_pct(self.annualized_return_pct)}\n"
            f"Return:              {fmt_pct(self.return_pct)}\n"
            f"Realized PnL:        {fmt_money(self.realized_pnl)}\n"
            f"Total cost basis:    {fmt_money(self.total_cost_basis_out)}\n"
            f"Total exit value:    {fmt_money(self.total_out_value)}\n"
            f"Avg holding period:  {fmt_days(self.avg_holding_days)}\n"
            f"Realizations:        {self.realization_count}"
        )