from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional, Tuple, Iterable, Iterator, Any, Dict, List

from core.income.model import WeeklyDistributionSeries

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
    
    def to_dict(self, places: int = 4) -> Dict[str, Any]:
        q = Decimal("1").scaleb(-places)
    
        def r(x: Optional[Decimal]) -> Optional[float]:
            return float(x.quantize(q)) if x is not None else None
    
        return {
            "annualized_return_pct": r(self.annualized_return_pct),
            "return_pct": r(self.return_pct),
            "realized_pnl": r(self.realized_pnl),
            "total_cost_basis_out": r(self.total_cost_basis_out),
            "total_out_value": r(self.total_out_value),
            "avg_holding_days": round(self.avg_holding_days, 2) if self.avg_holding_days is not None else None,
            "realization_count": self.realization_count,
        }

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
    

class UnrealizedPnLIndicator:
    """
    Final unrealized performance indicator for an open position (for a specific token).

    This object represents an estimated business result:
    - it is created from the current on-chain quantity and a current spot price
    - the cost basis is estimated from a WAC (average acquisition cost per token)
    - all aggregate figures are computed once at construction time
    - only the return and the annualized return are exposed as derived metrics
    """

    def __init__(
        self,
        current_unit_price: Decimal,
        current_quantity: Decimal,
        avg_cost_per_token: Decimal,
        avg_holding_days: Optional[float],
    ) -> None:
        self.current_unit_price: Decimal = current_unit_price
        self.current_quantity: Decimal = current_quantity
        self.avg_cost_per_token: Decimal = avg_cost_per_token

        # Aggregated values (materialized once)
        self.current_value: Decimal = current_unit_price * current_quantity
        self.cost_basis: Decimal = avg_cost_per_token * current_quantity
        self.unrealized_pnl: Decimal = self.current_value - self.cost_basis

        # Average Holding time
        self.avg_holding_days: Optional[float] = (
            avg_holding_days if (avg_holding_days is not None and avg_holding_days > 0) else None
        )

    @classmethod
    def aggregate(cls, indicators: Iterable["UnrealizedPnLIndicator"]) -> "UnrealizedPnLIndicator":
        """
        Aggregate multiple per-token UnrealizedPnLIndicator objects into a single
        portfolio-level UnrealizedPnLIndicator.

        Rules:
        - Sum monetary amounts (cost basis, current value, PnL).
        - Compute portfolio return from totals (do NOT average percentages).
        - Compute avg holding days as a cost-basis weighted average.
        """
        indicators = tuple(indicators)

        total_current_value = Decimal("0")
        total_cost_basis = Decimal("0")
        total_quantity = Decimal("0")

        # Weighted avg holding days (weight = cost_basis)
        holding_weight_sum = Decimal("0")
        holding_weighted_days_sum = Decimal("0")

        for ind in indicators:
            total_current_value += ind.current_value
            total_cost_basis += ind.cost_basis
            total_quantity += ind.current_quantity

            if ind.avg_holding_days is not None and ind.avg_holding_days > 0 and ind.cost_basis > 0:
                w = ind.cost_basis
                holding_weight_sum += w
                holding_weighted_days_sum += w * Decimal(str(ind.avg_holding_days))

        avg_holding_days: Optional[float] = None
        if holding_weight_sum > 0:
            avg_holding_days = float(holding_weighted_days_sum / holding_weight_sum)

        # "Portfolio avg cost per token" is only used to satisfy the constructor contract.
        # It does not have a strong meaning across different tokens.
        avg_cost_per_token = (total_cost_basis / total_quantity) if total_quantity > 0 else Decimal("0")

        # "Portfolio spot price" is also a placeholder to satisfy the constructor contract.
        # It is chosen so that current_unit_price * quantity == total_current_value.
        current_unit_price = (total_current_value / total_quantity) if total_quantity > 0 else Decimal("0")

        return cls(
            current_unit_price=current_unit_price,
            current_quantity=total_quantity,
            avg_cost_per_token=avg_cost_per_token,
            avg_holding_days=avg_holding_days,
        )


    # ------------------------------------------------------------------
    # Derived business metrics, computed from the aggregated values above
    # ------------------------------------------------------------------

    @property
    def return_pct(self) -> Optional[Decimal]:
        """
        Unrealized return (%) = unrealized_pnl / cost_basis * 100
        """
        if self.cost_basis == 0:
            return None
        return (self.unrealized_pnl / self.cost_basis) * Decimal("100")

    @property
    def annualized_return_pct(self) -> Optional[Decimal]:
        """
        Linear annualization:
            pnl_decimal * (365 / avg_holding_days) * 100
        """
        if self.cost_basis == 0 or self.avg_holding_days is None or self.avg_holding_days <= 0:
            return None

        pnl_decimal = self.unrealized_pnl / self.cost_basis
        return pnl_decimal * (Decimal("365") / Decimal(str(self.avg_holding_days))) * Decimal("100")

    # ------------------------------------------------------------------
    # Representation methods
    # ------------------------------------------------------------------

    def to_dict(self, places: int = 4) -> Dict[str, Any]:
        q = Decimal("1").scaleb(-places)
    
        def r(x: Optional[Decimal]) -> Optional[float]:
            return float(x.quantize(q)) if x is not None else None
    
        return {
            "annualized_return_pct": r(self.annualized_return_pct),
            "return_pct": r(self.return_pct),
            "unrealized_pnl": r(self.unrealized_pnl),
            "cost_basis": r(self.cost_basis),
            "current_value": r(self.current_value),
            "avg_holding_days": round(self.avg_holding_days, 2) if self.avg_holding_days is not None else None,
            "current_quantity": r(self.current_quantity),
            "current_unit_price": r(self.current_unit_price),
            "avg_cost_per_token": r(self.avg_cost_per_token),
        }

    def __repr__(self) -> str:
        return (
            f"UnrealizedPnLIndicator("
            f"current_unit_price={self.current_unit_price}, "
            f"current_quantity={self.current_quantity}, "
            f"avg_cost_per_token={self.avg_cost_per_token}, "
            f"current_value={self.current_value}, "
            f"cost_basis={self.cost_basis}, "
            f"unrealized_pnl={self.unrealized_pnl}, "
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
            "Unrealized PnL Summary\n"
            "-----------------------\n"
            f"Annualized return:   {fmt_pct(self.annualized_return_pct)}\n"
            f"Return:              {fmt_pct(self.return_pct)}\n"
            f"Unrealized PnL:      {fmt_money(self.unrealized_pnl)}\n"
            f"Cost basis:          {fmt_money(self.cost_basis)}\n"
            f"Current value:       {fmt_money(self.current_value)}\n"
            f"Avg holding period:  {fmt_days(self.avg_holding_days)}\n"
            f"Current quantity:    {self.current_quantity:.2f}\n"
            f"Current unit price:  {fmt_money(self.current_unit_price)}\n"
            f"Avg cost / token:    {fmt_money(self.avg_cost_per_token)}"
        )
    

class DistributedIncomeIndicator:
    """
    Distributed income indicator built from a WeeklyDistributionSeries.

    Scope:
    - token is None  -> aggregated indicator across all tokens
    - token is set   -> indicator for one specific token only

    Aggregate figures are computed once at construction time.
    annualized_return is kept as None for now.
    """

    def __init__(
        self,
        distribution_series: WeeklyDistributionSeries,
        token: Optional[str] = None,
    ) -> None:
        self.token: Optional[str] = token.strip().lower() if token is not None else None

        if self.token is None:
            total = distribution_series.total_revenue
        else:
            total = distribution_series.total_revenue_for_token(self.token)

        self.total_revenues_distributed: Decimal = Decimal(str(total))
        self.annualized_return: Optional[Decimal] = None # to do

    @property
    def is_global(self) -> bool:
        return self.token is None

    def to_dict(self, places: int = 4) -> Dict[str, Any]:
        q = Decimal("1").scaleb(-places)

        def r(x: Optional[Decimal]) -> Optional[float]:
            return float(x.quantize(q)) if x is not None else None

        return {
            "total_revenues_distributed": r(self.total_revenues_distributed),
            "annualized_return_pct": r(self.annualized_return),
        }

    def __repr__(self) -> str:
        return (
            f"DistributedIncomeIndicator("
            f"token={self.token!r}, "
            f"total_revenues_distributed={self.total_revenues_distributed}, "
            f"annualized_return={self.annualized_return}"
            f")"
        )

    def __str__(self) -> str:
        def fmt_money(x: Decimal) -> str:
            return f"{x:,.2f}"

        def fmt_pct(x: Optional[Decimal]) -> str:
            return f"{x:.2f}%" if x is not None else "N/A"

        scope_label = "All tokens" if self.is_global else f"Token {self.token}"

        return (
            "--------------------------\n"
            "Distributed Income Summary\n"
            "--------------------------\n"
            f"Scope:                      {scope_label}\n"
            f"Total revenues distributed: {fmt_money(self.total_revenues_distributed)}\n"
            f"Annualized return:          {fmt_pct(self.annualized_return)}"
        )
    

class OverallPerformanceIndicator:
    """
    Final overall performance indicator built from realized gain, unrealized gain,
    distributed income, and total invested cost basis.

    This object represents a finalized aggregate result:
    - it combines realized gain, unrealized gain, and distributed income
    - all aggregate figures are computed once at construction time
    - ROI is exposed as a derived metric
    - IRR is passed in as-is
    """

    def __init__(
        self,
        realized_gain: Decimal,
        unrealized_gain: Decimal,
        income_distributed: Decimal,
        total_cost_basis_realized: Decimal,
        total_cost_basis_unrealized: Decimal,
        irr: Decimal,
    ) -> None:
        self.realized_gain: Decimal = realized_gain
        self.unrealized_gain: Decimal = unrealized_gain
        self.income_distributed: Decimal = income_distributed

        self.total_cost_basis_realized: Decimal = total_cost_basis_realized
        self.total_cost_basis_unrealized: Decimal = total_cost_basis_unrealized

        self.total_return: Decimal = (
            self.realized_gain
            + self.unrealized_gain
            + self.income_distributed
        )

        self.irr: Decimal = irr

    @property
    def total_cost_basis(self) -> Decimal:
        return self.total_cost_basis_realized + self.total_cost_basis_unrealized

    @property
    def roi(self) -> Optional[Decimal]:
        """
        Overall ROI (%) = total_return / total_cost_basis * 100
        """
        if self.total_cost_basis == 0:
            return None
        return (self.total_return / self.total_cost_basis) * Decimal("100")

    def to_dict(self, places: int = 4) -> Dict[str, Any]:
        q = Decimal("1").scaleb(-places)

        def r(x: Optional[Decimal]) -> Optional[float]:
            return float(x.quantize(q)) if x is not None else None
        
        def r_pct(x: Optional[Decimal]) -> Optional[float]:
            return float((x * Decimal("100")).quantize(q)) if x is not None else None

        return {
            "realized_gain": r(self.realized_gain),
            "unrealized_gain": r(self.unrealized_gain),
            "income_distributed": r(self.income_distributed),
            "total_return": r(self.total_return),
            "total_cost_basis": r(self.total_cost_basis),
            "roi_pct": r(self.roi),
            "irr_pct": r_pct(self.irr),
        }

    def __repr__(self) -> str:
        return (
            f"OverallPerformanceIndicator("
            f"realized_gain={self.realized_gain}, "
            f"unrealized_gain={self.unrealized_gain}, "
            f"income_distributed={self.income_distributed}, "
            f"total_return={self.total_return}, "
            f"total_cost_basis_realized={self.total_cost_basis_realized}, "
            f"total_cost_basis_unrealized={self.total_cost_basis_unrealized}, "
            f"total_cost_basis={self.total_cost_basis}, "
            f"roi={self.roi}, "
            f"irr={self.irr}"
            f")"
        )

    def __str__(self) -> str:
        def fmt_money(x: Decimal) -> str:
            return f"{x:,.2f}"

        def fmt_pct(x: Optional[Decimal]) -> str:
            return f"{x:.2f}%" if x is not None else "N/A"

        return (
            "---------------------------\n"
            "Overall Performance Summary\n"
            "---------------------------\n"
            f"Realized gain:              {fmt_money(self.realized_gain)}\n"
            f"Unrealized gain:            {fmt_money(self.unrealized_gain)}\n"
            f"Income distributed:         {fmt_money(self.income_distributed)}\n"
            f"Total return:               {fmt_money(self.total_return)}\n"
            f"Realized cost basis:        {fmt_money(self.total_cost_basis_realized)}\n"
            f"Unrealized cost basis:      {fmt_money(self.total_cost_basis_unrealized)}\n"
            f"Total cost basis:           {fmt_money(self.total_cost_basis)}\n"
            f"ROI:                        {fmt_pct(self.roi)}\n"
            f"IRR:                        {fmt_pct(self.irr)}"
        )
    

@dataclass(frozen=True, slots=True)
class IRRCashFlow:
    """
    Single normalized cash flow used as input for IRR calculation.

    Conventions:
    - negative amount = cash outflow (investment, purchase, fees paid)
    - positive amount = cash inflow (sale proceeds, income received, terminal value)

    This object is intentionally generic and financial-only: it does not try to describe token movement semantics,
    only the dated cash impact relevant for IRR computation.
    """

    timestamp: datetime
    amount: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.timestamp, datetime):
            raise TypeError("timestamp must be a datetime instance")

        if self.timestamp.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")

        if not isinstance(self.amount, Decimal):
            raise TypeError("amount must be a Decimal")

        if self.amount == Decimal("0"):
            raise ValueError("amount must be non-zero")

    @property
    def is_inflow(self) -> bool:
        """Return True when the cash flow is positive."""
        return self.amount > 0

    @property
    def is_outflow(self) -> bool:
        """Return True when the cash flow is negative."""
        return self.amount < 0

    def to_dict(self) -> dict[str, str | None]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "amount": str(self.amount),
        }

    def __repr__(self) -> str:
        direction = "inflow" if self.amount > 0 else "outflow"
        return (
            f"IRRCashFlow(timestamp={self.timestamp.isoformat()}, "
            f"amount={self.amount}, direction='{direction}', "
        )


@dataclass(slots=True)
class IRRCashFlowSeries:
    """
    Collection of IRRCashFlow objects used to compute the annualized IRR
    from irregularly dated cash flows.
    """
    _items: List[IRRCashFlow] = field(init=False, default_factory=list)
    irr: Optional[Decimal] = field(init=False, default=None)

    def __init__(self, cash_flows: Optional[Iterable[IRRCashFlow]] = None) -> None:
        self._items = sorted(list(cash_flows or []), key=lambda cf: cf.timestamp)
        self.irr = self._compute_irr()

    @property
    def cash_flows(self) -> List[IRRCashFlow]:
        """
        Return cash flows sorted chronologically.
        """
        return list(self._items)

    def xnpv(self, rate: float) -> float:
        """
        Compute the NPV of irregularly dated cash flows for a given annual rate.
        """
        t0 = self._items[0].timestamp

        total = 0.0
        for cf in self._items:
            days = (cf.timestamp - t0).total_seconds() / 86400.0
            years = days / 365.0
            total += float(cf.amount) / ((1.0 + rate) ** years)

        return total

    def _compute_irr(
        self,
        *,
        guess: float = 0.10,
        tolerance: float = 1e-10,
        max_iterations: int = 100,
    ) -> Optional[Decimal]:
        """
        Compute the annualized IRR of the series.

        Returns None if no numerical solution is found.
        """
        # First attempt: Newton-Raphson
        try:
            rate = guess

            for _ in range(max_iterations):
                f = self.xnpv(rate)

                h = 1e-6
                f_plus = self.xnpv(rate + h)
                f_minus = self.xnpv(rate - h)
                derivative = (f_plus - f_minus) / (2.0 * h)

                if abs(derivative) < 1e-18:
                    break

                new_rate = rate - (f / derivative)

                if abs(new_rate - rate) < tolerance:
                    return Decimal(str(new_rate))

                rate = new_rate
        except Exception:
            pass

        # Fallback: bracket search + bisection
        brackets = [
            (-0.9999, -0.5),
            (-0.5, -0.1),
            (-0.1, 0.0),
            (0.0, 0.1),
            (0.1, 0.25),
            (0.25, 0.5),
            (0.5, 1.0),
            (1.0, 2.0),
            (2.0, 5.0),
            (5.0, 10.0),
        ]

        try:
            for low, high in brackets:
                f_low = self.xnpv(low)
                f_high = self.xnpv(high)

                if f_low * f_high > 0:
                    continue

                for _ in range(max_iterations):
                    mid = (low + high) / 2.0
                    f_mid = self.xnpv(mid)

                    if abs(f_mid) < tolerance or abs(high - low) < tolerance:
                        return Decimal(str(mid))

                    if f_low * f_mid < 0:
                        high = mid
                    else:
                        low = mid
                        f_low = f_mid
        except Exception:
            pass

        return None
    
    def to_dict(self, places: int = 4) -> Dict[str, Any]:
        q = Decimal("1").scaleb(-places)
    
        def r(x: Optional[Decimal]) -> Optional[float]:
            return float(x.quantize(q)) if x is not None else None
    
        return {
            "irr": r(self.irr),
        }

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[IRRCashFlow]:
        return iter(self._items)

    def __repr__(self) -> str:
        return f"IRRCashFlowSeries(count={len(self._items)}, irr={self.irr})"