from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Dict, Optional, Tuple
from eth_utils import to_checksum_address

from core.realtoken_event_history.model import RealtokenEventHistory, RealtokenEventType
from core.balance_snapshots.model import BalanceSnapshotSeries
from core.performance.model import Realization, RealizedPnLIndicator, UnrealizedPnLIndicator
from core.services.utilities import get_token_issuance_timestamp, get_token_price_at_timestamp
from job.utilities import load_json


class PerformanceCalculator:
    """
    Step 1:
    - Initialized with a RealtokenEventHistory
    - For a given token, converts its events into a list of Realization
      using the Weighted Average Cost (WAC) method
    """

    def __init__(self, history: RealtokenEventHistory, balance_snapshots_series: BalanceSnapshotSeries) -> None:
        
        self.realtoken_history = load_json("data_tmp/realtokens_history.json")
        
        self._history = history
        self._balance_snapshots_series = balance_snapshots_series

        self._realizations_by_token: Dict[str, List[Realization]] = {}

        # Realized performance
        self.realized_pnl_by_token: Dict[str, RealizedPnLIndicator] = {}
        self.global_realized_pnl: Optional[RealizedPnLIndicator] = None

        # Unrealized performance
        self.unrealized_pnl_by_token: Dict[str, UnrealizedPnLIndicator] = {}
        self.global_unrealized_pnl: Optional[UnrealizedPnLIndicator] = None

        self._build_performance()

    def _build_performance(self) -> Dict[str, List[Realization]]:
        """
        Build realizations for every token present in the history and store them on the instance.

        Returns the stored dict for convenience:
            { token_address: [Realization, ...], ... }
        """
        realizations_by_token: Dict[str, List[Realization]] = {}
        realized_pnl_by_token: Dict[str, RealizedPnLIndicator] = {}
        unrealized_pnl_by_token: Dict[str, UnrealizedPnLIndicator] = {}
        

        for token_address in self._history.tokens():
            token_address = to_checksum_address(token_address)

            # Build realizations, WAC price and average holding days for every token present in the history
            realizations_by_token[token_address], final_weighted_avg_cost, final_avg_holding_days = self._build_realizations_and_open_wac_state_for_token(token_address)

            # Build a Realized PnL for every token present in the history
            realized_pnl_by_token[token_address] = RealizedPnLIndicator(realizations_by_token[token_address])

            # Build a Unrealized PnL for every token present in the history
            current_price = Decimal(get_token_price_at_timestamp(self.realtoken_history, token_address, datetime.now(timezone.utc)))
            current_quantity = self._balance_snapshots_series.latest().balances_by_token.get(token_address.lower(), Decimal("0"))
            unrealized_pnl_by_token[token_address] = UnrealizedPnLIndicator(current_price, current_quantity, final_weighted_avg_cost, final_avg_holding_days)


        self._realizations_by_token = realizations_by_token
        self.realized_pnl_by_token = realized_pnl_by_token
        self.unrealized_pnl_by_token = unrealized_pnl_by_token

        # Build the global Realized PnL (we take all the realizations of every tokens)
        all_realizations = []
        for realizations in realizations_by_token.values():
            for realization in realizations:
                all_realizations.append(realization)
        self.global_realized_pnl = RealizedPnLIndicator(all_realizations)

        # Build the global Unrealized PnL (we take the unrealized indicator of every tokens)
        token_indicators = unrealized_pnl_by_token.values()
        self.global_unrealized_pnl = UnrealizedPnLIndicator.aggregate(token_indicators)


    def _build_realizations_and_open_wac_state_for_token(self, token_address: str) -> Tuple[List["Realization"], Decimal, float]:
        """
        Convert all events for a token into Realization objects using WAC, and also return
        final open-position metrics (WAC and avg holding days) for unrealized computations.
        
        Finance idea :
        - We build a "pool" of acquisitions (IN events). Each IN adds quantity and cost to the pool.
        - For each OUT (sell, detokenisation, being liquidated, ...), we:
            1) compute the WAC at that exact moment:
                   WAC = total_cost_in_pool / total_qty_in_pool
            2) compute cost basis removed by this OUT:
                   cost_basis_out = amount_out * WAC
            3) compute avg holding days of the pool at that moment (quantity-weighted holding time)
            4) reduce the pool *proportionally* (WAC equivalent):
                   each acquisition lot is reduced by the same fraction of the pool sold
    
        
        Transfers are ignored here for WAC because they do not represent a buy/sell price event
        
        Position consolidation:
        - Some datasets can contain OUT events that exceed the tracked IN events (missing acquisitions).
        - Instead of failing, we reconcile the position by injecting a synthetic acquisition lot *only inside*
          this computation (it is NOT added to the real event history).
        
        Returns:
        - realizations: List[Realization] built from each OUT event.
        - final_weighted_avg_cost: WAC of the remaining open pool after processing all events.
          This represents the average acquisition cost per token for the current (remaining) position.
          Returns 0 if the remaining quantity is 0.
        - final_avg_holding_days: Quantity-weighted average holding time (in days) of the remaining open pool,
          measured as of "now" (UTC). Returns 0 if the remaining quantity is 0.
        
        Notes:
        - The final metrics describe the open position (what remains after all realized OUT events).
          They are intended to be used to compute unrealized PnL from an external on-chain balance and spot price.
        """
        token_address = to_checksum_address(token_address)

        # ---- 1) Load & sort events chronologically ----
        events = self._history.events_for(token_address)

        # Sort by timestamp, then log_index to make ordering deterministic when multiple logs exist in the same transaction / same block time.
        events.sort(key=lambda e: (e.timestamp, e.log_index))

        # ---- 2) Define which event types are IN and which are OUT ----
        # IN events are acquisitions that increase the position and define the WAC.
        in_types = {
            RealtokenEventType.BUY_FROM_REALT,
            RealtokenEventType.BUY_YAM_V1,
            RealtokenEventType.LIQUIDATION,
            RealtokenEventType.BUY_SWAPCAT,
        }

        # OUT events are disposals that realize PnL (sell/detokenisation...).
        out_types = {
            RealtokenEventType.SELL_YAM_V1,
            RealtokenEventType.SELL_SWAPCAT,
            RealtokenEventType.LIQUIDATED,
            RealtokenEventType.DETOKENISATION,
        }

        # ---- 3) Position state: explicit acquisition lots (easy to read & audit) ----
        # Each lot represents a buy that still has some remaining quantity in the pool.
        #
        # Lot fields:
        # - qty_remaining: Decimal
        # - timestamp: datetime (UTC)
        # - price_per_token: Decimal
        # - event_id: (tx_hash, log_index) for traceability (or a synthetic id for consolidation lots)
        acquisition_lots: List[Dict] = []

        realizations: List[Realization] = []

        # Small epsilon to clean up tiny Decimal dust created by proportional reductions.
        # This prevents ending with 1E-27 quantities after repeated prorata reductions.
        EPS = Decimal("0.000000000000000001")

        # ---- helper functions (kept simple & explicit) ----
        def _pool_total_qty() -> Decimal:
            total = Decimal("0")
            for lot in acquisition_lots:
                total += lot["qty_remaining"]
            return total

        def _pool_total_cost() -> Decimal:
            total = Decimal("0")
            for lot in acquisition_lots:
                # cost of each remaining lot = qty_remaining * buy_price_per_token
                total += lot["qty_remaining"] * lot["price_per_token"]
            return total

        def _acquisitions_used_count() -> int:
            count = 0
            for lot in acquisition_lots:
                if lot["qty_remaining"] > EPS:
                    count += 1
            return count

        def _avg_holding_days(as_of: datetime) -> float:
            """
            Quantity-weighted average age of the current pool at `as_of`.

            Example (EN):
            - Buy 10 tokens on Jan 1
            - Buy 30 tokens on Jan 11
            - On Jan 21, pool = 40 tokens
              avg_age_days = (10*20days + 30*10days) / 40 = (200 + 300)/40 = 12.5 days
            """
            total_qty = _pool_total_qty()
            if total_qty <= 0:
                return 0.0

            weighted_age_days = Decimal("0")

            for lot in acquisition_lots:
                qty = lot["qty_remaining"]
                if qty <= EPS:
                    continue

                age_seconds = (as_of - lot["timestamp"]).total_seconds()
                age_days = Decimal(str(age_seconds)) / Decimal("86400")

                weighted_age_days += qty * age_days

            avg_days = weighted_age_days / total_qty
            return float(avg_days)

        def _reduce_pool_proportionally(amount_out: Decimal) -> None:
            """
            Reduce each lot by the same fraction of the pool sold.

            This is the most intuitive way to represent WAC as a pool:
            - Under WAC, you do not say "this sell consumed buy #1 then buy #2".
            - Instead, the sold quantity is assumed to be an average slice of the pool.

            Example (EN):
            - Pool contains:
            * Lot A: 10 tokens
            * Lot B: 30 tokens
              Total = 40
            - You sell 8 tokens -> fraction sold = 8/40 = 20%
            - Reduce each lot by 20%:
            * Lot A becomes 8
            * Lot B becomes 24
              New total = 32
            """
            total_qty_before = _pool_total_qty()
            if total_qty_before <= 0:
                return

            fraction_sold = amount_out / total_qty_before

            for lot in acquisition_lots:
                qty_before = lot["qty_remaining"]
                if qty_before <= EPS:
                    continue

                qty_reduction = qty_before * fraction_sold
                qty_after = qty_before - qty_reduction

                # Clean tiny negative due to Decimal rounding edge cases.
                if qty_after < 0 and abs(qty_after) <= EPS:
                    qty_after = Decimal("0")

                lot["qty_remaining"] = qty_after

            # Optionally, we can drop empty lots to keep the list clean.
            # This is business-friendly: it means "this buy no longer contributes to the position".
            kept: List[Dict] = []
            for lot in acquisition_lots:
                if lot["qty_remaining"] > EPS:
                    kept.append(lot)
            acquisition_lots.clear()
            acquisition_lots.extend(kept)


        def _compute_missing_qty_for_consolidation(token_address: str) -> Decimal:
            """
            Return the minimal synthetic IN quantity to:
              1) prevent running balance from going below 0 at any point
              2) ensure final tracked quantity is not below current balance
            """
            running = Decimal("0")
            min_running = Decimal("0")
        
            for ev in events:  # events sorted chronologically
                if ev.event_type == RealtokenEventType.TRANSFER:
                    continue
        
                amt = Decimal(ev.amount)
        
                if ev.event_type in in_types:
                    running += amt
                elif ev.event_type in out_types:
                    running -= amt
        
                if running < min_running:
                    min_running = running
        
            floor_qty = -min_running if min_running < 0 else Decimal("0")
        
            current_balance = self._balance_snapshots_series.latest().balances_by_token.get(
                token_address.lower(),
                Decimal("0"),
            )
        
            tracked_final = running
            final_gap = current_balance - tracked_final
            if final_gap < 0:
                final_gap = Decimal("0")
        
            return max(floor_qty, final_gap)


        # ---- 4) Consolidate the position once, up-front (no mutation of the real event history) ----
        missing_qty = _compute_missing_qty_for_consolidation(token_address)

        if missing_qty > EPS:
            # This synthetic lot exists only in this calculation to reconcile the pool. It is intentionally traceable (synthetic event_id) but has no on-chain counterpart.
            insuance_timestamp = get_token_issuance_timestamp(self.realtoken_history, token_address)
            token_price_at_insuance_timestamp = get_token_price_at_timestamp(self.realtoken_history, token_address, insuance_timestamp)
            ts = insuance_timestamp.replace(tzinfo=timezone.utc) if insuance_timestamp.tzinfo is None else insuance_timestamp.timestamp.astimezone(timezone.utc)
            consolidation_lot = {
                "event_id": ("__consolidation__", 0),
                "timestamp": ts,
                "qty_remaining": missing_qty,
                "price_per_token": Decimal(token_price_at_insuance_timestamp),
            }
            acquisition_lots.append(consolidation_lot)

        # ---- 5) Main loop: build pool from IN, create Realization for each OUT ----
        for ev in events:

            # Ignore transfers for WAC realization: no price, no realized event.
            if ev.event_type == RealtokenEventType.TRANSFER:
                continue

            # ---------------------------
            # IN movement (BUY)
            # ---------------------------
            if ev.event_type in in_types:
                # Each buy creates a lot that contributes to the WAC pool.
                # Example:
                # - Buy 5 tokens @ 100 => lot cost = 500
                # - Buy 2 tokens @ 110 => lot cost = 220
                # Pool totals:
                #   total_qty = 7
                #   total_cost = 720
                #   WAC = 720/7 = 102.857...

                # Normalize timestamp to UTC (ensure timezone-aware for deterministic age calculations)
                ts = ev.timestamp.replace(tzinfo=timezone.utc) if ev.timestamp.tzinfo is None else ev.timestamp.astimezone(timezone.utc)

                lot = {
                    "event_id": (ev.transaction_hash, ev.log_index),
                    "timestamp": ts,
                    "qty_remaining": Decimal(ev.amount),
                    "price_per_token": Decimal(ev.price_per_token),  # required for buy types
                }
                acquisition_lots.append(lot)
                continue

            # ---------------------------
            # OUT movement (SELL / LIQUIDATION)
            # ---------------------------
            if ev.event_type in out_types:
                amount_out = Decimal(ev.amount)

                # 1) Compute WAC at the time of the OUT
                total_qty = _pool_total_qty()
                total_cost = _pool_total_cost()

                # If total_qty is zero here, something is structurally wrong in the dataset. With consolidation enabled, this should normally not happen.
                if total_qty <= 0:
                    # We keep a clear error because WAC is undefined without holdings.
                    raise ValueError(
                        f"OUT event but WAC pool is empty for token={token_address}. "
                        f"event={(ev.transaction_hash, ev.log_index)} amount_out={amount_out}"
                    )

                weighted_avg_cost_used = total_cost / total_qty

                # 2) Compute OUT values from the event
                out_price_per_token = Decimal(ev.price_per_token)  # required for non-transfer events
                out_total_price = amount_out * out_price_per_token

                # 3) Compute cost basis removed by this OUT using WAC
                cost_basis_out = amount_out * weighted_avg_cost_used

                # 4) Compute average holding time of the pool at the time of the OUT
                avg_days = _avg_holding_days(ev.timestamp)

                # 5) Count how many acquisition lots are currently contributing to the pool
                acquisitions_count = _acquisitions_used_count()

                # 6) Create the Realization for this OUT
                realizations.append(
                    Realization(
                        event_id=(ev.transaction_hash, ev.log_index),
                        timestamp=ev.timestamp,
                        amount_out=amount_out,
                        out_price_per_token=out_price_per_token,
                        out_total_price=out_total_price,
                        weighted_avg_cost_used=weighted_avg_cost_used,
                        cost_basis_out=cost_basis_out,
                        acquisitions_used_count=acquisitions_count,
                        avg_holding_days=avg_days,
                    )
                )

                # 7) Reduce the pool after selling (WAC pool logic)
                _reduce_pool_proportionally(amount_out)

                continue

            # If the event type is neither IN nor OUT nor TRANSFER: we ignore it.
            continue

        # ---- 6) Compute final WAC and average holding days for the open position ----
        final_total_qty = _pool_total_qty()
        final_total_cost = _pool_total_cost()

        if final_total_qty > 0:
            final_weighted_avg_cost = final_total_cost / final_total_qty
            final_avg_holding_days = _avg_holding_days(datetime.now(timezone.utc))
        else:
            final_weighted_avg_cost = Decimal("0")
            final_avg_holding_days = 0.0
        
        return realizations, final_weighted_avg_cost, final_avg_holding_days

