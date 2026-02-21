from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, Sequence, Tuple

from config.settings import LIQUIDATION_BONUS
from core.realtoken_event_history.event_fetchers import get_liquidatied_realtoken_rmmV3_by_tx
from core.realtoken_event_history.model import RealtokenEvent, RealtokenEventType
from core.services.utilities import get_token_price_at_timestamp

logger = logging.getLogger(__name__)

REALTOKEN_DECIMALS = 18


def normalize_liquidations_rmm_v3(
    liquidations: Iterable[Dict[str, Any]] | Dict[str, Any],
    user_wallets: Sequence[str],
    realtoken_history_data: Dict[str, Dict[str, Any]],
) -> Tuple[RealtokenEvent, ...]:
    """
    Normalize RMM v3 LiquidationCall objects into *multiple* atomic RealtokenEvent entries
    (one for each RealToken liquidated) and return them as an immutable tuple.

    Notes:
    - Some tx may be incomplete in subgraph data (reserves != amounts). Those tx hashes are collected
      and resolved via get_liquidatied_realtoken_rmmV3_by_tx(), then normalized too.
    - Synthetic log_index is derived from the asset order (mov_idx).
        
    Event type rules:
      - If `liquidator.id` is in `user_wallets` => event_type = LIQUIDATION
      - If `user.id`       is in `user_wallets` => event_type = LIQUIDATED
      - If BOTH match => raise ValueError (this should not happen)

    source/destination convention:
      - source      = liquidated user (user.id)
      - destination = liquidator (liquidator.id)

    Uniqueness / log_index notes:
      - A single on-chain tx can contain:
          (a) multiple LiquidationCall "groups" (multiple liquidation objects),
          (b) and each group contains multiple movements (N assets).
      - The log emmited does not work with a per-movement realtokens logIndex, but rather with a single logIndex for all the realtokens liquidated within the transaction. To avoid collisions, we
        generate a deterministic synthetic `log_index` for each movement using based on the asset order
        It is not a real logIndex, but an artificial one 
      - This guarantees stable, collision-free indices while preserving ordering.
    """
    wallets_lc = {w.lower() for w in user_wallets if w}

    # Accept both:
    # - an iterable of liquidation dicts
    # - or the raw fetch payload dict (e.g. {"items": [...]})
    if isinstance(liquidations, dict):
        liquidations_iter: Iterable[Dict[str, Any]] = liquidations.get("items", []) or []
    else:
        liquidations_iter = liquidations

    # Group by txHash so we can assign stable ordering per tx
    by_tx: dict[str, list[Dict[str, Any]]] = {}
    for liq in liquidations_iter:
        if not isinstance(liq, dict):
            raise TypeError(f"Each liquidation must be a dict, got: {type(liq).__name__}")

        tx = (liq.get("txHash") or "").lower()
        if not tx:
            continue
        by_tx.setdefault(tx, []).append(liq)

    normalized_events: list[RealtokenEvent] = []
    tx_incomplete_data: list[str] = []

    for tx_hash, liqs in by_tx.items():
        # Stable ordering inside a tx
        liqs.sort(key=lambda x: (int(x.get("timestamp") or 0), str(x.get("id") or "")))

        for liq in liqs:
            ts_raw = liq.get("timestamp")
            if ts_raw is None:
                continue
            ts = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc)

            user_id = (((liq.get("user") or {}).get("id")) or "").lower()
            liquidator_id = (((liq.get("liquidator") or {}).get("id")) or "").lower()

            is_liquidator_user = liquidator_id in wallets_lc
            is_liquidated_user = user_id in wallets_lc

            if is_liquidator_user and is_liquidated_user:
                raise ValueError(
                    "Unexpected liquidation where BOTH user and liquidator are in user_wallets. "
                    f"tx={tx_hash} user={user_id} liquidator={liquidator_id}"
                )

            if is_liquidator_user:
                event_type = RealtokenEventType.LIQUIDATION
            elif is_liquidated_user:
                event_type = RealtokenEventType.LIQUIDATED
            else:
                continue

            reserves = liq.get("reserves") or []
            amounts = liq.get("amounts") or []

            if len(reserves) != len(amounts):
                logger.warning(
                    "Liquidation data inconsistency detected: reserves(%s) != amounts(%s) tx=%s id=%s. "
                    "Subgraph data appears incomplete; using fallback retrieval method",
                    len(reserves),
                    len(amounts),
                    tx_hash,
                    liq.get("id"),
                )
                tx_incomplete_data.append(tx_hash)
                continue

            for mov_idx, (reserve_obj, amount_str) in enumerate(zip(reserves, amounts)):
                token_addr = ((reserve_obj or {}).get("id") or "").lower()
                if not token_addr:
                    continue

                # BigInt-as-string -> Decimal in human units
                raw_amount = Decimal(amount_str)
                amount = raw_amount / (Decimal(10) ** REALTOKEN_DECIMALS)
                if amount <= 0:
                    continue

                # price (apply liquidation bonus)
                price = get_token_price_at_timestamp(
                    realtoken_history_data,
                    token_addr.lower(),
                    ts,
                )
                if price is None:
                    # depending on your function signature; keep safe
                    continue
                price_per_token = Decimal(price) / Decimal(LIQUIDATION_BONUS)

                normalized_events.append(
                    RealtokenEvent(
                        token_address=token_addr,
                        amount=amount,
                        source=user_id,
                        destination=liquidator_id,
                        timestamp=ts,
                        transaction_hash=tx_hash,
                        log_index=int(mov_idx),  # synthetic
                        event_type=event_type,
                        price_per_token=price_per_token,
                    )
                )

    # ---- Fallback for incomplete txs ----
    if tx_incomplete_data:
        liq_data_alternative_source = get_liquidatied_realtoken_rmmV3_by_tx(tx_incomplete_data)

        for liq in liq_data_alternative_source:
            tx_hash = liq["tx_hash"]
            timestamp = liq["timestamp"]
            user = liq["user"]
            liquidator = liq["liquidator"]

            assets = liq["collateral_assets"]
            amounts = liq["collateral_amounts"]

            # ensure timestamp is UTC-aware
            if isinstance(timestamp, datetime) and timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            for i, (asset, raw_amount) in enumerate(zip(assets, amounts)):
                if raw_amount == 0:
                    continue

                raw_amount_dec = Decimal(raw_amount)
                amount = raw_amount_dec / (Decimal(10) ** REALTOKEN_DECIMALS)

                price = get_token_price_at_timestamp(
                    realtoken_history_data,
                    str(asset).lower(),
                    timestamp,
                )
                if price is None:
                    continue
                price_per_token = Decimal(price) / Decimal(LIQUIDATION_BONUS)

                normalized_events.append(
                    RealtokenEvent(
                        token_address=str(asset).lower(),
                        amount=Decimal(amount),
                        source=str(user).lower(),
                        destination=str(liquidator).lower(),
                        timestamp=timestamp,
                        transaction_hash=f"0x{tx_hash}",
                        log_index=int(i),
                        event_type=RealtokenEventType.LIQUIDATION,
                        price_per_token=price_per_token,
                    )
                )

    return tuple(normalized_events)