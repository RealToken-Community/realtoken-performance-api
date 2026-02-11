from __future__ import annotations
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Sequence
from core.realtoken_event_history.model import RealtokenEventHistory, RealtokenEventType, RealtokenEvent
from core.realtoken_event_history.event_fetchers import get_liquidatied_realtoken_rmmV3_by_tx
from core.services.utilities import get_token_price_at_timestamp
from zoneinfo import ZoneInfo
from config.settings import LIQUIDATION_BONUS

import logging
logger = logging.getLogger(__name__)

REALTOKEN_DECIMALS = 18
PARIS_TZ = ZoneInfo("Europe/Paris")


def normalize_liquidations_rmm_v3(
    liquidations: Iterable[Dict[str, Any]],
    history: RealtokenEventHistory,
    user_wallets: Sequence[str],
    realtoken_history_data: Dict[str, Dict[str, Any]]
) -> None:
    """
    Normalize RMM v3 LiquidationCall objects into *multiple* atomic RealtokenEvent entries
    (one for each RealToken liquidated).

    Each liquidation contains N movements:
      - assets are in `reserves` (list of objects with `id`)
      - amounts are in `amounts` (list of bigint-as-string)
    We create 1 RealtokenEvent per (reserve, amount) pair (zip by index).

    Event type rules:
      - If `liquidator.id` is in `user_wallets` => event_type = LIQUIDATION
      - If `user.id`       is in `user_wallets` => event_type = LIQUIDATED
      - If BOTH match => raise ValueError (this should not happen)

    source/destination convention:
      - source      = liquidated user (user.id)
      - destination = liquidator (liquidator.id)

    Uniqueness / log_index notes:
      - history.add() deduplicates by (transaction_hash, log_index).
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
        liquidations = liquidations.get("items", [])

    # Group by txHash so we can assign a stable "liquidation_sequence_in_tx"
    by_tx: dict[str, list[Dict[str, Any]]] = {}
    for liq in liquidations:
        if not isinstance(liq, dict):
            raise TypeError(f"Each liquidation must be a dict, got: {type(liq).__name__}")

        tx = (liq.get("txHash") or "").lower()
        if not tx:
            continue
        by_tx.setdefault(tx, []).append(liq)

    tx_incomplete_data = []  # Some transactions are not correctly indexed by TheGraph. When detected, we collect their tx hashes to fall back to alternative liquidation data sources.

    for tx_hash, liqs in by_tx.items():
        # Stable ordering inside a tx: (timestamp, id) is deterministic enough for subgraph outputs
        # and keeps the sequence stable across runs.
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
                # If your fetch is already filtered, this shouldn't happen; keep safe.
                continue

            reserves = liq.get("reserves") or []
            amounts = liq.get("amounts") or []

            if len(reserves) != len(amounts):
                logger.warning(
                    f"Liquidation data inconsistency detected: reserves({len(reserves)}) != amounts({len(amounts)}) "
                    f"tx={tx_hash} id={liq.get('id')}. "
                    f"Subgraph data appears incomplete; using fallback retrieval method"
                )
                tx_incomplete_data.append(tx_hash)
                continue

            for mov_idx, (reserve_obj, amount_str) in enumerate(zip(reserves, amounts)):
                token_addr = ((reserve_obj or {}).get("id") or "").lower()
                if not token_addr:
                    continue

                decimals = REALTOKEN_DECIMALS

                # BigInt-as-string -> Decimal in human units
                raw_amount = Decimal(amount_str)
                amount = raw_amount / (Decimal(10) ** decimals)

                # sometimes amount can be 0 -> skip
                if amount <= 0:
                    continue

                # price
                price_per_token = get_token_price_at_timestamp(realtoken_history_data, token_addr.lower(), ts) / (LIQUIDATION_BONUS)

                # Deterministic synthetic log_index per movement: not a real logIndex, but an artificial one derived from the asset order
                log_index = mov_idx

                event = RealtokenEvent(
                    token_address=token_addr,
                    amount=amount,
                    source=user_id,
                    destination=liquidator_id,
                    timestamp=ts,
                    transaction_hash=tx_hash,
                    log_index=int(log_index),
                    event_type=event_type,
                    price_per_token=Decimal(price_per_token),
                )

                history.add(event)
    
    liq_data_alterante_source = get_liquidatied_realtoken_rmmV3_by_tx(tx_incomplete_data)

    for liq in liq_data_alterante_source:
        tx_hash   = liq["tx_hash"]
        timestamp = liq["timestamp"]
        user      = liq["user"]
        liquidator = liq["liquidator"]

        assets  = liq["collateral_assets"]
        amounts = liq["collateral_amounts"]

        for i, (asset, raw_amount) in enumerate(zip(assets, amounts)):
            # Skip empty collateral entries
            if raw_amount == 0:
                continue

            # BigInt-as-string -> Decimal in human units
            raw_amount = Decimal(raw_amount)
            amount = raw_amount / (Decimal(10) ** decimals)

            # price
            price_per_token = get_token_price_at_timestamp(realtoken_history_data, asset.lower(), timestamp) / (LIQUIDATION_BONUS)

            event = RealtokenEvent(
                token_address=asset,
                amount=Decimal(amount),
                source=user,
                destination=liquidator,
                timestamp=timestamp,
                transaction_hash=f"0x{tx_hash}",
                log_index=i,
                event_type=RealtokenEventType.LIQUIDATION,
                price_per_token=Decimal(price_per_token),
            )

            history.add(event)