from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, Sequence, Tuple

from config.settings import PAYMENT_TOKEN_FOR_YAM
from core.realtoken_event_history.model import RealtokenEvent, RealtokenEventType

logger = logging.getLogger(__name__)

REALTOKEN_DECIMALS = 18


def normalize_swapcat_offers(
    swapcat_offers: Iterable[dict] | Dict[str, Any],
    user_wallets: Sequence[str],
    blockchain_resources: Dict[str, Dict[str, Any]],
    realtoken_data: Dict[str, Dict[str, Any]],
) -> Tuple[RealtokenEvent, ...]:
    """
    Normalize SwapCat Purchase events and return them as an immutable tuple.

    This follows the exact same business logic as normalize_yam_offers:
      1. User creates a sell offer          => SELLING realtokens
      2. User responds to a sell offer      => BUYING realtokens
      3. User creates a purchase offer      => BUYING realtokens
      4. User responds to a purchase offer  => SELLING realtokens
      5. Payment token <-> payment token exchange (skip)
      6. Realtoken <-> realtoken exchange (skip)

    Accepted inputs:
    - raw iterable of purchase dicts
    - or the full payload returned by fetch_swapcat_events(...) i.e. {"data": [...], "meta": ...}
    """
    wallets = {w.lower() for w in user_wallets if w}

    payment_tokens = {
        (resource.get("address") or "").lower()
        for name, resource in (blockchain_resources or {}).items()
        if name in PAYMENT_TOKEN_FOR_YAM
        and isinstance(resource, dict)
        and resource.get("address")
    }

    payment_token_decimals_by_addr: dict[str, int] = {
        (resource.get("address") or "").lower(): int(resource.get("decimals"))
        for resource in (blockchain_resources or {}).values()
        if isinstance(resource, dict)
        and resource.get("address")
        and resource.get("decimals") is not None
    }

    realtokens = {addr.lower() for addr in (realtoken_data or {}).keys()}

    def is_payment_token(addr: str) -> bool:
        return (addr or "").lower() in payment_tokens

    def is_realtoken_token(addr: str) -> bool:
        return (addr or "").lower() in realtokens

    # Accept either the direct list of purchases or the full fetch payload
    if isinstance(swapcat_offers, dict):
        raw_offers = swapcat_offers.get("data") or []
    else:
        raw_offers = list(swapcat_offers)

    normalized_events: list[RealtokenEvent] = []

    for raw in raw_offers:
        buyer_obj = raw.get("buyer") or {}
        seller_obj = raw.get("seller") or {}
        buyer_token_obj = raw.get("buyerToken") or {}
        offer_token_obj = raw.get("offerToken") or {}

        buyer = str(buyer_obj.get("address") or buyer_obj.get("id") or "").lower()
        seller = str(seller_obj.get("address") or seller_obj.get("id") or "").lower()

        buyer_token = str(
            buyer_token_obj.get("address") or buyer_token_obj.get("id") or ""
        ).lower()
        offer_token = str(
            offer_token_obj.get("address") or offer_token_obj.get("id") or ""
        ).lower()

        if not buyer or not seller or not buyer_token or not offer_token:
            logger.debug(
                "Skipping SwapCat event with missing addresses/tokens (id=%s, tx=%s)",
                raw.get("id"),
                raw.get("txHash"),
            )
            continue

        buyer_is_user = buyer in wallets
        seller_is_user = seller in wallets

        try:
            price_unit256 = Decimal(str(raw["price"]))
            amount_unit256 = Decimal(str(raw["quantity"]))
        except Exception:
            logger.debug(
                "Skipping SwapCat event with invalid numeric fields (id=%s, tx=%s, price=%s, quantity=%s)",
                raw.get("id"),
                raw.get("txHash"),
                raw.get("price"),
                raw.get("quantity"),
            )
            continue

        # --- timestamp (subgraph unix timestamp -> UTC) ---
        try:
            ts = datetime.fromtimestamp(int(raw["createdAtTimestamp"]), tz=timezone.utc)
        except Exception:
            logger.debug(
                "Skipping SwapCat event with invalid timestamp (id=%s, tx=%s, createdAtTimestamp=%s)",
                raw.get("id"),
                raw.get("txHash"),
                raw.get("createdAtTimestamp"),
            )
            continue

        # -------------------------
        # Determine mode (1-6)
        # -------------------------
        if is_payment_token(buyer_token) and is_payment_token(offer_token):
            continue  # mode 5: payment <-> payment (skip)
        if is_realtoken_token(buyer_token) and is_realtoken_token(offer_token):
            continue  # mode 6: realtoken <-> realtoken (skip)

        if is_payment_token(buyer_token) and seller_is_user:
            mode = 1  # user created a sell offer -> SELLING realtokens
        elif is_payment_token(buyer_token) and buyer_is_user:
            mode = 2  # user responded to a sell offer -> BUYING realtokens
        elif is_payment_token(offer_token) and seller_is_user:
            mode = 3  # user created a purchase offer -> BUYING realtokens
        elif is_payment_token(offer_token) and buyer_is_user:
            mode = 4  # user responded to a purchase offer -> SELLING realtokens
        else:
            continue  # mode not identified

        # -------------------------
        # Compute fields
        # -------------------------
        realtoken_decimals = REALTOKEN_DECIMALS

        if mode in (1, 2):  # Sell offer
            realtoken_address = offer_token
            if realtoken_address == "0x0675e8f4a52ea6c845cb6427af03616a2af42170":
                realtoken_decimals = 9  # RWA has 9 decimals

            payment_token_address = buyer_token
            payment_token_decimals = payment_token_decimals_by_addr.get(payment_token_address)
            if payment_token_decimals is None:
                logger.debug(
                    "Skipping SwapCat event: unknown payment token decimals for %s (id=%s, tx=%s)",
                    payment_token_address,
                    raw.get("id"),
                    raw.get("txHash"),
                )
                continue

            price_per_token = Decimal(price_unit256 / (10 ** payment_token_decimals))
            amount = Decimal(amount_unit256 / (10 ** realtoken_decimals))

            if buyer_is_user and seller_is_user:
                event_type = RealtokenEventType.TRANSFER
                source = buyer
                destination = seller
                price_per_token = None
            else:
                if mode == 1:
                    event_type = RealtokenEventType.SELL_SWAPCAT
                    source = buyer
                    destination = seller
                else:  # mode == 2
                    event_type = RealtokenEventType.BUY_SWAPCAT
                    source = seller
                    destination = buyer

        else:  # mode in (3, 4) => Purchase offer
            realtoken_address = buyer_token
            if realtoken_address == "0x0675e8f4a52ea6c845cb6427af03616a2af42170":
                realtoken_decimals = 9  # RWA has 9 decimals

            payment_token_address = offer_token
            payment_token_decimals = payment_token_decimals_by_addr.get(payment_token_address)
            if payment_token_decimals is None:
                logger.debug(
                    "Skipping SwapCat event: unknown payment token decimals for %s (id=%s, tx=%s)",
                    payment_token_address,
                    raw.get("id"),
                    raw.get("txHash"),
                )
                continue

            price_per_token = Decimal((10 ** realtoken_decimals) / price_unit256)
            amount = Decimal(
                amount_unit256 * price_unit256 / (10 ** (payment_token_decimals + realtoken_decimals))
            )

            if buyer_is_user and seller_is_user:
                event_type = RealtokenEventType.TRANSFER
                source = seller
                destination = buyer
                price_per_token = None
            else:
                if mode == 4:
                    event_type = RealtokenEventType.SELL_SWAPCAT
                    source = seller
                    destination = buyer
                else:  # mode == 3
                    event_type = RealtokenEventType.BUY_SWAPCAT
                    source = buyer
                    destination = seller

        normalized_events.append(
            RealtokenEvent(
                token_address=realtoken_address,
                amount=amount,
                source=source,
                destination=destination,
                timestamp=ts,
                transaction_hash=str(raw.get("txHash") or ""),
                log_index=_extract_swapcat_log_index(raw),
                event_type=event_type,
                price_per_token=price_per_token,
            )
        )

    return tuple(normalized_events)


def _extract_swapcat_log_index(raw: Dict[str, Any]) -> int:
    """
    Best-effort extraction of log_index from SwapCat purchase payload.

    Tries, in order:
    - explicit logIndex / log_index fields if they exist later in the subgraph
    - parse trailing integer from `id` if formatted like 'txhash-logindex' or '...:123'
    - fallback to 0
    """
    for key in ("logIndex", "log_index"):
        value = raw.get(key)
        if value is not None:
            try:
                return int(value)
            except Exception:
                pass

    raw_id = str(raw.get("id") or "").strip()
    if raw_id:
        for sep in ("-", ":", "_"):
            if sep in raw_id:
                tail = raw_id.rsplit(sep, 1)[-1]
                try:
                    return int(tail)
                except Exception:
                    pass

    return 0