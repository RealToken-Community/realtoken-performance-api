from __future__ import annotations

import logging
from datetime import timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, Sequence, Tuple
from zoneinfo import ZoneInfo

from config.settings import PAYMENT_TOKEN_FOR_YAM
from core.realtoken_event_history.model import RealtokenEvent, RealtokenEventType

logger = logging.getLogger(__name__)

REALTOKEN_DECIMALS = 18
PARIS_TZ = ZoneInfo("Europe/Paris")


def normalize_yam_offers(
    yam_offers: Iterable[dict],
    user_wallets: Sequence[str],
    blockchain_resources: Dict[str, Dict[str, Any]],
    realtoken_data: Dict[str, Dict[str, Any]],
) -> Tuple[RealtokenEvent, ...]:
    """
    Normalize YAM OfferAccepted events and return them as an immutable tuple.

    YAM timestamps are in Europe/Paris time and are converted to UTC.

    BUY vs SELL depends on the "mode" (sell offer vs purchase offer), not only on buyer/seller fields.

    Modes (based on your reference logic):
      1. User creates a sell offer          => SELLING realtokens
      2. User responds to a sell offer      => BUYING realtokens
      3. User creates a purchase offer      => BUYING realtokens
      4. User responds to a purchase offer  => SELLING realtokens
      5. Payment token <-> payment token exchange (skip)
      6. Realtoken <-> realtoken exchange (skip)
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

    normalized_events: list[RealtokenEvent] = []

    for raw in yam_offers:
        buyer = str(raw["buyer_address"]).lower()
        seller = str(raw["seller_address"]).lower()

        buyer_token = str(raw["buyer_token"]).lower()
        offer_token = str(raw["offer_token"]).lower()

        buyer_is_user = buyer in wallets
        seller_is_user = seller in wallets

        price_unit256 = Decimal(raw["price_bought"])
        amount_unit256 = Decimal(raw["amount_bought"])

        # --- timestamp (Paris -> UTC) ---
        ts_raw = raw["event_timestamp"]
        if ts_raw.tzinfo is None:
            ts = ts_raw.replace(tzinfo=PARIS_TZ).astimezone(timezone.utc)
        else:
            ts = ts_raw.astimezone(timezone.utc)

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
                    "Skipping YAM event: unknown payment token decimals for %s (tx=%s, log_index=%s)",
                    payment_token_address,
                    raw.get("transaction_hash"),
                    raw.get("log_index"),
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
                    event_type = RealtokenEventType.SELL_YAM
                    source = buyer
                    destination = seller
                else:  # mode == 2
                    event_type = RealtokenEventType.BUY_YAM
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
                    "Skipping YAM event: unknown payment token decimals for %s (tx=%s, log_index=%s)",
                    payment_token_address,
                    raw.get("transaction_hash"),
                    raw.get("log_index"),
                )
                continue

            # Keep your original formulas
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
                    event_type = RealtokenEventType.SELL_YAM
                    source = seller
                    destination = buyer
                else:  # mode == 3
                    event_type = RealtokenEventType.BUY_YAM
                    source = buyer
                    destination = seller

        normalized_events.append(
            RealtokenEvent(
                token_address=realtoken_address,
                amount=amount,
                source=source,
                destination=destination,
                timestamp=ts,
                transaction_hash=raw["transaction_hash"],
                log_index=int(raw["log_index"]),
                event_type=event_type,
                price_per_token=price_per_token,
            )
        )

    return tuple(normalized_events)