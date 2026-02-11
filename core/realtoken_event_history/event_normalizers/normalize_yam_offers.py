from __future__ import annotations
from decimal import Decimal
from datetime import timezone
from typing import Any, Dict, Iterable, Sequence
from core.realtoken_event_history.model import RealtokenEventHistory, RealtokenEventType, RealtokenEvent
from zoneinfo import ZoneInfo
from config.settings import PAYMENT_TOKEN_FOR_YAM

import logging
logger = logging.getLogger(__name__)

REALTOKEN_DECIMALS = 18
PARIS_TZ = ZoneInfo("Europe/Paris")


def normalize_yam_offers(
    yam_offers: Iterable[dict],
    history: RealtokenEventHistory,
    user_wallets: Sequence[str],
    blockchain_resources: Dict[str, Dict[str, Any]],
    realtoken_data: Dict[str, Dict[str, Any]],
) -> None:
    """
    Normalize YAM OfferAccepted events and add them to the history.

    YAM timestamps are in Europe/Paris time and are converted to UTC.

    BUY vs SELL depends on the "mode" (sell offer vs purchase offer), not only on buyer/seller fields.

    Modes (based on your reference logic):
      1. User creates a sell offer          => SELLING realtokens
      2. User responds to a sell offer      => BUYING realtokens
      3. User creates a purchase offer      => BUYING realtokens
      4. User responds to a purchase offer  => SELLING realtokens
      5. Payment token <-> payment token exchange (skip / placeholder)
      6. Realtoken <-> realtoken exchange (skip / placeholder)
    """
    wallets = {w.lower() for w in user_wallets}

    payment_tokens = {
        (resource.get("address") or "").lower()
        for name, resource in blockchain_resources.items()
        if name in PAYMENT_TOKEN_FOR_YAM
        and isinstance(resource, dict)
        and resource.get("address")
    }

    realtokens = {addr.lower() for addr in realtoken_data.keys()}

    def is_payment_token(addr: str) -> bool:
        return (addr or "").lower() in payment_tokens

    def is_realtoken_token(addr: str) -> bool:
        return (addr or "").lower() in realtokens

    for raw in yam_offers:
        buyer = raw["buyer_address"].lower()
        seller = raw["seller_address"].lower()

        buyer_token = raw["buyer_token"].lower()
        offer_token = raw["offer_token"].lower()

        buyer_is_user = buyer in wallets
        seller_is_user = seller in wallets

        price_unit256 = Decimal(raw['price_bought'])
        amount_unit256 = Decimal(raw["amount_bought"])

        ts_raw = raw["event_timestamp"]
        if ts_raw.tzinfo is None:
            # Timestamp naive, stored as Paris time
            ts = ts_raw.replace(tzinfo=PARIS_TZ).astimezone(timezone.utc)
        else:
            # Already timezone-aware (likely TIMESTAMPTZ)
            ts = ts_raw.astimezone(timezone.utc)
        

        # -------------------------
        # Determine mode (1..6)
        # A user can be either a buyer or a seller, and interact with either a sell offer or a purchase offer.
        # This results in four possible modes (exchanged not included) + 2 others modes for exchange:
        # 1. The user creates a sell offer          => they are SELLING realtokens.
        # 2. The user responds to a sell offer      => they are BUYING realtokens.
        # 3. The user creates a purchase offer      => they are BUYING realtokens.
        # 4. The user responds to a purchase offer  => they are SELLING realtokens.
        #
        # 5. The user exchange a payment token against another payment token (e.g. REUSD for USDC)
        # 6. The user exchange a realtoken against another realtoken
        # -------------------------

        if is_payment_token(buyer_token) and is_payment_token(offer_token):
            mode = 5  # payment token <-> payment token (exchange)
            continue # skip this event
        elif is_realtoken_token(buyer_token) and is_realtoken_token(offer_token):
            mode = 6  # realtoken <-> realtoken (exchange)
            continue # skip this event
        elif is_payment_token(buyer_token) and seller_is_user:
            mode = 1  # user created a sell offer -> SELLING realtokens
        elif is_payment_token(buyer_token) and buyer_is_user:
            mode = 2  # user responded to a sell offer -> BUYING realtokens
        elif is_payment_token(offer_token) and seller_is_user:
            mode = 3  # user created a purchase offer -> BUYING realtokens
        elif is_payment_token(offer_token) and buyer_is_user:
            mode = 4  # user responded to a purchase offer -> SELLING realtokens
        else:
            continue # if mode not identified, skip event

        # -------------------------
        # Record into history
        # -------------------------

        realtoken_decimals = REALTOKEN_DECIMALS

        if mode in (1, 2): # Sell offer
            realtoken_address = offer_token
            if realtoken_address == '0x0675e8F4A52eA6c845CB6427Af03616a2af42170'.lower(): realtoken_decimals = 9 # RWA has 9 decimals and not 18
            
            payment_token_address = buyer_token
            for data in blockchain_resources.values():
                if data["address"].lower() == payment_token_address:
                    payment_token_decimals = data["decimals"]

            price_per_token = Decimal(price_unit256 / 10 ** payment_token_decimals) # convert price unit256 into dec
            amount = Decimal(amount_unit256 / 10 ** realtoken_decimals) # convert amount unit256 into dec

            if buyer_is_user and seller_is_user:
                event_type = RealtokenEventType.TRANSFER  # internal transfer
                source = buyer # could be either way: sller or buyer
                destination = seller
                price_per_token = None
            else:
                if mode in (1,):
                    event_type = RealtokenEventType.SELL_YAM
                    source = buyer
                    destination = seller
                elif mode in (2,):
                    event_type = RealtokenEventType.BUY_YAM
                    source = seller
                    destination = buyer

        elif mode in (3, 4): # Purchase offer
            realtoken_address = buyer_token
            if realtoken_address == '0x0675e8F4A52eA6c845CB6427Af03616a2af42170'.lower(): realtoken_decimals = 9 # RWA has 9 decimals and not 18
            
            payment_token_address = offer_token
            for data in blockchain_resources.values():
                if data["address"].lower() == payment_token_address:
                    payment_token_decimals = data["decimals"]
            
            price_per_token = Decimal(10 ** realtoken_decimals / price_unit256) # convert price unit256 into dec
            amount = Decimal(amount_unit256 * price_unit256 /  10 ** (payment_token_decimals + realtoken_decimals)) # convert amount unit256 into dec

            if buyer_is_user and seller_is_user:
                event_type = RealtokenEventType.TRANSFER  # internal transfer
                source = seller # could be either way: sller or buyer
                destination = buyer
                price_per_token = None
            else:
                if mode in (4,):
                    event_type = RealtokenEventType.SELL_YAM
                    source = seller
                    destination = buyer
                elif mode in (3,):
                    event_type = RealtokenEventType.BUY_YAM
                    source = buyer
                    destination = seller

        else:
            continue # if not a realtoken nor a payment token, skip this event

        event = RealtokenEvent(
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
        history.add(event)