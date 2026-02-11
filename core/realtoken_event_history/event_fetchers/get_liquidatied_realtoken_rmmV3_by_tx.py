from __future__ import annotations

import time
from typing import Any, Dict, Iterable, List, Union
from web3 import Web3
from web3.types import HexBytes
from core.services.w3_handler import w3_handler
from datetime import datetime, timezone

LIQUIDATIONCALL_TOPIC0 = "0x6894c33a648e7239e514bd83f9a3f5a3c7b460731bcafd782e38274b00817b91"

_LIQUIDATIONCALL_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": False, "name": "collateralAssets",   "type": "address[]"},
        {"indexed": False, "name": "collateralAmounts", "type": "uint256[]"},
        {"indexed": True,  "name": "debtAsset",         "type": "address"},
        {"indexed": False, "name": "debtToCover",       "type": "uint256"},
        {"indexed": True,  "name": "user",              "type": "address"},
        {"indexed": False, "name": "liquidator",        "type": "address"},
        {"indexed": False, "name": "receiveMethod",     "type": "uint8"},
    ],
    "name": "LiquidationCall",
    "type": "event",
}


def _hex_topic(topic: Union[str, bytes, HexBytes]) -> str:
    return (topic if isinstance(topic, str) else Web3.to_hex(topic)).lower()


def _rate_limit(last_call_ts: float, min_interval: float) -> float:
    """
    Sleep if needed so that calls are spaced by at least min_interval seconds.
    Returns the timestamp of the current call.
    """
    now = time.monotonic()
    elapsed = now - last_call_ts
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
        now = time.monotonic()
    return now

@w3_handler()
def get_liquidatied_realtoken_rmmV3_by_tx(
    w3: Web3,
    tx_hashes: Iterable[Union[str, bytes, HexBytes]],
) -> List[Dict[str, Any]]:
    """
    Fetch tx receipts, find LiquidationCall logs, decode them,
    with a max rate of 4 RPC requests per second.
    """

    decoder_contract = w3.eth.contract(abi=[_LIQUIDATIONCALL_EVENT_ABI])
    decoded_events: List[Dict[str, Any]] = []

    MIN_INTERVAL = 0.25  # seconds → 4 req / second
    last_rpc_call = 0.0

    for txh in tx_hashes:
        # --- rate limit BEFORE each RPC call ---
        last_rpc_call = _rate_limit(last_rpc_call, MIN_INTERVAL)

        try:
            receipt = w3.eth.get_transaction_receipt(txh)
            block = w3.eth.get_block(receipt["blockNumber"])
        except Exception:
            continue

        timestamp = datetime.fromtimestamp(block["timestamp"], tz=timezone.utc)

        for log in receipt.get("logs", []):
            topics = log.get("topics") or []
            if not topics:
                continue

            if _hex_topic(topics[0]) != LIQUIDATIONCALL_TOPIC0:
                continue

            evt = decoder_contract.events.LiquidationCall().process_log(log)

            decoded_events.append({
                "tx_hash": receipt["transactionHash"].hex()
                if hasattr(receipt["transactionHash"], "hex")
                else receipt["transactionHash"],

                "block_number": receipt["blockNumber"],
                "timestamp": timestamp,
                "log_index": log["logIndex"],
                "contract": log["address"],

                "collateral_assets": list(evt["args"]["collateralAssets"]),
                "collateral_amounts": list(evt["args"]["collateralAmounts"]),
                "debt_asset": evt["args"]["debtAsset"],
                "debt_to_cover": int(evt["args"]["debtToCover"]),
                "user": evt["args"]["user"],
                "liquidator": evt["args"]["liquidator"],
                "receive_method": int(evt["args"]["receiveMethod"]),
            })

    return decoded_events