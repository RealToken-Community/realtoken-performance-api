import logging
from typing import Any, Dict, List, Tuple

from config.settings import (
    MULTICALLV3_ADDRESS,
    MULTICALLV3_ABI_MINIMAL,
    REALTOKEN_ABI_MINIMAL,
)
from core.services.w3_handler import w3_handler
from core.services.send_telegram_alert import send_telegram_alert

from eth_abi import decode as abi_decode
from web3 import Web3

logger = logging.getLogger(__name__)

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


@w3_handler()
def _multicall_owner_batch(
    w3: Web3,
    calls: List[Tuple[str, bool, bytes]],
) -> List[Tuple[bool, bytes]]:
    """
    Executes ONE Multicall3 aggregate3 batch.
    Decorated so that if this batch fails, w3_handler can switch RPC and retry only this batch.
    """
    multicall = w3.eth.contract(
        address=Web3.to_checksum_address(MULTICALLV3_ADDRESS),
        abi=MULTICALLV3_ABI_MINIMAL,
    )
    return multicall.functions.aggregate3(calls).call()


@w3_handler()
def fill_missing_owner_in_realtokens_data(
    w3: Web3,
    realtokens_data: Dict[str, Dict[str, Any]],
    batch_size: int = 600,
) -> Dict[str, Dict[str, Any]]:
    """
    For each key in tokens, ensure tokens[address]["owner"] exists.
    If missing, fetch via Multicall3 aggregate3(), batched with at most `batch_size` calls per request.

    NOTE:
    - This function is decorated so callers do NOT pass `w3`.
    - Batch multicalls are also decorated; so if a given multicall batch fails, only that batch is retried/fails over.
    - Keys of `realtokens_data` are expected to be lowercased addresses.
    """
    if not realtokens_data:
        return realtokens_data

    if batch_size <= 0:
        logger.warning("Invalid batch_size=%s, defaulting to 600", batch_size)
        batch_size = 600

    # 1) collect missing
    missing: list[str] = []
    for addr, meta in realtokens_data.items():
        if not isinstance(meta, dict):
            realtokens_data[addr] = meta = {}
    
        # Only consider tokens that actually have a Gnosis contract
        gnosis_contract = meta.get("gnosisContract")
        if gnosis_contract in (None, "", ZERO_ADDRESS):
            continue
    
        owner = meta.get("owner")
        if not owner or owner.lower() == ZERO_ADDRESS:
            missing.append(addr)
    
        if not missing:
            return realtokens_data

    token_iface = w3.eth.contract(abi=REALTOKEN_ABI_MINIMAL)

    # calldata for owner()
    try:
        owner_calldata = token_iface.encode_abi("owner", args=[])
    except Exception:
        logger.exception("Failed to encode owner() calldata")
        return realtokens_data

    def _chunks(lst: list[str], n: int):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    total_missing = len(missing)

    # 2) process in batches
    for batch_idx, missing_batch in enumerate(_chunks(missing, batch_size), start=1):
        calls: List[Tuple[str, bool, bytes]] = []
        targets_lower: List[str] = []

        for addr_lower in missing_batch:
            try:
                target_checksum = Web3.to_checksum_address(addr_lower)
            except Exception:
                logger.exception("Invalid token address key: %r", addr_lower)
                send_telegram_alert(f"Invalid token address key: {addr_lower}")
                continue

            calls.append((target_checksum, True, owner_calldata))
            targets_lower.append(addr_lower)

        if not calls:
            continue

        # 3) execute this batch
        # We intentionally call the decorated batch function so failover is per-batch.
        try:
            results = _multicall_owner_batch(calls)
        except Exception:
            logger.exception(
                "Multicall aggregate3 batch failed after RPC failover (batch %s, size=%s, total_missing=%s)",
                batch_idx,
                len(calls),
                total_missing,
            )
            send_telegram_alert(
                f"Multicall aggregate3 failed after RPC failover (batch {batch_idx}, size={len(calls)})"
            )
            continue

        # 4) decode + write back
        for addr_lower, (success, return_data) in zip(targets_lower, results):
            if not success:
                logger.warning("owner() call failed for %s", addr_lower)
                send_telegram_alert(f"owner() call failed for {addr_lower}")
                continue

            try:
                (owner_addr,) = abi_decode(["address"], return_data)
                owner_checksum = Web3.to_checksum_address(owner_addr)
            except Exception:
                logger.exception("Failed decoding owner() for %s (data=%r)", addr_lower, return_data)
                send_telegram_alert(f"Failed decoding owner() result for {addr_lower}")
                continue

            meta = realtokens_data.get(addr_lower)
            if not isinstance(meta, dict):
                logger.exception(
                    "Invalid realtokens_data entry for %s (expected dict, got %s)",
                    addr_lower,
                    type(meta).__name__,
                )
                send_telegram_alert(
                    f"Invalid realtokens_data entry for {addr_lower} "
                    f"(expected dict, got {type(meta).__name__})"
                )
                continue

            meta["owner"] = owner_checksum

    return realtokens_data
