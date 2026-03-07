from __future__ import annotations

from flask import Blueprint, jsonify, request, current_app

import time
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from eth_utils import to_checksum_address
from job.utilities import load_json
from core.services.get_all_user_linked_addresses import get_all_user_linked_addresses
from core.realtoken_event_history.event_fetchers import (
    fetch_realtoken_transfers,
    fetch_liquidations_rmm_v3,
    fetch_yam_v1_events
)
from core.realtoken_event_history.event_normalizers import (
    extract_user_purchases_from_realt,
    extract_detokenisations
)
from core.realtoken_event_history.event_normalizers import (
    normalize_detokenisation,
    normalize_internal_transfer,
    normalize_liquidations_rmm_v3,
    normalize_realt_purchases,
    normalize_yam_offers
)
from core.realtoken_event_history.model import RealtokenEventHistory, RealtokenEventType
from core.balance_snapshots.balance_fetchers.fetch_current_realtoken_balances import fetch_current_realtoken_balances_aggregated
from core.balance_snapshots.model import BalanceSnapshot, BalanceSnapshotSeries
from core.performance.calculator import PerformanceCalculator

logger = logging.getLogger(__name__)

realtokens_performance_bp = Blueprint("realtokens_performance", __name__)


def _validate_wallet(wallet: str) -> bool:
    # Minimal EVM address validation
    wallet = wallet.strip()
    return wallet.startswith("0x") and len(wallet) == 42


@realtokens_performance_bp.get("/realtokens-performance")
def realtokens_performance():
    """
    """

    # Measure request duration to log slow requests and simplify debugging
    start = time.perf_counter()

    wallet = (request.args.get("wallet") or "").strip()

    if not wallet:
        return jsonify({"error": "Missing required query param: wallet"}), 400

    if not _validate_wallet(wallet):
        return jsonify({"error": "Invalid wallet format", "wallet": wallet}), 400
    
    POSTGRES_DATA = current_app.config['POSTGRES_DATA']
    THE_GRAPH_API_KEY = current_app.config['THE_GRAPH_API_KEY']
    REALTOKEN_GNOSIS_SUBGRAPH_ID = current_app.config['REALTOKEN_GNOSIS_SUBGRAPH_ID']
    RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID = current_app.config['RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID']
    POSTGRES_DATA = current_app.config['POSTGRES_DATA']
    BLOCKCHAIN_CONTRACTS = current_app.config['BLOCKCHAIN_CONTRACTS']

    wallets = get_all_user_linked_addresses(wallet, THE_GRAPH_API_KEY, REALTOKEN_GNOSIS_SUBGRAPH_ID)

    realtoken_data = load_json("data_tmp/realtokens_data.json")
    realtoken_history = load_json("data_tmp/realtokens_history.json")

    now = datetime.now(timezone.utc)

    # Parallelize the network/IO work:
    with ThreadPoolExecutor(max_workers=4) as ex:
        
        # --- RealToken transfers (The Graph) ---
        user_transfers_task = ex.submit(
            fetch_realtoken_transfers,
            REALTOKEN_GNOSIS_SUBGRAPH_ID,
            THE_GRAPH_API_KEY,
            wallets,
        )

        # --- fetch liquidations rmm v3 (The Graph) ---
        rmmv3_liquidations_task = ex.submit(
            fetch_liquidations_rmm_v3,
            RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID,
            THE_GRAPH_API_KEY,
            wallets,
        )

        # --- Current aggregated balances (The Graph: Realtoken and wrapper rmmv3) ---
        current_balances_task = ex.submit(
            fetch_current_realtoken_balances_aggregated,
            REALTOKEN_GNOSIS_SUBGRAPH_ID,
            RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID,
            THE_GRAPH_API_KEY,
            wallets,
        )

        # --- fetch yam v1 events (Postgres DB) ---
        yam_v1_events_task = ex.submit(
            fetch_yam_v1_events,
            wallets,
            datetime(2019, 1, 1),
            now,
            POSTGRES_DATA,
        )        
        
        user_transfers  = user_transfers_task.result()
        realt_purchases = extract_user_purchases_from_realt(user_transfers["data"]["inTransfers"], realtoken_data, realtoken_history)
        detokenisations_events = extract_detokenisations(user_transfers["data"]["outTransfers"], realtoken_history)
        rmmv3_liquidations = rmmv3_liquidations_task.result()
        current_balances = current_balances_task.result()
        offers_seller, offers_buyer = yam_v1_events_task.result()


    # ---- normalize data for the realtoken_event_history -----
    normalized_internal_transfer = normalize_internal_transfer(user_transfers, wallets)
    normalized_realt_purchases = normalize_realt_purchases(realt_purchases)
    normalized_yam_offers = normalize_yam_offers(offers_buyer + offers_seller, wallets, BLOCKCHAIN_CONTRACTS, realtoken_data)
    normalized_liquidations_rmm_v3 = normalize_liquidations_rmm_v3(rmmv3_liquidations, wallets, realtoken_history)
    normalized_detokenisation = normalize_detokenisation(detokenisations_events)

    # ------ RealtokenEventHistory --------
    realtoken_event_history = RealtokenEventHistory()
    realtoken_event_history.add(normalized_internal_transfer)
    realtoken_event_history.add(normalized_realt_purchases)
    realtoken_event_history.add(normalized_yam_offers)
    realtoken_event_history.add(normalized_liquidations_rmm_v3)
    realtoken_event_history.add(normalized_detokenisation)
    realtoken_event_history.sort_events_by_timestamp()

    # ----- balances -----
    snapshot_now = BalanceSnapshot(
        as_of=datetime.now(timezone.utc),
        balances_by_token=current_balances["data"],
    )
    balance_snapshots_series = BalanceSnapshotSeries([snapshot_now])

    # ---- all possible event types (from enum) ----
    all_event_types = [event_type.value for event_type in RealtokenEventType]

    ##### BUILDING THE ROI CALCULATOR ######
    realtokens_performance = PerformanceCalculator(realtoken_event_history, balance_snapshots_series)

    # Build performance by_token
    all_token_uuids = (
        set(realtokens_performance.realized_pnl_by_token.keys()) | set(realtokens_performance.unrealized_pnl_by_token.keys())
    )
    
    by_token: dict[str, dict] = {}
    for uuid in sorted(all_token_uuids):
        realized_indicator = realtokens_performance.realized_pnl_by_token.get(uuid)
        unrealized_indicator = realtokens_performance.unrealized_pnl_by_token.get(uuid)
    
        by_token[uuid] = {
            "realized": realized_indicator.to_dict() if realized_indicator else None,
            "unrealized": unrealized_indicator.to_dict() if unrealized_indicator else None,
            "roi": None,
        }
    
    
    response = {
        "wallets": [to_checksum_address(wallet) for wallet in wallets],
        "event_types": all_event_types,
        "events": realtoken_event_history.as_dict_serialized(),
        
        "performance": {
            "global": {
                "realized": realtokens_performance.global_realized_pnl.to_dict(),
                "unrealized": realtokens_performance.global_unrealized_pnl.to_dict(),
                "roi": None,
            },
            "by_token": by_token,
        },
    }

    # Measure request duration to log slow requests and simplify debugging
    end = time.perf_counter()
    duration = end - start
    logger.info(
        f"Realtoken performance requested for wallet {wallet}. Request completed in {duration:.3f} seconds."
    )
    
    return jsonify(response)
