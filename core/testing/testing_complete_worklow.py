'''
use this command to test the complete workflow from a wallet address to a global performance indicator:
python -m core.testing.testing_complete_worklow
'''


if __name__ == "__main__":
    from core.services.logging_config import setup_logging
    setup_logging()

from pprint import pprint
from dotenv import load_dotenv
import os
load_dotenv() 

THE_GRAPH_API_KEY = os.getenv("THE_GRAPH_API_KEY")
REALTOKEN_GNOSIS_SUBGRAPH_ID = os.getenv("REALTOKEN_GNOSIS_SUBGRAPH_ID")
YAM_INDEXING_DB_PATH = os.getenv('YAM_INDEXING_DB_PATH')
RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID = os.getenv("RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB   = os.getenv("POSTGRES_DB")
POSTGRES_READER_USER_NAME = os.getenv("POSTGRES_READER_USER_NAME")
POSTGRES_READER_USER_PASSWORD = os.getenv("POSTGRES_READER_USER_PASSWORD")
POSTGRES_DATA = [POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_READER_USER_NAME, POSTGRES_READER_USER_PASSWORD]

WALLET = "0x296fB7Be365498cdE47079c302e82A82721953d6" #"0x2Ce2830FC9e351BD23fDC54bB1BC992C68979C91" #"0x296fB7Be365498cdE47079c302e82A82721953d6"


import json
from eth_utils import to_checksum_address
from core.realtoken_event_history.event_fetchers import (
    fetch_realtoken_transfers,
    fetch_liquidations_rmm_v3,
    get_accepted_offers_by_buyer_datetime,
    get_accepted_offers_by_seller_datetime,
    fill_missing_owner_in_realtokens_data
)
from core.services.utilities import fetch_json, list_to_dict_by_uuid, get_pg_connection
from core.services.get_all_user_linked_addresses import get_all_user_linked_addresses
from config.settings import REALTOKENS_LIST_URL, REALTOKEN_HISTORY_URL
from datetime import datetime, timezone
from decimal import Decimal
from core.realtoken_event_history.model import RealtokenEventHistory
from core.realtoken_event_history.event_normalizers import normalize_realt_purchases, normalize_detokenisation, normalize_yam_offers, normalize_liquidations_rmm_v3, extract_user_purchases_from_realt, extract_detokenisations
from core.balance_snapshots.balance_fetchers.fetch_current_realtoken_balances import fetch_current_realtoken_balances_aggregated
from core.balance_snapshots.model import BalanceSnapshot, BalanceSnapshotSeries
from core.performance.calculator import PerformanceCalculator




# ----------- blockchain_ressources -----------
with open('Ressources/blockchain_contracts.json', 'r') as blockchain_ressources_file:
    blockchain_ressources = json.load(blockchain_ressources_file)['contracts']

# ------- update_realtoken_data_with_owner ---------
realtoken_data = list_to_dict_by_uuid(fetch_json(REALTOKENS_LIST_URL) or [])
realtoken_history_data = list_to_dict_by_uuid(fetch_json(REALTOKEN_HISTORY_URL) or [])
realtoken_data = fill_missing_owner_in_realtokens_data(realtoken_data)

# ------- get_all_user_linked_addresses --------
WALLETS = get_all_user_linked_addresses(WALLET, THE_GRAPH_API_KEY, REALTOKEN_GNOSIS_SUBGRAPH_ID)

#WALLETS = ["0xa22dc341c8dd53Ab1Dff6e66228e779832a449BF", "0x296fB7Be365498cdE47079c302e82A82721953d6", "0xA99e07efB152321117653a16727BF6Bc02106892", "0x6C85cBF6807Cbe59830e8270Bfd8701c72348585"]
#WALLETS = ["0x2a5a9AaeB65685e629e603a337B6d65Cf271B169", "0x2Ce2830FC9e351BD23fDC54bB1BC992C68979C91", "0x43a89C2f84e7e731D1787b523AE4e1c588C1Aa6A", "0x46D0Fb00d66d66E1A2feC6AEb670D53951C361c4", "0xd58B7F2722371aa92C929272094c3A65482c0429"]



# ------- fetch_realtoken_transfers --------
user_tranfers = fetch_realtoken_transfers(
    REALTOKEN_GNOSIS_SUBGRAPH_ID,
    THE_GRAPH_API_KEY,
    WALLETS
)

# ------- extract_user_purchases_from_realt ---------
realt_purchases = extract_user_purchases_from_realt(user_tranfers["data"]["inTransfers"], realtoken_data, realtoken_history_data)

# ------- extract_detokenisations_events ---------
detokenisations_events = extract_detokenisations(user_tranfers["data"]["outTransfers"], realtoken_history_data)

# ------ get_accepted_offers_by_seller_datetime --------
# ------ get_accepted_offers_by_buyer_datetime --------
pg_conn = get_pg_connection(*POSTGRES_DATA)
try:
    offers_seller = get_accepted_offers_by_seller_datetime(pg_conn, WALLETS, datetime(2023, 1, 1), datetime(2026, 1, 1))
    offers_buyer = get_accepted_offers_by_buyer_datetime(pg_conn, WALLETS, datetime(2023, 1, 1), datetime(2026, 1, 1))
finally:
    pg_conn.close()

# ----- fetch_liquidations_rmm_v3 --------
rmmv3_liquidations = fetch_liquidations_rmm_v3(
    RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID,
    THE_GRAPH_API_KEY,
    WALLETS
)

# ------ RealtokenEventHistory --------
realtoken_event_history = RealtokenEventHistory()

# normalize data into the realtoken_event_history
normalize_realt_purchases(realt_purchases, realtoken_event_history)
normalize_yam_offers(offers_buyer + offers_seller, realtoken_event_history, WALLETS, blockchain_ressources, realtoken_data)
normalize_liquidations_rmm_v3(rmmv3_liquidations, realtoken_event_history, WALLETS, realtoken_history_data)
normalize_detokenisation(detokenisations_events, realtoken_event_history)
realtoken_event_history.sort_events_by_timestamp()

#data = realtoken_event_history.as_dict_serialized()
#with open("testing_history.json", "w", encoding="utf-8") as f:
#    json.dump(data, f, indent=4, ensure_ascii=False)

#print(realtoken_event_history.count_events())


# ------- BalanceSnapshotsSeries -------
balances = fetch_current_realtoken_balances_aggregated(
    REALTOKEN_GNOSIS_SUBGRAPH_ID,
    RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID,
    THE_GRAPH_API_KEY,
    WALLETS
)
snapshot_now = BalanceSnapshot(
    as_of=datetime.now(timezone.utc),
    balances_by_token=balances["data"],
)
balance_snapshots_series = BalanceSnapshotSeries([snapshot_now])


# ------ PerformanceCalculator ---------

roi_calculator = PerformanceCalculator(realtoken_event_history, balance_snapshots_series)


#print()
#print('1610')
#print(roi_calculator.unrealized_pnl_by_token.get(to_checksum_address('0xd88e8873e90f734c9d3e3519e9e87345478c1df2')))
#print()
#print('601 Milton')
#print(roi_calculator.unrealized_pnl_by_token.get(to_checksum_address('0x2b683f8cc61de593f089bdddc01431c0d7ca2ee2')))
#print()
#print('5733 Neely')
#print(roi_calculator.realized_pnl_by_token.get(to_checksum_address('0x70724f4332d7ee1918f71236c1746cdda732d90a')))
#print()
#print('19000 Fenton')
#print(roi_calculator.realized_pnl_by_token.get(to_checksum_address('0xf7412e264fa85ae5e79ac3a4b64ce4669e32b98f')))
print()
print('global realized')
print(roi_calculator.global_realized_pnl)
print()
print('global unrealized')
print(roi_calculator.global_unrealized_pnl)


