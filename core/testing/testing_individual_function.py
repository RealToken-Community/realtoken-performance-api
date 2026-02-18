'''
use this command to test the function of your choice:
python -m core.testing.testing_individual_function
Set to 'True' the variable 'TEST_NOW' for each section you want to test
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
RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID = os.getenv("RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID")
YAM_INDEXING_DB_PATH = os.getenv('YAM_INDEXING_DB_PATH')

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB   = os.getenv("POSTGRES_DB")
POSTGRES_READER_USER_NAME = os.getenv("POSTGRES_READER_USER_NAME")
POSTGRES_READER_USER_PASSWORD = os.getenv("POSTGRES_READER_USER_PASSWORD")
POSTGRES_DATA = [POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_READER_USER_NAME, POSTGRES_READER_USER_PASSWORD]

#WALLETS = ["0xa22dc341c8dd53Ab1Dff6e66228e779832a449BF", "0x296fB7Be365498cdE47079c302e82A82721953d6", "0xA99e07efB152321117653a16727BF6Bc02106892", "0x6C85cBF6807Cbe59830e8270Bfd8701c72348585"]


# ------- get_all_user_linked_addresses --------

TEST_NOW = False

from core.services.get_all_user_linked_addresses import get_all_user_linked_addresses

if TEST_NOW:
    WALLETS = get_all_user_linked_addresses("0x296fB7Be365498cdE47079c302e82A82721953d6", THE_GRAPH_API_KEY, REALTOKEN_GNOSIS_SUBGRAPH_ID)
    print(WALLETS)


# ------- fetch_realtoken_transfers --------

TEST_NOW = False

from core.realtoken_event_history.event_fetchers import fetch_realtoken_transfers

if TEST_NOW:
    user_tranfers = fetch_realtoken_transfers(
        REALTOKEN_GNOSIS_SUBGRAPH_ID,
        THE_GRAPH_API_KEY,
        WALLETS
    )
    #pprint(user_tranfers['data']['outTransfers'])
    #pprint(user_tranfers['data']['inTransfers'])


# ------- update_realtoken_data_with_owner ---------

TEST_NOW = False

from core.services.utilities import fetch_json, list_to_dict_by_uuid
from config.settings import REALTOKENS_LIST_URL, REALTOKEN_HISTORY_URL
from core.realtoken_event_history.event_fetchers import fill_missing_owner_in_realtokens_data

if TEST_NOW:
    realtoken_data = list_to_dict_by_uuid(fetch_json(REALTOKENS_LIST_URL) or [])
    realtoken_history_data = list_to_dict_by_uuid(fetch_json(REALTOKEN_HISTORY_URL) or [])
    realtoken_data = fill_missing_owner_in_realtokens_data(realtoken_data)
    #pprint(realtoken_data)


# ------- compute_purchases_from_realt ---------
TEST_NOW = False

from core.realtoken_event_history.event_normalizers.extract_user_purchases_from_realt import extract_user_purchases_from_realt

if TEST_NOW:
    realt_purchases = extract_user_purchases_from_realt(user_tranfers["data"]["inTransfers"], realtoken_data, realtoken_history_data)
    #pprint(realt_purchases)

# ------- compute_detokenisation ---------
TEST_NOW = False

from core.realtoken_event_history.event_normalizers.extract_detokenisations import extract_detokenisations

if TEST_NOW:
    detokenisations_events = extract_detokenisations(user_tranfers["data"]["outTransfers"], realtoken_history_data)
    #pprint(detokenisations_events)

# ------ fetch_current_realtoken_balances_aggregated (fetch_current_realtoken_balances_the_graph and fetch_current_realtoken_balances_from_wrapper) --------
TEST_NOW = True

from core.balance_snapshots.balance_fetchers.fetch_current_realtoken_balances import fetch_current_realtoken_balances_the_graph, fetch_current_realtoken_balances_from_wrapper, fetch_current_realtoken_balances_aggregated
from core.balance_snapshots.model import BalanceSnapshot, BalanceSnapshotSeries
from datetime import datetime, timezone
from decimal import Decimal

if TEST_NOW:
    #balances_realtokens = fetch_current_realtoken_balances_the_graph(
    #    REALTOKEN_GNOSIS_SUBGRAPH_ID,
    #    THE_GRAPH_API_KEY,
    #    ["0x296fB7Be365498cdE47079c302e82A82721953d6"]
    #)
    #balances_wrapper = fetch_current_realtoken_balances_from_wrapper(
    #    RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID,
    #    THE_GRAPH_API_KEY,
    #    ["0x296fB7Be365498cdE47079c302e82A82721953d6"]
    #)
    balances = fetch_current_realtoken_balances_aggregated(
        REALTOKEN_GNOSIS_SUBGRAPH_ID,
        RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID,
        THE_GRAPH_API_KEY,
        ["0x296fB7Be365498cdE47079c302e82A82721953d6", "0x6C85cBF6807Cbe59830e8270Bfd8701c72348585"]
    )
    #pprint(balances)

    snapshot_now = BalanceSnapshot(
        as_of=datetime.now(timezone.utc),
        balances_by_token=balances["data"],
    )
    balance_snapshots_series = BalanceSnapshotSeries([snapshot_now])
    print(balance_snapshots_series.latest().balances_by_token.get("0x06d0e5aee443093ac5635b709c8a01342e59df19", Decimal("0")))



# ------ get_accepted_offers_by_seller_datetime --------
# ------ get_accepted_offers_by_buyer_datetime --------
TEST_NOW = False

from core.realtoken_event_history.event_fetchers import get_accepted_offers_by_buyer_datetime
from core.realtoken_event_history.event_fetchers import get_accepted_offers_by_seller_datetime
from core.services.utilities import get_pg_connection
from datetime import datetime

if TEST_NOW:
    pg_conn = get_pg_connection(*POSTGRES_DATA)
    try:
        offers_seller = get_accepted_offers_by_seller_datetime(pg_conn, WALLETS, datetime(2023, 1, 1), datetime(2026, 1, 1))
        offers_buyer = get_accepted_offers_by_buyer_datetime(pg_conn, WALLETS, datetime(2023, 1, 1), datetime(2026, 1, 1))
    finally:
        pg_conn.close()
    pprint(len(offers_seller))
    pprint(len(offers_buyer))

# ----- fetch_liquidations_rmm_v3 --------
TEST_NOW = False

from core.realtoken_event_history.event_fetchers import fetch_liquidations_rmm_v3
if TEST_NOW:
    rmmv3_liquidations = fetch_liquidations_rmm_v3(
        RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID,
        THE_GRAPH_API_KEY,
        WALLETS
    )
    #pprint(rmmv3_liquidations)
    #print(rmmv3_liquidations["count"])


# ------ RealtokenEventHistory and normalize_realt_purchases--------
TEST_NOW = False

from core.realtoken_event_history.model import RealtokenEventHistory
from core.realtoken_event_history.event_normalizers import normalize_realt_purchases
if TEST_NOW:
    realtoken_event_history = RealtokenEventHistory()
    normalize_realt_purchases(realt_purchases, realtoken_event_history)
    pprint(realtoken_event_history.as_dict())
    print(len(realtoken_event_history.tokens()))


# ------ RealtokenEventHistory and normalize_detokenisation--------
TEST_NOW = False

from core.realtoken_event_history.model import RealtokenEventHistory
from core.realtoken_event_history.event_normalizers import normalize_detokenisation
if TEST_NOW:
    realtoken_event_history = RealtokenEventHistory()
    normalize_detokenisation(detokenisations_events, realtoken_event_history)
    for k, v in realtoken_event_history.as_dict().items():
        for ev in v:
            print(ev)
    print(len(realtoken_event_history.tokens()))


# ------ RealtokenEventHistory and normalize_internal_transfer--------
TEST_NOW = False

from core.realtoken_event_history.model import RealtokenEventHistory
from core.realtoken_event_history.event_normalizers import normalize_internal_transfer
if TEST_NOW:
    realtoken_event_history = RealtokenEventHistory()
    normalize_internal_transfer(user_tranfers, realtoken_event_history, WALLETS)
    pprint(realtoken_event_history.as_dict())
    print(len(realtoken_event_history.tokens()))



# ------ RealtokenEventHistory and normalize_yam_offers--------
TEST_NOW = False

from core.realtoken_event_history.event_normalizers import normalize_yam_offers
import json
if TEST_NOW:
    realtoken_data = list_to_dict_by_uuid(fetch_json(REALTOKENS_LIST_URL) or [])
    with open('Ressources/blockchain_contracts.json', 'r') as blockchain_ressources_file:
        blockchain_ressources = json.load(blockchain_ressources_file)['contracts']
    realtoken_event_history = RealtokenEventHistory()
    normalize_yam_offers(offers_buyer + offers_seller, realtoken_event_history, WALLETS, blockchain_ressources, realtoken_data)
    #pprint(realtoken_event_history.as_dict())
    print(len(realtoken_event_history.tokens()))
    pprint(realtoken_event_history.events_for_tx('0x7b8b3c020c63e421f41b45dd92e7a2d084ce036155389b4f75e4b87af3280f06'))

# ------ RealtokenEventHistory and normalize_liquidations_rmm_v3--------
TEST_NOW = False

from core.realtoken_event_history.event_normalizers import normalize_liquidations_rmm_v3
if TEST_NOW:
    realtoken_history_data = list_to_dict_by_uuid(fetch_json(REALTOKEN_HISTORY_URL) or [])
    realtoken_event_history = RealtokenEventHistory()
    normalize_liquidations_rmm_v3(rmmv3_liquidations, realtoken_event_history, WALLETS, realtoken_history_data)
    pprint(realtoken_event_history.as_dict())


# ------- get_token_price_at_timestamp -------
TEST_NOW = False

from core.services.utilities import get_token_price_at_timestamp
if TEST_NOW:
    realtoken_history_data = list_to_dict_by_uuid(fetch_json(REALTOKEN_HISTORY_URL) or [])
    price = get_token_price_at_timestamp(
        realtoken_history_data,
        uuid="0x717bfbfa88859ac34f9772d92749c4b384c6b479",
        timestamp=datetime(2026, 4, 10),
    )
    print(price)