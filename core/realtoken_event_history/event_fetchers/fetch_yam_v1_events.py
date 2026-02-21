from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple, Union
from datetime import datetime

from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import RealDictCursor
from web3 import Web3

from core.services.utilities import get_pg_connection



def fetch_yam_v1_events(
    wallets: Union[str, Sequence[str]],
    from_datetime: Union[str, datetime],
    to_datetime: Union[str, datetime],
    POSTGRES_DATA: Tuple[Any, ...],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Fetch YAM v1 OfferAccepted events from PostgreSQL for a set of wallets within a datetime range.

    This function returns two datasets:
      1) Offers accepted where the wallet(s) are the seller (o.seller_address)
      2) Offers accepted where the wallet(s) are the buyer (oe.buyer_address)

    Args:
        wallets: A single wallet address (string) or a sequence of wallet addresses.
        from_datetime: Start of the time window (inclusive), as ISO-8601 string or datetime.
        to_datetime: End of the time window (inclusive), as ISO-8601 string or datetime.
        POSTGRES_DATA: Positional arguments forwarded to `get_pg_connection(*POSTGRES_DATA)`
            (e.g. host, port, dbname, user, password depending on your implementation).

    Returns:
        A tuple of:
            - seller_events: List of event dicts where the wallet(s) are the seller
            - buyer_events: List of event dicts where the wallet(s) are the buyer

    Raises:
        Any exception raised by `get_pg_connection` or the underlying query functions.
        (The connection is always closed via `finally`.)
    """
    conn: PGConnection = get_pg_connection(*POSTGRES_DATA)
    try:
        seller_events = get_accepted_offers_by_seller_datetime(
            conn, wallets, from_datetime, to_datetime
        )
        buyer_events = get_accepted_offers_by_buyer_datetime(
            conn, wallets, from_datetime, to_datetime
        )
        return seller_events, buyer_events
    finally:
        conn.close()



def get_accepted_offers_by_seller_datetime(
    conn: PGConnection,
    seller_addresses: Union[str, List[str]],
    from_datetime: Union[str, datetime],
    to_datetime: Union[str, datetime],
) -> List[Dict[str, Any]]:
    """
    Retrieve accepted offers for specific seller addresses within a datetime range (PostgreSQL).

    Args:
        conn (PGConnection): An existing psycopg2 connection.
        seller_addresses (Union[str, List[str]]): Single seller address or list of seller addresses.
        from_datetime (Union[str, datetime]): Starting datetime (ISO string or datetime).
        to_datetime (Union[str, datetime]): Ending datetime (ISO string or datetime).

    Returns:
        List[Dict[str, Any]]: List of dictionaries with event + offer data.
    """
    # Normalize seller_addresses to list
    if isinstance(seller_addresses, str):
        seller_list = [seller_addresses]
    else:
        seller_list = list(seller_addresses)

    # Checksum addresses
    seller_list = [Web3.to_checksum_address(addr) for addr in seller_list]

    # Normalize datetime inputs
    if isinstance(from_datetime, datetime):
        from_dt = from_datetime
    else:
        from_dt = datetime.fromisoformat(from_datetime)

    if isinstance(to_datetime, datetime):
        to_dt = to_datetime
    else:
        to_dt = datetime.fromisoformat(to_datetime)

    query = """
        SELECT
            oe.offer_id,
            oe.event_type,
            oe.buyer_address,
            oe.amount_bought,
            oe.block_number,
            oe.transaction_hash,
            oe.log_index,
            oe.price_bought,
            oe.event_timestamp,
            o.offer_token,
            o.buyer_token,
            o.seller_address
        FROM offer_events AS oe
        JOIN offers AS o ON oe.offer_id = o.offer_id
        WHERE oe.event_type = 'OfferAccepted'
          AND o.seller_address = ANY(%s)
          AND oe.event_timestamp BETWEEN %s AND %s
        ORDER BY oe.event_timestamp ASC
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (seller_list, from_dt, to_dt))
        rows = cur.fetchall()

    # Convert RealDictRow → plain dict
    return [dict(r) for r in rows]


def get_accepted_offers_by_buyer_datetime(
    conn: PGConnection,
    buyer_addresses: Union[str, List[str]],
    from_datetime: Union[str, datetime],
    to_datetime: Union[str, datetime],
) -> List[Dict[str, Any]]:
    """
    Retrieve accepted offers for specific buyer addresses within a datetime range (PostgreSQL).

    Args:
        conn (PGConnection): An existing psycopg2 connection.
        buyer_addresses (Union[str, List[str]]): Single buyer address or list of buyer addresses.
        from_datetime (Union[str, datetime]): Starting datetime (ISO string or datetime).
        to_datetime (Union[str, datetime]): Ending datetime (ISO string or datetime).

    Returns:
        List[Dict[str, Any]]: List of dictionaries with event + offer data.
    """
    # Normalize buyer_addresses to a list
    if isinstance(buyer_addresses, str):
        buyer_list = [buyer_addresses]
    else:
        buyer_list = list(buyer_addresses)

    # Checksum addresses
    buyer_list = [Web3.to_checksum_address(addr) for addr in buyer_list]

    # Normalize datetime inputs
    if isinstance(from_datetime, datetime):
        from_dt = from_datetime
    else:
        from_dt = datetime.fromisoformat(from_datetime)

    if isinstance(to_datetime, datetime):
        to_dt = to_datetime
    else:
        to_dt = datetime.fromisoformat(to_datetime)

    query = """
        SELECT
            oe.offer_id,
            oe.event_type,
            oe.buyer_address,
            oe.amount_bought,
            oe.block_number,
            oe.transaction_hash,
            oe.log_index,
            oe.price_bought,
            oe.event_timestamp,
            o.offer_token,
            o.buyer_token,
            o.seller_address
        FROM offer_events AS oe
        JOIN offers AS o ON oe.offer_id = o.offer_id
        WHERE oe.event_type = 'OfferAccepted'
          AND oe.buyer_address = ANY(%s)
          AND oe.event_timestamp BETWEEN %s AND %s
        ORDER BY oe.event_timestamp ASC
    """

    # Using RealDictCursor returns rows as dicts directly
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (buyer_list, from_dt, to_dt))
        rows = cur.fetchall()

    # RealDictRow -> plain dict (optional but safer for serialization)
    return [dict(r) for r in rows]
