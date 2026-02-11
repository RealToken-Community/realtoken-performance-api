from __future__ import annotations

from typing import List, Dict, Any, Union
from datetime import datetime

from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import RealDictCursor
from web3 import Web3


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
