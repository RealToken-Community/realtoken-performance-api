from __future__ import annotations

from typing import List, Dict, Union, Any
from datetime import datetime

from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import RealDictCursor
from web3 import Web3


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
