from typing import List, Dict, Any
from config.settings import TRUSTED_INTERMEDIARY_FOR_USER_ID
import requests

def get_all_user_linked_addresses(
    wallet_address: str,
    api_key: str,
    subgraph_id: str,
    timeout: int = 15,
) -> List[str]:
    """
    Resolve all wallet addresses associated with the same KYC/userId as `wallet_address`,
    by querying a The Graph subgraph.

    1) Query: account(id=wallet) -> latest userId (order by timestamp desc, first=1)
    2) Build composite id: "{TRUSTED_INTERMEDIARY_FOR_USER_ID}-{userId}"
    3) Query: accounts(where: { userIds: [composite_id] }) -> list of addresses

    Notes:
      - `wallet_address` and `TRUSTED_INTERMEDIARY_FOR_USER_ID` are normalized to lowercase, as subgraph IDs
        are typically stored in lowercase.

    Args:
        wallet_address: The wallet address to start from (case-insensitive).
        api_key: The Graph gateway API key.
        subgraph_id: Subgraph ID to query.
        TRUSTED_INTERMEDIARY_FOR_USER_ID: Address prefix used to build the composite userId.
        timeout: HTTP timeout in seconds.

    Returns:
        List of wallet addresses linked to the same KYC/userId.
        Returns an empty list if no userId exists for the provided wallet.

    Raises:
        ValueError: If a required argument is empty.
        requests.RequestException: On network-level errors (timeouts, connection errors, etc.).
        requests.HTTPError: If the HTTP status is not 2xx.
        KeyError/TypeError: If the response shape is unexpected.
    """
    if not wallet_address:
        raise ValueError("wallet_address must not be empty")
    if not api_key:
        raise ValueError("api_key must not be empty")
    if not subgraph_id:
        raise ValueError("subgraph_id must not be empty")
    if not TRUSTED_INTERMEDIARY_FOR_USER_ID:
        raise ValueError("TRUSTED_INTERMEDIARY_FOR_USER_ID must not be empty")

    endpoint = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
    address = wallet_address.lower()
    ti = TRUSTED_INTERMEDIARY_FOR_USER_ID.lower()

    def post_graphql(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        resp = requests.post(endpoint, json={"query": query, "variables": variables}, timeout=timeout)
        resp.raise_for_status()
        payload: Dict[str, Any] = resp.json()
        # If the subgraph returns GraphQL errors, surface them clearly (caller handles exceptions).
        if payload.get("errors"):
            raise RuntimeError(f"GraphQL errors: {payload['errors']}")
        return payload["data"]

    # 1) wallet -> latest userId
    q_user_id = """
    query GetUserId($address: String!) {
      account(id: $address) {
        userIds(orderBy: timestamp, orderDirection: desc, first: 1) {
          userId
        }
      }
    }
    """
    data = post_graphql(q_user_id, {"address": address})
    user_ids = (data.get("account") or {}).get("userIds") or []
    if not user_ids:
        return []

    user_id = user_ids[0]["userId"]
    composite_user_id = f"{ti}-{user_id}"

    # 2) composite userId -> all linked addresses
    q_addresses = """
    query GetUserAddresses($userId: String!) {
      accounts(where: { userIds: [$userId] }) {
        address
      }
    }
    """
    data = post_graphql(q_addresses, {"userId": composite_user_id})
    accounts = data.get("accounts") or []
    return [a["address"] for a in accounts]
