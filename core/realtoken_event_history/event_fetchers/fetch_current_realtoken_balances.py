from __future__ import annotations

from typing import Any, Dict, List, Sequence
import requests


def fetch_current_realtoken_balances(
    subgraph_id: str,
    api_key: str,
    wallets: Sequence[str],
    first: int = 1000,
    timeout_s: int = 30,
) -> Dict[str, Any]:
    """
    Fetch current (latest indexed) RealToken balances for multiple wallets from a The Graph subgraph.

    This queries `accountBalances` (current state) and paginates with `skip` for each wallet.

    Args:
        subgraph_id: The Graph subgraph id.
        api_key: The Graph API key.
        wallets: Wallet addresses to fetch balances for.
        first: Page size (accountBalances per request per wallet).
        timeout_s: HTTP timeout in seconds.

    Returns:
        Dict with:
          - "data": { wallet -> [ {amount, modified, block, token:{id,symbol}} ... ] }
          - "meta": {"requests": int, "pages": int, "wallets": int, "total_rows": int}
          - optional "errors": GraphQL errors if present

    Raises:
        ValueError: If `wallets` is empty.
        requests.HTTPError: If the HTTP request fails (non-2xx).
    """
    if not wallets:
        raise ValueError("wallets must not be empty")

    endpoint = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
    headers = {"Content-Type": "application/json"}

    wallets_lc = [w.lower() for w in wallets]

    query = """
    query CurrentBalances($account: Bytes!, $first: Int!, $skip: Int!) {
      accountBalances(
        first: $first
        skip: $skip
        where: { account: $account }
      ) {
        id
        amount
        modified
        token { id symbol }
      }
    }
    """

    results_by_wallet: Dict[str, List[Dict[str, Any]]] = {}
    errors: List[Any] = []
    requests_count = 0
    pages = 0
    total_rows = 0

    for wallet in wallets_lc:
        skip = 0
        rows_accum: List[Dict[str, Any]] = []

        while True:
            variables = {"account": wallet, "first": int(first), "skip": int(skip)}
            resp = requests.post(
                endpoint,
                json={"query": query, "variables": variables},
                headers=headers,
                timeout=timeout_s,
            )
            requests_count += 1
            resp.raise_for_status()

            payload = resp.json()
            if payload.get("errors"):
                errors.extend(payload["errors"])

            data = payload.get("data") or {}
            page_rows = data.get("accountBalances") or []

            rows_accum.extend(page_rows)
            pages += 1
            total_rows += len(page_rows)

            if len(page_rows) < first:
                break
            skip += first

        results_by_wallet[wallet] = rows_accum

    result: Dict[str, Any] = {
        "data": results_by_wallet,
        "meta": {
            "wallets": len(wallets_lc),
            "requests": requests_count,
            "pages": pages,
            "total_rows": total_rows,
        },
    }
    if errors:
        result["errors"] = errors

    return result
