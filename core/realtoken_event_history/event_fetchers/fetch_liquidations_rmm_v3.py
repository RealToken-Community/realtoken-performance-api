from __future__ import annotations

from typing import Any, Dict, Sequence, List
import requests


def fetch_liquidations_rmm_v3(
    subgraph_id: str,
    api_key: str,
    wallets: Sequence[str],
    first: int = 1000,
    timeout_s: int = 30,
) -> Dict[str, Any]:
    """
    Fetch LiquidationCall entries from an RMM v3 subgraph, filtered server-side:
    keep only those where one of `wallets` is either:
      - liquidationCall.user.id
      - liquidationCall.liquidator.id

    Pagination is done with `first` + `skip`.
    """

    if not subgraph_id or not api_key:
        raise ValueError("subgraph_id and api_key are required")

    wallets_lc = sorted({(w or "").lower() for w in wallets if w})
    if not wallets_lc:
        return {
            "subgraph_id": subgraph_id,
            "wallets": [],
            "count": 0,
            "items": [],
        }

    url = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"

    query = """
    query($first: Int!, $skip: Int!, $wallets: [String!]!) {
      liquidationCalls(
        first: $first
        skip: $skip
        orderBy: timestamp
        orderDirection: desc
        where: { or: [
          { user_: { id_in: $wallets } },
          { liquidator_: { id_in: $wallets } }
        ]}
      ) {
        id
        txHash
        timestamp
        user { id }
        liquidator { id }
        reserves { id }
        amounts
        debtAsset
        debtToCover
        receiveMethod
      }
    }
    """

    def _post(variables: Dict[str, Any]) -> Dict[str, Any]:
        resp = requests.post(
            url,
            json={"query": query, "variables": variables},
            timeout=timeout_s,
        )
        resp.raise_for_status()
        payload = resp.json()

        # If GraphQL errors exist, raise them (no fallback).
        if "errors" in payload and payload["errors"]:
            # Keep full errors for debugging, but also provide a readable message.
            msg = payload["errors"][0].get("message", "GraphQL error")
            raise RuntimeError(f"Subgraph query failed: {msg} | errors={payload['errors']}")

        return payload

    items_all: List[Dict[str, Any]] = []
    skip = 0

    while True:
        variables = {"first": first, "skip": skip, "wallets": wallets_lc}
        payload = _post(variables)

        items = (payload.get("data") or {}).get("liquidationCalls") or []
        if not items:
            break

        items_all.extend(items)

        # End if last page
        if len(items) < first:
            break

        skip += first

    return {
        "subgraph_id": subgraph_id,
        "wallets": wallets_lc,
        "count": len(items_all),
        "items": items_all,
    }
