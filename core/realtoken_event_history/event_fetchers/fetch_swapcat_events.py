from __future__ import annotations

from typing import Any, Dict, List, Sequence
import requests


def fetch_swapcat_events(
    subgraph_id: str,
    api_key: str,
    wallets: Sequence[str],
    first: int = 1000,
    timeout_s: int = 30,
) -> Dict[str, Any]:
    """
    Fetch all Purchase objects from the SwapCat subgraph where at least one
    tracked wallet appears either as buyer or seller.

    Important:
    - The subgraph exposes a root Purchase entity (`purchase` / `purchases`).
    - There is no distinct root `sales` entity.
    - "Sales" must be inferred later from the role played by the wallet in the
      Purchase object (buyer vs seller).
    - This function does NOT rebuild sales. It only returns matching Purchase
      objects in a single flat list.

    Strategy:
    - Query the root `purchases` entity twice inside one GraphQL request:
        1. purchases where buyer is in wallets
        2. purchases where seller is in wallets
    - Paginate with skip/first
    - Deduplicate by purchase id
    - Return all matching Purchase objects unfiltered in one list

    Returns:
        Dict with:
          - "data": list of unique Purchase objects
          - "meta": {"requests": int, "pages": int, ...}
          - optional "errors": GraphQL/HTTP errors if present
    """
    endpoint = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
    page_size = max(1, min(int(first), 1000))

    normalized_wallets = _normalize_wallets(wallets)
    if not normalized_wallets:
        return {
            "data": [],
            "meta": {
                "requests": 0,
                "pages": 0,
                "wallets": 0,
                "wallet_batches": 0,
                "page_size": page_size,
                "events_raw": 0,
                "events_deduped": 0,
            },
        }

    batch_size = _choose_wallet_batch_size(len(normalized_wallets))
    wallet_batches = [
        normalized_wallets[i : i + batch_size]
        for i in range(0, len(normalized_wallets), batch_size)
    ]

    query = """
    query FetchSwapCatPurchases(
      $buyerIds: [String!]
      $sellerIds: [String!]
      $first: Int!
      $skip: Int!
    ) {
      buyerMatches: purchases(
        first: $first
        skip: $skip
        orderBy: createdAtTimestamp
        orderDirection: asc
        where: { buyer_in: $buyerIds }
      ) {
        id
        txHash
        price
        quantity
        createdAtBlock
        createdAtTimestamp
        offerToken {
          id
          address
          symbol
          decimals
        }
        buyerToken {
          id
          address
          symbol
          decimals
        }
        buyer {
          id
          address
        }
        seller {
          id
          address
        }
      }

      sellerMatches: purchases(
        first: $first
        skip: $skip
        orderBy: createdAtTimestamp
        orderDirection: asc
        where: { seller_in: $sellerIds }
      ) {
        id
        txHash
        price
        quantity
        createdAtBlock
        createdAtTimestamp
        offerToken {
          id
          address
          symbol
          decimals
        }
        buyerToken {
          id
          address
          symbol
          decimals
        }
        buyer {
          id
          address
        }
        seller {
          id
          address
        }
      }
    }
    """

    session = requests.Session()
    errors: List[Any] = []
    purchases_by_id: Dict[str, Dict[str, Any]] = {}

    request_count = 0
    page_count = 0
    raw_event_count = 0

    for batch in wallet_batches:
        skip = 0

        while True:
            variables = {
                "buyerIds": batch,
                "sellerIds": batch,
                "first": page_size,
                "skip": skip,
            }

            try:
                response = session.post(
                    endpoint,
                    json={"query": query, "variables": variables},
                    timeout=timeout_s,
                )
                request_count += 1
                page_count += 1
                response.raise_for_status()
                payload = response.json()
            except requests.RequestException as exc:
                errors.append(
                    {
                        "message": f"HTTP error while querying The Graph: {exc}",
                        "batch_wallets": batch,
                        "skip": skip,
                    }
                )
                break
            except ValueError as exc:
                errors.append(
                    {
                        "message": f"Invalid JSON response from The Graph: {exc}",
                        "batch_wallets": batch,
                        "skip": skip,
                    }
                )
                break

            if payload.get("errors"):
                errors.extend(payload["errors"])
                break

            data = payload.get("data") or {}
            buyer_matches = data.get("buyerMatches") or []
            seller_matches = data.get("sellerMatches") or []

            raw_event_count += len(buyer_matches) + len(seller_matches)

            for item in buyer_matches:
                purchases_by_id[str(item["id"])] = item

            for item in seller_matches:
                purchases_by_id[str(item["id"])] = item

            if len(buyer_matches) < page_size and len(seller_matches) < page_size:
                break

            skip += page_size

    data_out = sorted(
        purchases_by_id.values(),
        key=lambda x: (
            _safe_int(x.get("createdAtTimestamp")),
            _safe_int(x.get("createdAtBlock")),
            str(x.get("id", "")),
        ),
    )

    result: Dict[str, Any] = {
        "data": data_out,
        "meta": {
            "requests": request_count,
            "pages": page_count,
            "wallets": len(normalized_wallets),
            "wallet_batches": len(wallet_batches),
            "wallet_batch_size": batch_size,
            "page_size": page_size,
            "events_raw": raw_event_count,
            "events_deduped": len(data_out),
            "endpoint": endpoint,
        },
    }

    if errors:
        result["errors"] = errors

    return result


def _normalize_wallets(wallets: Sequence[str]) -> List[str]:
    """Normalize wallet addresses to lowercase, remove duplicates, keep valid 0x values."""
    seen = set()
    out: List[str] = []

    for w in wallets:
        if w is None:
            continue
        w_norm = str(w).strip().lower()
        if not w_norm or not w_norm.startswith("0x"):
            continue
        if w_norm in seen:
            continue
        seen.add(w_norm)
        out.append(w_norm)

    return out


def _choose_wallet_batch_size(n_wallets: int) -> int:
    """Simple heuristic to reduce requests while keeping payload sizes reasonable."""
    if n_wallets <= 200:
        return n_wallets
    if n_wallets <= 1000:
        return 250
    return 500


def _safe_int(value: Any) -> int:
    """Safely cast a value to int for sorting."""
    try:
        return int(value)
    except Exception:
        return -1