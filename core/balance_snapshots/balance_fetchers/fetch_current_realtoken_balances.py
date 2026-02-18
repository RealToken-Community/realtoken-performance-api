from __future__ import annotations

from typing import Any, Dict, List, Sequence
from decimal import Decimal
import requests


def fetch_current_realtoken_balances_aggregated(
    subgraph_id_realtokens: str,
    subgraph_id_wrapper: str,
    api_key: str,
    wallets: Sequence[str],
    first: int = 1000,
    timeout_s: int = 30,
) -> Dict[str, Any]:
    """
    Fetch current RealToken balances for multiple wallets from:
      - RealTokens subgraph (accountBalances)
      - Wrapper subgraph (userRealTokens)

    Then aggregate into ONE global map:
        token_address -> total_amount_across_all_wallets

    Output:
      {
        "data": { "<token_address>": Decimal("..."), ... },
        "meta": { ... },
        "errors": [ ... ]   # optional
      }
    """
    if not wallets:
        raise ValueError("wallets must not be empty")

    wallets_lc = [w.lower() for w in wallets]

    graph_res = fetch_current_realtoken_balances_the_graph(
        subgraph_id=subgraph_id_realtokens,
        api_key=api_key,
        wallets=wallets_lc,
        first=first,
        timeout_s=timeout_s,
    )

    wrapper_res = fetch_current_realtoken_balances_from_wrapper(
        subgraph_id=subgraph_id_wrapper,
        api_key=api_key,
        wallets=wallets_lc,
        first=first,
        timeout_s=timeout_s,
    )

    def _to_decimal(x: Any) -> Decimal:
        if x is None:
            return Decimal("0")
        if isinstance(x, Decimal):
            return x
        try:
            return Decimal(str(x))
        except Exception:
            return Decimal("0")

    totals: Dict[str, Decimal] = {}

    # --- RealTokens subgraph (accountBalances) ---
    graph_data: Dict[str, List[Dict[str, Any]]] = graph_res.get("data") or {}
    for _wallet, rows in graph_data.items():
        for row in rows or []:
            token = row.get("token") or {}

            # token id in that subgraph is usually the token address
            token_addr = (token.get("id") or token.get("address") or "").lower()
            if not token_addr:
                continue

            amount = _to_decimal(row.get("amount"))
            if amount == 0:
                continue

            totals[token_addr] = totals.get(token_addr, Decimal("0")) + amount

    # --- Wrapper subgraph (userRealTokens) ---
    wrapper_data: Dict[str, List[Dict[str, Any]]] = wrapper_res.get("data") or {}
    for _wallet, rows in wrapper_data.items():
        for row in rows or []:
            token = row.get("token") or {}

            token_addr = (token.get("address") or token.get("id") or "").lower()
            if not token_addr:
                continue

            amount = _to_decimal(row.get("amount"))  # already Decimal in your wrapper function
            if amount == 0:
                continue

            totals[token_addr] = totals.get(token_addr, Decimal("0")) + amount

    # merge meta/errors
    errors: List[Any] = []
    if graph_res.get("errors"):
        errors.extend(graph_res["errors"])
    if wrapper_res.get("errors"):
        errors.extend(wrapper_res["errors"])

    meta = {
        "wallets": len(wallets_lc),
        "requests": int((graph_res.get("meta") or {}).get("requests", 0))
        + int((wrapper_res.get("meta") or {}).get("requests", 0)),
        "pages": int((graph_res.get("meta") or {}).get("pages", 0))
        + int((wrapper_res.get("meta") or {}).get("pages", 0)),
        "total_rows": {
            "realtokens_subgraph": int((graph_res.get("meta") or {}).get("total_rows", 0)),
            "wrapper_subgraph": int((wrapper_res.get("meta") or {}).get("total_rows", 0)),
        },
        "unique_tokens": len(totals),
    }

    result: Dict[str, Any] = {"data": totals, "meta": meta}
    if errors:
        result["errors"] = errors
    return result



def fetch_current_realtoken_balances_the_graph(
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
          - "data": { wallet -> [ {amount, modified, block, token:{id,symbol}} ... ] } (zero balances are excluded)
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

            filtered_rows = [
                row for row in page_rows
                if row.get("amount") not in (None, "0", 0)
            ]
            
            rows_accum.extend(filtered_rows)
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


def fetch_current_realtoken_balances_from_wrapper(
    subgraph_id: str,
    api_key: str,
    wallets: Sequence[str],
    first: int = 1000,
    timeout_s: int = 30,
) -> Dict[str, Any]:
    """
    Fetch current (latest indexed) RealToken balances for multiple wallets
    from a The Graph subgraph using userRealTokens.

    Pagination: uses `skip` + `first`.

    - amount is returned as uint256 string
    - normalized to Decimal via amount / (10 ** decimals)
    - zero balances are excluded
    """

    if not wallets:
        raise ValueError("wallets must not be empty")

    endpoint = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
    headers = {"Content-Type": "application/json"}

    wallets_lc = [w.lower() for w in wallets]

    query = """
    query WrapperBalances($users: [Bytes!]!, $first: Int!, $skip: Int!) {
      userRealTokens(
        where: { user_in: $users }
        first: $first
        skip: $skip
      ) {
        id
        user { id }
        amount
        token { symbol address decimals }
      }
    }
    """

    def _normalize_amount(amount_raw: Any, decimals: Any) -> Decimal:
        if amount_raw is None:
            return Decimal("0")

        try:
            raw = Decimal(str(amount_raw))
        except Exception:
            return Decimal("0")

        try:
            dec = int(decimals) if decimals is not None else 0
        except Exception:
            dec = 0

        if dec <= 0:
            return raw

        return raw / (Decimal(10) ** dec)

    results_by_wallet: Dict[str, List[Dict[str, Any]]] = {w: [] for w in wallets_lc}
    errors: List[Any] = []
    requests_count = 0
    pages = 0
    total_rows = 0

    skip = 0
    while True:
        variables = {"users": wallets_lc, "first": int(first), "skip": int(skip)}

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
        page_rows = data.get("userRealTokens") or []

        pages += 1
        total_rows += len(page_rows)

        for row in page_rows:
            user_obj = row.get("user") or {}
            user_id = (user_obj.get("id") or "").lower()

            token = row.get("token") or {}
            token_decimals = token.get("decimals")

            amount_norm = _normalize_amount(row.get("amount"), token_decimals)

            # Skip zero balances
            if amount_norm == 0:
                continue

            out_row = {
                "id": row.get("id"),
                "user": {"id": user_id} if user_id else user_obj,
                "amount": amount_norm,  # keep as Decimal
                "token": {
                    "symbol": token.get("symbol"),
                    "address": (token.get("address") or "").lower() if token.get("address") else token.get("address"),
                    "decimals": token_decimals,
                },
            }

            if user_id:
                results_by_wallet.setdefault(user_id, []).append(out_row)

        if len(page_rows) < first:
            break

        skip += first

        if pages > 100000:
            errors.append(
                {"message": "Safety stop: pagination exceeded 100000 pages."}
            )
            break

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

