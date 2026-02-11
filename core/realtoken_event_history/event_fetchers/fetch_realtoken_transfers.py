from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence
from config.settings import REALTOKEN_WRAPPER_RMMV3
import requests

#Exclude RealToken transfers related to RMM V2 deposits and withdrawals (i.e. any transfer to/from a armmREALTOKEN)
armmREALTOKEN = [
    '0x7349C9eaA538e118725a6130e0f8341509b9f8A0',
    '0x05d909006cD38ba9E73db72C083081726B67971D',
    '0xa0c95bEbe678EeeD33A51dC24ACf60FD1900552a',
    '0x890aB77C02c2E85e78C05Bbb431b46baB0BEE220',
    '0x2cfAB362fDB1e8c7D58659D0C6b04F575EaABa25',
    '0x5c1b324eb3d14A73ee6E16023941B991E311C2E1',
    '0x713d8Dd85eBDba53Ee7251018fb61F16f3e0fa83',
    '0x4bc88bd3C6e4F8cAD49DfF22296A4985Ed0A2BF8',
    '0x9C94769A69b060bDb0b38B7B27E6374626a41E75',
    '0xe1028FE49DF359710E86D43f7545F7c5A19CF287',
    '0x7B8515a849c8b7aE5Da5809d1a30dB5A6c834202',
    '0xd4edB9c07F81a00C176C28f6e60009C012e76CEe',
    '0xBE1bc03b25F5fedFC9eDCd36CBe7f1444e26a8b8',
    '0x03C4413365C7376a0Ab90288C142bED8c05d2E97',
    '0xD61A1fBb282c37FD47e087b01F3276E2a0838aBC',
    '0xf46606E539ac74728577B9d48af110e1d75c7D55',
    '0x899826209622C59b7215dF32E5B2A6f4E0B848d2',
    '0x360DE0358583abAEDC189e8346f01be00D992865',
    '0x4441Cd59bF9e245BC45e9B02a4737aaCBcbCF1e7',
    '0xB5FCA80BaAEf5E97eE1a9Bf831f1E20CC3Ac026C',
    '0xB07c985e03d778d9dE8148a7e1de888561a6A2D3',
    '0x55dA3F96cD11d2EF423DE1aDefC30D5bFE6dA5e5',
    '0xBc2517950c17f5c3a2576af0CBEDfc15A6a5018f',
    '0x2FDb770d0097a65D8f4695711C20ca8B3a0d4B32',
    '0x126c34E6bBA10abC8d13D9c3f42c655dd8B7E007',
    '0xf1AA7a5461A7e05118bA161596819ff99DbF1BC3',
    '0x7Bd5E015c1a4Fb21046D517997c065a0B0d2F4C6',
    '0xB60067DB99991BDB3FA282741A08Fa54187a464D',
    '0xF2cD8969794A75c6E2Aad3c74282cF782abF9DfF',
    '0x682E27232f14313542dFa5b88A688cf683f6C3A7',
    '0xC2D14c5fbA6858b017e0Db65744f82aBBB63F4Ef',
    '0xE7306d98f44C1f0554F4dE60b8326FF7d6eeFb56',
    '0x87D7bfd30eAF692782DdC777d19e7872FEf3AB52',
    '0x30f801FFe1edE65623B122Ed3F12B940f8533764',
    '0x2aE90487D34AfE9FA93EB9835A524fBfC0A044f5',
    '0x92e2E1311C672b8b99CEB0b4f32B670C051b8C75',
    '0x8e98F4c20414F0f8D7D8B1F46cf8279E4bFe9Ba5',
    '0x6F4B280DA15D4Dc77F7C4D77176fD01B6B62D640',
    '0x9a374C1280c5d712A9c19F938be4a6C0520B9b00',
    '0x1492860052d85d9Df610d32F2353d8D1D805D884',
    '0xa022e030D271e8FaEe5dF3987953499aDFc370CB',
    '0xADA0bC8dFA325203A2C0cd9cc5cBb60cFe94D59A',
    '0xBB554a7523dCe56a26FaE111378ca7a56fbb0644',
    '0x1ff148D348acD788cE4d8C434e028D029430A091',
    '0xbd1125bc32bF94C741B9ac86afDF0F1251b9A9D6',
    '0x12833F7523017E86A31252Ddab45782a302A37Ae',
    '0xFa9cD6404F6Daf084a9b2f7eE1e194A88F9f4eBb',
    '0xF4b8f59eb36bc56cAD663f65cF61E7D162b74258',
    '0x4238Cf9AeEa3Cb3952ea516A5e730dA9f7542fBA',
    '0x420806e463420744bfDB87a84e7321B42d4E3c3e',
    '0xd918eFc50cA1f4d6D368C319432436C5070d1dEB',
    '0x375a0f470E13E3e59bCE9103aAAb9aC3f5f93bf5',
    '0xbA73A608201Cf1D079d42a4B08761BE0ebd90c0e',
    '0xb56aA48aD41D9f3277E05F66A5cF576FDB212f6A',
    '0x29480b83B31161Ebf7cB501df122C7e69C4A22e7',
    '0x1f14542Dcb4129063eF7b987fDB1E108f0b00B39',
    '0x0C58e0d48e8170704f22B292Ebf50115bAa254C1',
    '0x0250226Fa5852405061ff957a2180306f7df91Be'
    ]
armmREALTOKEN = {w.lower() for w in armmREALTOKEN} if armmREALTOKEN else set()

def _is_excluded_armm(t: Dict[str, Any]) -> bool:
    src = (t.get("source") or "").lower()
    dst = (t.get("destination") or "").lower()
    return (src in armmREALTOKEN) or (dst in armmREALTOKEN)


def fetch_realtoken_transfers(
    subgraph_id: str,
    api_key: str,
    wallets: Sequence[str],
    excluded_wallets: Optional[Sequence[str]] = None,
    first: int = 1000,
    timeout_s: int = 30,
) -> Dict[str, Any]:
    """
    Fetch inbound and outbound RealToken ERC-20 transfer events from a The Graph subgraph.

    Notes:
    - Returns:
        - outTransfers: transfers where `source` is in `wallets`
        - inTransfers:  transfers where `destination` is in `wallets`
    - Excludes counterparties listed in `excluded_wallets`(useful to exclude for example RMM supply or withdraw):
        - outTransfers: destination_not_in excluded_wallets
        - inTransfers:  source_not_in excluded_wallets
    - Paginates with `skip` to fetch more than `first` events.

    Args:
        subgraph_id: The Graph subgraph id.
        api_key: The Graph API key.
        wallets: Wallet addresses to monitor.
        excluded_wallets: Counterparty wallets to exclude. Defaults to the RMM wallet used for
            "supply withdrawn" internal movements.
        first: Page size for each query (applies to outTransfers and inTransfers).
        timeout_s: HTTP timeout in seconds.

    Returns:
        Dict with:
          - "data": {"outTransfers": [...], "inTransfers": [...]}
          - "meta": {"requests": int, "pages": int, "unique_outTransfers": int, "unique_inTransfers": int}
          - optional "errors": GraphQL errors if present

    Raises:
        ValueError: If `wallets` is empty.
        requests.HTTPError: If the HTTP request fails (non-2xx).
    """
    if not wallets:
        raise ValueError("wallets must not be empty")

    # Default exclusion list: exclude RMM supply and withdrawn that are not real transfer
    default_excluded = REALTOKEN_WRAPPER_RMMV3
    excluded = list(excluded_wallets) if excluded_wallets is not None else default_excluded

    wallets_lc = [w.lower() for w in wallets]
    excluded_lc = [w.lower() for w in excluded]

    endpoint = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
    headers = {"Content-Type": "application/json"}

    query = """
    query TransferEvents($wallets: [Bytes!]!, $excluded: [Bytes!]!, $first: Int!, $skip: Int!) {
      outTransfers: transferEvents(
        first: $first
        skip: $skip
        orderBy: timestamp
        orderDirection: desc
        where: { source_in: $wallets, destination_not_in: $excluded }
      ) {
        id
        token { address symbol }
        amount
        source
        destination
        timestamp
        transaction { id }
      }

      inTransfers: transferEvents(
        first: $first
        skip: $skip
        orderBy: timestamp
        orderDirection: desc
        where: { destination_in: $wallets, source_not_in: $excluded }
      ) {
        id
        token { address symbol }
        amount
        source
        destination
        timestamp
        transaction { id }
      }
    }
    """

    out_by_id: Dict[str, Dict[str, Any]] = {}
    in_by_id: Dict[str, Dict[str, Any]] = {}
    errors: List[Any] = []

    requests_count = 0
    pages = 0
    skip = 0

    while True:
        variables = {
            "wallets": wallets_lc,
            "excluded": excluded_lc,
            "first": int(first),
            "skip": int(skip),
        }

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
        out_transfers = data.get("outTransfers") or []
        in_transfers = data.get("inTransfers") or []

        for t in out_transfers:
            if _is_excluded_armm(t):
                continue
            tid = t.get("id")
            if tid:
                out_by_id[tid] = _add_log_index(t)
        
        for t in in_transfers:
            if _is_excluded_armm(t):
                continue
            tid = t.get("id")
            if tid:
                in_by_id[tid] = _add_log_index(t)
        
        pages += 1

        # Stop when both lists are shorter than the page size.
        if len(out_transfers) < first and len(in_transfers) < first:
            break

        skip += first

    result: Dict[str, Any] = {
        "data": {
            "outTransfers": list(out_by_id.values()),
            "inTransfers": list(in_by_id.values()),
        },
        "meta": {
            "requests": requests_count,
            "pages": pages,
            "unique_outTransfers": len(out_by_id),
            "unique_inTransfers": len(in_by_id),
        },
    }
    if errors:
        result["errors"] = errors

    return result

def _add_log_index(t: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(t)

    # subgraph event id format: "<txHash>-<logIndex>"
    event_id = t.get("id")
    if not isinstance(event_id, str):
        raise ValueError(f"Missing or invalid event id: {event_id}")

    try:
        _, log_idx = event_id.rsplit("-", 1)
        out["log_index"] = int(log_idx)
    except Exception as e:
        raise ValueError(f"Unexpected event id format: {event_id}") from e

    return out
