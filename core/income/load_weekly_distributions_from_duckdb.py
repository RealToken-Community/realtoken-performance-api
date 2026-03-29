
"""
This module contains an alternative implementation using DuckDB instead of parquet.
It is NOT used in the current project and is only provided as a potential future
replacement for the parquet-based loader.
The goal is to keep this ready in case we decide to migrate the income storage layer to DuckDB.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import duckdb

from core.income.model import WeeklyDistribution


def load_weekly_distributions_from_duckdb(
    wallets: Iterable[str],
    duckdb_path: str | Path = Path("data/rent_files_duckdb/income.duckdb")
) -> List[WeeklyDistribution]:
    """
    Load weekly distributions from DuckDB for the given wallets.

    This function returns the same object structure as the previous parquet loader:
    - one WeeklyDistribution per (year, week, currency)
    - wallets = list of wallets that received revenue that week
    - revenues = {token: {wallet: amount}}
    - paid_in_currency = currency code

    Expected DuckDB schema:
    - investors(id, address)
    - tokens(id, address)
    - currencies(id, code)
    - weeks(id, year, week)
    - rents(week_id, currency_id, investor_id, token_id, amount)

    Args:
        wallets: Wallet addresses to load distributions for.
        duckdb_path: Path to the DuckDB file.

    Returns:
        A list of WeeklyDistribution objects.
    """
    normalized_wallets = tuple(
        dict.fromkeys(
            str(wallet).strip().lower()
            for wallet in wallets
            if wallet is not None and str(wallet).strip()
        )
    )

    if not normalized_wallets:
        return []

    db_path = Path(duckdb_path)
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db_path}")

    grouped: Dict[
        Tuple[int, int, str],
        Dict[str, Dict[str, float] | set[str]],
    ] = {}

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT
                w.year,
                w.week,
                c.code AS currency,
                i.address AS investor,
                t.address AS token,
                r.amount
            FROM rents AS r
            INNER JOIN weeks AS w
                ON w.id = r.week_id
            INNER JOIN currencies AS c
                ON c.id = r.currency_id
            INNER JOIN investors AS i
                ON i.id = r.investor_id
            INNER JOIN tokens AS t
                ON t.id = r.token_id
            WHERE i.address IN (SELECT UNNEST(?))
            ORDER BY
                w.year,
                w.week,
                c.code,
                i.address,
                t.address
            """,
            [list(normalized_wallets)],
        ).fetchall()
    finally:
        conn.close()

    for year, week, currency, wallet, token, amount in rows:
        key = (int(year), int(week), str(currency))

        item = grouped.get(key)
        if item is None:
            item = {
                "wallets": set(),
                "revenues": {},
            }
            grouped[key] = item

        item["wallets"].add(wallet)

        revenues = item["revenues"]
        by_wallet = revenues.get(token)
        if by_wallet is None:
            by_wallet = {}
            revenues[token] = by_wallet

        by_wallet[wallet] = by_wallet.get(wallet, 0.0) + float(amount)

    distributions: List[WeeklyDistribution] = []

    for (year, week, currency), item in grouped.items():
        distributions.append(
            WeeklyDistribution(
                year=year,
                week=week,
                wallets=list(item["wallets"]),
                revenues=item["revenues"],
                paid_in_currency=currency,
            )
        )

    return distributions