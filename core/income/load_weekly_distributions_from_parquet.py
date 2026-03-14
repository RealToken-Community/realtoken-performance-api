from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Tuple
import pyarrow.dataset as ds
from core.income.model import WeeklyDistribution


def load_weekly_distributions_from_parquet(
    wallets: Iterable[str],
    parquet_root: str | Path = "data/rent_files_parquet/parquet_by_year",
) -> List[WeeklyDistribution]:
    """
    Load weekly distributions from parquet files for the given wallets.

    This function is optimized for performance:
    - it reads parquet files through PyArrow Dataset,
    - selects only the required columns,
    - filters rows at scan level on the investor wallet,
    - avoids pandas entirely,
    - avoids unnecessary normalization because parquet rows are already normalized
      when written by the ingestion pipeline.

    Expected parquet columns:
    - year
    - week
    - currency
    - investor
    - token
    - amount

    Args:
        wallets: Wallet addresses to load distributions for.
        parquet_root: Root folder containing yearly quarter parquet files.

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

    parquet_root = Path(parquet_root)
    if not parquet_root.exists():
        raise FileNotFoundError(f"Parquet root folder not found: {parquet_root}")

    # Do not use hive partitioning here because the parquet files already contain
    # the "year" column, which can create a schema conflict with the folder
    # partition value (e.g. int64 in file vs int32 in partition).
    dataset = ds.dataset(parquet_root, format="parquet")

    grouped: Dict[
        Tuple[int, int, str],
        Dict[str, Dict[str, float] | set[str]],
    ] = {}

    scanner = dataset.scanner(
        columns=["year", "week", "currency", "investor", "token", "amount"],
        filter=ds.field("investor").isin(normalized_wallets),
        use_threads=True,
    )

    for batch in scanner.to_batches():
        year_col = batch.column("year")
        week_col = batch.column("week")
        currency_col = batch.column("currency")
        investor_col = batch.column("investor")
        token_col = batch.column("token")
        amount_col = batch.column("amount")

        for i in range(batch.num_rows):
            year = year_col[i].as_py()
            week = week_col[i].as_py()
            currency = currency_col[i].as_py()
            wallet = investor_col[i].as_py()
            token = token_col[i].as_py()
            amount = amount_col[i].as_py()

            key = (year, week, currency)

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