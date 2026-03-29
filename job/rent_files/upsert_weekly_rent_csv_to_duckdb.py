"""
This module contains an alternative implementation using DuckDB instead of parquet.
It is NOT used in the current project and is only provided as a potential future
replacement for the parquet-based loader.
The goal is to keep this ready in case we decide to migrate the income storage layer to DuckDB.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import IO, Any, Dict, List, Optional, Tuple, Union

import duckdb
import pandas as pd

import logging

logger = logging.getLogger(__name__)


CsvInput = Union[str, Path, IO[str]]

_RE_EVM_ADDRESS = re.compile(r"^0x[a-fA-F0-9]{40}$")
_RE_CURRENCY_CODE = re.compile(r"^[A-Z0-9._-]{1,16}$")


def _is_evm_address(value: Any) -> bool:
    return isinstance(value, str) and bool(_RE_EVM_ADDRESS.match(value.strip()))


def _normalize(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_currency_code(value: Any) -> str:
    currency = _normalize(value).upper()
    if not currency:
        raise ValueError("Currency cannot be empty.")
    if not _RE_CURRENCY_CODE.match(currency):
        raise ValueError(
            f"Invalid currency code '{currency}'. "
            "Expected 1..16 chars in [A-Z0-9._-]."
        )
    return currency


def _open_text_stream(csv_input: CsvInput) -> IO[str]:
    if isinstance(csv_input, (str, Path)):
        return open(str(csv_input), "r", encoding="utf-8-sig", newline="")
    if hasattr(csv_input, "seek"):
        csv_input.seek(0)
    return csv_input  # type: ignore[return-value]


def _build_week_id(year: int, week: int) -> int:
    if week < 1 or week > 53:
        raise ValueError(f"Invalid ISO week: {week}. Expected 1..53.")
    return int(year) * 100 + int(week)


def _detect_header_and_token_row(
    csv_input: CsvInput,
    *,
    investor_column_candidates: tuple[str, ...] = (
        "Investor",
        "Adresse ETH",
        "Adress ETH payOut",
    ),
    preview_lines: int = 400,
    token_row_search_window: int = 60,
) -> Tuple[int, List[str], str]:
    stream = _open_text_stream(csv_input)
    close_stream = isinstance(csv_input, (str, Path))

    try:
        reader = csv.reader(stream, delimiter=",")
        preview_rows: List[List[str]] = []
        for i, row in enumerate(reader):
            preview_rows.append(row)
            if i + 1 >= preview_lines:
                break
    finally:
        if close_stream:
            stream.close()

    header_row_idx: Optional[int] = None
    investor_column_name_found: Optional[str] = None

    for idx, row in enumerate(preview_rows):
        normalized = [_normalize(c) for c in row]

        for candidate in investor_column_candidates:
            if candidate in normalized:
                header_row_idx = idx
                investor_column_name_found = candidate
                break

        if header_row_idx is not None:
            break

    if header_row_idx is None or investor_column_name_found is None:
        raise ValueError(
            f"Could not detect header row containing one of: {investor_column_candidates}"
        )

    token_row_idx: Optional[int] = None

    start_above = max(0, header_row_idx - token_row_search_window)
    for idx in range(header_row_idx - 1, start_above - 1, -1):
        normalized = [_normalize(c) for c in preview_rows[idx]]
        if any(_is_evm_address(c) for c in normalized):
            token_row_idx = idx
            break

    if token_row_idx is None:
        end_below = min(len(preview_rows), header_row_idx + 1 + token_row_search_window)
        for idx in range(header_row_idx + 1, end_below):
            normalized = [_normalize(c) for c in preview_rows[idx]]
            if any(_is_evm_address(c) for c in normalized):
                token_row_idx = idx
                break

    if token_row_idx is None:
        raise ValueError("Could not detect token-address row around the header.")

    token_row = preview_rows[token_row_idx]

    if not any(_is_evm_address(_normalize(c)) for c in token_row):
        raise ValueError("Token-address row detected, but no valid EVM token address found.")

    return header_row_idx, token_row, investor_column_name_found


def _build_column_to_token_mapping(
    columns: List[str],
    token_row: List[str],
) -> Dict[str, str]:
    first_token_col_idx: Optional[int] = None

    for col_idx in range(len(columns)):
        cell = token_row[col_idx] if col_idx < len(token_row) else ""
        if _is_evm_address(_normalize(cell)):
            first_token_col_idx = col_idx
            break

    if first_token_col_idx is None:
        raise ValueError("Could not detect the first token column.")

    current_token: Optional[str] = None
    col_to_token: Dict[str, str] = {}

    for col_idx in range(first_token_col_idx, len(columns)):
        col_name = columns[col_idx]
        cell = token_row[col_idx] if col_idx < len(token_row) else ""
        value = _normalize(cell)

        if _is_evm_address(value):
            current_token = value.lower()
        elif value in ("", "-", "nan"):
            if current_token is None:
                raise ValueError(
                    f"Column '{col_name}' inherits previous token, "
                    "but no previous token was detected."
                )
        else:
            if current_token is None:
                raise ValueError(
                    f"Unexpected token-row value '{value}' in column '{col_name}' "
                    "before any token was detected."
                )

        if current_token is None:
            raise ValueError(f"No token could be mapped for column '{col_name}'.")

        col_to_token[col_name] = current_token

    token_addresses = sorted(set(col_to_token.values()))
    if not token_addresses:
        raise ValueError("No token addresses could be mapped from token columns.")
    if not all(_is_evm_address(t) for t in token_addresses):
        raise ValueError("At least one mapped token address is not a valid EVM address.")

    return col_to_token


def _normalize_long_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["week_id"] = out["week_id"].astype(int)
    out["currency_code"] = out["currency_code"].map(_normalize_currency_code)
    out["investor"] = out["investor"].astype(str).str.lower().str.strip()
    out["token"] = out["token"].astype(str).str.lower().str.strip()
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce").fillna(0.0).astype(float)

    out = out.sort_values(
        by=["week_id", "currency_code", "investor", "token"]
    ).reset_index(drop=True)

    return out


def _parse_weekly_csv_to_long_df(
    csv_path: Union[str, Path],
    *,
    year: int,
    week: int,
    paid_in_currency: str = "USD",
) -> pd.DataFrame:
    header_row_idx, token_row, investor_column_name = _detect_header_and_token_row(csv_path)

    header_df = pd.read_csv(
        csv_path,
        skiprows=header_row_idx,
        nrows=0,
        engine="c",
    )
    columns = list(header_df.columns)

    if investor_column_name not in columns:
        raise ValueError(f"Column '{investor_column_name}' not found after header detection.")

    col_to_token = _build_column_to_token_mapping(columns, token_row)

    token_columns = [col for col in columns if col in col_to_token]
    if not token_columns:
        raise ValueError("No token columns detected.")

    usecols = [investor_column_name] + token_columns

    df = pd.read_csv(
        csv_path,
        skiprows=header_row_idx,
        header=0,
        usecols=usecols,
        engine="c",
    )

    df[investor_column_name] = (
        df[investor_column_name]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    investor_mask = df[investor_column_name].apply(_is_evm_address)
    investors_df = df.loc[investor_mask].copy()

    if investors_df.empty:
        raise ValueError(
            f"No valid EVM investor address found in the '{investor_column_name}' column."
        )

    token_block = investors_df[token_columns].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    token_labels = [col_to_token[col].lower() for col in token_columns]
    token_block.columns = token_labels
    token_block = token_block.T.groupby(level=0).sum().T

    long_df = (
        token_block
        .assign(investor=investors_df[investor_column_name].values)
        .melt(id_vars=["investor"], var_name="token", value_name="amount")
    )

    long_df["investor"] = long_df["investor"].astype(str).str.lower().str.strip()
    long_df["token"] = long_df["token"].astype(str).str.lower().str.strip()
    long_df["amount"] = pd.to_numeric(long_df["amount"], errors="coerce").fillna(0.0)

    long_df = long_df[long_df["amount"] != 0.0].copy()

    long_df = (
        long_df.groupby(["investor", "token"], as_index=False)["amount"]
        .sum()
    )

    if long_df.empty:
        raise ValueError(
            "Parsing succeeded structurally, but no non-zero investor/token revenue row was found."
        )

    if not long_df["investor"].map(_is_evm_address).all():
        raise ValueError("At least one parsed investor is not a valid EVM address.")

    if not long_df["token"].map(_is_evm_address).all():
        raise ValueError("At least one parsed token is not a valid EVM address.")

    week_id = _build_week_id(int(year), int(week))

    long_df.insert(0, "currency_code", _normalize_currency_code(paid_in_currency))
    long_df.insert(0, "week_id", week_id)

    return _normalize_long_df(long_df)


def _initialize_duckdb_db(db_path: Union[str, Path]) -> Path:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    db_already_exists = db_path.exists()

    conn = duckdb.connect(str(db_path))
    try:
        if not db_already_exists:
            logger.info(f"Initializing new DuckDB at {db_path}.")


        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS investors_seq START 1
        """)
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS tokens_seq START 1
        """)
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS currencies_seq START 1
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS investors (
                id INTEGER PRIMARY KEY,
                address VARCHAR NOT NULL UNIQUE
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                id INTEGER PRIMARY KEY,
                address VARCHAR NOT NULL UNIQUE
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS currencies (
                id SMALLINT PRIMARY KEY,
                code VARCHAR NOT NULL UNIQUE
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS weeks (
                id INTEGER PRIMARY KEY,
                year INTEGER NOT NULL,
                week INTEGER NOT NULL,
                UNIQUE (year, week)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS rents (
                week_id INTEGER NOT NULL,
                currency_id SMALLINT NOT NULL,
                investor_id INTEGER NOT NULL,
                token_id INTEGER NOT NULL,
                amount DOUBLE NOT NULL
            )
        """)

        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_investors_address
            ON investors (address)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_rents_investor_week_token
            ON rents (investor_id, week_id, token_id)
        """)

    finally:
        conn.close()

    return db_path


def _ensure_week_exists(
    conn: duckdb.DuckDBPyConnection,
    *,
    week_id: int,
    year: int,
    week: int,
) -> None:
    conn.execute("""
        INSERT INTO weeks (id, year, week)
        SELECT ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM weeks
            WHERE id = ?
        )
    """, [week_id, int(year), int(week), week_id])


def _ensure_currency_exists(
    conn: duckdb.DuckDBPyConnection,
    *,
    currency_code: str,
) -> int:
    currency_code = _normalize_currency_code(currency_code)

    existing = conn.execute("""
        SELECT id
        FROM currencies
        WHERE code = ?
    """, [currency_code]).fetchone()

    if existing is not None:
        return int(existing[0])

    max_smallint = 32767

    current_max_id_row = conn.execute("""
        SELECT COALESCE(MAX(id), 0)
        FROM currencies
    """).fetchone()
    current_max_id = int(current_max_id_row[0]) if current_max_id_row is not None else 0

    if current_max_id >= max_smallint:
        raise OverflowError(
            "Cannot insert new currency: SMALLINT id space is exhausted "
            f"(max positive value = {max_smallint})."
        )

    conn.execute("""
        INSERT INTO currencies (id, code)
        VALUES (
            CAST(nextval('currencies_seq') AS SMALLINT),
            ?
        )
    """, [currency_code])

    inserted = conn.execute("""
        SELECT id
        FROM currencies
        WHERE code = ?
    """, [currency_code]).fetchone()

    if inserted is None:
        raise RuntimeError(f"Currency '{currency_code}' was inserted but could not be re-read.")

    return int(inserted[0])


def upsert_weekly_rent_csv_to_duckdb(
    csv_path: Union[str, Path],
    year: int,
    week: int,
    *,
    duckdb_root: Union[str, Path] = "data/rent_files_duckdb",
    duckdb_file_name: str = "income",
    paid_in_currency: str = "USD",
    replace_existing_week_currency_data: bool = False,
) -> Path:
    """
    Parse one weekly CSV and insert all parsed rows into a normalized DuckDB database.

    Schema:
      - investors(id, address)
      - tokens(id, address)
      - currencies(id, code)
      - weeks(id, year, week)
      - rents(week_id, currency_id, investor_id, token_id, amount)

    Behavior:
      - If DB does not exist: create it
      - If referenced week / investor / token / currency does not exist: create it automatically
      - By default: append parsed rows
      - If replace_existing_week_currency_data=True:
          delete rows for that (week_id, currency_id) before inserting the new batch
    """
    csv_path = Path(csv_path)
    duckdb_root = Path(duckdb_root)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    db_path = duckdb_root / f"{duckdb_file_name}.duckdb"
    db_path = _initialize_duckdb_db(db_path)

    new_week_df = _parse_weekly_csv_to_long_df(
        csv_path,
        year=int(year),
        week=int(week),
        paid_in_currency=paid_in_currency,
    )

    if new_week_df.empty:
        return db_path

    week_id = _build_week_id(int(year), int(week))
    currency_code = _normalize_currency_code(paid_in_currency)

    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("BEGIN TRANSACTION")

        _ensure_week_exists(conn, week_id=week_id, year=int(year), week=int(week))
        currency_id = _ensure_currency_exists(conn, currency_code=currency_code)

        unique_investors_df = (
            new_week_df[["investor"]]
            .drop_duplicates()
            .rename(columns={"investor": "address"})
            .reset_index(drop=True)
        )

        unique_tokens_df = (
            new_week_df[["token"]]
            .drop_duplicates()
            .rename(columns={"token": "address"})
            .reset_index(drop=True)
        )

        prepared_df = (
            new_week_df[["week_id", "investor", "token", "amount"]]
            .copy()
        )
        prepared_df["currency_id"] = int(currency_id)

        conn.register("unique_investors_view", unique_investors_df)
        conn.register("unique_tokens_view", unique_tokens_df)
        conn.register("new_week_df_view", prepared_df)

        conn.execute("""
            INSERT INTO investors (id, address)
            SELECT
                nextval('investors_seq') AS id,
                src.address
            FROM unique_investors_view AS src
            LEFT JOIN investors AS i
                ON i.address = src.address
            WHERE i.address IS NULL
        """)

        conn.execute("""
            INSERT INTO tokens (id, address)
            SELECT
                nextval('tokens_seq') AS id,
                src.address
            FROM unique_tokens_view AS src
            LEFT JOIN tokens AS t
                ON t.address = src.address
            WHERE t.address IS NULL
        """)

        if replace_existing_week_currency_data:
            conn.execute("""
                DELETE FROM rents
                WHERE week_id = ?
                  AND currency_id = ?
            """, [week_id, currency_id])

        conn.execute("""
            INSERT INTO rents (
                week_id,
                currency_id,
                investor_id,
                token_id,
                amount
            )
            SELECT
                src.week_id,
                src.currency_id,
                i.id AS investor_id,
                t.id AS token_id,
                src.amount
            FROM new_week_df_view AS src
            INNER JOIN investors AS i
                ON i.address = src.investor
            INNER JOIN tokens AS t
                ON t.address = src.token
        """)

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    return db_path