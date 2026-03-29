from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import IO, Any, Dict, List, Optional, Tuple, Union

import pandas as pd


CsvInput = Union[str, Path, IO[str]]

_RE_EVM_ADDRESS = re.compile(r"^0x[a-fA-F0-9]{40}$")


def _is_evm_address(value: Any) -> bool:
    return isinstance(value, str) and bool(_RE_EVM_ADDRESS.match(value.strip()))


def _normalize(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _open_text_stream(csv_input: CsvInput) -> IO[str]:
    if isinstance(csv_input, (str, Path)):
        return open(str(csv_input), "r", encoding="utf-8-sig", newline="")
    if hasattr(csv_input, "seek"):
        csv_input.seek(0)
    return csv_input  # type: ignore[return-value]


def _partition_label_for_week(week: int) -> str:
    if 1 <= week <= 6:
        return "p1_weeks01-06"
    if 7 <= week <= 13:
        return "p2_weeks07-13"
    if 14 <= week <= 19:
        return "p3_weeks14-19"
    if 20 <= week <= 26:
        return "p4_weeks20-26"
    if 27 <= week <= 32:
        return "p5_weeks27-32"
    if 33 <= week <= 39:
        return "p6_weeks33-39"
    if 40 <= week <= 46:
        return "p7_weeks40-46"
    if 47 <= week <= 53:
        return "p8_weeks47-53"

    raise ValueError(f"Invalid ISO week: {week}. Expected 1..53.")


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

    # 1) Detect header row
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

    # 2) Detect token row:
    #    first try above header, then below header
    token_row_idx: Optional[int] = None

    # Search above header
    start_above = max(0, header_row_idx - token_row_search_window)
    for idx in range(header_row_idx - 1, start_above - 1, -1):
        normalized = [_normalize(c) for c in preview_rows[idx]]
        if any(_is_evm_address(c) for c in normalized):
            token_row_idx = idx
            break

    # Search below header if not found above
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
                    f"Column '{col_name}' inherits previous token, but no previous token was detected."
                )
        else:
            if current_token is None:
                raise ValueError(
                    f"Unexpected token-row value '{value}' in column '{col_name}' before any token was detected."
                )

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
    out["year"] = out["year"].astype(int)
    out["week"] = out["week"].astype(int)
    out["currency"] = out["currency"].astype(str).str.upper().str.strip()
    out["investor"] = out["investor"].astype(str).str.lower().str.strip()
    out["token"] = out["token"].astype(str).str.lower().str.strip()
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce").fillna(0.0).astype(float)

    out = out.sort_values(
        by=["year", "week", "currency", "investor", "token"]
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

    long_df.insert(0, "currency", paid_in_currency.upper())
    long_df.insert(0, "week", int(week))
    long_df.insert(0, "year", int(year))

    return _normalize_long_df(long_df)


def upsert_weekly_rent_csv_to_parquet(
    csv_path: Union[str, Path],
    year: int,
    week: int,
    *,
    parquet_root: Union[str, Path] = "data/rent_files_parquet/parquet_by_year",
    paid_in_currency: str = "USD",
) -> Path:
    """
    Parse one weekly CSV and append all parsed rows into the correct yearly quarter parquet file.

    Behavior:
      - If quarter parquet does not exist: create it
      - If it exists: read it, append all parsed rows from the CSV, and rewrite it
      - No comparison with existing content
      - No replacement of the same week
    """
    csv_path = Path(csv_path)
    parquet_root = Path(parquet_root)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    partition_label = _partition_label_for_week(int(week))
    year_dir = parquet_root / f"year={int(year)}"
    year_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = year_dir / f"{partition_label}.parquet"

    new_week_df = _parse_weekly_csv_to_long_df(
        csv_path,
        year=int(year),
        week=int(week),
        paid_in_currency=paid_in_currency,
    )

    if not parquet_path.exists():
        new_week_df.to_parquet(parquet_path, index=False, engine="pyarrow")
        return parquet_path

    existing_df = pd.read_parquet(parquet_path, engine="pyarrow")
    existing_df = _normalize_long_df(existing_df)

    updated_df = pd.concat([existing_df, new_week_df], ignore_index=True)
    updated_df = _normalize_long_df(updated_df)

    updated_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    return parquet_path