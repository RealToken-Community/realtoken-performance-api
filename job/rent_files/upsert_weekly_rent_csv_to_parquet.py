from __future__ import annotations

import csv
import math
import re
from collections import defaultdict
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
    out["year"] = out["year"].astype("int32")
    out["week"] = out["week"].astype("int16")
    out["currency"] = out["currency"].astype(str).str.upper().str.strip()
    out["investor"] = out["investor"].astype(str).str.lower().str.strip()
    out["token"] = out["token"].astype(str).str.lower().str.strip()
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce").fillna(0.0).astype("float64")

    out = out.sort_values(
        by=["year", "week", "currency", "investor", "token"],
        kind="mergesort",
    ).reset_index(drop=True)

    return out


def _safe_float(value: Any) -> float:
    text = _normalize(value)
    if text == "":
        return 0.0

    try:
        number = float(text)
    except (TypeError, ValueError):
        return 0.0

    if not math.isfinite(number):
        return 0.0

    return number


def _read_header_columns(csv_path: Union[str, Path], header_row_idx: int) -> List[str]:
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, delimiter=",")
        for idx, row in enumerate(reader):
            if idx == header_row_idx:
                return [_normalize(c) for c in row]

    raise ValueError("Could not read detected header row from CSV.")


def _parse_weekly_csv_to_long_df(
    csv_path: Union[str, Path],
    *,
    year: int,
    week: int,
    paid_in_currency: str = "USD",
) -> pd.DataFrame:
    header_row_idx, token_row, investor_column_name = _detect_header_and_token_row(csv_path)

    columns = _read_header_columns(csv_path, header_row_idx)

    if investor_column_name not in columns:
        raise ValueError(f"Column '{investor_column_name}' not found after header detection.")

    col_to_token = _build_column_to_token_mapping(columns, token_row)

    token_columns = [col for col in columns if col in col_to_token]
    if not token_columns:
        raise ValueError("No token columns detected.")

    investor_col_idx = columns.index(investor_column_name)
    token_col_indices = [(columns.index(col), col_to_token[col].lower()) for col in token_columns]

    aggregated: Dict[Tuple[str, str], float] = defaultdict(float)

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, delimiter=",")

        for row_idx, row in enumerate(reader):
            if row_idx <= header_row_idx:
                continue

            investor_raw = row[investor_col_idx] if investor_col_idx < len(row) else ""
            investor = _normalize(investor_raw).lower()

            if not _is_evm_address(investor):
                continue

            for col_idx, token in token_col_indices:
                raw_amount = row[col_idx] if col_idx < len(row) else ""
                amount = _safe_float(raw_amount)

                if amount != 0.0:
                    aggregated[(investor, token)] += amount

    if not aggregated:
        raise ValueError(
            "Parsing succeeded structurally, but no non-zero investor/token revenue row was found."
        )

    records = []
    for (investor, token), amount in aggregated.items():
        if amount == 0.0:
            continue

        if not _is_evm_address(investor):
            raise ValueError("At least one parsed investor is not a valid EVM address.")

        if not _is_evm_address(token):
            raise ValueError("At least one parsed token is not a valid EVM address.")

        records.append(
            {
                "year": int(year),
                "week": int(week),
                "currency": paid_in_currency.upper().strip(),
                "investor": investor,
                "token": token,
                "amount": float(amount),
            }
        )

    if not records:
        raise ValueError(
            "Parsing succeeded structurally, but no non-zero investor/token revenue row was found."
        )

    long_df = pd.DataFrame.from_records(
        records,
        columns=["year", "week", "currency", "investor", "token", "amount"],
    )

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

    existing_df = pd.read_parquet(
        parquet_path,
        columns=["year", "week", "currency", "investor", "token", "amount"],
        engine="pyarrow",
    )

    updated_df = pd.concat([existing_df, new_week_df], ignore_index=True, copy=False)
    updated_df = _normalize_long_df(updated_df)

    updated_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    return parquet_path