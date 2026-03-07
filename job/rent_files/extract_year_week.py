import re
import csv
from pathlib import Path
from datetime import datetime, date
from typing import Union
import logging
logger = logging.getLogger(__name__)


_WEEK_RE = re.compile(r"(?i)\bwe(?:ek|kk)[\s_-]*0*(\d{1,2})(?!\d)")
_YEAR_RE = re.compile(r"\b(20\d{2})\b")
_DATE_IN_NAME_RE = re.compile(r"\b(20\d{2})[-_/](\d{2})[-_/](\d{2})\b")  # YYYY-MM-DD / YYYY_MM_DD / YYYY/MM/DD
_MD_RANGE_RE = re.compile(r"\b(\d{2})\.(\d{2})\s*to\s*(\d{2})\.(\d{2})\b", re.IGNORECASE)

CSV_FOLDER = Path("./data/rent_files_csv")

# Common date formats we may see in A2/B2
_DATE_FORMATS = (
    "%Y/%m/%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%Y/%-m/%-d %H:%M",   # may not work on Windows; kept for completeness
    "%Y/%-m/%-d",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%d.%m.%y",
    "%d.%m.%Y",
    "%d.%m.%y %H:%M",
    "%d.%m.%Y %H:%M",
)


def _parse_flexible_datetime(s: str) -> datetime:
    s = s.strip()

    # Fast-path: if it already looks ISO-ish, try fromisoformat
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        pass

    # Normalize single-digit month/day in the slash format by trying explicit patterns
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue

    # Last resort: handle "YYYY/M/D HH:MM" (single-digit month/day) explicitly
    m = re.match(r"^(\d{4})/(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?$", s)
    if m:
        year, month, day, hh, mm, ss = m.groups()
        return datetime(
            int(year), int(month), int(day),
            int(hh), int(mm), int(ss) if ss else 0
        )

    raise ValueError(f"Unsupported date format: '{s}'")



def extract_year_week(file_input: Union[str, Path]):
    """
    Extract (year, week) from rent filenames.

    Strategy:
    1) If 'Week NN' exists -> week = NN; year from filename or CSV (B2) if missing
    2) Else if dates exist in filename -> compute ISO week/year from the last date (prefer 'To' date)
    3) Else fallback to CSV (B2) if it exists
    """

    file_path = Path(file_input)
    filename = file_path.name
    
    # 1) "Week NN"
    week_match = _WEEK_RE.search(filename)
    if week_match:
        week = int(week_match.group(1))

        year_match = _YEAR_RE.search(filename)
        if year_match:
            return int(year_match.group(1)), week

        # No year in filename -> read CSV B2
        csv_path = CSV_FOLDER / filename
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found to infer year: {csv_path}")

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # row 1
            row2 = next(reader, None)  # row 2

        if not row2 or len(row2) < 2:
            raise ValueError(f"CSV does not contain A2/B2 as expected: {csv_path}")

        dt = _parse_flexible_datetime(row2[1])
        return dt.year, week

    # 2) Dates in filename -> ISO week/year from last date
    dates = [
        date(int(y), int(m), int(d))
        for (y, m, d) in _DATE_IN_NAME_RE.findall(filename)
    ]
    if dates:
        end = dates[-1]
        iso_year, iso_week, _ = end.isocalendar()
        return int(iso_year), int(iso_week)
    
    # 2b) Month/Day range in filename like "09.24 to 09.27" (year is elsewhere in name)
    md = _MD_RANGE_RE.search(filename)
    if md:
        year_match = _YEAR_RE.search(filename)
        if not year_match:
            raise ValueError(f"Found MM.DD range but no year in filename: {filename}")

        year = int(year_match.group(1))
        end_month = int(md.group(3))
        end_day = int(md.group(4))

        end = date(year, end_month, end_day)
        iso_year, iso_week, _ = end.isocalendar()
        return int(iso_year), int(iso_week)

    # 3) As a last resort, try reading CSV B2 and compute ISO week/year from it
    csv_path = CSV_FOLDER / filename
    if csv_path.exists():
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)
            row2 = next(reader, None)

        if row2 and len(row2) >= 2:
            dt = _parse_flexible_datetime(row2[1])
            iso_year, iso_week, _ = dt.date().isocalendar()
            return int(iso_year), int(iso_week)

    logger.warning(f"Could not extract (year, week) from filename: {filename}")
    return None, None