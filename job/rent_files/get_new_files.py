from typing import Iterable, List, Set
from job.rent_files.list_all_files_flat import DriveFile


EXCLUDED_FILES: Set[str] = {
    "Rent corrections manuel - New Correction next Week for Split tools.csv",
    "Rent Corrections - From_ 2025-01-13 To_2025-01-20 Cutoff_ 22_0.csv",
    "Rent Corrections - From_ 2024-12-02 To_2024-12-09 Cutoff_ 23_0.csv",
    "Rent Corrections - From_ 2024-11-25 To_2024-12-02 Cutoff_ 22_15.csv",
    "Rent Corrections - From_ 2024-04-08 To_2024-04-15 Cutoff_ 23_0.csv",
    "CORRECTED - Rent Corrections - From_ 2022-10-17 To_2022-10-24 Cutoff_ 22_30.csv",
    "CORRECTED - Rent Corrections - From_ 2025-12-29 To_2026-01-05 Cutoff_ 21_30__currency_USD.csv",
    "CORRECTED - Rent Corrections - From_ 2026-01-05 To_2026-01-12 Cutoff_ 22_0__currency_USD.csv",
    "CORRECTED - Rent Corrections - From_ 2026-01-12 To_2026-01-19 Cutoff_ 23_0__currency_USD.csv",
    "CORRECTED - Rent Corrections - From_ 2026-01-19 To_2026-01-26 Cutoff_ 21_0__currency_USD.csv",
    "CORRECTED - Rent Corrections - From_ 2026-01-26 To_2026-02-02 Cutoff_ 22_0__currency_USD.csv",



}


def get_new_files(
    all_files: Iterable[DriveFile],
    processed_ids: Set[str],
) -> List[DriveFile]:
    """
    Compare the full list of collected Drive files with the set of already
    processed file IDs and return only the new CSV files.
    """

    normalized_processed_ids = {str(x).strip() for x in processed_ids if x is not None}

    new_files: List[DriveFile] = []

    for file in all_files:

        # Skip files that are not CSV
        if file.mime_type != "text/csv":
            continue

        name = file.name.strip()

        # Skip explicitly excluded filenames
        if name in EXCLUDED_FILES:
            continue

        # Skip any file starting with "TOKEN QUANTITIES"
        if name.startswith("TOKEN QUANTITIES"):
            continue

        file_id = str(file.id).strip()

        # Skip files already processed
        if file_id in normalized_processed_ids:
            continue

        new_files.append(file)

    return new_files