from pathlib import Path
from typing import Union
from google.oauth2 import service_account

DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"


def build_google_credentials(
    service_account_json: Union[str, Path] = "./google-drive-service-account.json",
):
    """
    Build Google credentials from a Service Account JSON key.

    Parameters
    ----------
    service_account_json : str | Path, optional
        Path to the Service Account JSON credentials file.
        Defaults to "./google-drive-service-account.json".

    Returns
    -------
    google.auth.credentials.Credentials
        Authenticated credentials usable with Google APIs.
    """

    return service_account.Credentials.from_service_account_file(
        str(service_account_json),
        scopes=[DRIVE_READONLY_SCOPE],
    )