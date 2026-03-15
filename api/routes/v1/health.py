"""Health check endpoint."""

from __future__ import annotations

from datetime import datetime, timezone
from flask import Blueprint, jsonify

health_bp = Blueprint("health", __name__)


@health_bp.get("/health")
def health():
    """Simple health endpoint.
    Returns both UTC time and server-local time.
    """

    now_utc = datetime.now(timezone.utc)

    return jsonify(
        {
            "status": "ok",
            "utc_datetime": now_utc.isoformat(),
        }
    )
