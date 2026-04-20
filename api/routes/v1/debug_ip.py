"""Debug IP endpoint."""

from __future__ import annotations

from flask import Blueprint, jsonify, request

debug_ip_bp = Blueprint("debug_ip", __name__)


@debug_ip_bp.get("/debug-ip")
def debug_ip():
    """Return the client IP information seen by Flask."""

    return jsonify(
        {
            "remote_addr": request.remote_addr,
            "access_route": list(request.access_route),
            "x_forwarded_for": request.headers.get("X-Forwarded-For"),
            "x_real_ip": request.headers.get("X-Real-Ip"),
        }
    )