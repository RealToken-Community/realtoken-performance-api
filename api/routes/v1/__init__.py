from __future__ import annotations

from flask import Blueprint

from .health import health_bp
from .realtokens_performance import realtokens_performance_bp

v1_bp = Blueprint("v1", __name__)

# Register all V1 route blueprints here
v1_bp.register_blueprint(health_bp)
v1_bp.register_blueprint(realtokens_performance_bp)