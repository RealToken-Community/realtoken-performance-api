from __future__ import annotations
import os
import logging
import json
from typing import Any, Dict
from flask import Flask, jsonify, current_app
from flask_cors import CORS

from config.logging_config_api import setup_logging
from core.services.send_telegram_alert import send_telegram_alert
from core.services.utilities import test_postgres_connection
from api.routes.v1 import v1_bp


from dotenv import load_dotenv
load_dotenv()

CORS_ORIGIN_REGEX = os.getenv("CORS_ORIGIN_REGEX", '.*')
API_URL_PREFIX_V1 = os.getenv("API_URL_PREFIX_V1")
API_PORT = int(os.getenv('API_PORT_INTERNAL', '5001'))

THE_GRAPH_API_KEY = os.getenv("THE_GRAPH_API_KEY")
REALTOKEN_GNOSIS_SUBGRAPH_ID = os.getenv("REALTOKEN_GNOSIS_SUBGRAPH_ID")
RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID = os.getenv("RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID")

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_READER_USER_NAME = os.getenv("POSTGRES_READER_USER_NAME")
POSTGRES_READER_USER_PASSWORD = os.getenv("POSTGRES_READER_USER_PASSWORD")
POSTGRES_DATA = [POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_READER_USER_NAME, POSTGRES_READER_USER_PASSWORD]



def _register_error_handlers(app: Flask) -> None:
    """Register JSON error handlers."""

    @app.errorhandler(404)
    def not_found(_: Exception):  # type: ignore[override]
        return jsonify({"error": "not_found"}), 404

    @app.errorhandler(405)
    def method_not_allowed(_: Exception):  # type: ignore[override]
        return jsonify({"error": "method_not_allowed"}), 405

    @app.errorhandler(500)
    def internal_error(e: Exception):  # type: ignore[override]
        current_app.logger.exception("Unhandled error")
        send_telegram_alert(f"roi calculator api : Unhandled error, please check the logs.\n{type(e).__name__}: {e}")
        return jsonify({"error": "internal_server_error"}), 500



def create_app() -> Flask:
    """Create and configure the Flask application."""

    # Set up logging at the start of your application and handlers
    setup_logging()
    
    # Get a logger for this module
    logger = logging.getLogger(__name__)
    logger.info("Application started")
    send_telegram_alert("roi calculator api: Application has started")

    app = Flask(__name__)

    # CORS
    CORS(app, 
         origins=[CORS_ORIGIN_REGEX],
         methods=['GET', 'OPTIONS'],
         allow_headers=['Content-Type', 'Authorization'],
         supports_credentials=False
    )

    app.config['API_PORT'] = API_PORT
    app.config["API_URL_PREFIX_V1"] = API_URL_PREFIX_V1
    app.config['POSTGRES_DATA'] = POSTGRES_DATA
    app.config['THE_GRAPH_API_KEY'] = THE_GRAPH_API_KEY
    app.config['REALTOKEN_GNOSIS_SUBGRAPH_ID'] = REALTOKEN_GNOSIS_SUBGRAPH_ID
    app.config['RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID'] = RMMV3_WRAPPER_GNOSIS_SUBGRAPH_ID

    # Maximum size of paylaod (16 KB)
    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024
    
    try:
        with open('Ressources/blockchain_contracts.json', 'r') as contracts_file:
            app.config['BLOCKCHAIN_CONTRACTS'] = json.load(contracts_file)['contracts']
        logger.info("Blockchain contracts loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load blockchain contracts: {e}")
        send_telegram_alert(f"Failed to load blockchain contracts, please check blockchain_contracts.json: {e}")
        raise
    
    if not test_postgres_connection(POSTGRES_DATA):
        raise RuntimeError("Database connection failed")



    # all v1 routes
    app.register_blueprint(v1_bp, url_prefix=API_URL_PREFIX_V1)

    # Errors
    _register_error_handlers(app)

    @app.get("/")
    def root():
        return jsonify({"name": "roi-calculator-api", "status": "ok"})

    return app
