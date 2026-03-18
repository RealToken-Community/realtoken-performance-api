
"""Application configuration and constants for this repository layout."""
from __future__ import annotations
from pathlib import Path

FRENQUENCY_REALTOKENS_DATA_UPDATE = 2 # in days

# RealToken public endpoints
REALTOKENS_LIST_URL = "https://api.realtoken.community/v1/token"
REALTOKEN_HISTORY_URL = "https://api.realtoken.community/v1/tokenHistory"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = PROJECT_ROOT / "logs"

PAYMENT_TOKEN_FOR_YAM = ["usdc", "wxdai", "armmv3usdc", "armmv3wxdai", "reg", "reusd"]

LIQUIDATION_BONUS = 1.1

API_URL_PREFIX_V1="/api/v1"

CACHE_ENABLED = True
CACHE_TTL_SECONDS = 18 * 60 * 60

RATE_LIMITER_ENABLED = True
RATE_LIMITER_PARAMS = ["1 per 10 seconds", "10 per hour"]

# Google drive rent files
RENT_FILES_FOLDER_ID = "1hmlw04fNhj-1nN0S493fheLzcZOLai_G"

EXCLUDE_RWA_HOLDINGS_FROM_OVERALL_PERFORMANCE = True
RWA_HOLDINGS_ADDRESS = "0x0675e8f4a52ea6c845cb6427af03616a2af42170"

TRUSTED_INTERMEDIARY_FOR_USER_ID = '0x296033cb983747b68911244EC1A3f01d7708851b' # trusted_intermediary needed to resolve all addresses of user related to its KYC

MULTICALLV3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"
REALTOKEN_WRAPPER_RMMV3 = ["0x10497611Ee6524D75FC45E3739F472F83e282AD5"]
MULTICALLV3_ABI_MINIMAL = [
        {
            "inputs": [
                {
                    "components": [
                        {"internalType": "address", "name": "target", "type": "address"},
                        {"internalType": "bool", "name": "allowFailure", "type": "bool"},
                        {"internalType": "bytes", "name": "callData", "type": "bytes"},
                    ],
                    "internalType": "struct Multicall3.Call3[]",
                    "name": "calls",
                    "type": "tuple[]",
                }
            ],
            "name": "aggregate3",
            "outputs": [
                {
                    "components": [
                        {"internalType": "bool", "name": "success", "type": "bool"},
                        {"internalType": "bytes", "name": "returnData", "type": "bytes"},
                    ],
                    "internalType": "struct Multicall3.Result[]",
                    "name": "returnData",
                    "type": "tuple[]",
                }
            ],
            "stateMutability": "view",
            "type": "function",
        }
    ]

REALTOKEN_ABI_MINIMAL = [
        {
            "inputs": [],
            "name": "owner",
            "outputs": [
                {
                    "internalType": "address",
                    "name": "",
                    "type": "address"
                }
            ],
            "stateMutability": "view",
            "type": "function"
        }
    ]