import os
import logging
from logging.handlers import RotatingFileHandler
from config.settings import LOG_DIR

def setup_logging():

    os.makedirs("logs/", exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            RotatingFileHandler(
                LOG_DIR / "app_roi-calculator.log", 
                maxBytes=10*1024*1024,  # 10MB per file
                backupCount=5,          # Keep 5 backup files
                encoding='utf-8'
            )
        ]
    )