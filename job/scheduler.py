import time
import logging
from config.logging_config_job import setup_logging
from config.settings import REALTOKENS_LIST_URL, REALTOKEN_HISTORY_URL
from job.utilities import fetch_json, list_to_dict_by_uuid, sort_realtoken_history_in_place, load_json, save_json
from job.fill_missing_owner_in_realtokens_data import fill_missing_owner_in_realtokens_data

def run_job():
    logger.info("starting scheduler job...")
    
    ### Realtoken data ###
    raw_realtoken_data = list_to_dict_by_uuid(fetch_json(REALTOKENS_LIST_URL) or [])
    realtoken_data = load_json("data_tmp/realtokens_data.json")

    # Add only missing UUIDs
    for uuid, token_data in raw_realtoken_data.items():
        if uuid not in realtoken_data:
            realtoken_data[uuid] = token_data
    realtoken_data = fill_missing_owner_in_realtokens_data(realtoken_data)
    
    ### Realtoken history ###
    realtoken_history = list_to_dict_by_uuid(fetch_json(REALTOKEN_HISTORY_URL) or [])
    sort_realtoken_history_in_place(realtoken_history)

    # save to json file so th api can access updated file
    save_json(realtoken_data, "data_tmp/realtokens_data.json")
    save_json(realtoken_history, "data_tmp/realtokens_history.json")

    logger.info("realtoken data and realtoken history successfully updated")


if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger("job.scheduler")
    while True:
        try:
            run_job()
        except Exception as e:
            logger.exception("Scheduler crashed")
            raise  # Let supervisord restart it

        time.sleep(86400)  # 24h