import datetime
from pathlib import Path

home = str(Path.home())

DATA_PATH = f"{home}/tese/repo/data"  # change appropriately
RAW_DATA_PATH = f"{DATA_PATH}/raw"
PROCESSED_DATA_PATH = f"{DATA_PATH}/processed"


# BUS
BUS_STOPS_PATH = f"{PROCESSED_DATA_PATH}/stops.json"
BUS_ROUTES_PATH = f"{PROCESSED_DATA_PATH}/routes.json"
BUS_STAGE_TIMES_GTFS_PATH = f"{PROCESSED_DATA_PATH}/bus_stage_times_gtfs.json"
BUS_STOP_TIME = 30
