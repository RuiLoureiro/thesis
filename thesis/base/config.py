from pathlib import Path

home = str(Path.home())

REPO_PATH = Path(__file__).absolute().parents[2]

DATA_PATH = f"{REPO_PATH}/data"  # change appropriately
RAW_DATA_PATH = f"{DATA_PATH}/raw"
PROCESSED_DATA_PATH = f"{DATA_PATH}/processed"

# GTFS
METRO_GTFS_PATH = f"{RAW_DATA_PATH}/gtfs_metro_10_2019"
CARRIS_GTFS_PATH = f"{RAW_DATA_PATH}/gtfs_carris_02_2020"


# BUS
BUS_STOPS_PATH = f"{PROCESSED_DATA_PATH}/stops.json"
BUS_ROUTES_PATH = f"{PROCESSED_DATA_PATH}/routes.json"
BUS_STAGE_TIMES_GTFS_PATH = f"{PROCESSED_DATA_PATH}/bus_stage_times_gtfs.json"
BUS_STOP_TIME = 30
