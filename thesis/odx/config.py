import datetime
from pathlib import Path
from ..base.config import PROCESSED_DATA_PATH

home = str(Path.home())


# METRO
METRO_STOP_MAPPING_PATH = f"{PROCESSED_DATA_PATH}/metro_stop_mapping.json"

# AFC
PROCESSED_BUS_AFC_PATH = f"{PROCESSED_DATA_PATH}/afc_carris_10_2019.feather"
PROCESSED_METRO_AFC_PATH = f"{PROCESSED_DATA_PATH}/afc_metro_10_2019.feather"


# ODX
class ODXConfig:
    NEW_DAY_TIME = datetime.time(4, 0, 0)
    MAX_BUS_ALIGTHING_BOARDING_DISTANCE = 0.75  # km
