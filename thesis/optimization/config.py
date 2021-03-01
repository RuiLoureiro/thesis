from pathlib import Path
from ..base.config import PROCESSED_DATA_PATH

home = str(Path.home())
REPO_ROOT_DIR = Path(__file__).resolve().parent.parent

PARAMETERS_CONFIG_PATH = f"{REPO_ROOT_DIR}/genetic_config.yaml"
print(REPO_ROOT_DIR)
BASE_OSRM_URL = "http://localhost:5000"  # "https://router.project-osrm.org/"

BUS_STOP_DURATIONS_PATH = f"{PROCESSED_DATA_PATH}/bus_durations.json"
BUS_STOP_DISTANCES_PATH = f"{PROCESSED_DATA_PATH}/bus_distances.json"
BUS_ODX_MATRIX_PATH = f"{PROCESSED_DATA_PATH}/bus_odx_matrix.json"
BUS_ODX_MATRIX_FILTERED_PATH = (
    f"{PROCESSED_DATA_PATH}/bus_odx_matrix_filtered.json"
)
BUS_ROAD_GRAPH_PATH = f"{PROCESSED_DATA_PATH}/road_graph.gt"
DS_PATH = f"{PROCESSED_DATA_PATH}/ds.json"
DS_ERRORS_PATH = f"{PROCESSED_DATA_PATH}/ds_errors.json"

EXPERIMENTS_BASE_PATH = f"{home}/tese/genetic/experiments"
CARRIS_ROUTESET_PATH = f"{PROCESSED_DATA_PATH}/carris_routeset.json"

latex_table_kwargs = {}

RESULTS_IMAGES_PATH = f"{REPO_ROOT_DIR}/result_plots/"
