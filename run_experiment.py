from thesis.optimization.genetic import Algorithm

# from thesis.optimization.common import load_carris_routeset
from loguru import logger
import sys

logger.remove()
logger.add(sys.stderr, level="INFO")
a = Algorithm()

# saved_exp_name = "200r_64pop"
# run_from_saved(saved_exp_name="carris_64pop_16es_40ts_0.7pms_0.6_pdel_exclude", niterations=10000, append=True)
# a.run_from_routes(routes=load_carris_routeset(), niterations=10000)
# a.run_from_saved(nroutes=250, niterations=5000)
a.run_from_scratch(nroutes=20, niterations=100)
