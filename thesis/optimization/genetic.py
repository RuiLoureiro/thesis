from pathlib import Path
import datetime
import json
import time
import pathlib
import pickle
import numpy as np
from loguru import logger
from rich import print
from rich.progress import track
from .road_graph import RoadGraph
from .routeset_graph import RouteSetGraph
from .common import DS, ODX, Durations, get_initial_routeset
from . import config
from ..base.utils import load_yaml
import pandas as pd


class Metrics:
    """
    Class responsible for metric-related computations
    """

    @staticmethod
    def get_change(previous, current):
        """
        Percentual change between previous and current values
        """
        if current == previous:
            return 0
        try:
            return ((current - previous) / previous) * 100.0
        except ZeroDivisionError:
            return float("inf")

    def get_fitness_decrease(self, prev, curr):
        """
        Fitness value decrease between previous and current values
        """
        return -round(self.get_change(prev, curr), 2)

    @staticmethod
    def get_satisfied_demand_pct(report):
        """
        Percentage of satisfied demand
        """
        return round(
            report["satisfied_demand"]
            / (report["satisfied_demand"] + report["unsatisfied_demand"])
            * 100,
            2,
        )

    @staticmethod
    def get_satisfied_stops_pct(report):
        """
        Percentage of satisfied stops
        """
        return round(
            report["nsatisfied_stops"]
            / (report["nsatisfied_stops"] + report["nunsatisfied_stops"])
            * 100,
            2,
        )

    @staticmethod
    def get_satisified_od_pairs_pct(report):
        """
        Percentage of satisfied OD pairs
        """
        return round(
            report["nsatisfied_od_pairs"]
            / (report["nsatisfied_od_pairs"] + report["nunsatisfied_od_pairs"])
            * 100,
            2,
        )

    @staticmethod
    def get_average_travel_time(report):
        """
        Average travel time in minutes
        """
        return round(report["average_travel_time_min"], 2)

    @staticmethod
    def get_fittest(population, n):
        """
        Returns `n` fittest individuals in `population`
        """
        return sorted(population, key=lambda p: p.get_fitness())[:n]

    def get_row(self, P):
        best = self.get_fittest(P, 1)[0]
        report = best.get_report()
        fitness = best.get_fitness()

        mean_transfers = round(
            sum([n * count for n, count in report["transfers"].items()])
            / report["satisfied_demand"],
            2,
        )

        return {
            "fitness": fitness,
            "satisfied_demand_pct": self.get_satisfied_demand_pct(report),
            "satisfied_stops_pct": self.get_satisfied_stops_pct(report),
            "satisfied_od_pairs_pct": self.get_satisified_od_pairs_pct(report),
            "average_travel_time": self.get_average_travel_time(report),
            "mean_transfers": mean_transfers,
        }




class Algorithm:
    """
    Class responsible for running the optimization process
    """

    def __init__(
        self,
        road_G_path=config.BUS_ROAD_GRAPH_PATH,
        config_path=config.PARAMETERS_CONFIG_PATH,
        verbose=True,
    ):
        self.config = load_yaml(config_path)
        print("Loading Algorithm instance..")
        print(f"Reading config from {config_path}")

        if verbose:
            print("Parameters:")
            for param in [
                "pop_size",
                "elite_size",
                "tournament_size",
                # "pswap",
                "pms",
                "pdelete",
                "min_route_size",
            ]:
                print(f"\t{param}: {self.config[param]}")
                setattr(self, param, self.config[param])

        # road graph
        print(f"Loading road graph from {road_G_path}..")
        self.G = RoadGraph.load(road_G_path)

    @staticmethod
    def make_exp_dir(name):
        base_path = config.EXPERIMENTS_BASE_PATH
        result_dir = f"{base_path}/{name}"
        pathlib.Path(result_dir).mkdir(parents=True, exist_ok=True)

        pathlib.Path(f"{result_dir}/saved_population").mkdir(
            parents=True, exist_ok=True
        )

    def run_from_saved(
        self,
        saved_exp_name,
        niterations,
        append=False,
        name=None,
    ):
        """
        Runs (continues) optimization for previously saved experiment
        """
        logger.info(f"[{name}] Running from saved experiment {saved_exp_name}")
        print(f"Loading experiment [bold]{saved_exp_name}..")
        self.P = []
        base_path = config.EXPERIMENTS_BASE_PATH
        population_path = f"{base_path}/{saved_exp_name}/saved_population"
        for p in Path(population_path).iterdir():
            if p.suffix == ".pickle":
                with open(p, "rb") as f:
                    self.P.append(pickle.load(f))
        nroutes = len(self.P[0].routes)

        self.pswap = 1 / nroutes

        if append:
            assert not name
            result_dir = f"{base_path}/{saved_exp_name}"
            with open(f"{result_dir}/meta.json", "r") as f:
                meta = json.load(f)
            print("Setting all parameters to match saved experiment..")
            for param, val in meta["config"].items():
                print(f"{param}: {val}")
                setattr(self, param, val)

            meta["updated_on"] = str(datetime.datetime.now())

        else:
            with open(f"{base_path}/{saved_exp_name}/meta.json", "r") as f:
                meta = json.load(f)
            params = ["pop_size", "elite_size", "tournament_size"]
            print(f"Setting parameters {params} to match saved experiment..")

            for param in params:
                val = meta["config"][param]
                print(f"{param}: {val}")
                setattr(self, param, val)
            if name:
                self.make_exp_dir(name)
                meta = {
                    "date": str(datetime.datetime.now()),
                    "loaded_from": saved_exp_name,
                    "config": self.config,
                    "nroutes": nroutes,
                }
                result_dir = f"{base_path}/{name}"
            else:
                result_dir = None
        if name:
            with open(f"{result_dir}/meta.json", "w") as f:
                json.dump(meta, f)
        self._run(niterations, result_dir, append)

    def get_experiment_name_suffix(self):
        """
        Builds experiment name suffix from parameters
        """
        return f"pop={self.pop_size},es={self.elite_size},ts={self.tournament_size},pms={self.pms},pdel={self.pdelete}"

    def run_from_routes(self, routes, niterations):
        """
        Runs optimization using a pre-defined set of routes
        """

        name = "routes_" + self.get_experiment_name_suffix()
        logger.info(f"[{name}] Running from pre-defined routes")

        self.initial_routeset = RouteSetGraph()

        for rid, r in enumerate(routes):
            self.initial_routeset.add_route(r)
            if RouteSetGraph.SAVE_HISTORY:
                self.initial_routeset._history[rid] = [
                    {"initial_route": r.copy()}
                ]

        self.P = [self.initial_routeset.copy() for _ in range(self.pop_size)]

        self.pswap = 1 / len(routes)

        if name:
            base_path = config.EXPERIMENTS_BASE_PATH
            result_dir = f"{base_path}/{name}"
            self.make_exp_dir(name)

            meta = {
                "date": str(datetime.datetime.now()),
                "config": self.config,
                "nroutes": len(self.initial_routeset.routes),
            }

            with open(f"{result_dir}/meta.json", "w") as f:
                json.dump(meta, f)
        else:
            result_dir = None

        self._run(niterations, result_dir)

    def run_from_scratch(
        self,
        nroutes,
        niterations,
    ):
        """
        Runs optimization from scratch, generating an initial set of `nroutes` routes
        """
        name = f"r={nroutes}," + self.get_experiment_name_suffix()
        print(f"[{name}] Running from scratch ({nroutes} routes)")

        routes = get_initial_routeset(nroutes)
        self.pswap = 1 / nroutes

        self.initial_routeset = RouteSetGraph()
        for rid, r in enumerate(routes):
            self.initial_routeset.add_route(r)
            if RouteSetGraph.SAVE_HISTORY:
                self.initial_routeset._history[rid] = [
                    {"initial_route": r.copy()}
                ]
        self.P = [self.initial_routeset.copy() for _ in range(self.pop_size)]

        if name:
            base_path = config.EXPERIMENTS_BASE_PATH
            result_dir = f"{base_path}/{name}"
            self.make_exp_dir(name)

            meta = {
                "date": str(datetime.datetime.now()),
                "config": self.config,
                "nroutes": len(self.initial_routeset.routes),
            }

            with open(f"{result_dir}/meta.json", "w") as f:
                json.dump(meta, f)
        else:
            result_dir = None

        self._run(niterations, result_dir)

    @staticmethod
    def init_classes():
        """
        Initializes singleton helper classes
        """
        ODX()
        DS()
        Durations()

    def _run(self, niterations, result_dir=None, append=False):
        self.init_classes()

        metrics = Metrics()

        if append:
            results = pd.read_csv(f"{result_dir}/df.csv").to_dict("records")
        else:
            results = [metrics.get_row(self.P)]

        print("[bold]Initial stats")

        last_time = time.time()
        results[0]["time"] = 0
        self.print_row(results[0])
        for i in track(
            range(niterations),
            description=f"Running {niterations} iterations..",
        ):
            Q = self.get_fittest(self.P, self.elite_size)

            for _ in range(int((self.pop_size - self.elite_size) / 2)):
                tournament = self.get_tournament(self.P, self.tournament_size)
                P1, P2 = self.get_fittest(tournament, 2)

                C1, C2 = self.crossover(P1, P2, self.pswap)
                Q.extend([self.mutation(C1), self.mutation(C2)])

            self.P = Q
            row = metrics.get_row(self.P)
            row["time"] = round(time.time() - last_time, 2)
            results.append(row)
            last_time = time.time()

            print_freq = 20
            save_freq = 20
            if i % print_freq == 0:
                if i > 0:
                    print(f"[bold]Iteration {i}")
                    print(
                        f"Decrease in fitness: {self.get_fitness_decrease(results[-(print_freq+1)]['fitness'], row['fitness'])}%"
                    )
                    self.print_row(row)

            if result_dir:
                if (i % save_freq) == 0:
                    pd.DataFrame(results).to_csv(
                        f"{result_dir}/df.csv", index=False
                    )
                    self.serialize_population(
                        self.P, f"{result_dir}/saved_population"
                    )
        if result_dir:
            pd.DataFrame(results).to_csv(f"{result_dir}/df.csv", index=False)
            self.serialize_population(self.P, f"{result_dir}/saved_population")

    
    @staticmethod
    def serialize_population(P, dir_):
        for pidx, p in enumerate(P):
            with open(f"{dir_}/{pidx}.pickle", "wb") as f:
                pickle.dump(p, f)

    @staticmethod
    def print_row(row):
        """
        Prints metric values
        """
        print(f"Satisfied demand: {row['satisfied_demand_pct']}%")
        print(f"Satisfied stops: {row['satisfied_stops_pct']}%")
        print(f"Satisfied OD pairs: {row['satisfied_od_pairs_pct']}%")
        print(f"Average travel time: {row['average_travel_time']} min")
        print(f"Mean transfers: {row['mean_transfers']}")
        if row["time"]:
            print(f"Computation time: {row['time']}")
        print("--------------------------------------------------------")

    @staticmethod
    def get_fittest(population, n):
        """
        Returns `n` fittest individuals in `population`
        """
        return sorted(population, key=lambda p: p.get_fitness())[:n]

    @staticmethod
    def get_tournament(population, tournament_size):
        tournament = np.random.choice(
            population, tournament_size, replace=False
        )
        return tournament

    @staticmethod
    def crossover(p1, p2, pswap, copy=True):
        """
        Applies crossover operator to `p1` and `p2`
        """
        if copy:
            p1 = p1.copy()
            p2 = p2.copy()
        nroutes = p1.nroutes()

        to_swap_idxs = np.where(np.random.random_sample(nroutes) < pswap)[0]

        logger.debug(f"Swapping routes {to_swap_idxs}")

        for idx in to_swap_idxs:
            rp1 = p1.get_route(idx)
            rp2 = p2.get_route(idx)

            if rp1 == rp2:
                logger.debug("Routes are identical, skipping swap..")

            else:
                p1.replace_route(idx, rp2)
                p2.replace_route(idx, rp1)

                if RouteSetGraph.SAVE_HISTORY:
                    # p1.register_history("swap_route", route_id=idx, by=rp2, new_history=p2._history[idx])
                    # p2.register_history("swap_route", route_id=idx, by=rp1, new_history=p1._history[idx])

                    p1._history[idx], p2._history[idx] = (
                        p2._history[idx],
                        p1._history[idx],
                    )

        return p1, p2

    @staticmethod
    def get_route_invds(route):
        o, d = route[0], route[-1]
        try:
            return 1 / DS().get_ds_total(o, d)
        except KeyError:
            return 2

    def mutation(self, p, copy=False):
        """
        Applies mutation to `p`
        """
        if copy:
            p = p.copy()
        mutation_probs = np.array(
            [self.get_route_invds(tuple(route)) for route in p.get_routes()]
        )

        probs = mutation_probs / sum(mutation_probs)

        route_idx = np.random.choice(p.nroutes(), p=probs)

        mutate_fn = np.random.choice(
            [self.small_mod, self.big_mod], p=[self.pms, 1 - self.pms]
        )
        mutate_fn(p, route_idx)

        return p

    @staticmethod
    def small_mod_delete(p, route_id, stop_seq):
        """
        Applies 'delete' small modification to the `stop_seq`-th stop of route `route_idx` in `p`
        """
        sid = p.get_route(route_id)[stop_seq]
        p.remove_node(sid, route_id)

        p.register_history(
            "small_mod_delete", route_id=route_id, stop_seq=stop_seq
        )

    def small_mod_extend(self, p, route_id, stop_seq):
        """
        Applies 'extend' small modification to the `stop_seq`-th stop of route `route_idx` in `p`
        """
        sid = p.get_route(route_id)[stop_seq]

        # first node
        if stop_seq == 0:
            # in neighbors
            candidates = set(self.G.in_neighbors(sid)) - set(
                p.get_route(route_id)
            )
            if candidates:
                new_sid = np.random.choice(tuple(candidates))
                p.prepend_stop(new_sid, route_id)

        # last node
        elif stop_seq == -1:
            # out neighbors
            candidates = set(self.G.out_neighbors(sid)) - set(
                p.get_route(route_id)
            )

            if candidates:
                new_sid = np.random.choice(tuple(candidates))
                p.append_stop(new_sid, route_id)

        if not candidates:
            logger.info(f"Stop {sid} has no candidates.")

        else:
            p.register_history(
                "small_mod_extend",
                route_id=route_id,
                new_sid=new_sid,
                stop_seq=stop_seq,
            )

    def small_mod(self, p, route_idx):
        """
        Applies small modification to route `route_idx` in `p`
        """
        # select one of the route terminals
        node_idx = np.random.choice([0, -1])

        if len(p.routes[route_idx]) <= self.min_route_size:
            logger.warning("Route too small, using small_mod_extend")
            mod_fn = self.small_mod_extend

        else:
            # randomly select delete or extend modification
            mod_fn = np.random.choice(
                [self.small_mod_delete, self.small_mod_extend],
                p=[self.pdelete, 1 - self.pdelete],
            )

        logger.debug(
            f"Applying {mod_fn.__name__} to route {route_idx} and node {node_idx}"
        )

        # get modified route
        mod_fn(p, route_idx, node_idx)

    def big_mod(self, p, route_idx):
        """
        Applies big modification to route `route_idx` in `p`
        """
        if p.get_route(route_idx)[-1] in DS().origins:
            if p.get_route(route_idx)[0] in DS().origins:
                node_idx = np.random.choice([0, -1])
            else:
                node_idx = -1
        elif p.get_route(route_idx)[0] in DS().origins:
            node_idx = 0

        else:
            logger.warning(
                "Both route ends have no ds_totals entry, deleting both ends.."
            )
            if len(p.get_route(route_idx)) == 1:
                logger.warning(f"Route {route_idx} has only one stop")
            else:
                p.remove_node(p.get_route(route_idx)[0], route_idx)
                p.remove_node(p.get_route(route_idx)[-1], route_idx)
                # p.replace_route(route_idx, p.routes[route_idx][1:])

                p.register_history("big_mod_delete", route_id=route_idx)

            return p

        i = p.routes[route_idx][node_idx]

        logger.debug(
            f"Applying big_mod to route {route_idx} and node {node_idx} ({i})"
        )

        possible_k = np.fromiter(DS().get_dests(i), dtype="int")
        probs = np.fromiter(DS().get_ds_totals(i), dtype="int")

        k = np.random.choice(possible_k, p=probs / probs.sum())
        logger.debug(f"Selected node {k}")

        new_route = self.G.shortest_path(i, k)

        p.register_history(
            "big_mod",
            route_id=route_idx,
            node_idx=node_idx,
            i=i,
            k=k,
            new_route=new_route,
        )

        p.replace_route(route_idx, new_route)
