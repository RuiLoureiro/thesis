import json
from collections import defaultdict
from rich.progress import track
from tqdm.auto import tqdm
from rich import print
from loguru import logger
from . import config
from .road_graph import RoadGraph
from ..base.utils import Singleton, nested_dict_to_int, ddict2dict
from ..base.bus_schedule import BusSchedule


class DS(metaclass=Singleton):
    @staticmethod
    def load_ds(path=config.DS_PATH, ds_errors_path=config.DS_ERRORS_PATH):
        with open(path, "r") as f:
            ds = nested_dict_to_int(json.load(f))

        for o in ds:
            for d in ds[o]:
                ds[o][d] = [tuple(pair) for pair in ds[o][d]]

        return ds

    def __init__(self, ds_path=config.DS_PATH):

        print(f"Loading ds from {ds_path}")
        ds = self.load_ds(ds_path)
        ds_totals = defaultdict(lambda: defaultdict(int))
        for o in ds:
            for d in ds[o]:
                for m, n in ds[o][d]:
                    try:
                        ds_totals[o][d] += ODX().get_odx(m, n)
                    except KeyError:
                        continue

        circ_routes = [
            r for r in BusSchedule().routes if r.route_direction == "CIRC"
        ]
        exclude_pairs = set()
        for ridx, r in enumerate(circ_routes):
            if r.route_direction == "CIRC":
                for sidx, o in enumerate(r.route_stop_ids):
                    if o == r.route_stop_ids[0]:
                        continue
                    for d in r.route_stop_ids[:sidx]:
                        exclude_pairs.add((o, d))

        for o, d in exclude_pairs:
            try:
                del ds[o][d]
                if ds[o] == {}:
                    del ds[o]
                del ds_totals[o][d]
                if ds_totals[o] == {}:
                    del ds_totals[o]
            except:
                pass

        self.ds = ds
        self.ds_totals = ddict2dict(ds_totals)
        self.origins = set(self.ds_totals.keys())

    def get_ds_total(self, o, d):
        return self.ds_totals[o][d]

    def get_ds(self, o, d):
        return self.ds[o][d]

    def get_dests(self, o):
        return self.ds_totals[o].keys()

    def get_ds_totals(self, o):
        return self.ds_totals[o].values()


class ODX(metaclass=Singleton):
    def __init__(self, odx_path=config.BUS_ODX_MATRIX_FILTERED_PATH):
        print(f"Loading odx from {odx_path}..")
        with open(odx_path, "r") as f:
            self.odx = nested_dict_to_int(json.load(f))

        circ_routes = [
            r for r in BusSchedule().routes if r.route_direction == "CIRC"
        ]
        exclude_pairs = set()
        for ridx, r in enumerate(circ_routes):
            if r.route_direction == "CIRC":
                for sidx, o in enumerate(r.route_stop_ids):
                    if o == r.route_stop_ids[0]:
                        continue
                    for d in r.route_stop_ids[:sidx]:
                        exclude_pairs.add((o, d))

        for o, d in exclude_pairs:
            try:
                del self.odx[o][d]
                if self.odx[o] == {}:
                    del self.odx[o]
            except:
                pass

        self.origins = set(self.odx.keys())

    def get_odx(self, o, d):
        return self.odx[o][d]

    def get_dests(self, o):
        return self.odx[o].keys()


class Durations(metaclass=Singleton):
    def __init__(self, durations_path=config.BUS_STOP_DURATIONS_PATH):
        print(f"Loading durations from {durations_path}")
        with open(durations_path, "r") as f:
            self.durations = nested_dict_to_int(json.load(f))

    def get_duration(self, o, d):
        return self.durations[o][d]


def get_initial_routeset(n_routes, road_G_path=config.BUS_ROAD_GRAPH_PATH):
    routes = []
    totals = {}

    road_G = RoadGraph.load(road_G_path)

    satisfied_by = defaultdict(set)

    newly_satisfied = set()

    for o in DS().origins:
        for d in DS().get_dests(o):
            totals[(o, d)] = 0
            for m, n in set(DS().get_ds(o, d)):
                try:
                    odx_val = ODX().get_odx(m, n)
                    satisfied_by[(m, n)].add((o, d))
                except KeyError:
                    continue
                totals[(o, d)] += odx_val

    print(f"Generating initial route set with {n_routes} routes")

    i = 0

    while i < n_routes:
        for m, n in newly_satisfied:
            for o, d in satisfied_by[(m, n)]:
                totals[(o, d)] -= ODX().get_odx(m, n)

        totals = {
            k: v
            for k, v in sorted(
                totals.items(), key=lambda item: item[1], reverse=True
            )
        }
        o, d = list(totals.keys())[0]
        path = road_G.shortest_path(o, d)
        if path == []:
            logger.error(f"No path from {o} to {d}")
            continue
        routes.append(path)
        newly_satisfied = set(DS().get_ds(o, d))
        i += 1
    return routes


def compute_ds(
    road_graph=config.BUS_ROAD_GRAPH_PATH, save_path=config.DS_PATH
):

    if isinstance(road_graph, str):
        road_graph = RoadGraph.load(road_graph)

    no_path = set()

    odx_pairs = set()
    ds = defaultdict(lambda: defaultdict(list))
    for o in ODX().origins:
        for d in ODX().get_dests(o):
            odx_pairs.add((o, d))

    for o, d in tqdm(odx_pairs):
        path = road_graph.shortest_path(o, d)

        if path == []:
            no_path.add((o, d))
            continue

        for idx, m in enumerate(path):
            if idx == len(path) - 1:
                continue
            for n in path[idx + 1 :]:
                ds[o][d].append((m, n))
    ds = ddict2dict(ds)

    print(f"Saving DS to {save_path}..")
    with open(save_path, "w") as f:
        json.dump(ds, f)

    logger.warning(
        f"{len(no_path)} pairs without path (out of {len(odx_pairs)})."
    )
    return ds