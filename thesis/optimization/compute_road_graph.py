import json
import numpy as np
from tqdm.auto import tqdm
from loguru import logger
from rich import print
from . import config
from .osrm import get_table_url, need_osrm_up
from ..base.utils import nested_dict_to_int

from ..base.geo import _broadcasting_based_haversine
from .concurrent_requests import make_requests
from .road_graph import RoadGraph
from ..base.bus_schedule import BusSchedule


def get_odx_stops(odx_path=config.BUS_ODX_MATRIX_PATH):
    """
    Returns list of stops that are part of the OD matrix
    """
    with open(odx_path, 'r') as file:
        odx_og = json.load(file)
    odx = {}

    stops_in_odx = set()
    for k in odx_og:
        odx[int(k)] = {}
        stops_in_odx.add(int(k))
        for kk, v in odx_og[k].items():
            odx[int(k)][int(kk)] = v
            stops_in_odx.add(int(kk))

    return stops_in_odx


def get_stop_to_stops_table_url(origin_stop, destination_stops):
    """Generates URLs for OSRM's table service, using the `street_point` of
    `origin_stop` and `destination_stops` as the origin and destinations,
    respectively.

    Parameters
    ----------
    origin_stop: :obj:`schedule.Stop`
        The origin stop
    destination_stops: list of :obj:`schedule.Stop`
        The destination stops

    Returns
    -------
    str
        The OSRM table url
    """
    if not destination_stops:
        return None
    origin_coords = (
        origin_stop.street_point["stop_lat"],
        origin_stop.street_point["stop_lon"],
    )
    origin_bearing = origin_stop.street_point["bearing"]

    destination_coords = []
    destination_bearings = []

    for destination_stop in destination_stops:
        destination_coords.append(
            (
                destination_stop.street_point["stop_lat"],
                destination_stop.street_point["stop_lon"],
            )
        )
        destination_bearings.append(destination_stop.street_point["bearing"])

    return get_table_url(
        [origin_coords],
        destination_coords,
        [origin_bearing],
        destination_bearings,
    )


def compute_stops_neighbors(stops, dist):
    """For every stop in `stops`, compute the stops in `stops`
    that are within a radius of `dist`.
    Parameters
    ----------
    stops: list of :obj:`schedule.Stop`
        The list of stops
    dist: float
        The radius, in km, used to calculate the neighbors of every stop
    Returns
    -------
    dict
        Dictionary with the stop_ids as keys and a list of neighbors as value.
    """
    # we want only stops that have a computed street point
    valid_stops = [s for s in stops if s.has_street_point()]
    invalid_stops = [s for s in stops if not s.has_street_point()]

    if len(invalid_stops) > 0:
        logger.warning(
            f"{len(invalid_stops)} of the {len(stops)} "
            "supplied stops don't have a stop mapping, skipping those.."
        )
        # https://stackoverflow.com/questions/44780357/how-to-use-newline-n-in-f-string-to-format-output-in-python-3-6
        nl = "\n\t"

        logger.debug(
            f"Stops with missing street_points: "
            f"\n\t{nl.join([str(s) for s in invalid_stops])}"
        )

    points_array = np.array(
        [
            [s.street_point["stop_lat"], s.street_point["stop_lon"]]
            for s in valid_stops
        ]
    )

    dists = _broadcasting_based_haversine(points_array, points_array)

    neighbors = {}

    for idx, stop in enumerate(valid_stops):
        neighbors[stop.stop_id] = [
            valid_stops[nidx].stop_id
            for nidx in np.where(dists[idx] < dist)[0]
            if idx != nidx
        ]

    return neighbors


@need_osrm_up
def compute_distances_durations(stops, dist):
    """Computes the distances and durations of the shortest path between every stop in `stops`
    and its neighbor stops, i.e, the stops that are within a radius of `dist`.

    Parameters
    ----------
    stops: list of :obj:`schedule.Stop`
        The list of stops
    dist: float
        The radius, in km, used to calculate the neighbors of every stop

    Returns
    -------
    tuple of dicts
        A nested dictionary, with the origin stop_id as outter key,
        the stop_id of the destination stop as the inner dictionary key,
        and the distance/duration (in meters/seconds) of the shortest path between the origin
        and destination stops as the inner dictionary value.
    """
    logger.info("Computing stop neighbors..")
    neighbors = compute_stops_neighbors(stops, dist)
    urls = {}

    sched = BusSchedule()

    for stop_id in neighbors:

        if len(neighbors[stop_id]) == 0:
            logger.warning(
                f"Stop {sched.get_stop(stop_id)} has no neighbor stops, "
                "skipping.."
            )
            continue

        # dict keys must be str
        urls[str(stop_id)] = get_stop_to_stops_table_url(
            sched.get_stop(stop_id),
            [sched.get_stop(ns) for ns in neighbors[stop_id]],
        )

    tables = make_requests(urls)

    # convert sid back to int
    tables = {int(k): v for k, v in tables.items()}

    durations = {}
    distances = {}
    for from_stop in tables:
        durations[from_stop] = {}
        distances[from_stop] = {}

        # OSRM supports n origins, returning a 2d array.
        # since we have a single origin,
        # we extract the first (and only) row from the array
        for idx, (dist_, durat_) in enumerate(zip(tables[from_stop]["distances"][0], tables[from_stop]["durations"][0])):
            to_stop = neighbors[from_stop][idx]

            distances[from_stop][to_stop] = dist_
            durations[from_stop][to_stop] = durat_

    return distances, durations


def same_dist(dist1, dist2, thresh=1e-1):
    """
    True if the dists absolute difference is less than `thresh`
    """
    return round(abs(dist1 - dist2), 1) <= thresh


def build_graph(
    stops,
    dist=1000, # meters
    durations=config.BUS_STOP_DURATIONS_PATH,
    distances=config.BUS_STOP_DISTANCES_PATH,
):

    if isinstance(durations, str):
        with open(durations, 'r') as f:
            durations = nested_dict_to_int(json.load(f))

    if isinstance(distances, str):
        with open(distances, 'r') as f:
            distances = nested_dict_to_int(json.load(f))

    G = RoadGraph()

    for sid in tqdm(stops):

        if np.isnan(
            np.array(list(durations[sid].values())).astype(float)
        ).all():
            logger.error(f"Stop {sid} has no paths to any other stop, ignoring stop..")
            continue

        if (np.array(list(durations[sid].values())) == 0).all():
            logger.error(f"Stop {sid} has every path with durtation 0 ignoring stop..")
            continue

        d = sorted(list(durations[sid].items()), key=lambda t: t[1])

        d = [t for t in d if t[0] in stops]

        for idx, (candidate, duration) in enumerate(d):
            if distances[sid][candidate] > dist:
                continue
            # possible stops between sid and candidate, ordered by dist
            possible_middles = d[:idx]

            for m, vv in possible_middles:

                if vv is None:
                    continue

                # check if candidate is a neighbor of m
                try:
                    dd = durations[m][candidate]
                except KeyError:
                    continue

                # check if a path between m and candidate exists
                if dd is None:
                    continue

                # if the dist of sid->candidate is the same
                # as sid->m + m->candidate
                if same_dist(duration, vv + dd):
                    break

            # if no valid middle stop has been found, add edge sid->candidate
            else:
                G.add_edge(
                    sid,
                    candidate,
                    duration=duration,
                    distance=distances[sid][candidate]
                )

    return G


def compute_road_graph(
    distances_path=config.BUS_STOP_DISTANCES_PATH,
    durations_path=config.BUS_STOP_DURATIONS_PATH,
    road_graph_path=config.BUS_ROAD_GRAPH_PATH,
):
    stops_in_odx = get_odx_stops()
    stops = [BusSchedule().get_stop(sid) for sid in stops_in_odx]

    distances = durations = None

    print(f"Loading distances from {distances_path} and durations from {durations_path}")
    try:
        with open(distances_path, 'r') as f:
            distances = json.load(f)
        with open(durations_path, 'r') as f:
            durations = json.load(f)
    except Exception as e:
        print(f"[red]Error loading distances and durations: {e}")

    if (distances is None) or (durations is None):
        print("Computing stop distances and durations..")
        distances, durations = compute_distances_durations(stops, 6000)

        print(f"Saving distances to {distances_path} and durations to {durations_path}")
        # save distances and durations
        with open(distances_path, 'w') as f:
            json.dump(distances, f)
        with open(durations_path, 'w') as f:
            json.dump(durations, f)
    

    print(f"Computing road graph..")
    road_graph = build_graph(dist=3000, stops=stops_in_odx)

    print(f"Saving road graph to {road_graph_path}..")
    road_graph.save(road_graph_path)
