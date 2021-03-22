# CONFIG PARAMETERS
from . import config
import polyline
import requests
from collections import namedtuple

from ..base.bus_schedule import BusStop

# Represents a route returned by OSRM
RouteResult = namedtuple("RouteResult", ["points", "distance", "duration"])


def is_osrm_up(url=config.BASE_OSRM_URL):
    """
    Checks if the endpoint in `url` is reachable
    """
    try:
        requests.get(url)
    except requests.exceptions.ConnectionError:
        return False
    return True


def _need_osrm_up(func):
    """
    Decorator that raises an exception if the osrm url is not reachable
    To be applied to methods that rely on osrm
    """
    def wrapper(*args, **kwargs):
        if is_osrm_up():
            return func(*args, **kwargs)
        else:
            raise RuntimeError(f"No OSRM instance in {config.BASE_OSRM_URL}")
        return func(*args, **kwargs)

    return wrapper


def _bearing_to_str(bearing):
    """takes the int part of a float bearing
    and adds a range of `BEARING_RANGE` degrees."""
    return f"{int(bearing)},{config.OSRM_BEARING_RANGE}"


def _coords_to_str(coords):
    """takes list of coordinates in lat,lon format and returns a string
    to be used in an OSRM request"""

    return ";".join([f"{c[1]},{c[0]}" for c in coords])


def format_route_response(r: dict):
    """
    Formats OSRM route response into a `RouteResult` object
    """
    geometry = r["routes"][0]["geometry"]

    points = polyline.decode(geometry)
    distance = r["routes"][0]["distance"]
    duration = r["routes"][0]["duration"]

    return RouteResult(points=points, distance=distance, duration=duration)


def get_route_url(
    origin_coords, destination_coords, origin_bearing, destination_bearing
):
    """
    Builds OSRM table service URL from lists of coordinates and bearings

    Returns
    -------
    str
        OSRM url
    """
    bearings_str = ";".join(
        _bearing_to_str(b) for b in [origin_bearing, destination_bearing]
    )

    coords_str = _coords_to_str([origin_coords, destination_coords])

    return (
        f"{config.BASE_OSRM_URL}/route/v1/driving/{coords_str}"
        f"?bearings={bearings_str}&continue_straight=true"
    )


def get_table_url(
    origin_coords: list,
    destination_coords: list,
    origin_bearings: list,
    destination_bearings: list,
):
    """
    Builds OSRM table service URL from lists of coordinates and bearings

    Returns
    -------
    str
        OSRM url
    """
    # coord arguments must come in lat, lon!!

    assert len(origin_coords) == len(origin_bearings), "len of `origin_coords` is different from len of `origin_bearings`"
    assert len(destination_coords) == len(destination_bearings), "len of `destination_coords` is different from len of `destination_bearings`"

    coords_str = _coords_to_str(origin_coords + destination_coords)

    bearings_str = ";".join(
        _bearing_to_str(b) for b in origin_bearings + destination_bearings
    )

    n_origins = len(origin_coords)
    n_destinations = len(destination_coords)

    origins_idx = ";".join(str(x) for x in list(range(n_origins)))
    destinations_idx = ";".join(
        str(x) for x in list(range(n_origins, n_origins + n_destinations))
    )

    return (
        f"{config.BASE_OSRM_URL}/table/v1/driving/{coords_str}"
        f"?bearings={bearings_str}&sources={origins_idx}"
        f"&destinations={destinations_idx}&annotations=distance,duration"
    )


@_need_osrm_up
def get_route(
    origin_coords: tuple, destination_coords: tuple, origin_bearing: float, destination_bearing: float
):
    """
    Computes shortest route (coordinates, distance and duration) between
    two points

    Returns
    -------
    RouteResult
    """
    url = get_route_url(
        origin_coords, destination_coords, origin_bearing, destination_bearing
    )
    r = requests.get(url)

    return format_route_response(r.json())


def get_stop_route_url(origin_stop: BusStop, destination_stop: BusStop):

    return get_route_url(
        origin_stop.get_street_coords(),
        destination_stop.get_street_coords(),
        origin_stop.get_street_bearing(),
        destination_stop.get_street_bearing(),
    )


@_need_osrm_up
def get_stop_route(origin_stop, destination_stop):

    return get_route(
        origin_stop.get_street_coords(),
        destination_stop.get_street_coords(),
        origin_stop.get_street_bearing(),
        destination_stop.get_street_bearing(),
    )
