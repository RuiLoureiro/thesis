import json
from collections import namedtuple
from loguru import logger
from .utils import Singleton
from .common import Stop
from .geo import StopsDistance
from . import config

BusRouteTuple = namedtuple(
    "BusRouteTuple", ["route_id", "route_direction", "route_variant"]
)


def convert_gtfs_bus_stop_id(gtfs_stop_id):
    return int(gtfs_stop_id.split("_")[1])


class BusRoute:
    class Directions:
        ASC = "ASC"
        DESC = "DESC"
        CIRC = "CIRC"
        UNDEFINED = ""

    def __init__(
        self, route_id, route_direction, route_variant, route_stop_ids=None
    ):
        assert isinstance(route_variant, int)
        self.route_id = route_id
        self.route_direction = route_direction
        self.route_variant = route_variant
        self.route_stop_ids = route_stop_ids
        self.stage_times = None
        self.stage_dists = None

        self._sid_to_idx = {
            sid: idx for idx, sid in enumerate(self.route_stop_ids)
        }

        if self.route_direction == self.Directions.CIRC:
            self._sid_to_idx = {
                sid: idx for idx, sid in enumerate(self.route_stop_ids[:-1])
            }
        else:
            self._sid_to_idx = {
                sid: idx for idx, sid in enumerate(self.route_stop_ids)
            }

    def has_stop(self, stop_id):
        return stop_id in self._sid_to_idx

    def add_route_stops(self, route_stops):
        self.route_stops = route_stops

    def set_stage_times(self, stage_times):
        self.stage_times = stage_times

    def set_stage_dists(self, dists):
        self.stage_dists = dists

    def get_subsequent_stop_ids(self, entry_stop_id):

        try:
            idx = self._sid_to_idx[entry_stop_id]
        except KeyError:
            raise RuntimeError(f"stop_id `{entry_stop_id} not in route {self}")

        if self.route_direction == "CIRC":
            # circ routes have the first stop_id twice, in indices 0 and -1
            if idx == 0:
                return self.route_stop_ids[1:-1]
            else:
                return (
                    self.route_stop_ids[idx + 1 :] + self.route_stop_ids[:idx]
                )
        else:
            return self.route_stop_ids[idx + 1 :]

    def get_stage_dist(self, entry_sid, exit_sid):
        entry_idx = self._sid_to_idx[entry_sid]
        exit_idx = self._sid_to_idx[exit_sid]

        if entry_idx > exit_idx:
            if self.route_direction != self.Directions.CIRC:
                raise RuntimeError()

            if exit_idx == 0:
                stage_dist = (
                    self.stage_dists[-1] - self.stage_dists[entry_idx - 1]
                )
            else:
                stage_dist = (
                    self.stage_dists[-1]
                    - self.stage_dists[entry_idx - 1]
                    + self.stage_dists[exit_idx - 1]
                )
        else:
            if entry_idx > 0:
                stage_dist = (
                    self.stage_dists[exit_idx - 1]
                    - self.stage_dists[entry_idx - 1]
                )
            else:
                stage_dist = self.stage_dists[exit_idx - 1]

        return stage_dist

    def get_stage_time(self, entry_sid, exit_sid):
        entry_idx = self._sid_to_idx[entry_sid]
        exit_idx = self._sid_to_idx[exit_sid]

        if entry_idx > exit_idx:
            if self.route_direction != self.Directions.CIRC:
                raise RuntimeError()

            if exit_idx == 0:
                stage_time = (
                    self.stage_times[-1] - self.stage_times[entry_idx - 1]
                )
            else:
                stage_time = (
                    self.stage_times[-1]
                    - self.stage_times[entry_idx - 1]
                    + self.stage_times[exit_idx - 1]
                )

        else:
            if entry_idx > 0:
                stage_time = (
                    self.stage_times[exit_idx - 1]
                    - self.stage_times[entry_idx - 1]
                )
            else:
                stage_time = self.stage_times[exit_idx - 1]

        return round(stage_time)

    def to_dict(self):
        dict_ = {}

        attrs_to_save = [
            "route_id",
            "route_direction",
            "route_variant",
            "route_stop_ids",
        ]

        for attr in attrs_to_save:
            dict_[attr] = self.__getattribute__(attr)
        return dict_

    def __eq__(self, other):
        if isinstance(other, BusRoute):
            return (
                (self.route_id == other.route_id)
                and (self.route_direction == other.route_direction)
                and (self.route_variant == other.route_variant)
            )
        return False

    def __repr__(self):
        s = f"{self.route_id} {self.route_direction} [{self.route_variant}]"
        if self.route_stop_ids:
            s += f" ({self.route_stop_ids[0]}->{self.route_stop_ids[-1]})"
        return s


class BusStop(Stop):
    def __init__(
        self, stop_id, stop_name, stop_lat, stop_lon, street_point=None
    ):
        super().__init__(stop_id, stop_name, stop_lat, stop_lon)
        self.street_point = street_point
        self.gtfs_id = f"1_{self.stop_id}"

    def has_street_point(self):
        return self.street_point is not None

    def get_street_coords(self):
        return self.street_point["stop_lat"], self.street_point["stop_lon"]

    def get_street_bearing(self):
        return self.street_point["bearing"]

    @classmethod
    def from_dict(cls, dict_):
        return BusStop(**dict_)

    def to_dict(self):
        dict_ = {}

        attrs_to_save = [
            "stop_id",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "street_point",
        ]
        for attr in attrs_to_save:
            dict_[attr] = self.__getattribute__(attr)
        return dict_


class BusSchedule(metaclass=Singleton):
    def __init__(
        self,
        stops_path=config.BUS_STOPS_PATH,
        routes_path=config.BUS_ROUTES_PATH,
        # stage_times_osrm_path=config.BUS_STAGE_TIMES_OSRM_PATH,
        stage_times_gtfs_path=config.BUS_STAGE_TIMES_GTFS_PATH,
    ):

        logger.info(
            f"Initializing Schedule object. Loading routes in {routes_path} "
            f"and stops in {stops_path}.."
        )
        with open(stops_path) as file:
            self.stops_json = json.load(file)

        self.stops = [BusStop.from_dict(s) for s in self.stops_json]

        self.stop_distances = StopsDistance(self.stops)

        self._sid_to_idx = {}
        for idx, stop in enumerate(self.stops):
            self._sid_to_idx[stop.stop_id] = idx

        with open(routes_path) as file:
            self.routes_json = json.load(file)

        self.routes = [
            BusRoute(
                r["route_id"],
                r["route_direction"],
                r["route_variant"],
                r["route_stop_ids"],
            )
            for r in self.routes_json
        ]

        self._rid_to_idx = {}

        for idx, route in enumerate(self.routes):
            self._rid_to_idx[
                (route.route_id, route.route_direction, route.route_variant)
            ] = idx

        # SET STAGE TIMES
        with open(stage_times_gtfs_path) as file:
            stage_times_gtfs = json.load(file)
        # with open(stage_times_osrm_path) as file:
        #     stage_times_osrm = json.load(file)

        for r in self.routes:
            route_stage_times = []
            route_dists = []
            route_dist_acc = 0
            stage_time_acc = 0

            for idx, sid in enumerate(r.route_stop_ids):
                from_sid = sid
                try:
                    to_sid = r.route_stop_ids[idx + 1]
                except IndexError:
                    break

                try:
                    stage_time = stage_times_gtfs[str(from_sid)][str(to_sid)]
                except KeyError:
                    stage_time = (
                        30  # stage_times_osrm[str(from_sid)][str(to_sid)]
                    )

                route_dist_acc += self.get_distance(from_sid, to_sid)
                route_dists.append(route_dist_acc)

                if stage_time:
                    stage_time_acc += stage_time + config.BUS_STOP_TIME
                route_stage_times.append(stage_time_acc)
            r.set_stage_times(route_stage_times)
            r.set_stage_dists(route_dists)

    def get_distance(self, sid1, sid2):
        return self.stop_distances.get_distance(sid1, sid2)

    def get_route_by_id(self, rid):
        # ATTENTION: very slow!! to be used for debugging purposes!
        routes = []
        for r in self.routes:
            if r.route_id == rid:
                routes.append(r)

        return routes

    def save(
        self,
        routes_path=config.BUS_ROUTES_PATH,
        stops_path=config.BUS_STOPS_PATH,
    ):
        routes = [r.to_dict() for r in self.routes]
        with open(routes_path, "w") as out:
            json.dump(routes, out)

        stops = [s.to_dict() for s in self.stops]
        with open(stops_path, "w") as out:
            json.dump(stops, out)

    def check_duplicate_stops(self):
        unique_locations = {}

        duplicates = {}

        for stop in self.stops:
            location = (stop.stop_lon, stop.stop_lat)
            if location in unique_locations:
                duplicates[stop.stop_id] = unique_locations[location]
            else:
                unique_locations[location] = stop.stop_id

        return duplicates

    def get_stop(self, sid):
        try:
            idx = self._sid_to_idx[sid]
        except KeyError:
            return None
        return self.stops[idx]

    def get_route(self, *args):

        if len(args) == 1:
            route_tuple = args[0]
        elif len(args) == 3:
            route_tuple = BusRouteTuple(args[0], args[1], args[2])
        try:
            return self.routes[
                self._rid_to_idx[
                    (
                        route_tuple.route_id,
                        route_tuple.route_direction,
                        route_tuple.route_variant,
                    )
                ]
            ]
        except KeyError:
            return None

    def get_route_stops(self, route):
        return self.routes[
            self._rid_to_idx[
                (route.route_id, route.route_direction, route.route_variant)
            ]
        ].route_stop_ids

    def __repr__(self):
        msg = f"Bus Schedule with {len(self.routes)} routes and {len(self.stops)} stops"

        return msg
