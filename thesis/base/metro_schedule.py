from .common import Stop
from . import config
from .gtfs import RawGTFSReader
from .utils import Singleton
from .geo import StopsDistance


class MetroStop(Stop):
    def __init__(self, stop_id, stop_name, stop_lat, stop_lon):
        self.stop_id = stop_id
        self.stop_name = stop_name
        self.stop_lat = stop_lat
        self.stop_lon = stop_lon


class MetroRoute:
    def __init__(self, name, line_stop_ids):
        self.name = name
        self.line_stop_ids = line_stop_ids
        self.sid_to_idx = {
            sid: idx for idx, sid in enumerate(self.line_stop_ids)
        }

    def set_stage_dists(self, dists):
        self.stage_dists = dists

    def has_sid(self, sid):
        return sid in self.sid_to_idx

    def _get_dist(self, entry_idx, exit_idx):
        dists = self.stage_dists
        if exit_idx > entry_idx:
            if exit_idx > 0:
                dist = dists[exit_idx - 1]
            else:
                raise RuntimeError

            if entry_idx > 0:
                dist -= dists[entry_idx - 1]
            return dist
        else:
            return self._get_dist(exit_idx, entry_idx)

    def get_stage_dist(self, entry_sid, exit_sid):
        entry_idx = self.sid_to_idx[entry_sid]
        exit_idx = self.sid_to_idx[exit_sid]
        return self._get_dist(entry_idx, exit_idx)

    def __repr__(self):
        ms = MetroSchedule()
        return f"[Metro Route '{self.name.upper()}'] ({ms.get_stop(self.line_stop_ids[0]).stop_name}<->{ms.get_stop(self.line_stop_ids[-1]).stop_name})"


class MetroSchedule(metaclass=Singleton):
    def __init__(self, gtfs_path=config.METRO_GTFS_PATH):
        reader = RawGTFSReader(gtfs_path)
        self.routes = []

        self.stops = [
            MetroStop(
                int(r.stop_id[1:]),  # remove the M
                r.stop_name,
                r.stop_lat,
                r.stop_lon,
            )
            for r in reader.stops.itertuples()
        ]

        self._sid_to_idx = {}
        for idx, stop in enumerate(self.stops):
            self._sid_to_idx[stop.stop_id] = idx

        self.stops_distance = StopsDistance(self.stops)

        line_stops = {}
        self._name_to_route_idx = {}
        for route_id in reader.routes.route_id.unique():
            route_name = reader.routes[
                reader.routes.route_id == route_id
            ].route_long_name.iloc[0]
            trip_id = reader.trips[
                reader.trips.route_id == route_id
            ].trip_id.iloc[-1]
            stop_sequence = (
                reader.stop_times[reader.stop_times.trip_id == trip_id]
                .sort_values(by="stop_sequence")["stop_id"]
                .tolist()
            )
            line = route_name.split(" - ")[0].lower()

            # need this ugly 'if' because the gtfs is bad. amarela->odivelas is repeated..
            # one of them ends in campo grande
            # this way we assume the one with more stops is the correct one
            if line not in line_stops:
                line_stops[line] = [int(s[1:]) for s in stop_sequence]
            else:
                if len([int(s[1:]) for s in stop_sequence]) > len(
                    line_stops[line]
                ):
                    line_stops[line] = [int(s[1:]) for s in stop_sequence]
        for idx, (line_name, stops) in enumerate(line_stops.items()):
            ml = MetroRoute(line_name, stops)
            self.routes.append(ml)
            self._name_to_route_idx[line_name] = idx
            route_dists = []
            route_dist_acc = 0

            for idx, sid in enumerate(stops):
                from_sid = sid
                try:
                    to_sid = stops[idx + 1]
                except IndexError:
                    break

                route_dist_acc += self.stops_distance.get_distance(
                    from_sid, to_sid
                )
                route_dists.append(route_dist_acc)
            ml.set_stage_dists(route_dists)

    def get_route(self, name):
        return self.routes[self._name_to_route_idx[name]]

    def get_stop(self, sid):
        try:
            idx = self._sid_to_idx[sid]
        except KeyError:
            return None
        return self.stops[idx]

    def __repr__(self):
        msg = f"Metro Schedule with {len(self.routes)} routes and {len(self.stops)} stops"

        return msg
