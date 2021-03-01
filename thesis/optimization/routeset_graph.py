import copy
import inspect
from collections import defaultdict, namedtuple
import graph_tool as gt
import numpy as np
from .common import ODX, Durations

ORIGIN = -1
DEST = -2

VertexToStop = namedtuple("VertexToStop", ["stop_id", "route_id"])


def _routes_changed(fn):
    """
    Decorator used to keep track of route changes that change the fitness value
    """

    def magic(self, *args, **kwargs):

        # saves change history
        if self.__class__.SAVE_HISTORY:
            if fn.__name__ not in self.__class__._sigs:
                self.__class__._sigs[fn.__name__] = inspect.signature(fn)
            args_dict = (
                self.__class__._sigs[fn.__name__]
                .bind(self, *args, **kwargs)
                .arguments
            )
            del args_dict["self"]
            self._history.append((fn.__name__, args_dict))
        self.routes_changed = True
        fn(self, *args, **kwargs)

    return magic


class Stop:
    """
    Represents a stop in the route set graph
    A stop can have several nodes in the graph (route nodes, origin nodes, destination nodes and a transfer node)
    """

    class RouteNode:
        """
        Represents a route node
        """

        def __init__(self, route_id, stop_seq, vertex_idx):
            self.route_id = route_id
            self.stop_seq = stop_seq
            self.vertex_idx = vertex_idx

        def __repr__(self):
            return f"Stop #{self.stop_seq}"

        def copy(self):
            new = self.__class__(self.route_id, self.stop_seq, self.vertex_idx)
            return new

    def __init__(self, stop_id, origin_idx, destination_idx):
        self.stop_id = stop_id
        self.origin_idx = origin_idx
        self.destination_idx = destination_idx
        self.route_nodes = {}

    def add_route_node(self, route_id, stop_seq, vertex_idx):
        assert route_id not in self.route_nodes
        self.route_nodes[route_id] = self.RouteNode(
            route_id, stop_seq, vertex_idx
        )

    def __repr__(self):
        msg = f"Stop {self.stop_id}, serviced by routes {tuple(self.route_nodes.keys())}"
        return msg

    def update_reference(self, route_id, new_idx):
        """
        https://graph-tool.skewed.de/static/doc/graph_tool.html#graph_tool.Graph.remove_vertex
        """
        if route_id == ORIGIN:
            self.origin_idx = new_idx
        elif route_id == DEST:
            self.destination_idx = new_idx
        else:
            self.route_nodes[route_id].vertex_idx = new_idx

    def delete_node(self, route_id):
        del self.route_nodes[route_id]

    def get_seq(self, route_id):
        return self.route_nodes[route_id].stop_seq

    def update_seq(self, route_id, new_seq):
        self.route_nodes[route_id].stop_seq = new_seq

    def copy(self):
        new = Stop(self.stop_id, self.origin_idx, self.destination_idx)
        new.route_nodes = {k: rn.copy() for k, rn in self.route_nodes.items()}
        return new

    def get_route_ids(self):
        return self.route_nodes.keys()


class RouteSetGraph:
    _sigs = {}

    bus_stop_time = 30
    w1 = 300
    w2_offset = 50 * 60

    transfer_time = 300
    SAVE_HISTORY = True

    @classmethod
    def set_parameters(cls, bus_stop_time, w1, w2_offset):
        cls.bus_stop_time = bus_stop_time
        cls.w1 = w1
        cls.w2_offset = w2_offset

    def register_history(self, name, **kwargs):

        if self.__class__.SAVE_HISTORY:
            d = {"name": name}
            d.update(kwargs)
            rid = kwargs["route_id"]

            self._history[rid].append(d)

    def __init__(self):
        self._history = []
        self.gtG = gt.Graph(directed=True)
        self.gtG.ep["duration"] = self.gtG.new_ep("float")
        self.vertex_to_stop = {}
        self.routes = {}
        self.stops = {}
        self.routes_changed = False
        self._fitness = -1
        self._report = None

    def copy(self):
        new = RouteSetGraph()
        new.gtG = gt.Graph(self.gtG)
        new.vertex_to_stop = self.vertex_to_stop.copy()
        new.routes = {k: [*sids] for k, sids in self.routes.items()}
        new.stops = {sid: s.copy() for sid, s in self.stops.items()}
        new.routes_changed = self.routes_changed
        new._fitness = self._fitness
        if self.__class__.SAVE_HISTORY:
            new._history = copy.deepcopy(self._history)
        return new

    def nroutes(self):
        return len(self.routes)

    def get_fitness(self):
        if self.routes_changed:
            self._report, self._fitness = self._compute_fitness()
            self.routes_changed = False
        return self._fitness

    def get_report(self):
        self.get_fitness()
        return self._report

    def _compute_fitness(self):
        TT = 0
        transfers = defaultdict(int)

        TTR = 0
        TU = 0

        unsatisfied_od_pairs = set()
        unsatisfied_demand = 0
        unsatisfied_stops = set()

        no_path = set()
        no_path_l2 = set()

        satisfied_od_pairs = set()
        satisfied_demand = 0
        satisfied_stops = set()
        travel_times = []

        for o in ODX().origins:
            destinations = []

            if o not in self.stops:
                unsatisfied_stops.add(o)

                unsatisfied_od_pairs.update(
                    [(o, d) for d in ODX().get_dests(o)]
                )
                unsatisfied_demand += sum(
                    [ODX().get_odx(o, d) for d in ODX().get_dests(o)]
                )
                continue
            satisfied_stops.add(o)

            for d in ODX().get_dests(o):
                if d not in self.stops:
                    unsatisfied_stops.add(d)

                    unsatisfied_od_pairs.add((o, d))
                    unsatisfied_demand += ODX().get_odx(o, d)
                    continue

                # satisfied
                satisfied_stops.add(d)

                destinations.append(d)

            distances_ntransfers = self.get_distances_transfers(
                o, destinations
            )

            for d, val in distances_ntransfers.items():
                dist, ntransfers = val
                odx_val = ODX().get_odx(o, d)

                if dist is None:
                    no_path.add((o, d))
                    unsatisfied_od_pairs.add((o, d))
                    unsatisfied_demand += odx_val
                else:
                    TTR += ntransfers * odx_val
                    transfers[ntransfers] += odx_val
                    travel_times.append(dist)

                    if ntransfers > 2:
                        no_path_l2.add((o, d))
                        unsatisfied_od_pairs.add((o, d))
                        unsatisfied_demand += odx_val
                    else:
                        satisfied_od_pairs.add((o, d))
                        satisfied_demand += odx_val

                    TT += dist * odx_val

        TU = unsatisfied_demand
        ATT = np.mean(travel_times)
        w2 = ATT + self.w2_offset

        report = {
            "nsatisfied_od_pairs": len(satisfied_od_pairs),
            "nunsatisfied_od_pairs": len(unsatisfied_od_pairs),
            "nsatisfied_stops": len(satisfied_stops),
            "nunsatisfied_stops": len(unsatisfied_stops),
            "satisfied_demand": satisfied_demand,
            "unsatisfied_demand": unsatisfied_demand,
            "average_travel_time_min": round(ATT / 60, 2),
            "transfers": transfers,
            "no_path": len(no_path),
            "no_path_less_2_transfers": len(no_path_l2),
        }
        self.no_path = no_path
        return report, TT + TTR + TU * w2

    def add_vertex_to_stop_mapping(self, vertex, stop_id, route_id):
        self.vertex_to_stop[self.get_vertex_index(vertex)] = VertexToStop(
            stop_id, route_id
        )

    def get_origin_vertex(self, stop_id):
        return self.gtG.vertex(self.stops[stop_id].origin_idx)

    def get_destination_vertex(self, stop_id):
        return self.gtG.vertex(self.stops[stop_id].destination_idx)

    def get_route_vertex(self, stop_id, route_id):
        stop = self.stops[stop_id]
        return self.gtG.vertex(stop.route_nodes[route_id].vertex_idx)

    def get_vertex_index(self, vertex):
        """
        Returns vertex_index of `vertex`
        """
        return self.gtG.vertex_index[vertex]

    def get_route(self, route_id):
        """
        Returns route `route_id` stops
        """
        return self.routes[route_id]

    @_routes_changed
    def append_stop(self, stop_id, route_id):
        """
        Appends `stop_id` to route `route_id`
        """
        route = self.routes[route_id]
        self._add_stop(stop_id, route_id, len(route))

        if len(route) > 1:
            prev_sid = route[-2]
            duration = (
                # Durations().get_duration(prev_sid, stop_id)
                100
                + self.bus_stop_time
            )
            self._add_route_edge(prev_sid, stop_id, route_id, duration)

    @_routes_changed
    def prepend_stop(self, stop_id, route_id):
        """
        Prepends `stop_id` to route `route_id`
        """
        route = self.routes[route_id]
        self._add_stop(stop_id, route_id, 0)

        for sid in route[1:]:
            stop = self.stops[sid]
            stop.update_seq(route_id, stop.get_seq(route_id) + 1)

        next_sid = route[1]
        #         duration = 1 + self.bus_stop_time
        duration = (
            Durations().get_duration(stop_id, next_sid) + self.bus_stop_time
        )
        self._add_route_edge(stop_id, next_sid, route_id, duration)

    def _add_stop_base(self, stop_id: int):
        """
        Adds origin and destination nodes to graph and appends stop to list of stops
        """
        # create origin node
        origin_v = self.gtG.add_vertex()
        # create destination node
        dest_v = self.gtG.add_vertex()

        self.add_vertex_to_stop_mapping(origin_v, stop_id, ORIGIN)
        self.add_vertex_to_stop_mapping(dest_v, stop_id, DEST)

        stop = Stop(
            stop_id,
            self.get_vertex_index(origin_v),
            self.get_vertex_index(dest_v),
        )
        self.stops[stop_id] = stop

    def _add_stop(self, stop_id: int, route_id: int, stop_seq: int):
        """
        Adds `stop_id` to `route_id`, at index `stop_seq`
        """

        # create base nodes
        if stop_id not in self.stops:
            self._add_stop_base(stop_id)

        # create route node and append stop to route
        route_node = self.gtG.add_vertex()

        self.routes[route_id].insert(stop_seq, stop_id)
        self.add_vertex_to_stop_mapping(route_node, stop_id, route_id)

        # add stop edges from route node to base nodes
        origin_v = self.get_origin_vertex(stop_id)
        dest_v = self.get_destination_vertex(stop_id)

        e = self.gtG.add_edge(origin_v, route_node)
        self.gtG.ep["duration"][e] = 0
        e = self.gtG.add_edge(route_node, dest_v)
        self.gtG.ep["duration"][e] = 0

        stop = self.stops[stop_id]

        # add edges to every other route node for this stop
        for other_route_id in stop.get_route_ids():
            other_node = self.get_route_vertex(stop_id, other_route_id)
            e = self.gtG.add_edge(route_node, other_node)
            self.gtG.ep["duration"][e] = self.transfer_time
            e = self.gtG.add_edge(other_node, route_node)
            self.gtG.ep["duration"][e] = self.transfer_time

        # add route_node to stop
        stop.add_route_node(
            route_id, stop_seq, self.get_vertex_index(route_node)
        )

    def _add_route_edge(self, from_sid, to_sid, route_id, duration):
        """
        Adds edge between `from_sid` and `to_sid`, in route `route_id`, with duration `duration`
        """
        from_v = self.get_route_vertex(from_sid, route_id)
        to_v = self.get_route_vertex(to_sid, route_id)

        e = self.gtG.add_edge(from_v, to_v)
        self.gtG.ep["duration"][e] = duration

    @_routes_changed
    def add_route(self, route, route_id=None):
        """
        Adds new route
        """
        if route_id is None:
            route_id = len(self.routes)

        self.routes[route_id] = []
        for sid in route:
            self.append_stop(sid, route_id)

    @_routes_changed
    def replace_route(self, route_id: int, new_route: list):
        """
        Replaces route with id `route_id` with `new_route`
        """
        self.remove_route(route_id)
        self.add_route(new_route, route_id)

    def _get_last_vertex(self):
        """
        Returns vertex with highest index
        See:
        https://graph-tool.skewed.de/static/doc/graph_tool.html#graph_tool.Graph.remove_vertex
        """
        num_vertices = self.gtG.num_vertices(ignore_filter=True)
        last_v = self.gtG.vertex(num_vertices - 1)
        return last_v

    def _update_last_node_references(self, node):
        """
        Called before `node` is deleted.

        https://graph-tool.skewed.de/static/doc/graph_tool.html#graph_tool.Graph.remove_vertex
        """
        last_v = self._get_last_vertex()
        if last_v == node:
            # node to be deleted is the last one, so nothing to be done
            # we only return to save cpu cycles
            return

        stop_map = self.vertex_to_stop[self.gtG.vertex_index[last_v]]
        stop_id = stop_map.stop_id
        route_id = stop_map.route_id

        stop = self.stops[stop_id]
        stop.update_reference(route_id, self.gtG.vertex_index[node])
        self.vertex_to_stop[self.gtG.vertex_index[node]] = copy.copy(
            self.vertex_to_stop[self.gtG.vertex_index[last_v]]
        )
        del self.vertex_to_stop[self.gtG.vertex_index[last_v]]

    def get_edge(self, from_sid: int, to_sid: int, route_id: int):
        """
        Returns edge between `from_sid` and `to_sid`, in route `route_id`
        """
        from_v = self.stops[from_sid].route_nodes[route_id].vertex_idx
        to_v = self.stops[to_sid].route_nodes[route_id].vertex_idx
        return self.gtG.edge(from_v, to_v)

    @_routes_changed
    def remove_route(self, route_id: int):
        """
        Removes route `route_id`
        """
        route = self.routes[route_id]
        for stop_id in reversed(route):
            stop = self.stops[stop_id]
            node = self.get_route_vertex(stop_id, route_id)
            self._update_last_node_references(node)
            self.gtG.remove_vertex(node, fast=True)
            stop.delete_node(route_id)

            if not stop.route_nodes:
                self._delete_stop(stop_id)

        del self.routes[route_id]

    def get_routes(self):
        """
        Returns routes
        """
        return self.routes.values()

    def _delete_stop(self, stop_id):
        """
        Deletes stop `stop_id`
        """
        stop = self.stops[stop_id]

        destination_node = self.gtG.vertex(stop.destination_idx)
        self._update_last_node_references(destination_node)
        self.gtG.remove_vertex(destination_node, fast=True)

        origin_node = self.gtG.vertex(stop.origin_idx)
        self._update_last_node_references(origin_node)
        self.gtG.remove_vertex(origin_node, fast=True)

        del self.stops[stop_id]

    @_routes_changed
    def remove_node(self, stop_id: int, route_id: int):
        """
        Removes stop `stop_id` from route `route_id`
        """
        stop = self.stops[stop_id]

        seq = stop.get_seq(route_id)
        route = self.routes[route_id]

        # update stop seqs
        if seq > 0 and seq < (len(self.routes[route_id]) - 1):
            e1 = self.get_edge(route[seq - 1], route[seq], route_id)
            e2 = self.get_edge(route[seq], route[seq + 1], route_id)
            duration = (
                self.gtG.ep["duration"][e1]
                + self.gtG.ep["duration"][e2]
                - self.bus_stop_time
            )
            self._add_route_edge(
                route[seq - 1], route[seq + 1], route_id, duration
            )

        node = self.get_route_vertex(stop_id, route_id)
        self._update_last_node_references(node)
        self.gtG.remove_vertex(node, fast=True)
        stop.delete_node(route_id)

        # update seqs
        for sid in route[seq + 1 :]:
            stop = self.stops[sid]
            stop.update_seq(route_id, stop.get_seq(route_id) - 1)
        del route[seq]

        if not stop.route_nodes:
            self._delete_stop(stop_id)

    def shortest_distance(self, from_sid: int, to_sids: list):
        """
        Computes shortest distance between from_sid` and each sid in `to_sids`
        Returns
        -------
        Distances and pred_map

        """
        # get origin
        from_v = self.get_origin_vertex(from_sid)
        # get destinations
        to_vs = [self.get_destination_vertex(to_sid) for to_sid in to_sids]

        dists, pred_map = gt.topology.shortest_distance(
            self.gtG,
            from_v,
            to_vs,
            weights=self.gtG.ep["duration"],
            pred_map=True,
        )

        return dists, pred_map

    @staticmethod
    def is_route_node(node):
        return node.route_id >= 0

    def get_distances_transfers(self, from_sid: int, to_sids: list):
        """
        Computes shortest path distance and number of transfers between `from_sid` and each sid in `to_sids`
        """

        # compute shortest distances
        dists, pred_map = self.shortest_distance(
            from_sid,
            to_sids,
        )
        res = {}

        for to_sid, dist in zip(to_sids, dists):

            # no path
            if dist == float("inf"):
                res[to_sid] = (None, None)
                continue

            sid = to_sid
            v = self.get_destination_vertex(sid)
            rids = set()
            while sid != from_sid:
                next_idx = pred_map[v]
                vts = self.vertex_to_stop[next_idx]
                sid = vts.stop_id
                rid = vts.route_id
                if self.is_route_node(vts):
                    rids.add(rid)
                v = self.gtG.vertex(next_idx)

            ntransfers = len(rids) - 1

            if ntransfers == -1:
                raise RuntimeError

            # passenger doesn't have to wait for bus stop time when transferring and alighting
            dist = dist - (
                (self.bus_stop_time * ntransfers) + self.bus_stop_time
            )
            res[to_sid] = (dist, ntransfers)
        return res

        # """

    # put this here only for thesis document. out of convinience.
    # doesnt make any sense to be in this class
    # """

    # def get_stats_latex(self):
    #     import tabulate
    #     from genetic.config import latex_table_kwargs

    #     self.get_fitness()

    #     def get_pct(satisfied_name, unsatisfied_name):
    #         satisfied = self._report[satisfied_name]
    #         unsatisfied = self._report[unsatisfied_name]
    #         return (satisfied / (satisfied + unsatisfied)) * 100

    #     pct_od_pairs = get_pct("nsatisfied_od_pairs", "nunsatisfied_od_pairs")
    #     pct_demand = get_pct("satisfied_demand", "unsatisfied_demand")
    #     pct_stops = get_pct("nsatisfied_stops", "nunsatisfied_stops")
    #     att = self._report["average_travel_time_min"]

    #     headers = ["Measure", "Value"]

    #     values = [
    #         ("Objective function", self.get_fitness()),
    #         ("Satisfied OD pairs (%)", round(pct_od_pairs, 2)),
    #         ("Satisfied demand (%)", round(pct_demand, 2)),
    #         ("Satisfied stops (%)", round(pct_stops, 2)),
    #         ("Average travel time (min)", round(att, 2)),
    #         (
    #             "Average number of transfers",
    #             round(
    #                 sum(
    #                     [
    #                         n * count
    #                         for n, count in self._report["transfers"].items()
    #                     ]
    #                 )
    #                 / self._report["satisfied_demand"],
    #                 2,
    #             ),
    #         ),
    #     ]

    #     return tabulate.tabulate(
    #         tabular_data=values,
    #         headers=headers,
    #         tablefmt="latex",
    #         **latex_table_kwargs
    #     )

    #     # print(F"Fitness: {self.get_fitness()}")
    #     # print(f"Demand: {pct_demand}%")
    #     # print(f"Stops: {pct_stops}%")
    #     # print(f"OD pairs: {pct_od_pairs}%")
    #     # print(f"ATT: {att} min")
    #     # print(f"transfers: {ddict2dict(self._report['transfers'])}")
