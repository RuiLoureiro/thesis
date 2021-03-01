from collections import namedtuple
import graph_tool as gt
from graph_tool import topology, load_graph
from . import config


class RoadGraph:
    """
    Represents the road network, wrapping a graphtool directed graph.
    """

    Edge = namedtuple("Edge", ["source", "target", "duration", "distance"])

    def __init__(self):
        """
        Don't call __init__ directly! Use the class method `load` instead.
        """
        self.gtG = gt.Graph(directed=True)

        self.gtG.gp["sid2idx"] = self.gtG.new_gp("object")
        self.gtG.gp["sid2idx"] = {}
        self.gtG.vp["stop_id"] = self.gtG.new_vp("int")
        self.gtG.ep["duration"] = self.gtG.new_ep("float")
        self.gtG.ep["distance"] = self.gtG.new_ep("float")

    def save(self, path):
        self.gtG.save(path)

    @classmethod
    def load(cls, path=config.BUS_ROAD_GRAPH_PATH):
        """ Loads a graphtool graph
        """
        gtG = load_graph(path)

        inst = cls()
        inst.gtG = gtG
        return inst

    def get_vertex(self, stop_id):
        try:
            vidx = self.gtG.gp["sid2idx"][stop_id]
        except KeyError:
            return None
        return self.gtG.vertex(vidx)

    def edge(self, f_sid, t_sid):
        """
        Returns edge from `f_sid` to `to_sid` if it exists, otherwise returns None
        """
        e = self.gtG.edge(self.get_vertex(f_sid), self.get_vertex(t_sid))
        if not e:
            return None

        duration = self.gtG.ep["duration"][e]
        distance = self.gtG.ep["distance"][e]

        return self.Edge(f_sid, t_sid, duration, distance)

    def node_exists(self, stop_id):
        """
        True if node representing stop `stop_id` exists in the graph
        """
        return self.get_vertex(stop_id) is not None

    def add_node(self, stop_id):
        """
        Adds node representing stop `stop_id` to the graph
        """
        assert not self.node_exists(stop_id)

        v = self.gtG.add_vertex()
        self.gtG.vp["stop_id"][v] = stop_id
        self.gtG.gp["sid2idx"][stop_id] = self.gtG.vertex_index[v]

    def add_edge(self, from_sid, to_sid, duration, distance):
        """
        Adds edge from `from_sid` to `to_sid`
        """
        if not self.node_exists(from_sid):
            self.add_node(from_sid)

        if not self.node_exists(to_sid):
            self.add_node(to_sid)

        from_v = self.get_vertex(from_sid)
        to_v = self.get_vertex(to_sid)
        e = self.gtG.add_edge(from_v, to_v)
        self.gtG.ep["duration"][e] = duration
        self.gtG.ep["distance"][e] = distance

    def in_neighbors(self, stop_id):
        """
        In neighbors of `stop_id`
        """
        v = self.get_vertex(stop_id)
        return [self.gtG.vp["stop_id"][nv] for nv in v.in_neighbors()]

    def out_neighbors(self, stop_id):
        """
        Out neighbors of `stop_id`
        """
        v = self.get_vertex(stop_id)
        return [self.gtG.vp["stop_id"][nv] for nv in v.out_neighbors()]

    def shortest_distance(self, from_sid, to_sids: list):
        """
        Computes the shortest distances between stop `from_sid` and stops `to_sids`
        """

        from_v = self.get_vertex(from_sid)
        to_vs = [self.get_vertex(to_sid) for to_sid in to_sids]
        dists, pred_map = topology.shortest_distance(
            self.gtG,
            from_v,
            to_vs,
            weights=self.gtG.ep["duration"],
            pred_map=True,
        )
        return dists, pred_map

    def shortest_path(self, from_sid, to_sid, pred_map=None):
        """
        Computes the shortest path between stop `from_sid` and stop `to_sid`
        """
        from_v = self.get_vertex(from_sid)
        to_v = self.get_vertex(to_sid)

        path_vs, _ = topology.shortest_path(
            self.gtG,
            from_v,
            to_v,
            weights=self.gtG.ep["duration"],
            pred_map=pred_map,
        )

        if not path_vs:
            return []

        return [self.gtG.vp["stop_id"][v] for v in path_vs]

    def nodes(self):
        """
        Nodes in the graph
        """
        return list(self.sid2vertex.keys())
