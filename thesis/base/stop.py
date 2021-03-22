class Stop(object):
    def __init__(self, stop_id, stop_name, stop_lat, stop_lon):
        self.stop_id = stop_id
        self.stop_name = stop_name
        self.stop_lat = stop_lat
        self.stop_lon = stop_lon

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.stop_id == other.stop_id
        return False

    def __repr__(self):
        return f"<{self.stop_id}> [{self.stop_name}] ({self.stop_lat}, {self.stop_lon})"

    def __hash__(self):
        return hash(self.stop_id)
