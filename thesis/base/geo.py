import numpy as np


EARTH_RADIUS_M = 6371000


def _broadcasting_based_haversine(data1, data2):
    """Computes the haversine distance between every point in `data1`
    and every point in `data2`.
    Uses numpy broadcasting for very speed

    `data` and `data2` must have the same shape.

    Parameters
    ----------
    data1: np.array
        Array with 2 columns (holding lat,lon values) and
        arbitrary number of rows
    data2: np.array
        Array with 2 columns (holding lat,lon values) and
        arbitrary number of rows (equal to data1)
    """
    assert data1.shape[1] == 2
    assert data1.shape == data2.shape

    data1 = np.deg2rad(data1)
    data2 = np.deg2rad(data2)

    lat1 = data1[:, 0]
    lng1 = data1[:, 1]

    lat2 = data2[:, 0]
    lng2 = data2[:, 1]

    diff_lat = lat1[:, None] - lat2
    diff_lng = lng1[:, None] - lng2

    # haversine formula
    d = (
        np.sin(diff_lat / 2) ** 2
        + np.cos(lat1[:, None]) * np.cos(lat2) * np.sin(diff_lng / 2) ** 2
    )

    return 2 * EARTH_RADIUS_M * np.arcsin(np.sqrt(d))


class StopsDistance:
    def __init__(self, stops: list):
        _stops_distance = {}

        points_array = np.array([[s.stop_lat, s.stop_lon] for s in stops])
        dists = _broadcasting_based_haversine(points_array, points_array)

        for from_idx, from_stop in enumerate(stops):
            _stops_distance[from_stop.stop_id] = {}

            for to_idx, to_stop in enumerate(stops):
                _stops_distance[from_stop.stop_id][to_stop.stop_id] = dists[
                    from_idx
                ][to_idx]

        self._stops_distance = _stops_distance

    def get_distance(self, sid1, sid2):
        return self._stops_distance[sid1][sid2]
