from rich import print
from collections import defaultdict


def validate_stops(stops):
    locations = defaultdict(set)

    for stop in stops:
        locations[(stop.stop_lat, stop.stop_lon)].add(stop.stop_id)

    for location, sids in locations.items():
        if len(sids) > 1:
            print(f"[red]Stops {sids} have the same location")
