import datetime
from tqdm.auto import tqdm
from collections import defaultdict
from rich import print
from ..base.bus_schedule import BusSchedule, BusRouteTuple
from ..base.metro_schedule import MetroSchedule
from .config import ODXConfig
from ..base.geo import StopsDistance
from ..base.utils import ddict2dict


class ODX_ENUMS:
    METRO = "metro"
    BUS = "bus"
    METRO_OUT = "OUT"
    METRO_IN = "IN"


class BusStage:
    """
    Represents a bus stage
    """

    mode = "bus"

    def __init__(
        self,
        boarding,
    ):

        self.boarding = boarding
        self.entry_ts = boarding.timestamp
        self.entry_stop = BusSchedule().get_stop(boarding.stop_id)
        self.route = self.route_from_transaction(boarding)
        self.exit_ts = None
        self.exit_stop = None

    @staticmethod
    def route_from_transaction(transaction):
        """
        Extract bus route from an AFC transaction
        """

        route_tup = BusRouteTuple(
            transaction.route_id,
            transaction.route_direction,
            transaction.route_variant,
        )
        return BusSchedule().get_route(route_tup)

    def __repr__(self):
        return f"[BUS] [{self.entry_ts}] ({self.entry_stop}) -> [{self.exit_ts }] ({self.exit_stop}) [{self.route}]"


class MetroStage:
    """
    Represents a metro stage
    """

    mode = "metro"

    def __init__(
        self,
        boarding,
        alighting,
    ):
        self.boarding = boarding
        self.alighting = alighting
        if boarding:
            self.entry_ts = boarding.timestamp
            self.entry_stop = MetroSchedule().get_stop(boarding.stop_id)
        else:
            self.entry_ts = None
            self.entry_stop = None

        if alighting:
            self.exit_ts = alighting.timestamp
            self.exit_stop = MetroSchedule().get_stop(alighting.stop_id)
        else:
            self.exit_ts = None
            self.exit_stop = None

    def __repr__(self):
        return f"[METRO] [{self.entry_ts}] ({self.entry_stop}) -> [{self.exit_ts }] ({self.exit_stop})"


class ODX:
    """
    Computes odx using bus and metro afc data.
    """

    def __init__(self):
        self.bus_schedule = BusSchedule()
        self.metro_schedule = MetroSchedule()
        self.stops_distance = StopsDistance(
            self.bus_schedule.stops + self.metro_schedule.stops
        )

    @staticmethod
    def get_record_day(row):
        time = row.timestamp.time()
        date = row.timestamp.date()
        time_limit = ODXConfig.NEW_DAY_TIME

        if time < time_limit:
            return date - datetime.timedelta(days=1)
        else:
            return date

    def get_stages(self, afc):
        """
        Builds stages
        Metro stages are built from 2 afc records (entry and exit)
        Bus stages are built from a single afc record (boarding).
        Divides stages by date and by card_id
        """
        stages = defaultdict(lambda: defaultdict(list))
        print("Splitting dataframe by card id and date..")
        transactions = defaultdict(lambda: defaultdict(list))
        for row in tqdm(afc.itertuples(), total=len(afc)):
            transactions[row.card_id][self.get_record_day(row)].append(row)
        print(
            f"Processing {len(transactions)} transactions.., between {afc.timestamp.iloc[0]} and {afc.timestamp.iloc[-1]}.."
        )
        for cid in tqdm(transactions):
            for date in transactions[cid]:
                iter_ = enumerate(transactions[cid][date])
                for idx, transaction in iter_:
                    if transaction.mode == ODX_ENUMS.METRO:
                        try:
                            next_transaction = transactions[cid][date][idx + 1]
                        except IndexError:
                            next_transaction = None
                        if transaction.way == ODX_ENUMS.METRO_IN:
                            if (
                                next_transaction
                                and next_transaction.mode == ODX_ENUMS.METRO
                                and next_transaction.way == ODX_ENUMS.METRO_OUT
                            ):
                                stage = MetroStage(
                                    transaction, next_transaction
                                )
                                # since we already processed
                                # the next transaction, skip it
                                next(iter_)
                            else:
                                stage = MetroStage(transaction, None)

                        if transaction.way == ODX_ENUMS.METRO_OUT:
                            stage = MetroStage(None, transaction)
                    elif transaction.mode == ODX_ENUMS.BUS:
                        stage = BusStage(transaction)

                    if (stage.mode == "metro") and (
                        stage.entry_stop == stage.exit_stop
                    ):
                        continue

                    stages[cid][date].append(stage)

        print("Converting to simple dict from defaultdict..")
        return ddict2dict(stages)

    def add_report(self, message, stage):
        pass

    def get_closest_stop(self, stage, next_stage):
        route_stops = self.bus_schedule.get_route_stops(stage.route)

        try:
            stop_number = route_stops.index(stage.stop.stop_id)

        except ValueError:
            raise RuntimeError(
                f"Stop {stage.stop.stop_id} not in route {stage.route}"
            )

        # get stop_ids in the trip, after previous transaction's stop.
        # if the route is circular, every stop is subsequent to the current one
        if stage.route.route_direction == "CIRC":
            subsequent_stop_ids = (
                route_stops[stop_number + 1:] + route_stops[:stop_number]
            )

        else:
            subsequent_stop_ids = route_stops[stop_number + 1:]

        if not subsequent_stop_ids:
            raise RuntimeError(
                f"Boarding stop ({stage.stop.stop_id}) is route's last stop"
            )

        # direct (same stop) transfer
        if next_stage.entry_stop.stop_id in subsequent_stop_ids:
            closest_sid = next_stage.entry_stop.stop_id

        else:
            distances = {}
            for sid in subsequent_stop_ids:
                distances[sid] = self.bus_schedule.get_stops_distance(
                    sid, next_stage.entry_stop.stop_id
                )

            closest_sid = min(distances, key=distances.get)

        return self.bus_schedule.get_stop(closest_sid)

    def get_stops_distance(self, sid1, sid2):
        return self.stop_distances.get_distance(sid1, sid2)

    @staticmethod
    def is_boarding_last_stop(stage):
        return (
            stage.route.route_direction != "CIRC"
            and stage.entry_stop.stop_id == stage.route.route_stop_ids[-1]
        )

    @staticmethod
    def get_stage_time(stage):
        stage_time_sec = stage.route.get_stage_time(
            stage.entry_stop.stop_id,
            stage.exit_stop.stop_id,
        )

        return datetime.timedelta(seconds=stage_time_sec)

    def infer_destinations(self, stages):
        for cid in tqdm(stages):
            for date in stages[cid]:
                day_stages = stages[cid][date]

                # check if only one stage in day
                if len(day_stages) == 1:
                    continue

                for idx, stage in enumerate(day_stages):
                    if stage.mode != ODX_ENUMS.BUS:
                        continue

                    if not (stage.entry_stop and stage.route):
                        continue
                    # check if boarding is on route's last stop
                    if self.is_boarding_last_stop(stage):
                        continue

                    try:
                        next_stage = day_stages[idx + 1]
                    except IndexError:
                        next_stage = day_stages[0]

                    if not next_stage.entry_stop:
                        continue

                    closest_stop = self.get_closest_stop(stage, next_stage)

                    if (
                        self.get_stops_distance(
                            closest_stop.stop_id,
                            next_stage.entry_stop.stop_id,
                        )
                        > ODXConfig.MAX_BUS_ALIGTHING_BOARDING_DISTANCE
                    ):
                        continue

                    stage.exit_stop = closest_stop
                    stage.exit_ts = stage.entry_ts + self.get_stage_time(stage)

        return stages
