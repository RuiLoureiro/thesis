from collections import defaultdict
from tqdm.auto import tqdm

from ..base.geo import StopsDistance
from ..base.bus_schedule import BusSchedule
from ..base.metro_schedule import MetroSchedule
from ..base.utils import ddict2dict

from . import config
from .odx import ODX_ENUMS

ODX_PARAMETERS = config.ODXConfig

# INTERCHANGE MODE CHECKING
bus_bus = lambda s1, s2: s1.mode == "bus" and s2.mode == "bus"
metro_metro = lambda s1, s2: s1.mode == "metro" and s2.mode == "metro"
any_any = lambda s1, s2: True
any_bus = lambda s1, s2: s2.mode == "bus"


bus_sched = BusSchedule()
metro_sched = MetroSchedule()
stops_distance = StopsDistance(bus_sched.stops + metro_sched.stops)


def get_distance(sid1, sid2):
    return stops_distance.get_distance(sid1, sid2)


# INTERCHANGE INFERENCE METHODS
def repeated_service(s1, s2):
    return (s1.mode == s2.mode == "bus") and (
        s1.route.route_id == s2.route.route_id
    )


def repeated_metro(s1, s2):
    if s1.mode == s2.mode == "metro":
        s1.unlinked_reason = "Repeated Metro"
        return True
    return False


def max_interchange_time(s1, s2):
    distance = get_distance(s1.exit_stop.stop_id, s2.entry_stop.stop_id)
    interchange_time = (s2.entry_ts - s1.exit_ts).seconds

    _max_interchange_time = max(
        ODX_PARAMETERS.MAX_INTERCHANGE_TIME_PER_KM * distance,
        ODX_PARAMETERS.MIN_INTERCHANGE_TIME,
    )

    if interchange_time > _max_interchange_time:
        if s2.mode == ODX_ENUMS.BUS:
            # max wait time
            s1.ulinked_reason = "Max bus wait time exceeded"
            return interchange_time > (
                _max_interchange_time + ODX_PARAMETERS.MAX_BUS_WAIT_TIME
            )
        else:
            return True
    else:
        return False


def max_interchange_distance(s1, s2):
    return (
        get_distance(s1.exit_stop.stop_id, s2.entry_stop.stop_id)
        > ODX_PARAMETERS.MAX_INTERCHANGE_DISTANCE
    )


def get_circuity(s1, s2):
    direct_distance = get_distance(
        s1.entry_stop.stop_id, s2.exit_stop.stop_id
    )

    combined_distance = 0

    # stage distances
    for s in [s1, s2]:
        if s.mode == "bus":
            s_dist = s.route.get_stage_dist(
                s.entry_stop.stop_id, s.exit_stop.stop_id
            )
        elif s.mode == "metro":
            # assume direct distance in metro
            s_dist = get_distance(
                s.entry_stop.stop_id, s.exit_stop.stop_id
            )

        combined_distance += s_dist
    # transfer distance
    combined_distance += get_distance(
        s1.exit_stop.stop_id, s2.entry_stop.stop_id
    )

    return combined_distance / direct_distance


def circuity(s1, s2):
    return get_circuity(s1, s2) > ODX_PARAMETERS.CIRCUITY_RATIO


class InterchangeChecker:

    # caveat: this assumes the dict is ordered
    # only true for python 3.6+
    _checks_order = {
        repeated_service: bus_bus,
        repeated_metro: metro_metro,
        max_interchange_time: any_any,
        max_interchange_distance: any_any,
        circuity: any_any,
    }

    def __init__(self, checks=None):
        self.report = defaultdict(list)

        if checks is not None:
            self._checks_order = {
                k: v
                for k, v in InterchangeChecker._checks_order.items()
                if k.__name__ in checks
            }

    def are_linked(self, stage, next_stage):
        for check_fn, mode_fn in self._checks_order.items():
            if not mode_fn(stage, next_stage):
                continue

            if check_fn(stage, next_stage):
                stage.unlinked_reason = check_fn.__name__
                self.report[check_fn.__name__].append([stage, next_stage])
                return False

        self.report[f"linked_{stage.mode}_{next_stage.mode}"].append(
            (stage, next_stage)
        )
        return True


class JourneyBuilder:
    def __init__(self):
        self.journeys = defaultdict(lambda: defaultdict(list))

    def new_journey(self, cid, date):
        self.journeys[cid][date].append([])

    def add_stage_to_current_journey(self, cid, date, stage):
        try:
            current_journey = self.journeys[cid][date][-1]
        except IndexError:
            self.journeys[cid][date].append([])
            current_journey = self.journeys[cid][date][-1]

        current_journey.append(stage)

    @staticmethod
    def is_valid_stage(stage):
        return all(
            getattr(stage, attr) is not None
            for attr in ["entry_ts", "entry_stop", "exit_ts", "exit_stop"]
        )

    def delete_day_journey(self, cid, date):
        try:
            del self.journeys[cid][date]
        except KeyError:
            pass

    def get_journeys(self, stages, checks=None, minimum_journey_length=True):
        ic = InterchangeChecker(checks)

        for cid in tqdm(stages):
            for date in stages[cid]:
                if len(stages[cid][date]) < 2:
                    continue

                for idx, stage in enumerate(stages[cid][date]):

                    self.add_stage_to_current_journey(cid, date, stage)
                    if not self.is_valid_stage(stage):
                        if not stage.entry_stop:
                            ic.report[
                                f"{stage.mode}_current_no_boarding"
                            ].append(stage)
                        elif not stage.exit_stop:
                            ic.report[
                                f"{stage.mode}_current_no_destination"
                            ].append(stage)
                        else:
                            raise RuntimeError(f"{stage}")

                        self.new_journey(cid, date)
                        break

                    try:
                        next_stage = stages[cid][date][idx + 1]
                    except IndexError:
                        ic.report["final_stage"].append(stage)
                        break

                    if not self.is_valid_stage(next_stage):
                        if not next_stage.entry_stop:
                            ic.report[
                                f"{next_stage.mode}_next_no_boarding"
                            ].append(next_stage)
                        elif not next_stage.exit_stop:
                            ic.report[
                                f"{next_stage.mode}_next_no_destination"
                            ].append(next_stage)
                        else:
                            raise RuntimeError(f"{stage}")
                        self.new_journey(cid, date)
                        break

                    if not ic.are_linked(stage, next_stage):
                        self.new_journey(cid, date)

        if minimum_journey_length:
            for cid in self.journeys:
                for date in self.journeys[cid]:
                    invalid_idxs = []
                    for idx, journey in enumerate(self.journeys[cid][date]):
                        if min_journey_length(journey):
                            invalid_idxs.append(idx)

                    for idx in invalid_idxs:
                        old_journey = self.journeys[cid][date][idx]
                        self.journeys[cid][date][idx: idx + 1] = [
                            [s] for s in old_journey
                        ]

                        for stage in old_journey[:-1]:
                            ic.report["minimum_journey_length"].append(stage)

        return ddict2dict(self.journeys), ddict2dict(ic.report)


def min_journey_length(journey):
    if len(journey) <= 1:
        return False

    return (
        get_distance(
            journey[0].entry_stop.stop_id, journey[-1].exit_stop.stop_id
        )
        < ODX_PARAMETERS.MIN_JOURNEY_LENGTH
    )


def journeys_to_bus_odx(journeys):
    odx_matrix = defaultdict(lambda: defaultdict(int))

    for cid in tqdm(journeys):
        for date in journeys[cid]:
            for journey in journeys[cid][date]:
                bus_origin = None
                bus_destination = None
                for stage in journey:
                    if not JourneyBuilder.is_valid_stage(stage):
                        continue
                    if stage.mode == "bus":
                        if bus_origin is None:
                            bus_origin = stage.entry_stop.stop_id
                            bus_destination = stage.exit_stop.stop_id
                        else:
                            bus_destination = stage.exit_stop.stop_id
                    if (
                        (stage.mode != "bus")
                        and bus_origin
                        and bus_destination
                    ):
                        odx_matrix[bus_origin][bus_destination] += 1
                        bus_origin = None
                        bus_destination = None

                if bus_origin:
                    odx_matrix[bus_origin][bus_destination] += 1

    return ddict2dict(odx_matrix)
