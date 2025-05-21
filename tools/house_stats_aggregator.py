# house_stats_aggregator.py

from pyflink.datastream.functions import AggregateFunction

from models.score_event import ScoreEvent
from models.house_stats import HouseStats
from models.house_stats_accumulator import HouseStatsAccumulator


class HouseStatsAggregator(AggregateFunction):
    def create_accumulator(self) -> HouseStatsAccumulator:
        return HouseStatsAccumulator()

    def add(self, value: ScoreEvent, accumulator: HouseStatsAccumulator) -> HouseStatsAccumulator:
        accumulator.add(value)
        return accumulator

    def get_result(self, accumulator: HouseStatsAccumulator) -> HouseStats:
        return accumulator.get_result()

    def merge(self, a: HouseStatsAccumulator, b: HouseStatsAccumulator) -> HouseStatsAccumulator:
        return a.merge(b)
