from pyflink.datastream.functions import ProcessWindowFunction
from models.house_stats import HouseStats
from models.house_stats_result import HouseStatsResult


class HouseStatsProcessWindowFunction(ProcessWindowFunction):
    """
    Python ProcessWindowFunction for DataStream API.
    The `process` method should return an iterable of results rather than use a Collector.
    """

    def process(self, key, context, elements):
        """
        :param key: key value (house)
        :param context: ProcessWindowFunction.Context, provides window metadata
        :param elements: iterable of HouseStats (from AggregateFunction)
        :return: iterable of HouseStatsResult
        """
        # Zakładamy, że elements zawiera dokładnie jeden element (z agregacji)
        stats: HouseStats = next(iter(elements))

        result = HouseStatsResult(
            house=key,
            how_many=stats.how_many,
            sum_score=stats.sum_score,
            no_characters=stats.no_characters,
            from_timestamp=context.window().start,
            to_timestamp=context.window().end
        )

        # Zwracamy listę/w generator wyników
        return [result]
