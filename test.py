from pyflink.common import Time
from pyflink.datastream.window import TumblingEventTimeWindows

from connectors.mysql_sink import MySQLSink
from connectors.mysql_table_sink import MySQLTableAPISink
from testdata.inputs import Inputs
from tools.environment import get_environment
from tools.house_stats_aggregator import HouseStatsAggregator
from tools.house_stats_process_window import HouseStatsProcessWindowFunction
from tools.properties import load_properties
from tools.sinks_and_sources import get_source_data_stream


def main():

    cfg = load_properties()

    env = get_environment(cfg)

    score_event_ds = get_source_data_stream(env, cfg, Inputs.get_json_unordered_strings())

    # 4. Okno tumbling co 6h + agregacja
    result_stream = (
        score_event_ds
        .key_by(lambda e: e.house)
        .window(TumblingEventTimeWindows.of(Time.hours(6)))
        .aggregate(HouseStatsAggregator(),
                   HouseStatsProcessWindowFunction())
    )
    # result_stream.print()
    MySQLTableAPISink.add_sink(env, result_stream, cfg, MySQLSink.UPSERT_COMMAND)

    env.execute('House Stats Analysis')

if __name__ == '__main__':
    main()
