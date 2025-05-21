from pyflink.common.time import Time
from pyflink.datastream.window import TumblingEventTimeWindows

from connectors.mysql_sink import MySQLSink
from testdata.inputs import Inputs
from tools.environment import get_environment
from tools.house_stats_aggregator import HouseStatsAggregator
from tools.house_stats_process_window import HouseStatsProcessWindowFunction
from tools.properties import load_properties
from tools.sinks_and_sources import get_source_data_stream


def main():
    # 1. Wczytanie konfiguracji
    cfg = load_properties()

    # 2. Środowisko Flink
    env = get_environment(cfg)

    # 3. Źródło danych
    score_event_ds = get_source_data_stream(env, cfg, Inputs.get_json_unordered_strings())

    # 4. Okno tumbling co 6h + agregacja
    result_stream = (
        score_event_ds
        .key_by(lambda e: e.house)
        .window(TumblingEventTimeWindows.of(Time.hours(6)))
        .aggregate(HouseStatsAggregator(),
                   HouseStatsProcessWindowFunction())
    )

    # 5. Sink: konsola lub MySQL
    data_output = cfg["data"]["output.type"]
    if data_output == 'console':
        result_stream.print()
    else:
        sink = MySQLSink.create(cfg, MySQLSink.INSERT_COMMAND)
        result_stream.add_sink(sink)

    # 6. Uruchomienie
    env.execute('House Stats Analysis')


if __name__ == '__main__':
    main()
