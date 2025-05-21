import json

from pyflink.common import WatermarkStrategy, Types, Duration

from connectors.score_event_array_string_source import ScoreEventArrayStringSourceFunction
from connectors.score_event_kafka_source import ScoreEventKafkaSource
from models.score_event import ScoreEvent
from tools.my_timestamp_assigner import MyTimestampAssigner


def get_source_data_stream(env, cfg, data):
    data_input = cfg["data"]["input.type"]
    data_input_delay = int(cfg["data"]["input.data.delay"])
    data_ts_delay = int(cfg["data"]["input.ts.delay"])

    if data_input == 'array':
        score_event_ds = (
            env
            .add_source(
                ScoreEventArrayStringSourceFunction(
                    json_strings=data,
                    element_delay_millis=data_input_delay
                ),
                type_info=Types.STRING(),
                source_name="Array Source"
            )
            .map(json.loads, output_type=Types.MAP(Types.STRING(), Types.STRING()))
            .map(lambda d: ScoreEvent.from_dict(d))
            .assign_timestamps_and_watermarks(
                WatermarkStrategy
                .for_monotonous_timestamps()
                .with_timestamp_assigner(MyTimestampAssigner())
            )
        )
    else:
        # Źródło Kafka
        kafka_source = ScoreEventKafkaSource.create(cfg)
        score_event_ds = (
            env.from_source(
                source=kafka_source,
                watermark_strategy=WatermarkStrategy
                .for_monotonous_timestamps(),
                source_name='Kafka Source'
            )
            .map(json.loads, output_type=Types.MAP(Types.STRING(), Types.STRING()))
            .map(lambda d: ScoreEvent.from_dict(d))
        )
    return score_event_ds
