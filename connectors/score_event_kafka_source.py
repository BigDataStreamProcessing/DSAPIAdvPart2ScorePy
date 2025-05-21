# score_event_kafka_source.py
from configparser import ConfigParser

from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from tools.score_event_schema import ScoreEventSchema


class ScoreEventKafkaSource:
    """
    Tworzy źródło Kafka dla obiektów ScoreEvent przy użyciu Flink 2.0.0 Source API.
    """

    @staticmethod
    def create(properties: ConfigParser) -> KafkaSource:
        """
        Zwraca skonfigurowany KafkaSource<ScoreEvent>.

        :param properties: ConfigParser z kluczami:
            - "kafka.bootstrap-servers"
            - "kafka.input-topic"
        :return: KafkaSource instancja
        """
        bootstrap_servers = properties["kafka"]["bootstrap.servers"]
        topic = properties["kafka"]["input-topic"]

        return (
            KafkaSource
            .builder()
            .set_bootstrap_servers(bootstrap_servers)
            .set_topics(topic)
            .set_group_id("house-stats-app")
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(ScoreEventSchema())
            .build()
        )
