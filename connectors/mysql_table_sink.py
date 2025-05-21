from pyflink.common import Row, Types
from pyflink.table import (
    TableDescriptor,
    Schema,
    DataTypes,
    StreamTableEnvironment,
    EnvironmentSettings
)
from pyflink.datastream import StreamExecutionEnvironment
from configparser import ConfigParser


class MySQLTableAPISink:

    @staticmethod
    def get_base_schema(include_pk: bool = False) -> Schema:
        """
        Zwraca wspólny schemat danych dla sinka.
        Jeśli include_pk=True, dodaje PRIMARY KEY(house, start_date, end_date).
        """
        b = Schema.new_builder() \
            .column("how_many", DataTypes.BIGINT()) \
            .column("sum_score", DataTypes.BIGINT()) \
            .column("characters", DataTypes.BIGINT()) \
            .column("house", DataTypes.STRING().not_null()) \
            .column("start_date", DataTypes.BIGINT().not_null()) \
            .column("end_date", DataTypes.BIGINT().not_null())

        if include_pk:
            # włączamy tryb upsert w Table API / JDBC Connector
            b = b.primary_key("house", "start_date", "end_date")
        return b.build()

    @staticmethod
    def create_table_env(env: StreamExecutionEnvironment):
        settings = EnvironmentSettings.in_streaming_mode()
        t_env = StreamTableEnvironment.create(env, environment_settings=settings)
        t_env.get_config().get_configuration().set_boolean("table.dml-sync", True)
        return t_env

    @staticmethod
    def register_sink_table(t_env: StreamTableEnvironment,
                            properties: ConfigParser,
                            table_name: str,
                            is_upsert: bool):
        """
        Rejestruje tymczasową tabelę do MySQL.
        Jeśli is_upsert=True, to definicja schematu zawiera PRIMARY KEY.
        """
        jdbc_url = properties["mysql"]["url"]

        descriptor = (
            TableDescriptor
            .for_connector("jdbc")
            .option("url", jdbc_url)
            .option("table-name", "house_stats_sink")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("username", properties["mysql"]["username"])
            .option("password", properties["mysql"]["password"])
            .schema(MySQLTableAPISink.get_base_schema(include_pk=is_upsert))
            .build()
        )

        t_env.create_temporary_table(table_name, descriptor)
        return table_name

    @staticmethod
    def add_sink(env: StreamExecutionEnvironment,
                 result_stream,
                 properties: ConfigParser,
                 sql_command: str,
                 table_name: str = "HouseStatsSink"):
        """
        Dodaje sinka do MySQL przez JDBC Table API.

        :param env:             StreamExecutionEnvironment, na którym działa pipeline
        :param result_stream:   DataStream[HouseStatsResult] — strumień wyników agregacji
        :param properties:      ConfigParser z sekcją „mysql” (url, username, password)
        :param sql_command:     INSERT lub UPSERT command (MySQLSink.INSERT_COMMAND lub .UPSERT_COMMAND)
        :param table_name:      (opcjonalnie) nazwa tymczasowej tabeli w TableEnvironment
        """
        # 1. TableEnv
        t_env = MySQLTableAPISink.create_table_env(env)

        # 2. Czy mamy wykonywać upsert?
        is_upsert = "ON DUPLICATE" in sql_command.upper() or sql_command.strip().upper().startswith("REPLACE")

        # 3. Zarejestruj tabela‑sink
        MySQLTableAPISink.register_sink_table(t_env, properties, table_name, is_upsert)

        # 4. Mapowanie HouseStatsResult → Row z nazwami
        mapped = result_stream.map(
            lambda h: Row(
                how_many=h.how_many,
                sum_score=h.sum_score,
                characters=h.no_characters,
                house=h.house,
                start_date=h.from_timestamp,
                end_date=h.to_timestamp
            ),
            output_type=Types.ROW_NAMED(
                ["how_many", "sum_score", "characters", "house", "start_date", "end_date"],
                [Types.LONG(), Types.LONG(), Types.LONG(),
                 Types.STRING(), Types.LONG(), Types.LONG()]
            )
        )

        # 5. Konwersja do Table z wykorzystaniem tego samego schematu
        table = t_env.from_data_stream(mapped, MySQLTableAPISink.get_base_schema(include_pk=is_upsert))

        # 6. Wykonaj insert (Table API z automatycznym trybem append vs upsert)
        table.execute_insert(table_name)
