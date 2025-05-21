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
        builder = Schema.new_builder() \
            .column("how_many",  DataTypes.BIGINT()) \
            .column("sum_score",  DataTypes.BIGINT()) \
            .column("characters", DataTypes.BIGINT()) \
            .column("house",      DataTypes.STRING().not_null()) \
            .column("start_date", DataTypes.BIGINT().not_null()) \
            .column("end_date",   DataTypes.BIGINT().not_null())
        if include_pk:
            builder = builder.primary_key("house", "start_date", "end_date")
        return builder.build()

    @staticmethod
    def create_table_env(env: StreamExecutionEnvironment):
        settings = EnvironmentSettings.in_streaming_mode()
        return StreamTableEnvironment.create(env, environment_settings=settings)

    @staticmethod
    def register_sink_table(t_env: StreamTableEnvironment,
                            properties: ConfigParser,
                            table_name: str,
                            is_upsert: bool):
        jdbc_url = properties["mysql"]["url"]
        descriptor = (
            TableDescriptor.for_connector("jdbc")
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
                 temp_table_name: str = "HouseStatsTemp",
                 sink_table_name: str = "HouseStatsSink"):
        # 1. Create unified TableEnvironment
        t_env = MySQLTableAPISink.create_table_env(env)

        # 2. Determine upsert or insert mode
        is_upsert = "ON DUPLICATE" in sql_command.upper() or sql_command.strip().upper().startswith("REPLACE")

        # 3. Register sink table with PK if needed
        MySQLTableAPISink.register_sink_table(t_env, properties, sink_table_name, is_upsert)

        # 4. Map DataStream to named Row and register as temporary view
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
                [Types.LONG(), Types.LONG(), Types.LONG(), Types.STRING(), Types.LONG(), Types.LONG()]
            )
        )
        t_env.create_temporary_view(temp_table_name, mapped)

        # 5. Execute SQL insert from temp view into sink table synchronously
        insert_sql = f"INSERT INTO {sink_table_name} SELECT how_many, sum_score, characters, house, start_date, end_date FROM {temp_table_name}"
        table_result = t_env.execute_sql(insert_sql)
        # wait for completion (blocks until job finishes)
        table_result.wait()
        return table_result
