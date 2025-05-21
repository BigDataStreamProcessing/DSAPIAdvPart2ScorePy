from configparser import ConfigParser

from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.jdbc import (
    JdbcSink,
    JdbcConnectionOptions,
    JdbcExecutionOptions
)

class MySQLSink:

    @staticmethod
    def create(properties: ConfigParser, sql: str):
        """
        Tworzy JdbcSink dla obiektów HouseStatsResult w PyFlink 2.0.0
        Sygnatura sink(sql, type_info, connection_options, execution_options)
        """

        # 1) RowTypeInfo przez Types.ROW_NAMED (lista nazw kolumn, lista TypeInformation)
        type_info = Types.ROW_NAMED(
            ["how_many", "sum_score", "no_characters",
             "house", "from_timestamp", "to_timestamp"],
            [Types.LONG(), Types.LONG(), Types.LONG(),
             Types.STRING(), Types.LONG(), Types.LONG()]
        )

        # 2) Opcje połączenia
        conn_opts = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
            .with_url(properties["mysql"]["url"]) \
            .with_driver_name("com.mysql.cj.jdbc.Driver") \
            .with_user_name(properties["mysql"]["username"]) \
            .with_password(properties["mysql"]["password"]) \
            .build()

        # 3) (Opcjonalnie) Opcje batchowania
        exec_opts = JdbcExecutionOptions.builder() \
            .with_batch_interval_ms(200) \
            .with_batch_size(100) \
            .with_max_retries(5) \
            .build()

        # 4) Tworzymy sink – dokładnie pod kolejność parametrów
        return JdbcSink.sink(
            sql,           # 1. Twój INSERT/UPSERT z '?' placeholderami
            type_info,     # 2. RowTypeInfo stworzony przez Types.ROW_NAMED
            conn_opts,     # 3. JdbcConnectionOptions
            exec_opts      # 4. JdbcExecutionOptions (opcjonalne)
        )

    INSERT_COMMAND = """insert into house_stats_sink 
                (how_many, sum_score, characters, 
                house, start_date, end_date) 
                values (?, ?, ?, ?, ?, ?)"""

    UPDATE_COMMAND = """update house_stats_sink 
                set how_many = ?, sum_score = ?, characters = ? 
                where house = ? and start_date = ? and end_date = ? """

    UPSERT_COMMAND = """insert into house_stats_sink 
                (how_many, sum_score, characters, 
                house, start_date, end_date) 
                values (?, ?, ?, ?, ?, ?) 
                ON DUPLICATE KEY UPDATE
                  how_many = ?, 
                  sum_score = ?, 
                  characters = ?;"""