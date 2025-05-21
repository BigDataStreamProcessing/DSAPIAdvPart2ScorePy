from pyflink.datastream.functions import ProcessFunction

class MySQLFakeSink(ProcessFunction):
    def __init__(self, command: str):
        self.command = command

    def process_element(self, value, ctx):
        result_string = self.replace_query_parameters(
            self.command,
            value.how_many,
            value.sum_score,
            value.no_characters,
            value.house,
            value.get_from_as_string(),
            value.get_to_as_string()
        )
        yield result_string  # <-- zamiast out.collect()

    @staticmethod
    def replace_query_parameters(command: str, *parameters) -> str:
        for param in parameters:
            command = command.replace("?", str(param), 1)
        return command
