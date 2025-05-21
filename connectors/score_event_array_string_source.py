from pyflink.datastream.functions import SourceFunction
from pyflink.java_gateway import get_gateway

class ScoreEventArrayStringSourceFunction(SourceFunction):
    """
    Python wrapper dla Java SourceFunction<Row>:
    com.example.bigdata.sources.ScoreEventArrayRowSource(String[] jsonStrings, int intervalMillis)
    """

    def __init__(self,
                 json_strings: list,
                 element_delay_millis: int = 1000):
        # 1. Gateway do JVM
        gateway = get_gateway()
        # 2. Pełna nazwa klasy Java (dostosuj do swojego pakietu)
        java_source_class = gateway.jvm.ScoreEventArrayStringSource
        # 3. Konwersja listy JSON-ów do tablicy String w JVM
        j_string_array = gateway.new_array(gateway.jvm.java.lang.String, len(json_strings))
        for i, s in enumerate(json_strings):
            j_string_array[i] = s
        # 4. Utworzenie instancji Java źródła
        j_source = java_source_class(j_string_array, element_delay_millis)
        # 5. Init rodzica, ustawia self._j_function
        super().__init__(j_source)
