import json
import re
from datetime import datetime, timezone
from pyflink.common.serialization import DeserializationSchema, SerializationSchema
from pyflink.common.typeinfo import Types
from models.score_event import ScoreEvent

class ScoreEventSchema(DeserializationSchema, SerializationSchema):
    def __init__(self, charset='utf-8'):
        super().__init__()
        self.charset = charset

    def deserialize(self, message: bytes):
        json_string = message.decode(self.charset)
        json_obj = json.loads(json_string)

        timestamp_in_millis = self._parse_str_time_to_millis(json_obj.get("ts", ""))

        return ScoreEvent(
            house=json_obj.get("house", ""),
            character=json_obj.get("character", ""),
            score=int(json_obj.get("score", 0)),
            ts=timestamp_in_millis
        )

    @staticmethod
    def is_end_of_stream():
        return False

    def serialize(self, element: ScoreEvent) -> bytes:
        # Formatowanie czasu z timestamp (ms) na string
        dt = datetime.fromtimestamp(element.ts / 1000, tz=timezone.utc)
        formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # trim do ms

        json_obj = {
            "house": element.house,
            "character": element.character,
            "score": element.score,
            "ts": formatted_date
        }

        json_string = json.dumps(json_obj)
        return json_string.encode(self.charset)

    @staticmethod
    def get_produced_type():
        return Types.PICKLED_BYTE_ARRAY()

    @staticmethod
    def _parse_str_time_to_millis(str_time: str) -> int:
        pattern = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(\.\d+)?")
        matcher = pattern.match(str_time)
        if matcher:
            date_time_part = matcher.group(1)
            milliseconds_part = matcher.group(2) or ".000"

            dt = datetime.strptime(date_time_part, "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=timezone.utc)
            timestamp_in_millis = int(dt.timestamp() * 1000)

            ms = int(float(milliseconds_part) * 1000)
            timestamp_in_millis += ms
            return timestamp_in_millis
        return 0
