from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any

from tools.parse_ts import parse_ts_to_millis


@dataclass
class ScoreEvent:
    house: str = ""
    character: str = ""
    score: int = 0
    ts: int = 0  # znacznik czasu w milisekundach od epoch

    def __str__(self) -> str:
        return (
            f"ScoreEvent{{"
            f"house='{self.house}', "
            f"character='{self.character}', "
            f"score={self.score}, "
            f"ts={self.ts}"
            f"}}"
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ScoreEvent":
        # parsowanie timestampu na int millis
        raw_ts = data["ts"]  # zakładamy, że to int lub string
        ts = parse_ts_to_millis(raw_ts)
        return cls(
            house=data["house"],
            character=data["character"],
            score=int(data["score"]),
            ts=ts
        )
