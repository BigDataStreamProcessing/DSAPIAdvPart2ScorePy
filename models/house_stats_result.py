from dataclasses import dataclass
from datetime import datetime
from models.house_stats import HouseStats


@dataclass
class HouseStatsResult(HouseStats):
    house: str = ""
    from_timestamp: int = 0  # epoch millis
    to_timestamp: int = 0

    def get_from_as_string(self) -> str:
        return datetime.fromtimestamp(self.from_timestamp / 1000).strftime("%H:%M")

    def get_to_as_string(self) -> str:
        return datetime.fromtimestamp(self.to_timestamp / 1000).strftime("%H:%M")

    def __str__(self) -> str:
        return (
            f"HouseStatsResult{{"
            f"house='{self.house}', "
            f"from={self.get_from_as_string()}, "
            f"to={self.get_to_as_string()}, "
            f"how_many={self.how_many}, "
            f"sum_score={self.sum_score}, "
            f"no_characters={self.no_characters}"
            f"}}"
        )
