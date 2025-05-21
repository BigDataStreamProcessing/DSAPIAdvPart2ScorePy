from typing import Set
from models.house_stats import HouseStats
from models.score_event import ScoreEvent


class HouseStatsAccumulator:
    def __init__(self):
        self.how_many: int = 0
        self.sum_score: int = 0
        self.characters: Set[str] = set()

    def add(self, event: ScoreEvent):
        self.how_many += 1
        self.sum_score += event.score
        self.characters.add(event.character)

    def get_result(self) -> HouseStats:
        return HouseStats(
            how_many=self.how_many,
            sum_score=self.sum_score,
            no_characters=len(self.characters)
        )

    def merge(self, other: 'HouseStatsAccumulator') -> 'HouseStatsAccumulator':
        self.how_many += other.how_many
        self.sum_score += other.sum_score
        self.characters.update(other.characters)
        return self
