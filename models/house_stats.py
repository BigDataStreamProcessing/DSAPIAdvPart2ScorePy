from dataclasses import dataclass

@dataclass
class HouseStats:
    how_many: int = 0
    sum_score: int = 0
    no_characters: int = 0

    def __str__(self):
        return f"HouseStats(how_many={self.how_many}, sum_score={self.sum_score}, no_characters={self.no_characters})"
