from dataclasses import dataclass, asdict
from typing import Optional

@dataclass
class Tweet:
    tweet_id: str
    author: str
    text: str
    inbound: bool
    created_at: str  # Ми будемо зберігати в ISO форматі

    def to_dict(self):
        """Метод для легкого перетворення об'єкта в словник для JSON"""
        return asdict(self)