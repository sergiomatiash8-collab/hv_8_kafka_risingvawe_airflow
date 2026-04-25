from dataclasses import dataclass, asdict
from typing import Optional

@dataclass
class Tweet:
    tweet_id: str
    author: str
    text: str
    inbound: bool
    created_at: str  

    def to_dict(self):
        """Method for easily converting an object into a dictionary for JSON"""
        return asdict(self)