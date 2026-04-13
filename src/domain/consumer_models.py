from dataclasses import dataclass, asdict

@dataclass
class TweetSentiment:
    """Модель твіта ПІСЛЯ аналізу настрою для збереження в БД"""
    tweet_id: str
    author: str
    text: str
    sentiment: str
    polarity: float

    def to_tuple(self):
        """Для зручного вставляння в Postgres через %s"""
        return (self.tweet_id, self.author, self.text, self.sentiment, self.polarity)

    def to_dict(self):
        return asdict(self)