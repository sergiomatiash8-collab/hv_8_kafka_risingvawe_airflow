from dataclasses import dataclass, asdict

@dataclass
class TweetSentiment:
    """Tweet model AFTER sentiment analysis for storage in the database"""
    tweet_id: str
    author: str
    text: str
    sentiment: str
    polarity: float

    def to_tuple(self):
        """For easy insertion into Postgres via %s"""
        return (self.tweet_id, self.author, self.text, self.sentiment, self.polarity)

    def to_dict(self):
        return asdict(self)