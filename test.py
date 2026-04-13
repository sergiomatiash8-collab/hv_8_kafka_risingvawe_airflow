import psycopg2
print("START")

conn = psycopg2.connect(
    dbname="twitter_sentiment",
    user="admin",
    password="admin123",
    host="localhost"
)

print("CONNECTED")