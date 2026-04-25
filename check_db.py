import psycopg2
from config import settings

def check_results():
    conn_params = {
        "user": settings.DB_USER,
        "password": settings.DB_PASSWORD,
        "host": settings.DB_HOST,
        "port": settings.DB_PORT,
        "database": settings.DB_NAME
    }

    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cursor:
                
                query = """
                SELECT sentiment, COUNT(*) 
                FROM tweet_sentiments 
                GROUP BY sentiment;
                """
                cursor.execute(query)
                results = cursor.fetchall()

                print("\nSENTIMENT ANALYSIS STATISTICS:")
                print("-" * 30)

                if not results:
                    print("Table is empty. Try running the producer!")

                for sentiment, count in results:
                    print(f"{sentiment.capitalize():<10} | {count} tweets")

                print("-" * 30)

    except Exception as e:
        print(f"Failed to connect to database: {e}")

if __name__ == "__main__":
    check_results()