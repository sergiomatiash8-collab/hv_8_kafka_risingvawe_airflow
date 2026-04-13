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
                # Запит на статистику
                query = """
                SELECT sentiment, COUNT(*) 
                FROM tweet_sentiments 
                GROUP BY sentiment;
                """
                cursor.execute(query)
                results = cursor.fetchall()

                print("\n📊 СТАТИСТИКА АНАЛІЗУ НАСТРОЮ:")
                print("-" * 30)
                if not results:
                    print("Таблиця порожня. Спробуй запустити продюсера!")
                for sentiment, count in results:
                    print(f"{sentiment.capitalize():<10} | {count} твітів")
                print("-" * 30)

    except Exception as e:
        print(f"❌ Не вдалося підключитися до бази: {e}")

if __name__ == "__main__":
    check_results()