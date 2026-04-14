import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def test_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            port=os.getenv("DB_PORT")
        )
        
        with conn.cursor() as cursor:
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            cursor.execute(query)
            result = cursor.fetchall()
            print(f"Connection result: {result}")
                
    except Exception as error:
        print(f"Error with PostgreSQL: {error}")
    finally:
        if conn:
            conn.close()

test_connection()