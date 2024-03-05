import os
import psycopg2 as pg
from dotenv import load_dotenv

load_dotenv()



def connect_database():
    try:
        conn = pg.connect(
            database = os.getenv("DB_NAME"),
            host = os.getenv("DB_HOST"),
            port = int(os.getenv("DB_PORT")),
            user= os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        return conn
    
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def create_table_if_not_exists(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,


                )
        """)
        connection.commit()
        cursor.close()
    except Exception as e:
        print("Error creating table:", e)

def insert_data(connection, data, unique_key):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO therapist_data 
            VALUES (%s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s, %s)
                       
            ON CONFLICT(%s) DO NOTHING
                       
            RETURNING id
        """, (
                data.values(),
                unique_key

              ))
        inserted_id = cursor.fetchone()[0]
        connection.commit()
        cursor.close()
        return inserted_id
    except Exception as e:
        print("Error inserting data:", e)
        return None