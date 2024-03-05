import os
import psycopg2 as pg
from dotenv import load_dotenv

load_dotenv()

def select_all_links(conn, table_name):
    try:
        cursor = conn.cursor()

        cursor.execute(f"SELECT profile_url FROM {table_name}")
        links = cursor.fetchall()

        cursor.close()

        return links
    except pg.Error as e:
        print("postgres error selecting links")
        return []


def bulk_insert_links(conn, links):
    try:
        cursor = conn.cursor()
        # Use executemany to insert multiple rows at once
        cursor.executemany("INSERT INTO your_table_name (profile_url) VALUES (%s) ON CONFLICT(profile_url) DO NOTHING", [(link,) for link in links])
        conn.commit()
        cursor.close()
    except pg.Error as e:
        print("Error bulk inserting links:", e)

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