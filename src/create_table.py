from dotenv import load_dotenv
import os 
import psycopg2

def connect_to_database():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = conn.cursor()
        
        print("Connection Open.")
        return conn, cursor
    except psycopg2.errors as e:
        print(f"Error Connecting to database: {e}")
        return None, None

def close_connection(conn, cursor):
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        print("Connection Closed.")
        
def create_table(conn, cursor):
    try:
        query = '''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                age INT,
                phone VARCHAR(20)
            )
        '''
        cursor.execute(query)
        conn.commit()
        
        print("Table Created Successfully.")
    except psycopg2.errors as e:
        print(f"Error Created table: {e}")

if __name__ == "__main__":
    #load env
    load_dotenv()
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    #connect to database
    db_conn, db_cursor = connect_to_database()
    
    if db_conn and db_cursor:
        #create_table
        create_table(db_conn, db_cursor)
        
        #closed connection
        close_connection(db_conn, db_cursor)

