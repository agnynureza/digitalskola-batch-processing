import pandas as pd 
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from create_table import connect_to_database, close_connection


def main():
    #load env
    load_dotenv()
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    
    #set up path
    parent_directory = os.path.dirname(os.getcwd())
    src_directory = os.path.join(parent_directory, 'dataset')
    os.chdir(src_directory)

    #read csv
    filename = f"{os.getcwd()}/region.csv"
    df = pd.read_csv(filename, sep=',')

    #prerequised, already create table regions
    #connection
    db_conn, db_cursor = connect_to_database({
        'DB_HOST':DB_HOST,
        'DB_PORT':DB_PORT,
        'DB_NAME':DB_NAME,
        'DB_USER':DB_USER,
        'DB_PASSWORD':DB_PASSWORD,
    })
    
    
   # insert data
    with open(filename, 'r') as f:
        next(f)
        db_cursor.copy_from(f, 'regions', sep=',', columns=('postalZip', 'region', 'country'))
    print("Successfully insert regions data.")
    db_conn.commit()

    #closed connection
    close_connection(db_conn, db_cursor)
            
if __name__ == "__main__":
    main() 
    