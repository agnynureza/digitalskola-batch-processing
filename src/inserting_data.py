import pandas as pd 
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import csv


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

    #connection
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    #check schema
    print(pd.io.sql.get_schema(df, name="regions", con=engine))
    
    # create table
    df.head(n=0).to_sql(name="regions", con=engine, index=False, if_exists="replace")
    
   # Create a connection
    conn= engine.connect()

   # insert data
    with open(filename, 'r') as f:
       reader = csv.reader(f)
       next(reader)
       for row in reader:
           conn.execute("INSERT INTO regions VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                row
           )
           
            
if __name__ == "__main__":
    main() 
    