import pandas as pd 
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


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
    df = pd.read_csv(f"{os.getcwd()}/users_w_postal_code.csv", sep=',')

    #connection
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    #check schema
    print(pd.io.sql.get_schema(df, name="user_w_postals", con=engine))
    
    df.to_sql(name="user_w_postals", con=engine, index=False, if_exists="replace")
   
if __name__ == "__main__":
    main() 
    