
import argparse
import gzip
import os
from time import time

import pandas as pd
import requests
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db_name = params.db_name
    table_name = params.table_name
    url = params.url

    gz_file = 'out_gz.gz'
    csv_name = 'output.csv'

    # download the csv
    import requests
    response = requests.get(url)

    print(f"Requested data size: {len(response.content)} bytes")
    print("Downloading file...")

    with open(gz_file, "wb") as file:
        file.write(response.content)

    with gzip.open(gz_file, 'rb') as f_in, open(csv_name, 'wb') as f_out:
        f_out.write(f_in.read())

    print("File downloaded successfully.")

    print("Connect to postgres..")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True:
        t_start = time()

        df = next(df_iter)
        
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print(f"Inserted another chunk... Took {t_end - t_start} seconds")

if __name__ == "__main__":        
    parser = argparse.ArgumentParser(description="Ingest data into PostgreSQL")

    # user, password, host, port, database name, table name
    # url of the csv

    parser.add_argument('--user', help='User name for PostgreSQL')
    parser.add_argument('--password', help='Password for PostgreSQL')
    parser.add_argument('--host', help='Host address of PostgreSQL server')
    parser.add_argument('--port', help='Port number of PostgreSQL server')
    parser.add_argument('--db_name', help='Name of the database to connect to')
    parser.add_argument('--table_name', help='Name of the table to insert data into')
    parser.add_argument('--url', help='URL of the CSV file')

    args = parser.parse_args()

    main(args)