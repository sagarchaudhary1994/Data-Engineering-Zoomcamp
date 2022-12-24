import pandas as pd
from pyarrow.parquet import ParquetFile
import pyarrow as pa
from sqlalchemy import create_engine
from time import time
import argparse
import os

BATCH_SIZE = 100000


def main(params):
    try:
        user = params.user
        password = params.password
        host = params.host
        port = params.port
        db = params.db
        table_name = params.table_name
        url = params.url

        file_name = "output.parquet"

        # download the file
        print("Download for data file- started")

        os.system(f"wget {url} --no-check-certificate -O {file_name}")

        print("Data download finished!")

        # Connect to Postgre
        engine = create_engine(
            f'postgresql://{user}:{password}@{host}:{port}/{db}')
        engine.connect()
        print("Database connection succeded!!")

        pf = ParquetFile(file_name)

        print("Data ingestion started!!")
        for batchId, batch in enumerate(pf.iter_batches(batch_size=BATCH_SIZE)):
            start_time = time()
            df = batch.to_pandas()
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            if (batchId == 0):
                df.head(n=0).to_sql(name=table_name,
                                    con=engine, if_exists='replace')
                print("Table created!")
            df.to_sql(name=table_name, con=engine, if_exists='append')
            end_time = time()
            print(
                f"Data batch- {batchId}, ingested successfully . Wall Time- {end_time - start_time} second")

        print("Data Ingested completed!!")
    except Exception as e:
        print(f"Error in ingestion pipeline-\n{e}")


if __name__ == "__main__":

    # Parse command line arguments to get
    parser = argparse.ArgumentParser(description='Ingest Data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name',
                        help='name of the table where we will write our results to')
    parser.add_argument('--url', help='Url to download the file from')

    args = parser.parse_args()

    # Call the main method
    main(args)
