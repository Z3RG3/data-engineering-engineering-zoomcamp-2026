import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
import time
import click

# 1. Keep Static Definitions Outside
dtype = {
    "VendorID": "Int64", "passenger_count": "Int64", "trip_distance": "float64",
    "RatecodeID": "Int64", "store_and_fwd_flag": "string", "PULocationID": "Int64",
    "DOLocationID": "Int64", "payment_type": "Int64", "fare_amount": "float64",
    "extra": "float64", "mta_tax": "float64", "tip_amount": "float64",
    "tolls_amount": "float64", "improvement_surcharge": "float64",
    "total_amount": "float64", "congestion_surcharge": "float64"
}
parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--year', default=2021, type=int, help='Year of data')
@click.option('--month', default=1, type=int, help='Month of data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, year, month, chunksize):
    
    # 2. Dynamic Setup (Must be inside so it sees the arguments)
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f"{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz"
    
    conn_string = f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    engine = create_engine(conn_string)

    print(f"Connecting to {pg_host}...")
    
    try:
        # 3. The Ingestion Logic
        df_iter = pd.read_csv(
            url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=chunksize
        )

        print("Fetching first chunk...")
        df = next(df_iter)

        # Create schema
        df.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')
        print(f"Table '{target_table}' created.")

        # Insert first chunk
        df.to_sql(name=target_table, con=engine, if_exists='append')

        # Loop through rest
        for df_chunk in tqdm(df_iter):
            df_chunk.to_sql(name=target_table, con=engine, if_exists='append')
            
    except Exception as e:
        print(f"Error occurred: {e}")

    print("Finished ingesting all data!")

if __name__ == "__main__":
    run()
