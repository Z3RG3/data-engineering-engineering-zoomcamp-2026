import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
import time
import click

# # 1. Configuration / Parameters  - >  insted of hardcoding these, we will pass them as command line arguments using click
# year = 2021
# month = 1
# chunksize = 100000

# # Connection details (Change these if using Docker)
# pg_user = 'root'
# pg_pass = 'root'
# pg_host = 'localhost' 
# pg_port = 5432
# pg_db = 'ny_taxi'
# table_name = 'yellow_taxi_data'

# 2. Data Definitions (Types & Dates)
# We define these early so Pandas handles the data correctly from the start
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

# 3. Execution Setup
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
url = f"{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz"

# Create Postgres Connection Engine
engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

# 4. The Ingestion Logic
# added click livrary to make it easier to run with different parameters from the command line
@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):

# def main():
    print(f"Connecting to {pg_host}...")pipeline/ingest_data.py
    
    # Initialize the CSV iterator (This doesn't load data yet, just prepares the connection)
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    # Step A: Get the first chunk to create the table structure
    print("Fetching first chunk...")
    df = next(df_iter)

    # Create the table (if_exists='replace' creates the schema based on df headers)
    # We use n=0 to just create the structure first, or just write the first chunk
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print(f"Table '{table_name}' created/reset.")

    # Step B: Loop through all chunks and append them
    # tqdm gives us that nice progress bar
    for df_chunk in tqdm(df_iter):
        t_start = time.time()
        
        df_chunk.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time.time()
        print(f" - Inserted chunk (took {t_end - t_start:.3f} seconds)")

    print("Finished ingesting all data!")

if __name__ == "__main__":
    run()
