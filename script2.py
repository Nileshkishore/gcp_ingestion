import os
import pandas as pd
from google.cloud import storage
from sqlalchemy import create_engine

# Set up Google Cloud credentials using the service account JSON file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/sigmoid/Documents/gcp_ingestion/nileshproject-435805-6f719ed47122.json'

# Function to download the CSV file from GCS
def download_csv_from_gcs(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket using the provided service account JSON."""
    # Create a storage client using the service account JSON
    storage_client = storage.Client()

    # Get the bucket and blob
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    # Download the file
    blob.download_to_filename(destination_file_name)
    print(f"File {source_blob_name} downloaded to {destination_file_name}.")

# Function to insert CSV data into PostgreSQL using Pandas
def insert_csv_into_postgres(csv_file_path, postgres_config, table_name):
    """Insert CSV data into the Postgres database using Pandas."""
    try:
        # Create a connection string
        connection_string = f"postgresql://{postgres_config['user']}:{postgres_config['password']}@" \
                            f"{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"

        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        # Read CSV file into a DataFrame
        df = pd.read_csv(csv_file_path)

        # Insert DataFrame into PostgreSQL table
        df.to_sql(table_name, engine, if_exists='replace', index=False)  # Change to 'append' to add data without dropping the table

        print("CSV data inserted into the PostgreSQL database successfully.")
    
    except Exception as error:
        print(f"Error: {error}")

# Main function
def main():
    # GCS bucket info
    bucket_name = 'post_gres_check_nilesh'
    source_blob_name = 'nilesh_kishore/iris_cleaned.csv'  # GCS path
    destination_file_name = '/home/sigmoid/Documents/gcp_ingestion/downloaded.csv'  # Local path

    # PostgreSQL database config
    postgres_config = {
        'host': 'localhost',  # Change to 'host.docker.internal' if necessary
        'port': 5430,
        'database': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword'
    }
    
    table_name = 'gcs_ingest_3'
    
    # Download the CSV file from GCS
    download_csv_from_gcs(bucket_name, source_blob_name, destination_file_name)

    # Insert CSV into PostgreSQL using Pandas
    insert_csv_into_postgres(destination_file_name, postgres_config, table_name)

if __name__ == '__main__':
    main()
