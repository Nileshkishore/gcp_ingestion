import os
import psycopg2
from google.cloud import storage

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

def create_table_if_not_exists(cursor, table_name):
    """Create a table if it does not exist."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        sepal_length NUMERIC,
        sepal_width NUMERIC,
        petal_length NUMERIC,
        petal_width NUMERIC,
        species VARCHAR(50)
    );
    """
    cursor.execute(create_table_query)
    print(f"Table {table_name} checked/created successfully.")

# Function to insert CSV data into PostgreSQL
def insert_csv_into_postgres(csv_file_path, postgres_config, table_name):
    """Insert CSV data into the Postgres database."""
    conn = None  # Initialize conn to None
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=postgres_config['host'],
            port=postgres_config['port'],  
            database=postgres_config['database'],
            user=postgres_config['user'],
            password=postgres_config['password']
        )
        cursor = conn.cursor()

        create_table_if_not_exists(cursor, table_name)

        # Load the CSV file into the specified table
        with open(csv_file_path, 'r') as f:
            # Specify the columns to avoid inserting into 'id'
            cursor.copy_expert(f"COPY {table_name} (sepal_length, sepal_width, petal_length, petal_width, species) FROM STDIN WITH CSV HEADER DELIMITER ','", f)
        
        conn.commit()
        print("CSV data inserted into the PostgreSQL database successfully.")
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    
    finally:
        if conn is not None:  # Only close if conn was successfully created
            cursor.close()
            conn.close()

# Main function
def main():
    # GCS bucket info
    bucket_name = 'post_gres_check_nilesh'
    source_blob_name = 'nilesh_kishore/iris_cleaned.csv'  # GCS path
    destination_file_name = '/home/sigmoid/Documents/gcp_ingestion/downloaded-file.csv'  # Local path

    # Service account JSON file path
    service_account_json = '/home/sigmoid/Documents/gcp_ingestion/nileshproject-435805-6f719ed47122.json'  # Service account file location

    # PostgreSQL database config
    postgres_config = {
        'host': 'localhost',  # Change to 'host.docker.internal' if necessary
        'port': 5430,
        'database': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword'
    }
    
    table_name = 'gcs_ingest_2'
    
    # Download the CSV file from GCS using the service account JSON
    download_csv_from_gcs(bucket_name, source_blob_name, destination_file_name)

    # Insert CSV into PostgreSQL
    insert_csv_into_postgres(destination_file_name, postgres_config, table_name)

if __name__ == '__main__':
    main()
