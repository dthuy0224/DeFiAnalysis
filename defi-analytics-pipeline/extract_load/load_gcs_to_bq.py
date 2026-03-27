import os
import logging
from datetime import datetime
from google.cloud import bigquery
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=env_path, override=True)

def get_bq_client() -> bigquery.Client:
    """Initializes and returns a Google BigQuery client."""
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    if not credentials_path:
        logger.error("GOOGLE_APPLICATION_CREDENTIALS not found in environment variables.")
        raise ValueError("Missing GCP Credentials")
        
    full_cred_path = os.path.join(project_root, credentials_path)
    if not os.path.exists(full_cred_path):
        logger.error(f"Service account key file not found at: {full_cred_path}")
        raise FileNotFoundError("GCP Service Account JSON key missing")
        
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = full_cred_path
    
    try:
        client = bigquery.Client()
        logger.info(f"Successfully authenticated with BigQuery. Default project: {client.project}")
        return client
    except Exception as e:
        logger.error(f"Failed to authenticate with BigQuery: {e}")
        raise

def load_csv_from_gcs_to_bq(client: bigquery.Client, gcs_uri: str, table_id: str):
    """Loads a CSV file from GCS into a BigQuery table."""
    logger.info(f"Loading data from {gcs_uri} to BigQuery table {table_id}...")
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        # Overwrite the table if you just want latest data, 
        # use WRITE_APPEND if you want to keep historical data. 
        # For this exercise, we will append it but since we're loading 7 days of 
        # market data each time, it might cause duplicates. 
        # A true pipeline would handle it via dbt or use WRITE_TRUNCATE for staging.
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        load_job = client.load_table_from_uri(
            gcs_uri, table_id, job_config=job_config
        )  # Make an API request.
        
        load_job.result()  # Waits for the job to complete.
        
        destination_table = client.get_table(table_id)  # Make an API request.
        logger.info(f"Loaded {destination_table.num_rows} rows into {table_id}.")
    except Exception as e:
        logger.error(f"Error loading data to BigQuery: {e}")
        raise

def main():
    try:
        logger.info("Starting BigQuery data loading pipeline...")
        
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        if not bucket_name:
             raise ValueError("GCS_BUCKET_NAME not found in environment variables.")
             
        client = get_bq_client()
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        market_data_uri = f"gs://{bucket_name}/raw_data/{today_str}/ethereum_market_data.csv"
        onchain_data_uri = f"gs://{bucket_name}/raw_data/{today_str}/ethereum_onchain_data.csv"
        
        dataset_name = "defi_raw"
        market_table_id = f"{dataset_name}.raw_market_data"
        onchain_table_id = f"{dataset_name}.raw_onchain_data"
        
        _csv_from_gcs_to_bq(client, market_data_uri, market_table_id)
        load_csv_from_gcs_to_bq(client, onchain_data_uri, onchain_table_id)
            
        logger.info("BigQuery loading pipeline executed successfully.")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    main()
