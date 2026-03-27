import os
import logging
from datetime import datetime
from google.cloud import storage
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

def get_gcs_client() -> storage.Client:
    """Initializes and returns a Google Cloud Storage client."""
    # The google-cloud-storage library automatically looks for the GOOGLE_APPLICATION_CREDENTIALS
    # environment variable. We just need to ensure it's set in our .env file.
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    if not credentials_path:
        logger.error("GOOGLE_APPLICATION_CREDENTIALS not found in environment variables.")
        raise ValueError("Missing GCP Credentials")
        
    full_cred_path = os.path.join(project_root, credentials_path)
    if not os.path.exists(full_cred_path):
        logger.error(f"Service account key file not found at: {full_cred_path}")
        raise FileNotFoundError("GCP Service Account JSON key missing")
        
    # Set the environment variable explicitly for the client to pick up
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = full_cred_path
    
    try:
        client = storage.Client()
        logger.info("Successfully authenticated with Google Cloud.")
        return client
    except Exception as e:
        logger.error(f"Failed to authenticate with GCP: {e}")
        raise

def upload_to_gcs(client: storage.Client, file_path: str, bucket_name: str, destination_blob_name: str):
    """Uploads a single file to a GCS bucket."""
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        logger.info(f"Uploading {file_path} to gs://{bucket_name}/{destination_blob_name}...")
        blob.upload_from_filename(file_path)
        
        logger.info(f"Successfully uploaded {destination_blob_name}.")
    except Exception as e:
        logger.error(f"Error uploading {file_path} to GCS: {e}")
        raise

def main():
    try:
        logger.info("Starting GCS data loading pipeline...")
        
        # 1. Configuration Setup
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        if not bucket_name:
             raise ValueError("GCS_BUCKET_NAME not found in environment variables.")
             
        # 2. Authenticate
        client = get_gcs_client()
        
        # 3. Locate files to upload
        temp_data_dir = os.path.join(project_root, "temp_data")
        if not os.path.exists(temp_data_dir):
            logger.warning(f"Directory {temp_data_dir} does not exist. No files to upload.")
            return
            
        files_to_upload = [f for f in os.listdir(temp_data_dir) if f.endswith('.csv')]
        
        if not files_to_upload:
            logger.warning("No CSV files found in temp_data/. Exiting.")
            return
            
        # 4. Upload each file with a date-partitioned folder structure
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        for filename in files_to_upload:
            local_file_path = os.path.join(temp_data_dir, filename)
            
            # Destination path: raw_data/YYYY-MM-DD/filename.csv
            gcs_destination_path = f"raw_data/{today_str}/{filename}"
            
            upload_to_gcs(client, local_file_path, bucket_name, gcs_destination_path)
            
        logger.info("GCS loading pipeline executed successfully.")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    main()
