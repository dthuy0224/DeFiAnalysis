import os
import logging
from datetime import datetime, timezone
import pandas as pd
from web3 import Web3
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
# We look for .env in the root directory (one level up from extract_load/)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=env_path, override=True)

def connect_to_node() -> Web3:
    """Establishes connection to the Ethereum node via RPC."""
    provider_uri = os.getenv("WEB3_PROVIDER_URI")
    
    if not provider_uri:
        logger.error("WEB3_PROVIDER_URI not found in environment variables.")
        logger.error("Please create a .env file based on .env.example")
        raise ValueError("Missing Web3 Provider URI")

    w3 = Web3(Web3.HTTPProvider(provider_uri))
    
    if not w3.is_connected():
        logger.error("Failed to connect to the Ethereum node. Check your RPC URL.")
        raise ConnectionError("Web3 connection failed")
        
    logger.info(f"Successfully connected to Ethereum Node. Client version: {w3.client_version}")
    return w3

def extract_block_data(w3: Web3, num_blocks: int = 100) -> list:
    """Extracts on-chain data for the latest N blocks."""
    latest_block_number = w3.eth.block_number
    logger.info(f"Latest block number: {latest_block_number}")
    logger.info(f"Extracting data for the last {num_blocks} blocks...")
    
    start_block = latest_block_number - num_blocks + 1
    if start_block < 0:
         start_block = 0
         
    data = []
    
    for block_num in range(latest_block_number, start_block - 1, -1):
        try:
            # Fetch the full block, but without full transaction objects to save bandwidth
            block = w3.eth.get_block(block_num, full_transactions=False)
            
            # Convert Unix timestamp to readable UTC datetime string
            dt_utc = datetime.fromtimestamp(block.timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            
            # Base fee per gas (introduced in EIP-1559). Convert from Wei to Gwei.
            base_fee_gwei = 0.0
            if 'baseFeePerGas' in block:
                base_fee_gwei = w3.from_wei(block.baseFeePerGas, 'gwei')
                
            block_data = {
                "block_number": block.number,
                "timestamp": dt_utc,
                "transaction_count": len(block.transactions),
                "base_fee_per_gas_gwei": float(base_fee_gwei) # Explicit cast
            }
            
            data.append(block_data)
            
            if len(data) % 20 == 0:
                logger.info(f"Processed {len(data)}/{num_blocks} blocks...")
                
        except Exception as e:
             logger.error(f"Error extracting data for block {block_num}: {e}")
             # Decide whether to raise or continue. Continuing allows partial data extraction.
             continue
             
    return data

def save_to_csv(data: list, filename: str):
    """Saves the list of dictionaries to a CSV file using Pandas."""
    if not data:
        logger.warning("No data to save.")
        return
        
    df = pd.DataFrame(data)
    
    output_dir = os.path.join(project_root, "temp_data")
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, filename)
    logger.info(f"Saving {len(df)} records to {output_path}...")
    
    try:
        df.to_csv(output_path, index=False)
        logger.info("On-chain data saved successfully.")
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        raise

def main():
    try:
        logger.info("Starting on-chain data extraction pipeline...")
        
        # 1. Connect
        w3 = connect_to_node()
        
        # 2. Extract
        blocks_data = extract_block_data(w3, num_blocks=100)
        
        # 3. Save
        save_to_csv(blocks_data, "ethereum_onchain_data.csv")
        
        logger.info("Pipeline executed successfully.")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    main()
