import os
import time
import logging
import requests
import pandas as pd

# Configure logging to output to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_market_data(coin_id: str = "ethereum", currency: str = "usd", days: int = 7, max_retries: int = 3) -> dict:
    url = f"https://api.coingecko.com/api/v3/coins/CG-2GwBoDyKGFHywJ2gaSdtrNd7/market_chart"
    params = {
        "vs_currency": currency,
        "days": days,
        "interval": "daily"
    }
    
    logger.info(f"Fetching {days} days of market data for {coin_id} in {currency}...")
    
    for attempt in range(1, max_retries + 1):
        try:
            # 10 second timeout for good practice
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status() # Raise an exception for bad status codes
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning(f"Rate limit hit (HTTP 429). Attempt {attempt}/{max_retries}. Retrying in 5 seconds...")
                if attempt < max_retries:
                    time.sleep(5)
                else:
                    logger.error("Max retries reached. Failing.")
                    raise
            else:
                logger.error(f"HTTP Error: {e}")
                raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request Error fetching data: {e}")
            if attempt < max_retries:
                time.sleep(5)
            else:
                raise

def process_market_data(raw_data: dict) -> pd.DataFrame:
    """Processes raw JSON API response into a structured Pandas DataFrame."""
    logger.info("Processing raw JSON data into DataFrame...")
    
    try:
        # Extract prices and volumes
        prices = raw_data.get("prices", [])
        volumes = raw_data.get("total_volumes", [])
        
        if not prices or not volumes:
            raise ValueError("API response is missing 'prices' or 'total_volumes' data.")
            
        # Create DataFrames
        df_prices = pd.DataFrame(prices, columns=["timestamp", "price"])
        df_volumes = pd.DataFrame(volumes, columns=["timestamp", "volume"])
        
        # Merge DataFrames on the timestamp column
        df = pd.merge(df_prices, df_volumes, on="timestamp")
        
        # Convert timestamp (Unix milliseconds) to readable date
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date
        
        # Drop the raw timestamp and keep clean columns
        df = df[["date", "price", "volume"]]
        
        # Explicitly cast types to numerical format to prevent issues downstream (e.g., BigQuery)
        df["price"] = df["price"].astype(float)
        df["volume"] = df["volume"].astype(float)
        
        # CoinGecko's daily interval can return a live, current-day data point, 
        # so we drop duplicates by date, keeping the most recent one.
        df = df.drop_duplicates(subset=["date"], keep="last")
        
        return df
        
    except Exception as e:
        logger.error(f"Error processing market data: {e}")
        raise

def save_to_csv(df: pd.DataFrame, filename: str):
    """Saves DataFrame to a CSV file in the temp_data directory."""
    # Find the root of the project (one level up from extract_load/)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(project_root, "temp_data")
    
    # Create temp_data directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, filename)
    logger.info(f"Saving data to {output_path}...")
    
    try:
        df.to_csv(output_path, index=False)
        logger.info("Data saved successfully.")
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        raise

def main():
    try:
        logger.info("Starting market data extraction pipeline...")
        
        coin_id = "ethereum"
        currency = "usd"
        
        # 1. Fetch
        raw_data = fetch_market_data(coin_id=coin_id, currency=currency, days=7)
        
        # 2. Process
        df = process_market_data(raw_data)
        logger.info(f"Generated DataFrame with {len(df)} records for {coin_id}.")
        
        # 3. Save
        save_to_csv(df, f"{coin_id}_market_data.csv")
        
        logger.info("Pipeline executed successfully.")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    main()
