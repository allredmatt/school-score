import pandas as pd
import requests
import time
import random
from typing import Tuple, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MultiAPIGeocoder:
    def __init__(self):
        self.current_api = 0
        self.request_counts = {}
        self.failure_counts = {}
        self.api_disabled = {}
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'PostcodeGeocoder/1.0'})
        
        # Initialize counters
        for i in range(2):
            self.request_counts[i] = 0
            self.failure_counts[i] = 0
            self.api_disabled[i] = False
    
    def geocode_nominatim(self, postcode: str) -> Optional[Tuple[float, float]]:
        """Geocode using OpenStreetMap Nominatim (1 req/sec limit)"""
        try:
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                'q': postcode,
                'format': 'json',
                'limit': 1,
                'countrycodes': 'gb'  # Assuming UK postcodes, change as needed
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data:
                # Reset failure count on success
                self.failure_counts[1] = 0
                return float(data[0]['lat']), float(data[0]['lon'])
            return None
            
        except Exception as e:
            self.failure_counts[1] += 1
            
            # If 5 consecutive failures, temporarily disable this API
            if self.failure_counts[1] >= 5:
                logger.warning(f"Nominatim has failed 5 times consecutively - temporarily disabling")
                self.api_disabled[1] = True
            
            logger.warning(f"Nominatim failed for {postcode}: {e}")
            return None
    
    def geocode_postcodes_io(self, postcode: str) -> Optional[Tuple[float, float]]:
        """Geocode using postcodes.io (UK specific, 1000 req/day limit)"""
        try:
            url = f"https://api.postcodes.io/postcodes/{postcode.replace(' ', '')}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 200:
                result = data['result']
                # Reset failure count on success
                self.failure_counts[0] = 0
                return float(result['latitude']), float(result['longitude'])
            return None
            
        except Exception as e:
            self.failure_counts[0] += 1
            
            # If 5 consecutive failures, temporarily disable this API
            if self.failure_counts[0] >= 5:
                logger.warning(f"Postcodes.io has failed 5 times consecutively - temporarily disabling")
                self.api_disabled[0] = True
            
            logger.warning(f"Postcodes.io failed for {postcode}: {e}")
            return None
    

    
    def geocode_postcode(self, postcode: str) -> Optional[Tuple[float, float]]:
        """Alternate between Nominatim and postcodes.io with smart fallback"""
        
        # If both APIs are disabled, try to re-enable them
        if self.api_disabled[0] and self.api_disabled[1]:
            logger.info("Both APIs disabled - re-enabling and resetting failure counts")
            self.api_disabled[0] = False
            self.api_disabled[1] = False
            self.failure_counts[0] = 0
            self.failure_counts[1] = 0
        
        # Determine which API to try first
        primary_api = self.current_api
        fallback_api = 1 - self.current_api
        
        # Skip disabled APIs
        if self.api_disabled[primary_api]:
            primary_api, fallback_api = fallback_api, primary_api
        
        # Try primary API
        if not self.api_disabled[primary_api]:
            if primary_api == 0:  # Postcodes.io
                logger.info(f"Trying postcodes.io for {postcode}")
                coords = self.geocode_postcodes_io(postcode)
                if coords:
                    self.request_counts[0] += 1
                    logger.info(f"✓ Postcodes.io success for {postcode}")
                    self.current_api = 1  # Switch to Nominatim next
                    time.sleep(0.1)
                    return coords
                    
            else:  # Nominatim
                logger.info(f"Trying Nominatim for {postcode}")
                coords = self.geocode_nominatim(postcode)
                if coords:
                    self.request_counts[1] += 1
                    logger.info(f"✓ Nominatim success for {postcode}")
                    self.current_api = 0  # Switch to postcodes.io next
                    time.sleep(0.7)
                    return coords
        
        # Try fallback API if primary failed
        if not self.api_disabled[fallback_api]:
            logger.info(f"Primary API failed, trying fallback for {postcode}")
            
            if fallback_api == 0:  # Postcodes.io
                coords = self.geocode_postcodes_io(postcode)
                if coords:
                    self.request_counts[0] += 1
                    logger.info(f"✓ Postcodes.io fallback success for {postcode}")
                    self.current_api = 1
                    time.sleep(0.1)
                    return coords
                    
            else:  # Nominatim
                # Need to respect rate limit for Nominatim
                time.sleep(0.7)
                coords = self.geocode_nominatim(postcode)
                if coords:
                    self.request_counts[1] += 1
                    logger.info(f"✓ Nominatim fallback success for {postcode}")
                    self.current_api = 0
                    time.sleep(0.4)  # Additional delay since we already waited
                    return coords
        
        # Switch API for next request even if both failed
        self.current_api = 1 - self.current_api
        
        logger.error(f"All available APIs failed for postcode: {postcode}")
        return None

def process_postcodes(input_file: str, output_file: str, postcode_column: str):
    """Process postcodes from CSV/Excel file"""
    
    # Read the file
    try:
        if input_file.endswith('.csv'):
            df = pd.read_csv(input_file)
        else:
            df = pd.read_excel(input_file)
        logger.info(f"Loaded {len(df)} rows from {input_file}")
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return
    
    # Check if postcode column exists
    if postcode_column not in df.columns:
        logger.error(f"Column '{postcode_column}' not found. Available columns: {list(df.columns)}")
        return
    
    # Initialize geocoder
    geocoder = MultiAPIGeocoder()
    
    # Add new columns for coordinates
    df['latitude'] = None
    df['longitude'] = None
    df['geocoded'] = False
    
    # Process each postcode
    total_rows = len(df)
    successful = 0
    
    for index, row in df.iterrows():
        postcode = str(row[postcode_column]).strip()
        
        if pd.isna(postcode) or postcode == '' or postcode.lower() == 'nan':
            logger.info(f"Skipping empty postcode at row {index + 1}")
            continue
        
        logger.info(f"Processing {index + 1}/{total_rows}: {postcode}")
        
        # Try to geocode
        coords = geocoder.geocode_postcode(postcode)
        
        if coords:
            df.at[index, 'latitude'] = coords[0]
            df.at[index, 'longitude'] = coords[1]
            df.at[index, 'geocoded'] = True
            successful += 1
            logger.info(f"✓ {postcode} -> {coords[0]:.6f}, {coords[1]:.6f}")
        else:
            logger.warning(f"✗ Failed to geocode: {postcode}")
        
        # Save progress every 50 rows
        if (index + 1) % 50 == 0:
            logger.info(f"Saving progress... ({successful}/{index + 1} successful)")
            df.to_csv(f"{output_file}.temp", index=False)
    
    # Save final results
    try:
        if output_file.endswith('.csv'):
            df.to_csv(output_file, index=False)
        else:
            df.to_excel(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
    except Exception as e:
        logger.error(f"Error saving file: {e}")
        return
    
    # Print summary
    logger.info(f"\nSUMMARY:")
    logger.info(f"Total rows processed: {total_rows}")
    logger.info(f"Successfully geocoded: {successful}")
    logger.info(f"Failed: {total_rows - successful}")
    logger.info(f"Success rate: {(successful/total_rows)*100:.1f}%")
    
    # Print API usage
    logger.info(f"\nAPI Usage:")
    api_names = ["Postcodes.io", "Nominatim"]
    for i, count in geocoder.request_counts.items():
        logger.info(f"{api_names[i]}: {count} requests")

if __name__ == "__main__":
    # Configuration
    INPUT_FILE = "post_codes.csv"  # Change this to your file path
    OUTPUT_FILE = "geocoded_results.csv"  # Output file path
    POSTCODE_COLUMN = "PCODE"  # Change this to your postcode column name
    
    # Run the geocoding
    process_postcodes(
        input_file=INPUT_FILE,
        output_file=OUTPUT_FILE,
        postcode_column=POSTCODE_COLUMN
    )