import pandas as pd
import pytz
import os
from glob import glob

# ========== CONFIGURATION ==========
ASSET = "NQ"  # Change to ES, NQ, MNQ, MES, etc.
INPUT_DIR = "input"
OUTPUT_FILE = f"{ASSET.lower()}_ticks.csv"
# ===================================

def process_tick_data():
    """Process all tick data files in input directory"""
    
    # Get all CSV files in input directory
    files = sorted(glob(os.path.join(INPUT_DIR, "*.csv")))
    print(f"Found {len(files)} files to process")
    
    all_data = []
    
    for file in files:
        df = pd.read_csv(file)
        
        # Filter for the specified asset
        df = df[df['symbol'].str.startswith(ASSET)].copy()
        
        # Remove spread data (symbols with "-" like NQZ5-NQH6)
        df = df[~df['symbol'].str.contains('-', na=False)].copy()
        
        if len(df) > 0:
            all_data.append(df)
            print(f"  {os.path.basename(file)}: {len(df):,} {ASSET} ticks")
    
    if not all_data:
        print(f"No {ASSET} data found!")
        return None
    
    # Combine all data
    df = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal raw ticks: {len(df):,}")
    
    # Parse timestamp (databento uses UTC)
    df['ts_event'] = pd.to_datetime(df['ts_event'], utc=True)
    
    # Convert to Eastern Time
    eastern = pytz.timezone('US/Eastern')
    df['DateTime_ET'] = df['ts_event'].dt.tz_convert(eastern)
    
    # Sort by timestamp
    df = df.sort_values('ts_event').reset_index(drop=True)
    
    # Keep only desired columns
    output_df = pd.DataFrame({
        'DateTime_ET': df['DateTime_ET'].dt.strftime('%Y-%m-%d %H:%M:%S.%f'),
        'DateTime_UTC': df['ts_event'].dt.strftime('%Y-%m-%d %H:%M:%S.%f'),
        'Symbol': df['symbol'],
        'Action': df['action'],
        'Side': df['side'],
        'Depth': df['depth'],
        'Price': df['price'],
        'Size': df['size'],
        'Sequence': df['sequence']
    })
    
    # Save to CSV
    output_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nOutput saved to {OUTPUT_FILE}")
    print(f"Total ticks: {len(output_df):,}")
    print(f"Date range: {output_df['DateTime_ET'].iloc[0]} to {output_df['DateTime_ET'].iloc[-1]}")
    print(f"Unique symbols: {df['symbol'].unique()}")
    
    return output_df

if __name__ == "__main__":
    result = process_tick_data()
    if result is not None:
        print("\nFirst few rows:")
        print(result.head(5).to_string())
        print("\nLast few rows:")
        print(result.tail(5).to_string())

