import pandas as pd
import os
import argparse
from glob import glob
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== CONFIGURATION ==========
ASSET = "NQ"  # Change to ES, NQ, MNQ, MES, etc.
INPUT_DIR = "input"
MAX_WORKERS = 8  # Number of threads
# ===================================

def process_file(file):
    """Process a single CSV file"""
    df = pd.read_csv(file)
    
    # Filter for the specified asset
    df = df[df['symbol'].str.startswith(ASSET)].copy()
    
    # Remove spread data (symbols with "-" like NQZ5-NQH6)
    df = df[~df['symbol'].str.contains('-', na=False)].copy()
    
    if len(df) > 0:
        # Keep only desired columns
        df = df[['ts_event', 'action', 'side', 'depth', 'price', 'size', 'sequence']]
        return df, os.path.basename(file), len(df)
    
    return None, os.path.basename(file), 0

def process_tick_data(output_format):
    """Process all tick data files in input directory using multithreading"""
    
    # Set output file based on format
    ext = "parquet" if output_format == "parquet" else "csv"
    output_file = f"{ASSET.lower()}_ticks.{ext}"
    
    # Get all CSV files in input directory
    files = sorted(glob(os.path.join(INPUT_DIR, "*.csv")))
    print(f"Found {len(files)} files to process using {MAX_WORKERS} threads")
    
    all_data = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_file, f): f for f in files}
        
        for future in as_completed(futures):
            result, filename, count = future.result()
            if result is not None:
                all_data.append(result)
                print(f"  {filename}: {count:,} {ASSET} ticks")
    
    if not all_data:
        print(f"No {ASSET} data found!")
        return None
    
    # Combine all data
    print("\nCombining data...")
    df = pd.concat(all_data, ignore_index=True)
    print(f"Total raw ticks: {len(df):,}")
    
    # Parse and sort by timestamp
    print("Sorting by timestamp...")
    df['ts_event'] = pd.to_datetime(df['ts_event'], utc=True)
    df = df.sort_values('ts_event').reset_index(drop=True)
    
    # Save based on format
    print(f"Saving to {output_file}...")
    if output_format == "parquet":
        df.to_parquet(output_file, index=False)
    else:
        df['ts_event'] = df['ts_event'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        df.to_csv(output_file, index=False)
    
    print(f"\nDone! Output saved to {output_file}")
    print(f"Total ticks: {len(df):,}")
    print(f"Date range: {df['ts_event'].iloc[0]} to {df['ts_event'].iloc[-1]}")
    
    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sanitize tick data from Databento")
    parser.add_argument(
        "-f", "--format",
        choices=["csv", "parquet"],
        default="csv",
        help="Output format: csv or parquet (default: csv)"
    )
    args = parser.parse_args()
    
    result = process_tick_data(args.format)
    if result is not None:
        print("\nFirst few rows:")
        print(result.head(5).to_string())
        print("\nLast few rows:")
        print(result.tail(5).to_string())
