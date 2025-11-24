import pandas as pd
import pytz

# ========== CONFIGURATION ==========
ASSET = "ES"  # Change to MES, NQ, YM, etc.
INPUT_FILE = "databento_data.csv"
OUTPUT_FILE = "sanitized_data.csv"
# ===================================

def process_databento_data(input_file):
    """Main processing function"""
    # Read databento CSV
    df = pd.read_csv(input_file)
    
    # Filter for the specified asset
    df = df[df['symbol'].str.startswith(ASSET)].copy()
    
    # Remove spread data (symbols with "-" like ESZ0-ESH1)
    initial_count = len(df)
    df = df[~df['symbol'].str.contains('-', na=False)].copy()
    spread_count = initial_count - len(df)
    if spread_count > 0:
        print(f"Removed {spread_count:,} spread data rows")
    
    # Parse timestamp (databento uses UTC)
    df['DateTime_UTC'] = pd.to_datetime(df['ts_event'], utc=True)
    
    # Convert to Eastern Time
    eastern = pytz.timezone('US/Eastern')
    df['DateTime_ET'] = df['DateTime_UTC'].dt.tz_convert(eastern)
    
    # Sort by timestamp
    df = df.sort_values('DateTime_UTC').reset_index(drop=True)
    
    # Create output format
    output_df = pd.DataFrame({
        'DateTime_ET': df['DateTime_ET'].dt.strftime('%Y-%m-%d %H:%M:%S'),
        'Open': df['open'],
        'High': df['high'],
        'Low': df['low'],
        'Close': df['close'],
        'Volume': df['volume'],
        'DateTime_UTC': df['DateTime_UTC'].dt.strftime('%Y-%m-%d %H:%M:%S'),
        'Symbol': df['symbol']  # Keep symbol for future rollover handling
    })
    
    # Save to CSV
    output_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Processing complete! Output saved to {OUTPUT_FILE}")
    print(f"Total bars: {len(output_df)}")
    print(f"Date range: {output_df['DateTime_ET'].iloc[0]} to {output_df['DateTime_ET'].iloc[-1]}")
    print(f"\nUnique symbols found: {df['symbol'].unique()}")
    
    return output_df

if __name__ == "__main__":
    result = process_databento_data(INPUT_FILE)
    print("\nFirst few rows of output:")
    print(result.head(10))
    print("\nLast few rows of output:")
    print(result.tail(10))
