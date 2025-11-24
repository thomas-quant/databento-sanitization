import pandas as pd
import pytz
from datetime import datetime, timedelta
import re

# ========== CONFIGURATION ==========
ASSET = "ES"  # Change to MES, NQ, YM, etc.
ROLLOVER_WINDOW_DAYS = 14  # Days before expiry to start checking for rollover
CONSECUTIVE_DAYS_THRESHOLD = 2  # Days of higher volume needed to trigger rollover
INPUT_FILE = "sanitized_data.csv"
OUTPUT_FILE = "continuous_futures.csv"
# ===================================

def parse_contract_month(symbol):
    """Extract year and month from contract symbol (e.g., ESZ0 -> 2020, 12)"""
    month_codes = {
        'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
        'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
    }
    
    match = re.match(rf'{ASSET}([FGHJKMNQUVXZ])(\d)', symbol)
    if not match:
        return None, None
    
    month_code, year_digit = match.groups()
    month = month_codes[month_code]
    
    # Handle year: 0-9 could be 2020-2029 or 2010-2019
    # Check the data to determine the decade
    year_digit = int(year_digit)
    year = 2020 + year_digit  # Assume 2020s for now
    
    return year, month

def get_third_friday(year, month):
    """Calculate the third Friday of a given month"""
    # Start with first day of month
    first_day = datetime(year, month, 1)
    
    # Find first Friday
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_until_friday)
    
    # Third Friday is 14 days later
    third_friday = first_friday + timedelta(days=14)
    
    return third_friday

def calculate_rollover_dates(df):
    """Calculate rollover dates based on volume analysis"""
    # Get unique contracts sorted by expiry
    contracts = df['Symbol'].unique()
    contract_info = []
    
    for contract in contracts:
        year, month = parse_contract_month(contract)
        if year and month:
            expiry = get_third_friday(year, month)
            contract_info.append({
                'symbol': contract,
                'expiry': expiry,
                'year': year,
                'month': month
            })
    
    contract_info = sorted(contract_info, key=lambda x: x['expiry'])
    rollover_dates = {}
    
    print("\nContract Expiry Schedule:")
    print("=" * 60)
    for info in contract_info:
        print(f"{info['symbol']}: Expires {info['expiry'].strftime('%Y-%m-%d')}")
    print("=" * 60)
    
    for i in range(len(contract_info) - 1):
        current_contract = contract_info[i]
        next_contract = contract_info[i + 1]
        
        expiry_date = current_contract['expiry']
        window_start = expiry_date - timedelta(days=ROLLOVER_WINDOW_DAYS)
        
        print(f"\nAnalyzing rollover: {current_contract['symbol']} -> {next_contract['symbol']}")
        print(f"Window: {window_start.strftime('%Y-%m-%d')} to {expiry_date.strftime('%Y-%m-%d')}")
        
        # Convert to date for comparison
        window_start_date = pd.to_datetime(window_start.date())
        expiry_date_date = pd.to_datetime(expiry_date.date())
        
        # Filter data in the rollover window
        df['Date'] = pd.to_datetime(df['DateTime_ET']).dt.date
        df['Date'] = pd.to_datetime(df['Date'])
        
        mask = (df['Date'] >= window_start_date) & (df['Date'] < expiry_date_date)
        window_data = df[mask].copy()
        
        # Calculate daily volume for both contracts
        current_daily = window_data[window_data['Symbol'] == current_contract['symbol']].groupby('Date')['Volume'].sum()
        next_daily = window_data[window_data['Symbol'] == next_contract['symbol']].groupby('Date')['Volume'].sum()
        
        # Find rollover date: first occurrence of N consecutive days where next > current
        rollover_date = None
        consecutive_count = 0
        
        common_dates = sorted(set(current_daily.index) & set(next_daily.index))
        
        for date in common_dates:
            curr_vol = current_daily.get(date, 0)
            next_vol = next_daily.get(date, 0)
            
            print(f"  {date.strftime('%Y-%m-%d')}: {current_contract['symbol']}={curr_vol:,} vs {next_contract['symbol']}={next_vol:,}")
            
            if next_vol > curr_vol:
                consecutive_count += 1
                if consecutive_count >= CONSECUTIVE_DAYS_THRESHOLD:
                    rollover_date = date
                    print(f"  >>> ROLLOVER TRIGGERED on {rollover_date.strftime('%Y-%m-%d')} <<<")
                    break
            else:
                consecutive_count = 0
        
        # Fallback: if no volume crossover, roll 3 days before expiry
        if rollover_date is None:
            rollover_date = pd.to_datetime((expiry_date - timedelta(days=3)).date())
            print(f"  >>> No volume crossover found, using fallback date: {rollover_date.strftime('%Y-%m-%d')} <<<")
        
        rollover_dates[current_contract['symbol']] = rollover_date
    
    return rollover_dates, contract_info

def create_continuous_series(df, rollover_dates, contract_info):
    """Create continuous futures series by selecting appropriate contract for each date"""
    df['Date'] = pd.to_datetime(df['DateTime_ET']).dt.date
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Sort contracts by expiry
    sorted_contracts = sorted(contract_info, key=lambda x: x['expiry'])
    
    result_rows = []
    
    # For each contract period, determine the date range to use
    for i, contract in enumerate(sorted_contracts):
        symbol = contract['symbol']
        
        if i == 0:
            # First contract: use from start of data
            start_date = df['Date'].min()
        else:
            # Use from previous contract's rollover date
            prev_symbol = sorted_contracts[i-1]['symbol']
            start_date = rollover_dates.get(prev_symbol)
            if start_date is None:
                continue
        
        if symbol in rollover_dates:
            # Use until rollover date (exclusive)
            end_date = rollover_dates[symbol]
            mask = (df['Symbol'] == symbol) & (df['Date'] >= start_date) & (df['Date'] < end_date)
        else:
            # Last contract: use until end of data
            mask = (df['Symbol'] == symbol) & (df['Date'] >= start_date)
        
        contract_data = df[mask].copy()
        
        if len(contract_data) > 0:
            print(f"\n{symbol}: {contract_data['Date'].min().strftime('%Y-%m-%d')} to {contract_data['Date'].max().strftime('%Y-%m-%d')} ({len(contract_data):,} bars)")
            result_rows.append(contract_data)
    
    # Combine all contract periods
    continuous_df = pd.concat(result_rows, ignore_index=True)
    continuous_df = continuous_df.sort_values('DateTime_ET').reset_index(drop=True)
    
    # Drop the temporary Date column
    continuous_df = continuous_df.drop('Date', axis=1)
    
    return continuous_df

def process_rollovers(input_file):
    """Main processing function"""
    # Read cleaned CSV
    df = pd.read_csv(input_file)
    
    print(f"Loaded {len(df):,} rows from {input_file}")
    print(f"Date range: {df['DateTime_ET'].min()} to {df['DateTime_ET'].max()}")
    print(f"Symbols found: {sorted(df['Symbol'].unique())}")
    
    # Calculate rollover dates
    rollover_dates, contract_info = calculate_rollover_dates(df)
    
    print("\n" + "=" * 60)
    print("ROLLOVER SCHEDULE:")
    print("=" * 60)
    for symbol, date in rollover_dates.items():
        print(f"{symbol} -> Roll on {date.strftime('%Y-%m-%d')}")
    print("=" * 60)
    
    # Create continuous series
    print("\nCreating continuous futures series...")
    continuous_df = create_continuous_series(df, rollover_dates, contract_info)
    
    # Save to CSV
    continuous_df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"\n{'='*60}")
    print(f"Processing complete!")
    print(f"Input rows: {len(df):,}")
    print(f"Output rows: {len(continuous_df):,}")
    print(f"Removed rows: {len(df) - len(continuous_df):,}")
    print(f"Output saved to: {OUTPUT_FILE}")
    print(f"{'='*60}")
    
    return continuous_df

if __name__ == "__main__":
    result = process_rollovers(INPUT_FILE)
    print("\nFirst few rows of continuous series:")
    print(result.head(10))
    print("\nLast few rows of continuous series:")
    print(result.tail(10))
