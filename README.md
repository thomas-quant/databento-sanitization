# Databento Futures Data Tools

Simple scripts for processing Databento futures data.

## Scripts

### `sanitize_ticks.py`
Processes raw tick/trade data from Databento CSVs. Filters by asset, removes spreads, and outputs clean tick data.

```bash
pip install polars

python sanitize_ticks.py              # CSV output
python sanitize_ticks.py -f parquet   # Parquet output
```

### `sanitize.py`
Processes OHLCV bar data. Filters by asset, converts timestamps to Eastern Time, removes spreads.

```bash
pip install pandas pytz

python sanitize.py
```

### `rollover.py`
Creates continuous futures series by detecting rollovers based on volume crossover analysis.

```bash
python rollover.py
```

## Configuration

Edit the `ASSET` variable at the top of each script (e.g., `ES`, `NQ`, `MES`, `MNQ`).

## Input

Place Databento CSV files in the `input/` directory.

