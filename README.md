### Market Data Processing and Technical Analysis Toolkit

Toolkit for importing equity market data, enriching it with technical indicators, computing fundamentals-derived analytics, and exploring sector/industry behavior. The code integrates multiple data providers (Polygon.io, SEC API, Seeking Alpha via RapidAPI, Yahoo Finance) with rate-limited, concurrent data collection and a flexible indicator pipeline.

This repository assumes a Python package layout named `market_data` (as referenced throughout the modules). If you are using it directly from the repository, ensure the package is importable (see Installation). 


## Contents
- **Overview**: What this repository provides
- **Installation**: Python version, environment setup, dependencies
- **Configuration**: API keys, environment specifics
- **External services/APIs**: Providers and endpoint references
- **Usage examples**: Common tasks and patterns
- **Modules**: What each module offers
- **Data & caching**: Files written/read and expected structure
- **Concurrency & rate limits**: How requests are throttled
- **Troubleshooting & caveats**


## Overview
Core capabilities:
- **Price data import** from Polygon.io (daily and intraday) with pagination handling, concurrency, and rate limiting.
- **Technical indicators** via a composable pipeline (SMA, EMA, ATR, RSI, MACD, Bollinger Bands, AVWAP, relative ATR metrics, and more) for both daily and intraday data.
- **Fundamentals utilities** leveraging SEC API floats and Polygon financials to derive price-to-fundamental series and related metrics.
- **Seeking Alpha integration** through RapidAPI for ratings, earnings, transcripts, and analyst targets. 
- **Watchlist utilities** for sector/industry aggregation, relative strength, and screening integrations.


## Installation
- **Python**: 3.10+ (uses modern typing like `X | Y` and `list[str]`).

1) Create and activate a virtual environment
```bash
python -m venv .venv
source .venv/bin/activate
```

2) Install dependencies
```bash
pip install \
  pandas numpy matplotlib tqdm requests ratelimit yfinance \
  scipy scikit-posthocs plotly sec-api beautifulsoup4 \
  polygon-api-client SQLAlchemy mysql-connector-python finvizfinance \
  backtesting
```

3) Make the package importable
- Option A (recommended if the folder name is `market_data`): run code from the repo root so `market_data` is importable as a package.
- Option B: install in editable mode:
```bash
pip install -e .
```
Note: `setup.py` is included but `install_requires` is intentionally empty; prefer installing the dependencies above explicitly.


## Configuration
Create a `market_data/api_keys.py` file to supply API keys used by the integrations:
```python
# market_data/api_keys.py
polygon_api_key = "YOUR_POLYGON_API_KEY"
sec_api_key = "YOUR_SEC_API_KEY"  # from sec-api.io
seeking_alpha_api_key = "YOUR_RAPIDAPI_KEY_FOR_SEEKING_ALPHA"
```
- Keep keys out of version control.
- Some modules expect local data caches and file paths. Update those paths for your environment (see Data & caching, Troubleshooting & caveats).


## External services and API references
- **Polygon.io**: market data (aggregates, financials)
  - Docs: [Polygon REST docs](https://polygon.io/docs)
  - Client: `polygon-api-client` (`from polygon.stocks import StocksClient` and `from polygon import RESTClient`)
- **SEC API** (sec-api.io): float shares and filing data
  - Docs: [SEC API documentation](https://sec-api.io/)
  - Client: `sec_api.FloatApi`
- **Seeking Alpha via RapidAPI**: meta-data, summary, ratings, earnings, transcripts, analyst targets
  - Hub: [RapidAPI – Seeking Alpha](https://rapidapi.com/)
  - Access: `X-RapidAPI-Key` header with `seeking_alpha_api_key`
- **Yahoo Finance** via `yfinance` for convenience downloads
  - Docs: [`yfinance` project page](https://pypi.org/project/yfinance/)
- **Finviz** via `finvizfinance` for screening helpers
  - Docs: [`finvizfinance` project page](https://pypi.org/project/finvizfinance/)


## Usage examples
Below are representative tasks using the public APIs in this repo.

- **1) Import daily OHLCV from Polygon and add daily technicals**
```python
from market_data.price_data_import import api_import
from market_data.add_technicals import run_pipeline

symbols = ["AAPL", "MSFT"]
data = api_import(symbols, from_date="2024-01-01")

# Enrich one symbol with the daily pipeline
df = data["AAPL"]
df = run_pipeline(df)
print(df.columns)  # includes SMA/EMA/ATR/RSI/MACD/VWAP-derived columns
```

- **2) Import intraday series and compute intraday technicals**
```python
from market_data.price_data_import import intraday_import
from market_data.add_technicals import run_pipeline, intraday_pipeline

idata = intraday_import(["AAPL"],
                        from_date="2024-06-01",
                        to_date="2024-06-07",
                        timespan="second",
                        multiplier=10,
                        resample=False)

df_intraday = idata["AAPL"]
df_intraday = run_pipeline(df_intraday, steps=intraday_pipeline)
```

- **3) Price-to-fundamental time series (market cap to metric)**
```python
from market_data.fundamentals import price_to_fundamental, revenue_growth

series = price_to_fundamental(
    symbol="AAPL",
    fundamental=revenue_growth,   # uses Polygon financials under the hood
    timeframe="quarterly",
    limit=16,
    plot=False
)
print(series.tail())
```

- **4) SEC float shares time series**
```python
from market_data.fundamentals import float_time_series

float_df = float_time_series("AAPL")
print(float_df.tail())
```

- **5) Seeking Alpha ratings (RapidAPI)**
- Quant ratings are an aggregated score for stock fundamental and technical performance compared to peers. Seeking Alpha provides details here: https://seekingalpha.com/article/4263303-quant-ratings-and-factor-grades-faq
```python
import market_data.seeking_alpha as sa

# Example: get quant ratings dictionary for tickers (fetches if missing)
ratings = sa.quant_rating(["AAPL", "MSFT"])  # {"AAPL": 4.56, ...}
print(ratings)
```
Note: The `seeking_alpha` module loads local caches on import in its current state. See Caveats to run it without Windows paths.

- **6) Simple backtest with the `backtesting` library**
```python
from market_data.backtesting_module import run_backtest, SmaCross
from market_data.price_data_import import api_import

data = api_import(["AAPL"], from_date="2024-01-01")
df = data["AAPL"]

backtest, stats = run_backtest(df, strategy=SmaCross)
print(stats[["Return [%]", "Sharpe Ratio", "# Trades"]])
```


## Modules
- `price_data_import.py`
  - `api_import(wl, from_date, to_date, transfer=None) -> dict[str, pd.DataFrame]`: Daily aggregates via Polygon `StocksClient.get_aggregate_bars`.
  - `intraday_import(...) -> dict[str, pd.DataFrame]`: Intraday aggregates (pagination handled) with optional resampling and per‑symbol date offsets.
  - `nonconsecutive_intraday_import(...)` and `fragmented_intraday_import(...)`: Collections of per-day or per-range intraday windows.

- `add_technicals.py`
  - Composable `Step` pipeline with a comprehensive set of indicators: `SMA`, `EMA`, `ATR`, `RSI`, `MACD`, `Bollinger Bands`, `AVWAP_by_date`, rolling VWAPs, relative metrics, and grouped intraday indicators.
  - `pipeline`: Daily stack; `intraday_pipeline`: Intraday stack; `run_pipeline(df, steps=pipeline)` applies in order.

- `fundamentals.py`
  - Integrations with `sec_api.FloatApi` and Polygon financials (`RESTClient`) to compute series like revenue growth and price‑to‑fundamental ratios. Includes helper metrics (EBIT/EBITDA, EV, margins), and a `price_to_fundamental` series with peak/trough detection.

- `seeking_alpha.py`
  - RapidAPI-based wrappers to fetch meta-data, summary, ratings, earnings, transcripts, and analyst price targets. Also manages a local JSON cache strategy.

- `support_functions.py`
  - Relative strength computation against a benchmark (e.g., SPY), sector/industry index construction from watchlists, technical score helpers.

- Additional modules
  - `anchored_vwap.py`: Utilities for anchored VWAP calculations.
  - `backtesting_module.py`: Helpers for running strategy backtests using the `backtesting` library.
  - `watchlist_filters.py`: Plotting and screening utilities (uses `finvizfinance`, `plotly`, and `matplotlib`).
  - `regimes.py`, `episodic_pivots.py`, `intraday_study.py`, etc.: Exploratory studies and specialized analytics.


## Data and caching
- Several functions write/read cached JSON/CSV files. In the current codebase, some paths are Windows-specific (e.g., `E:\Market Research\...`). Update these to directories accessible in your environment.
- Recommended approach:
  - Centralize a data directory via environment variables or a config module and modify the path constants accordingly.
  - Avoid checking large data artifacts into version control.


## Concurrency and rate limits
- **Rate Limits**: Requests to Polygon are wrapped by `ratelimit.limits` and `sleep_and_retry` with defaults (`CALLS = 75`, `PERIOD = 1`). Adjust carefully to your plan limits.
- **Concurrency**: Functions utilize `ThreadPoolExecutor` with adaptive `max_workers` based on input size to parallelize per-symbol retrieval/processing.


## Troubleshooting & caveats
- **Seeking Alpha module side effects**: `seeking_alpha.py` calls `load_all()` at import and expects Windows-only cache paths. On a fresh environment:
  - Either create equivalent directories/files, or
  - Edit `seeking_alpha.py` to disable the eager load and to use configurable, cross‑platform cache paths.
- **Package layout**: Imports assume a package named `market_data`. Ensure this repository directory is importable as `market_data` or install it in editable mode.
- **Database connectivity**: A draft `db_import` helper shows how to connect to MySQL via SQLAlchemy; you will need valid credentials, an engine driver (`mysql-connector-python`), and existing tables.
- **API keys**: Ensure `market_data/api_keys.py` exists and contains valid keys. Invalid or missing keys will lead to 401s or empty responses.
- **Timezones**: Intraday processing localizes to `UTC` then converts to `US/Eastern`. Adjust if your workflow requires different handling.
- **Pandas version**: Some code uses Pandas 2+ idioms (and adapts for 3.0 behaviors in comments). If you hit compatibility issues, upgrade Pandas.


## Acknowledgments
- Polygon.io for market data APIs.
- SEC API for float and filings data.
- Seeking Alpha via RapidAPI for analytics endpoints.
- Yahoo Finance (via `yfinance`) and Finviz tooling.

If you have questions or need help adapting paths for your environment, consider opening an issue with details about your OS, Python version, and the exact error/traceback.
