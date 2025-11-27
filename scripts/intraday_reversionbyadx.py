#TODO Add Processing for less than 14 bars
#TODO Add probability of crossover.
#TODO Make compatible for starting anytime of day.


import sys
sys.path.insert(0, r"C:\Users\jdejo\Market_Data_Processing")
import market_data.stats_objects as so
from market_data.api_keys import polygon_api_key
from market_data.add_technicals import get_adx
from market_data.Symbol_Data import SymbolData
from market_data.price_data_import import intraday_import

import pickle
import gzip
with gzip.open(r"E:\Market Research\Dataset\daily_after_close_study\interest_list_long.pkl.gz", 'rb') as f:
    interest_list_long = pickle.load(f)
    
with gzip.open(r"E:\Market Research\Dataset\daily_after_close_study\symbols.pkl.gz", 'rb') as f:
    symbols = pickle.load(f)


import pandas as pd

import datetime as dt
from typing import List
from polygon.websocket.models import WebSocketMessage, Feed, Market
from polygon.websocket import WebSocketClient

import redis
import json
import time
from redis.exceptions import BusyLoadingError, ConnectionError


# Build SymbolData objects only long enough to filter the interest list,
# then drop the heavy dictionary to free memory.
# symbols = {k: SymbolData(k, v) for k, v in symbols.items() if k in interest_list_long}
interest_list_long = [
    sym
    for sym in interest_list_long
    if (symbols[sym].df.Relative_ATR.iloc[-1] > 4)
    and ("Technical" in symbols[sym].interest_factor)
]
del symbols


API_KEY = polygon_api_key
LOOKBACK = 5  # number of 3‑minute bars required for ADX
RESAMPLE_RULE = "3min"

# Dictionary of per‑symbol 1‑minute bars (used as the raw source)
symbol_minute_data: dict[str, pd.DataFrame] = {}

# Dictionary of per‑symbol 3‑minute bars (this is what we run indicators on)
symbol_3min_data: dict[str, pd.DataFrame] = {}

# Redis client for publishing qualifying symbols (compatible with Docker setup)
r = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True,
    socket_keepalive=True,
    retry_on_timeout=True,
)

# Wait until Redis is done loading (same pattern as current_rvol.py)
while True:
    try:
        r.ping()
        break
    except (BusyLoadingError, ConnectionError):
        time.sleep(1.0)

def _backfill_symbol(symbol: str, first_ts: pd.Timestamp) -> None:
    """
    On the first websocket tick for a symbol, backfill intraday 1‑minute data
    from today's 9:30 Eastern up to the time of the first tick using Polygon REST.
    The backfilled data is stored in symbol_minute_data[symbol].
    """
    # Ensure we have a timezone‑aware Eastern timestamp
    if first_ts.tzinfo is None:
        first_ts_eastern = first_ts.tz_localize("US/Eastern")
    else:
        first_ts_eastern = first_ts.tz_convert("US/Eastern")

    day = first_ts_eastern.date()
    start_dt = dt.datetime.combine(day, dt.time(9, 30))
    start_ts_eastern = pd.Timestamp(start_dt, tz="US/Eastern")

    # If for some reason first tick is before or at 9:30, nothing to backfill
    if first_ts_eastern <= start_ts_eastern:
        return

    # Use existing concurrent intraday_import helper to fetch 1‑minute data
    # from 9:30 Eastern up to the time of the first tick.
    from_date = start_ts_eastern.tz_convert("UTC").to_pydatetime()
    to_date = first_ts_eastern.tz_convert("UTC").to_pydatetime()

    try:
        data_dict = intraday_import(
            wl=[symbol],
            from_date=from_date,
            to_date=to_date,
            resample=False,
            timespan="minute",
            multiplier=1,
            market_open_only=True,
        )
    except Exception as e:
        print(f"Backfill intraday_import error for {symbol}: {e}")
        return

    df = data_dict.get(symbol)
    if df is None or df.empty:
        return

    # Ensure chronological order
    df = df.sort_index()

    # Store as initial 1‑minute history; _update_symbol_data will handle
    # de‑duplication when live websocket bars arrive.
    symbol_minute_data[symbol] = df


def _update_symbol_data(symbol: str, ts: pd.Timestamp, open_, high, low, close, volume):
    """
    Update the 1‑minute and 3‑minute dataframes for a given symbol.
    """
    # One‑row DataFrame for this new 1‑minute bar
    row = pd.DataFrame(
        {"Open": [open_], "High": [high], "Low": [low], "Close": [close], "Volume": [volume]},
        index=[ts],
    )

    # Append/update 1‑minute data
    if symbol in symbol_minute_data:
        df_1m = pd.concat([symbol_minute_data[symbol], row])
        df_1m = df_1m[~df_1m.index.duplicated(keep="last")].sort_index()
    else:
        df_1m = row

    symbol_minute_data[symbol] = df_1m

    # Resample to 3‑minute candles
    df_3m = (
        df_1m.resample(RESAMPLE_RULE)
        .agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }
        )
        .dropna(subset=["Open", "High", "Low", "Close"])
    )
    symbol_3min_data[symbol] = df_3m


def _compute_vwap(df: pd.DataFrame) -> pd.Series:
    """
    Compute running VWAP over the dataframe and return the VWAP series.
    """
    pv = df["Close"] * df["Volume"]
    cum_pv = pv.cumsum()
    cum_vol = df["Volume"].cumsum()
    vwap = cum_pv / cum_vol
    df["VWAP"] = vwap
    return vwap


def handle_msg(msgs: List[WebSocketMessage]):
    """
    Websocket callback. Expects Polygon aggregate‑minute ('AM') messages.
    For each symbol, maintain 3‑minute candles and print the symbol when:
      Close < VWAP, ADX > 30, and +DI < -DI.
    """
    for m in msgs:
        # Adjust attribute names here if your Polygon model differs.
        event_type = getattr(m, "event_type", None)
        if event_type not in ("AM", "A"):  # aggregate‑minute events
            continue
        symbol = getattr(m, "symbol", None) or getattr(m, "ticker", None)
        if symbol not in interest_list_long:
            continue
        # Timestamps are typically in nanoseconds; adjust if needed.
        ts_raw = getattr(m, "start_timestamp", None) or getattr(m, "sip_timestamp", None)
        if ts_raw is None:
            continue
        # Convert to Eastern time, then drop timezone so all indices are tz‑naive
        ts = (
            pd.to_datetime(ts_raw, unit="ms", utc=True)
            .tz_convert("US/Eastern")
            .tz_localize(None)
        )
        open_ = getattr(m, "open", None)
        high = getattr(m, "high", None)
        low = getattr(m, "low", None)
        close = getattr(m, "close", None)
        volume = getattr(m, "volume", None) or getattr(m, "v", None)

        if None in (open_, high, low, close, volume):
            continue

        # On the first tick for this symbol, backfill today's intraday data
        # from 9:30 Eastern up to this tick using the REST API.
        if symbol not in symbol_minute_data:
            _backfill_symbol(symbol, ts)

        _update_symbol_data(symbol, ts, open_, high, low, close, volume)

        df_3m = symbol_3min_data[symbol]
        if len(df_3m) < LOOKBACK:#Maybe add a loading data statement?
            print(f"Less than {LOOKBACK} bars for {symbol}")
            # Need at least 14 three‑minute bars
            continue
        # Compute VWAP and ADX indicators
        _compute_vwap(df_3m)
        get_adx(df_3m, high="High", low="Low", close="Close", lookback=LOOKBACK)

        latest = df_3m.iloc[-1]
        close_val = latest["Close"]
        vwap_val = latest["VWAP"]
        adx_val = latest["ADX"]
        plus_di = latest["+DI"]
        minus_di = latest["-DI"]

        if (close_val < vwap_val) and (adx_val > 30) and (plus_di < minus_di):
            ts_str = df_3m.index[-1].strftime("%Y-%m-%d %H:%M:%S")
            payload = [
                ts_str,
                float(close_val),
                float(vwap_val),
                float(adx_val),
                float(plus_di),
                float(minus_di),
            ]
            r.hset("intraday_reversionbyadx", mapping={symbol: json.dumps(payload, default=str)})

def main():
    # Subscribe to per‑minute aggregate bars for each symbol in the filtered interest list.
    # subscriptions = [f"AM.{sym}" for sym in interest_list_long]
    _symbols = 'AM.' + ', AM.'.join(interest_list_long)
    ws = WebSocketClient(
        api_key=API_KEY,
        feed=Feed.Delayed,
        market=Market.Stocks,
    )
    ws.subscribe(_symbols)
    ws.run(handle_msg=handle_msg)


if __name__ == "__main__":
    main()