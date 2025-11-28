import sys
sys.path.insert(0, r"C:\Users\jdejo\Market_Data_Processing")

"""
Scan for symbols in the intraday-long universe that:

1. Have a strong daily ADX trend (latest ADX >= min_adx, default 30).
2. Since the most recent transition from ADX < adx_cross (default 25) to
   ADX >= adx_cross, the ADX_diff column has never decreased
   (monotonic non‑decreasing).
3. Have made a higher high today versus yesterday on daily bars
   (today's High > yesterday's High), using Polygon daily data
   via market_data.price_data_import.api_import.

The script prints the symbols that pass all of the above filters.
Paths to the pickled objects can be edited below if needed.
"""

import argparse
import datetime as dt
import gzip
import pickle
from typing import Dict, Iterable

import pandas as pd

from market_data.Symbol_Data import SymbolData
from market_data.price_data_import import api_import


# Adjust these paths if your pickles live somewhere else
SYMBOLS_PICKLE_PATH = r"E:\Market Research\Dataset\daily_after_close_study\symbols.pkl.gz"
INTEREST_LIST_LONG_PICKLE_PATH = r"E:\Market Research\Dataset\daily_after_close_study\interest_list_long.pkl.gz"


def _load_pickles() -> tuple[Dict[str, SymbolData], Iterable[str]]:
    """
    Load the pickled symbol universe and interest_list_long container.

    Returns:
        symbols: dict[str, SymbolData]
        interest_list_long: iterable of symbols (e.g. list[str] or set[str])
    """
    with gzip.open(SYMBOLS_PICKLE_PATH, "rb") as f:
        symbols: Dict[str, SymbolData] = pickle.load(f)

    with gzip.open(INTEREST_LIST_LONG_PICKLE_PATH, "rb") as f:
        interest_list_long = pickle.load(f)

    return symbols, interest_list_long


def _filter_by_interest_list_long(
    symbols: Dict[str, SymbolData],
    interest_list_long: Iterable[str],
) -> Dict[str, SymbolData]:
    """
    Keep only symbols that are in the interest_list_long universe.
    """
    interest_list_long_set = set(interest_list_long)
    return {sym: obj for sym, obj in symbols.items() if sym in interest_list_long_set}


def _filter_by_adx_trend(
    symbols: Dict[str, SymbolData],
    min_adx: float = 30.0,
    adx_cross: float = 25.0,
) -> Dict[str, SymbolData]:
    """
    Filter symbols whose:
      - Last row has ADX >= min_adx.
      - Since the most recent row where ADX crossed from < adx_cross to
        >= adx_cross, the ADX_diff column has never decreased.
    """
    kept: Dict[str, SymbolData] = {}

    for sym, obj in symbols.items():
        df = obj.df
        # Require the necessary columns
        if not {"ADX", "ADX_diff"}.issubset(df.columns):
            continue

        df_valid = df.dropna(subset=["ADX", "ADX_diff"])
        if df_valid.empty:
            continue

        # Latest ADX check
        if df_valid["ADX"].iloc[-1] < min_adx:
            continue

        adx = df_valid["ADX"]

        # Find last cross-up from < adx_cross to >= adx_cross
        cross_mask = (adx >= adx_cross) & (adx.shift(1) < adx_cross)
        if not cross_mask.any():
            continue

        start_idx = cross_mask[cross_mask].index[-1]
        window = df_valid.loc[start_idx:]
        if window.empty:
            continue

        adx_diff = window["ADX_diff"]
        # Require ADX_diff to be monotonic non‑decreasing over this window
        if (adx_diff.diff().dropna() < 0).any():
            continue

        kept[sym] = obj

    return kept


def _fetch_recent_daily_data(symbols: Iterable[str]) -> Dict[str, pd.DataFrame]:
    """
    Use api_import to get daily bars for yesterday and today.
    """
    symbols = list(symbols)
    if not symbols:
        return {}

    today = dt.date.today()
    prev_day = today - dt.timedelta(days=2)

    data_dict = api_import(
        wl=symbols,
        from_date=prev_day,
        to_date=today,
    )
    return data_dict


def _filter_higher_highs(data_dict: Dict[str, pd.DataFrame]) -> list[str]:
    """
    From the provided daily data, keep symbols where today's High
    is greater than yesterday's High.
    """
    result: list[str] = []

    for sym, df in data_dict.items():
        if df is None or df.empty:
            continue

        df_sorted = df.sort_index()
        if len(df_sorted) < 2:
            # Need at least yesterday + today
            continue

        last_two = df_sorted.iloc[-2:]
        prev_high = last_two["High"].iloc[0]
        curr_high = last_two["High"].iloc[1]

        if pd.isna(prev_high) or pd.isna(curr_high):
            continue

        if curr_high > prev_high:
            result.append(sym)

    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filter symbols in interest_list_long for strong ADX uptrends on the "
            "daily timeframe that have made a higher high today."
        )
    )
    parser.add_argument(
        "--min-adx",
        type=float,
        default=30.0,
        help="Minimum latest ADX value on the daily timeframe (default: 30).",
    )
    parser.add_argument(
        "--adx-cross",
        type=float,
        default=25.0,
        help=(
            "Threshold used to detect the most recent ADX cross from below "
            "to above this level (default: 25)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    # Load heavy objects
    symbols, interest_list_long = _load_pickles()

    # Filter for symbols that are in interest_list_long
    symbols_filtered = _filter_by_interest_list_long(symbols, interest_list_long)

    # Drop the original big container to conserve memory
    del symbols

    # Apply ADX/ADX_diff trend filters
    symbols_trending = _filter_by_adx_trend(
        symbols_filtered,
        min_adx=args.min_adx,
        adx_cross=args.adx_cross,
    )

    if not symbols_trending:
        print("No symbols passed the ADX / ADX_diff trend filters.")
        return

    # Use api_import to get yesterday + today, then check for higher highs
    daily_data = _fetch_recent_daily_data(symbols_trending.keys())
    higher_high_symbols = _filter_higher_highs(daily_data)
    if not higher_high_symbols:
        print("No symbols made a higher high today versus yesterday.")
        return

    print("Symbols with strong ADX trend and a higher high today:")
    for sym in sorted(higher_high_symbols):
        print(sym)


if __name__ == "__main__":
    main()


