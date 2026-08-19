import argparse
import gzip
import pickle
import threading

from tqdm import tqdm


BASE = r"E:\Market Research\Dataset\daily_after_close_study"

AVAILABLE_OBJECTS = (
    "symbols",
    "sec",
    "ind",
    "sp500",
    "mdy",
    "iwm",
    "etfs",
    "stock_stats",
    "ev",
    "all_returns",
    "sector_close_vwap_ratio",
    "industry_close_vwap_ratio",
    "episodic_pivots",
    "ep_curdur",
    "ep_rr",
    "rel_stren",
    "prev_perf_since_earnings",
    "perf_since_earnings",
    "days_elevated_rvol",
    "days_range_expansion",
    "results_finvizsearch",
    "tsc",
    "tsc_sec",
    "tsc_ind",
    "qplus1",
    "qplus4",
    "interest_list_long",
)

def _load_one(name: str, _input: dict):
    """Worker: load one object and store it in the shared dict."""
    path = fr"{BASE}\{name}.pkl.gz"
    with gzip.open(path, "rb") as f:          # rb + pickle.load for reading
        _input[name] = pickle.load(f)

def load_all(names=None):
    """Load the requested saved objects, or all objects when names is omitted."""
    names = list(AVAILABLE_OBJECTS if names is None else names)
    invalid_names = sorted(set(names) - set(AVAILABLE_OBJECTS))
    if invalid_names:
        raise ValueError(f"Unknown object name(s): {', '.join(invalid_names)}")

    loaded = {}
    threads = []

    for name in names:
        t = threading.Thread(target=_load_one, args=(name, loaded), daemon=False)
        t.start()
        threads.append(t)

    for t in tqdm(threads, desc='Loading Variables'):
        t.join()

    return loaded

if __name__ == "__main__":  
    parser = argparse.ArgumentParser(
        description="Load all daily variables, or only explicitly selected objects."
    )
    parser.add_argument(
        "--objects",
        nargs="+",
        choices=AVAILABLE_OBJECTS,
        metavar="NAME",
        help="one or more objects to load (default: load all objects)",
    )
    args = parser.parse_args()

    try:
        from market_data.Symbol_Data import SymbolData
    except ModuleNotFoundError:
        import sys
        sys.path.insert(0, r"C:\Users\jdejo\Market_Data_Processing")
        from market_data.Symbol_Data import SymbolData
    from market_data.price_data_import import *
    from market_data.add_technicals import *
    from market_data.add_technicals import _add_technicals_worker
    from market_data.watchlists_locations import make_watchlist, hadv, sp500, iwm, mdy, etfs
    from market_data.watchlist_filters import Technical_Score_Calculator
    import market_data.watchlist_filters as wf
    import market_data.watchlists_locations as wl
    import market_data.seeking_alpha as sa
    import market_data.regimes as rg
    import market_data.support_functions as sf
    import market_data.fundamentals as fu
    import market_data.stats_objects as so
    import market_data.anchored_vwap as av
    from market_data.episodic_pivots import Episodic_Pivots
    from market_data import operator, np, ProcessPoolExecutor, as_completed
    from market_data.stats_objects import IntradaySignalProcessing as isp
    from market_data import create_engine, text, DateTime, pymysql, redis, json
    from market_data.api_keys import database_password, seeking_alpha_api_key
    from market_data.interest_list import InterestList as il
    
    loaded = load_all(args.objects)


    # Unpack only the objects that were loaded. Keep the existing short name for
    # episodic_pivots; all other variable names match their saved object names.
    for object_name, value in loaded.items():
        variable_name = "ep" if object_name == "episodic_pivots" else object_name
        globals()[variable_name] = value

    if "episodic_pivots" in loaded and "symbols" in loaded:
        loaded["episodic_pivots"].symbols = loaded["symbols"]
    
    url = f"mysql+pymysql://root:{database_password}@127.0.0.1:3306/stocks"
    engine = create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})
    daily_quant_rating_df = pd.read_sql("SELECT * FROM daily_quant_rating", con=engine)
    daily_quant_rating_df.set_index('index', inplace=True)
    daily_quant_rating_df.index.name = 'Symbol'
    daily_quant_rating_df['diff'] = daily_quant_rating_df[daily_quant_rating_df.columns[-1]] - daily_quant_rating_df[daily_quant_rating_df.columns[-2]]
    