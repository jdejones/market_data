if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    try:
        from market_data.Symbol_Data import Intraday_SymbolData
    except ModuleNotFoundError:
        import sys
        sys.path.insert(0, r"C:\Users\jdejo\Market_Data_Processing")
        from market_data.Symbol_Data import Intraday_SymbolData
    from market_data.price_data_import import intraday_import
    from market_data.watchlists_locations import make_watchlist, hadv, episodic_pivots
    from market_data.add_technicals import *
    from market_data.add_technicals import _add_intraday_technicals_worker
    from market_data import ProcessPoolExecutor, as_completed, tqdm, json, pd
    
    #*Import price data.
    ep_list = make_watchlist(episodic_pivots)
    offset_dates = json.load(open(r"E:\Market Research\Dataset\current_ep_start_end_dates.txt"))
    ep_list = intraday_import(ep_list, offset_dates=offset_dates)
    ep_symbols = {k: Intraday_SymbolData(k, v) for k,v in ep_list.items()}

    def resampler(df: pd.DataFrame, resample: str = '1min') -> pd.DataFrame:
        return df.resample(resample).agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum', 'VWAP': 'mean'})

    #*Add technicals.
    num_workers = 8
    chunksize = 7
    items = [(sd.symbol, sd.df) for sd in ep_symbols.values()]
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        for symbol, df in tqdm(
            executor.map(_add_intraday_technicals_worker, items, chunksize=chunksize),
            total=len(items),
            desc="Adding technicals"
        ):
            ep_symbols[symbol].df = df


