if __name__ == "__main__":
    import warnings
    # Suppress all warnings
    warnings.filterwarnings('ignore')
    from market_data.Symbol_Data import SymbolData
    from market_data.price_data_import import *
    from market_data.add_technicals import *
    from market_data.watchlists_locations import make_watchlist, hadv
    from market_data.watchlist_filters import Technical_Score_Calculator
    import market_data.watchlist_filters as wf
    import market_data.watchlists_locations as wl
    import market_data.seeking_alpha as sa
    
    #hadv == high average dollar volume
    hadv = make_watchlist(hadv)
    symbols = {k: SymbolData(k, v) for k,v in api_import(hadv[:500]).items()}
    for symbol in tqdm(symbols, desc='Adding technicals'):        
        run_pipeline(symbols[symbol].df)
    wf.run_all(symbols)
    
    