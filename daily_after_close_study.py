if __name__ == "__main__":
    import warnings
    # Suppress all warnings
    warnings.filterwarnings('ignore')
    #I added the try/except block as a quick fix to avoid conflicts with the
    #interactive interpreter and the github repo. There should be a
    #cleaner way to do this.
    try:
        from market_data.Symbol_Data import SymbolData
    except ModuleNotFoundError:
        import sys
        sys.path.insert(0, r"C:\Users\jdejo\Market_Data_Processing")
        from market_data.Symbol_Data import SymbolData
    from market_data.price_data_import import *
    from market_data.add_technicals import *
    from market_data.watchlists_locations import make_watchlist, hadv
    from market_data.watchlist_filters import Technical_Score_Calculator
    import market_data.watchlist_filters as wf
    import market_data.watchlists_locations as wl
    import market_data.seeking_alpha as sa
    import market_data.regimes as rg
    
    #hadv == high average dollar volume
    hadv = make_watchlist(hadv)
    symbols = {k: SymbolData(k, v) for k,v in api_import(hadv[:50]).items()}
    for symbol in tqdm(symbols, desc='Adding technicals'):        
        run_pipeline(symbols[symbol].df)
    wf.run_all(symbols)
    
    