if __name__ == "__main__":
    # """
    # This is needed to circumvent the import error when running the functions from the interactive interpreter.
    # Before running the objects defined below included the names and signals, such as 'ep', can be changed to
    # ones of current interest.
    
    # To run execute %run -i "stats_objects_script.py"
    # """
    
    
    # import warnings
    # # Suppress all warnings
    # warnings.filterwarnings('ignore')
    # #I added the try/except block as a quick fix to avoid conflicts with the
    # #interactive interpreter and the github repo. There should be a
    # #cleaner way to do this.
    # try:
    #     from market_data.Symbol_Data import SymbolData
    # except ModuleNotFoundError:
    #     import sys
    #     sys.path.insert(0, r"C:\Users\jdejo\Market_Data_Processing")
    #     from market_data.Symbol_Data import SymbolData
    # from market_data.price_data_import import *
    # from market_data.add_technicals import *
    # from market_data.add_technicals import _add_technicals_worker
    # from market_data.watchlists_locations import make_watchlist, hadv, sp500, iwm, mdy, etfs
    # from market_data.watchlist_filters import Technical_Score_Calculator
    # import market_data.watchlist_filters as wf
    # import market_data.watchlists_locations as wl
    # import market_data.seeking_alpha as sa
    # import market_data.regimes as rg
    # import market_data.support_functions as sf
    # import market_data.fundamentals as fu
    # from market_data.episodic_pivots import Episodic_Pivots
    # from market_data import operator, np, ProcessPoolExecutor, as_completed, pickle
    # from market_data.stats_objects import IntradaySignalProcessing as isp
    
    # #*Import price data.
    # #hadv == high average dollar volume
    # hadv = make_watchlist(hadv)
    # data = api_import(hadv)
    # symbols = {k: SymbolData(k, v) for k,v in data.items()}
    
    # #*Add technicals.
    # items = [(sd.symbol, sd.df) for sd in symbols.values()]
    # # Tune the number of workers and chunk size
    # num_workers = 8  # adjust as needed
    # chunksize = 7    # adjust chunk size as needed
    # with ProcessPoolExecutor(max_workers=num_workers) as executor:
    #     for symbol, df in tqdm(
    #         executor.map(_add_technicals_worker, items, chunksize=chunksize),
    #         total=len(items),
    #         desc="Adding technicals"
    #     ):
    #         # re‐attach the processed DataFrame back to SymbolData
    #         symbols[symbol].df = df
    
    # #TODO Add concurrency for regimes               
    # r = rg.Regimes(symbols)
    # r.run_all_combos()
    # wf.run_all(symbols)
    # ep = Episodic_Pivots(symbols)
    # ep.load_all()  
    
    
    
    
    from stats_objects import IntradaySignalProcessing as isp
    from concurrent.futures import ProcessPoolExecutor

    ep_intraday = isp(symbols=symbols, interday_signals={sym: symbols[sym].df['ep'][-100:] for sym in symbols}, consecutive_signals=True)
    ep_intraday.identify_signal_dates()
    ep_intraday.import_intraday_data()
    ep_intraday.add_intraday_technicals()
    ep_intraday.condition_statistics(conditions={'below_vwap2std_ema59cross': [['Close', 'vwap_lband_2std_byday', '<'], ['ema5', 'ema9', '>']]})#conditions={'below_vwap2std_ema59cross': [['Close', 'vwap_lband_2std_byday', '<'], ['ema5', 'ema9', '>']]}
    ep_intraday.measure_intraday_returns()

