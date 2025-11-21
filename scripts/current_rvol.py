if __name__ == "__main__":
    VERBOSE = True
    
    #TODO This needs an error log.
    
    import pickle
    import gzip
    with gzip.open(r"E:\Market Research\Dataset\daily_after_close_study\interest_list_long.pkl.gz", 'rb') as f:
        interest_list_long = pickle.load(f)

    import sys
    sys.path.insert(0, r"C:\Users\jdejo\Market_Data_Processing")
    import market_data.stats_objects as so
    import pandas as pd
    interest_list_rvol = so.intraday_rvol(interest_list_long, 
                                          market_open_only=False,
                                          timespan='minute',
                                          multiplier=1)

    avg_vol_df = pd.DataFrame()
    for sym in interest_list_rvol:
        _avg_hist_vol = interest_list_rvol[sym]['avg_historical_volume']
        _avg_hist_vol.name = sym
        avg_vol_df = pd.concat([avg_vol_df, _avg_hist_vol], axis=1)
    full_idx = pd.date_range(
        start=avg_vol_df.index.min().floor("min"),
        end=avg_vol_df.index.max().ceil("min"),
        freq="1min",
    )

    # Reindex to full grid, then forward‑fill values into gaps
    avg_vol_df = avg_vol_df.reindex(full_idx).ffill()    
    avg_vol_df.index = avg_vol_df.index.map(lambda x: x.time())


    from polygon.websocket.models import WebSocketMessage, Feed, Market
    from typing import List
    from polygon.websocket import WebSocketClient
    import datetime
    import pytz
    from pandas import isna
    import redis
    import json
    import time
    from redis.exceptions import BusyLoadingError, ConnectionError
    from market_data.api_keys import polygon_api_key

    r = redis.Redis(host="localhost", port=6379, decode_responses=True, 
                    socket_keepalive=True, retry_on_timeout=True)
    
    # Wait until Redis is done loading
    while True:
        try:
            r.ping()
            break
        except (BusyLoadingError, ConnectionError):
            if VERBOSE:
                print("Redis not ready yet, waiting...")
            time.sleep(1.0)

    client = WebSocketClient(
        api_key=polygon_api_key,
        feed=Feed.Delayed,
        market=Market.Stocks
        )
    _symbols = 'AM.' + ', AM.'.join(interest_list_long)
    client.subscribe(_symbols)
    def handle_msg(msgs: List[WebSocketMessage]):
        for m in msgs:
            ts = m.start_timestamp / 1000
            dt_utc = datetime.datetime.fromtimestamp(ts, pytz.UTC)
            dt_eastern = dt_utc.astimezone(pytz.timezone("US/Eastern"))
            dt_eastern_naive = dt_eastern.replace(tzinfo=None)
            symbol = m.symbol
            
            #If this symbol has no RVol history, skip it
            if symbol not in avg_vol_df.columns:
                continue
            
            #Use the last available bar at or before this time
            series = avg_vol_df[symbol]
            
            series_up_to_now = series.loc[:dt_eastern_naive.time()]
            #* series_up_to_now = series[series.index <= dt_eastern_naive]
            if series_up_to_now.empty:
                continue

            denom = series_up_to_now.iloc[-1]
            
            #Skip if denom is NaN or zero
            if isna(denom) or denom == 0:
                continue
            
            rvol = m.accumulated_volume / denom
            
            #Skip if rvol is NaN
            if isna(rvol):
                continue
                       
            if VERBOSE:
                print(symbol, dt_eastern, rvol, m.accumulated_volume, denom)
            r.hset('current_rvol', mapping={symbol:json.dumps([dt_eastern_naive.time().strftime("%H:%M:%S"), rvol], default=str)})
    client.run(handle_msg)