from market_data.decorators import retry_on_read_timeout, retry_on_missing_results
from functools import wraps
from market_data.api_keys import polygon_api_key
from market_data import pd, datetime, tqdm, limits, sleep_and_retry, requests, time, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import bindparam, create_engine, text
from polygon.rest import RESTClient

# at most 75 calls per 1 second window
CALLS = 75
PERIOD = 1

@retry_on_read_timeout(max_retries=3)
def api_import(
    wl: list[str], 
    from_date: int|datetime.datetime = 0, 
    to_date: datetime = datetime.datetime.today(), 
    transfer: dict[str, pd.DataFrame] = None
    ) -> dict[str, pd.DataFrame]:
    client = RESTClient(api_key=polygon_api_key)
    if transfer == None:
        data_dict = {}
    else:
        data_dict = {k:v for k,v in transfer.items() if k in wl}
        wl = [sym for sym in wl if sym not in transfer]

    @sleep_and_retry
    @limits(calls=CALLS, period=PERIOD)
    def _fetch_ohlcv(sym: str):
        """Function to make api requests with rate limits."""
        aggs = client.list_aggs(ticker=sym, multiplier=1, timespan='day', from_=from_date, to=to_date)
        # Convert Agg objects to dictionaries for compatibility
        return [{'o': agg.open, 'h': agg.high, 'l': agg.low, 'c': agg.close,
                 'v': agg.volume, 'vw': agg.vwap, 't': agg.timestamp, 'n': agg.transactions}
                for agg in aggs]

    def _process(sym: str):
        """Fetch + turn into a DataFrame."""
        price_data = _fetch_ohlcv(sym)
        df = pd.DataFrame(price_data)
        # …your existing timestamp + rename logic here…
        df['t'] = pd.to_datetime(df['t'], unit='ms')
        df.index = (df['t']
                    .dt.tz_localize('UTC')
                    .dt.tz_convert('US/Eastern')
                    .map(lambda x: x.strftime('%Y-%m-%d')))
        df.index = pd.to_datetime(df.index)
        df.index.name = 'Date'
        df = df.rename(columns={
            'v': 'Volume',
            'vw': 'VWAP',
            'o': 'Open',
            'c': 'Close',
            'h': 'High',
            'l': 'Low',
        }).drop(['t','n'], axis=1)
        return sym, df
    
    
    max_workers = min(20, len(wl))   #* Can be tuned.
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = {exe.submit(_process, sym): sym for sym in wl}
        for future in tqdm(as_completed(futures),
                           total=len(futures),
                           desc='Importing Price Data'):
            sym = futures[future]
            try:
                sym, df = future.result()
                data_dict[sym] = df
            except Exception as e:
                print(f"{sym}: {e}")

    return data_dict


@retry_on_read_timeout(max_retries=3)
def intraday_import(wl: list[str], 
                    from_date: int|str|datetime.datetime = 0, 
                    to_date: int|str|datetime.datetime = datetime.datetime.now(),
                    resample: str = False,
                    timespan: str = 'second',
                    multiplier: int = 10,
                    limit: int = 50000,
                    offset_dates: dict[str, list[int|str|datetime.datetime,int|str|datetime.datetime]] = None,
                    market_open_only: bool = True) -> dict[str, pd.DataFrame]:
    """Offset dates is used if all symbols don't start/end with the same date.

    Args:
        wl (list[str]): _description_
        from_date (int | str | datetime.datetime, optional): _description_. Defaults to 0.
        to_date (int | str | datetime.datetime, optional): _description_. Defaults to datetime.datetime.now().
        resample (str, optional): _description_. Defaults to False.
        timespan (str, optional): _description_. Defaults to 'second'.
        multiplier (int, optional): _description_. Defaults to 10.
        limit (int, optional): _description_. Defaults to 50000.
        offset_dates (dict[str, list[int | str | datetime.datetime,int | str | datetime.datetime]], optional): _description_. Defaults to None.

    Returns:
        dict[str, pd.DataFrame]: _description_
    """
    client = RESTClient(api_key=polygon_api_key)
    data_dict = {}
    
    #! _fetch_ohlcv is returning KeyError('results'). It is unclear why.
    #! I am able to execute the function line by line.
    #! I have tried the following two decorators and they have been uneffective.
    #! I tried adding time.sleep(1) in case I was being throttled, but that was uneffective.
    #TODO The next step may be to add erroneous symbols to a container,
    #TODO iterate through the container noncurrently, and update 
    #TODO data_dict with the results.
    # @retry_on_missing_results(max_retries=3, backoff=1.0)
    # @retry_on_empty_or_missing_results(max_retries=3, backoff=1.0)
    @sleep_and_retry
    @limits(calls=CALLS, period=PERIOD)
    def _fetch_ohlcv(sym: str, offset_dates: list[int|str|datetime.datetime,int|str|datetime.datetime] = None):
        if offset_dates:
            aggs = client.list_aggs(ticker=sym, multiplier=multiplier, timespan=timespan,
                                    from_=offset_dates[0], to=offset_dates[1], limit=limit)
        else:
            aggs = client.list_aggs(ticker=sym, multiplier=multiplier, timespan=timespan,
                                    from_=from_date, to=to_date, limit=limit)
        # Convert Agg objects to dictionaries for compatibility
        return [{'o': agg.open, 'h': agg.high, 'l': agg.low, 'c': agg.close,
                 'v': agg.volume, 'vw': agg.vwap, 't': agg.timestamp, 'n': agg.transactions}
                for agg in aggs]
    
    def _process(sym: str, offset_dates: list[int|str|datetime.datetime,int|str|datetime.datetime] = None):
        """Fetch + turn into a DataFrame."""
        price_data = _fetch_ohlcv(sym, offset_dates=offset_dates)
        df = pd.DataFrame(price_data)
        
        df['t'] = pd.to_datetime(df['t'], unit='ms')
        df.index = (df['t']
                    .dt.tz_localize('UTC')
                    .dt.tz_convert('US/Eastern')
                    .map(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')))
        df.index = pd.to_datetime(df.index)
        df.index.name = 'Timestamp'
        df = df.rename(columns={
            'v': 'Volume',
            'vw': 'VWAP',
            'o': 'Open',
            'c': 'Close',
            'h': 'High',
            'l': 'Low',
        }).drop(['t','n'], axis=1)
        if market_open_only:
            df = df.between_time('09:30:00', '15:59:50')
        if resample:
            df = df.resample(resample).agg({'Volume': 'sum', 'VWAP': 'mean', 
                                            'Open': 'first', 'Close': 'last', 
                                            'High': 'max', 'Low': 'min'})
        return sym, df

    if offset_dates:
        max_workers = min(20, len(wl))   #* Can be tuned.
        with ThreadPoolExecutor(max_workers=max_workers) as exe:
            futures = {exe.submit(_process, sym, offset_dates=offset_dates[sym]): sym for sym in wl}
            for future in tqdm(as_completed(futures),
                            total=len(futures),
                            desc='Importing Price Data'):
                sym = futures[future]
                try:
                    sym, df = future.result()
                    data_dict[sym] = df
                except Exception as e:
                    print(f"{sym}: {e}")        
    else:
        max_workers = min(20, len(wl))   #* Can be tuned.
        with ThreadPoolExecutor(max_workers=max_workers) as exe:
            futures = {exe.submit(_process, sym): sym for sym in wl}
            for future in tqdm(as_completed(futures),
                            total=len(futures),
                            desc='Importing Price Data'):
                sym = futures[future]
                try:
                    sym, df = future.result()
                    data_dict[sym] = df
                except Exception as e:
                    print(f"{sym}: {e}")

    return data_dict

@retry_on_read_timeout(max_retries=3)
def nonconsecutive_intraday_import(dates_dict: dict[str, list[datetime.date]],
                                    resample: str = False,
                                    timespan: str = 'second',
                                    multiplier: int = 10,
                                    limit: int = 50000) -> dict[str, list[pd.DataFrame]]:
    data_dict = {sym: [] for sym in dates_dict}
    client = RESTClient(api_key=polygon_api_key)

    @sleep_and_retry
    @limits(calls=CALLS, period=PERIOD)
    def _fetch_ohlcv(sym: str, from_date: datetime.date, to_date: datetime.date):
        aggs = client.list_aggs(ticker=sym, multiplier=multiplier, timespan=timespan,
                                from_=from_date, to=to_date, limit=limit)
        # Convert Agg objects to dictionaries for compatibility
        return [{'o': agg.open, 'h': agg.high, 'l': agg.low, 'c': agg.close,
                 'v': agg.volume, 'vw': agg.vwap, 't': agg.timestamp, 'n': agg.transactions}
                for agg in aggs]
    
    def _process(sym: str, from_date: datetime.date, to_date: datetime.date):   
        price_data = _fetch_ohlcv(sym, from_date, to_date)
        df = pd.DataFrame(price_data)
        df['t'] = pd.to_datetime(df['t'], unit='ms')
        df.index = (df['t']
                    .dt.tz_localize('UTC')
                    .dt.tz_convert('US/Eastern')
                    .map(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')))
        df.index = pd.to_datetime(df.index)
        df.index.name = 'Timestamp'
        df = df.rename(columns={
            'v': 'Volume',
            'vw': 'VWAP',
            'o': 'Open',
            'c': 'Close',
            'h': 'High',
            'l': 'Low',
        }).drop(['t','n'], axis=1)
        df = df.between_time('09:30:00', '15:59:50')
        
        if resample:
            df = df.resample(resample).agg({'Volume': 'sum', 'VWAP': 'mean', 
                                            'Open': 'first', 'Close': 'last', 
                                            'High': 'max', 'Low': 'min'})
        
        return sym, df
    
    max_workers = min(20, len([_ for sym in dates_dict for _ in dates_dict[sym]]))   #* Can be tuned.
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = {exe.submit(_process, sym, from_date=date, to_date=date): sym for sym in dates_dict for date in dates_dict[sym]}
        for future in tqdm(as_completed(futures),
                        total=len(futures),
                        desc='Importing Price Data'):
            sym = futures[future]
            try:
                sym, df = future.result()
                data_dict[sym].append(df)
            except Exception as e:
                print(f"{sym}: {e}")

    return data_dict

@retry_on_read_timeout(max_retries=3)
def fragmented_intraday_import(dates_dict: dict[str, list[Tuple[datetime.date, datetime.date]]],
                               resample: str = False,
                               timespan: str = 'second',
                               multiplier: int = 10,
                               limit: int = 50000) -> dict[str, list[pd.DataFrame]]:
    data_dict = {sym: [] for sym in dates_dict}
    client = RESTClient(api_key=polygon_api_key)

    @sleep_and_retry
    @limits(calls=CALLS, period=PERIOD)
    def _fetch_ohlcv(sym: str, from_date: datetime.date, to_date: datetime.date):
        aggs = client.list_aggs(ticker=sym, multiplier=multiplier, timespan=timespan,
                                from_=from_date, to=to_date, limit=limit)
        # Convert Agg objects to dictionaries for compatibility
        return [{'o': agg.open, 'h': agg.high, 'l': agg.low, 'c': agg.close,
                 'v': agg.volume, 'vw': agg.vwap, 't': agg.timestamp, 'n': agg.transactions}
                for agg in aggs]
    
    def _process(sym: str, from_date: datetime.date, to_date: datetime.date):
        price_data = _fetch_ohlcv(sym, from_date, to_date)
        df = pd.DataFrame(price_data)
        df['t'] = pd.to_datetime(df['t'], unit='ms')
        df.index = (df['t']
                    .dt.tz_localize('UTC')
                    .dt.tz_convert('US/Eastern')
                    .map(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')))
        df.index = pd.to_datetime(df.index)
        df.index.name = 'Timestamp'
        df = df.rename(columns={
            'v': 'Volume',
            'vw': 'VWAP',
            'o': 'Open',
            'c': 'Close',
            'h': 'High',
            'l': 'Low',
        }).drop(['t','n'], axis=1)
        df = df.between_time('09:30:00', '15:59:50')
        
        if resample:
            df = df.resample(resample).agg({'Volume': 'sum', 'VWAP': 'mean', 
                                            'Open': 'first', 'Close': 'last', 
                                            'High': 'max', 'Low': 'min'})
                    
        return sym, df

    max_workers = min(20, len([_ for sym in dates_dict for _ in dates_dict[sym]]))   #* Can be tuned.
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = {exe.submit(_process, sym, from_date=date[0], to_date=date[1]): sym for sym in dates_dict for date in dates_dict[sym]}
        for future in tqdm(as_completed(futures),
                        total=len(futures),
                        desc='Importing Price Data'):
            sym = futures[future]
            try:
                sym, df = future.result()
                data_dict[sym].append(df)
            except Exception as e:
                print(f"{sym}: {e}")
                
    return data_dict



def _daily_ohlcv_engine(database_password=None):
    if database_password is None:
        from market_data.api_keys import database_password as database_password

    url = f"mysql+pymysql://root:{database_password}@127.0.0.1:3306/daily_ohlcv"
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


def _symbol_filter_query(base_query, symbols):
    symbols = list(dict.fromkeys(symbols))
    if not symbols:
        return text(base_query), {}
    return (
        text(f"{base_query} WHERE symbol IN :symbols").bindparams(
            bindparam("symbols", expanding=True)
        ),
        {"symbols": symbols},
    )


def daily_ohlcv_latest_dates(wl, database_password=None):
    symbols = list(dict.fromkeys(wl))
    if not symbols:
        return {}

    engine = _daily_ohlcv_engine(database_password)
    query = text(
        """
        SELECT symbol, MAX(date) AS latest_date
        FROM daily_symbol_bars
        WHERE symbol IN :symbols
        GROUP BY symbol
        """
    ).bindparams(bindparam("symbols", expanding=True))
    latest_dates = pd.read_sql_query(query, con=engine, params={"symbols": symbols})
    if latest_dates.empty:
        return {}

    latest_dates["latest_date"] = pd.to_datetime(latest_dates["latest_date"]).dt.date
    return dict(zip(latest_dates["symbol"], latest_dates["latest_date"]))


def db_import(wl, database_password=None):
    symbols = list(dict.fromkeys(wl))
    if not symbols:
        return {}

    engine = _daily_ohlcv_engine(database_password)
    bars_query = text(
        """
        SELECT *
        FROM daily_symbol_bars
        WHERE symbol IN :symbols
        ORDER BY symbol, date
        """
    ).bindparams(bindparam("symbols", expanding=True))
    avwap_query = text(
        """
        SELECT symbol, date, anchor_date, avwap, atrs_from_avwap
        FROM daily_symbol_avwap
        WHERE symbol IN :symbols
        ORDER BY symbol, date, anchor_date
        """
    ).bindparams(bindparam("symbols", expanding=True))
    params = {"symbols": symbols}
    bars = pd.read_sql_query(bars_query, con=engine, params=params)
    avwap = pd.read_sql_query(avwap_query, con=engine, params=params)

    data_dict = {}
    if bars.empty:
        return {sym: pd.DataFrame() for sym in symbols}

    bars["date"] = pd.to_datetime(bars["date"])
    if not avwap.empty:
        avwap["date"] = pd.to_datetime(avwap["date"])
        avwap["anchor_date"] = pd.to_datetime(avwap["anchor_date"]).dt.strftime("%Y-%m-%d")

    for sym in symbols:
        df = bars.loc[bars["symbol"] == sym].drop(columns=["symbol"]).copy()
        if df.empty:
            data_dict[sym] = pd.DataFrame()
            continue

        df = df.set_index("date").sort_index()
        df.index.name = "Date"

        sym_avwap = avwap.loc[avwap["symbol"] == sym] if not avwap.empty else pd.DataFrame()
        if not sym_avwap.empty:
            avwap_wide = sym_avwap.pivot(index="date", columns="anchor_date", values="avwap")
            avwap_wide = avwap_wide.rename(columns=lambda anchor: f"VWAP {anchor}")
            atrs_wide = sym_avwap.pivot(index="date", columns="anchor_date", values="atrs_from_avwap")
            atrs_wide = atrs_wide.rename(columns=lambda anchor: f"ATRs_from_AVWAP_{anchor}")
            df = df.join(avwap_wide).join(atrs_wide)

        data_dict[sym] = df

    return data_dict

