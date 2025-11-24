from market_data.decorators import retry_on_read_timeout, retry_on_missing_results
from functools import wraps
from market_data.api_keys import polygon_api_key
from market_data import pd, datetime, tqdm, limits, sleep_and_retry, requests, time, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
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



#TODO Update this code.
def db_import(self, wl):
        # Define the database connection parameters
        user = 'User1'
        password = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
        host = 'localhost'
        database = 'historic_price_data_processed'

        # Create a connection string
        connection_string = f'mysql+mysqlconnector://{user}:{password}@{host}/{database}'

        # Create an SQLAlchemy engine
        engine = create_engine(connection_string)
        # for sym in self.symbols:
        #     try:
        #         table_name = sym
        #         df = pd.read_sql_table(table_name, engine)
        #         # df.drop('id', axis=1, inplace=True)
        #         # df.set_index('date', drop=True, inplace=True)
        #         self.saved_dict[sym] = df
        #     except Exception as e:
        #         print(sym , e, sep=': ')
        # Function to read a single table
        def read_table(sym, engine):
            try:
                sql_kws = pd.read_csv(r"E:\Programming\SQL\keywords", usecols=[1], names=['keywords']).keywords.to_list()
                if sym in sql_kws:
                    sym += '_'
                table_name = sym
                df = pd.read_sql_table(table_name, engine)
                query = f"SELECT * FROM {sym}"
                df = pd.read_sql_query(query, engine)
                # df.drop('id', axis=1, inplace=True)
                # df.set_index('date', drop=True, inplace=True)
                return sym, df
            except Exception as e:
                print(sym, e, sep=': ')
                return sym, None
        # Use ThreadPoolExecutor to read tables concurrently
        with ThreadPoolExecutor(max_workers=20) as executor:
            future_to_sym = {executor.submit(read_table, sym, engine): sym for sym in wl}
            for future in as_completed(future_to_sym):
                sym = future_to_sym[future]
                try:
                    sym, df = future.result()
                    if df is not None:
                        self.saved_dict[sym] = df
                except Exception as e:
                    print(sym, e, sep=': ')

