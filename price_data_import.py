from market_data.decorators import retry_on_read_timeout, retry_on_missing_results
from functools import wraps
from market_data.api_keys import polygon_api_key
from market_data import pd, datetime, tqdm, limits, sleep_and_retry, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from polygon.stocks import StocksClient

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
    client = StocksClient(api_key=polygon_api_key)
    if transfer == None:
        data_dict = {}
    else:
        data_dict = {k:v for k,v in transfer.items() if k in wl}
        wl = [sym for sym in wl if sym not in transfer]

    @sleep_and_retry
    @limits(calls=CALLS, period=PERIOD)
    def _fetch_ohlcv(sym: str):
        """Function to make api requests with rate limits."""
        raw = client.get_aggregate_bars(symbol=sym, from_date=from_date, to_date=to_date)
        return raw['results']

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
def intraday_import(wl: list[str], from_date: int|str|datetime.datetime = 0, 
                    to_date: int|str|datetime.datetime = datetime.datetime.now(),
                    resample: str = False,
                    timespan: str = 'second',
                    multiplier: int = 10,
                    limit: int = 50000,
                    offset_dates: dict[str, list[int|str|datetime.datetime,int|str|datetime.datetime]] = None) -> dict[str, pd.DataFrame]:
    client = StocksClient(api_key=polygon_api_key)
    data_dict = {}
    
    # @retry_on_missing_results(max_retries=3, backoff=1.0)
    @sleep_and_retry
    @limits(calls=CALLS, period=PERIOD)
    def _fetch_ohlcv(sym: str, offset_dates: list[int|str|datetime.datetime,int|str|datetime.datetime] = None):
        if offset_dates:
            raw = client.get_aggregate_bars(symbol=sym, multiplier=multiplier, 
                                            timespan=timespan, from_date=offset_dates[0], 
                                            limit=limit,to_date=offset_dates[1])
        else:
            raw = client.get_aggregate_bars(symbol=sym, multiplier=multiplier, 
                                            timespan=timespan, from_date=from_date, 
                                            limit=limit,to_date=to_date)
        if 'next_url' in list(raw.keys()):
            all_results = raw['results']
            # Follow pagination if there’s a next_url
            while raw.get('next_url'):
                next_url = raw['next_url']
                url = next_url if next_url.startswith('http') else f'https://api.polygon.io{next_url}'
                resp = requests.get(url, params={'apiKey': polygon_api_key}).json()            
                # raw = resp.json()
                all_results.extend(resp['results'])
            return all_results
        else:
            return raw['results']
    
    def _process(sym: str, offset_dates: list[int|str|datetime.datetime,int|str|datetime.datetime] = None):
        """Fetch + turn into a DataFrame."""
        price_data = _fetch_ohlcv(sym, offset_dates=offset_dates)
        df = pd.DataFrame(price_data)
        # …your existing timestamp + rename logic here…
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