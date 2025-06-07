from market_data import date, timedelta, List, dataclass, pd, datetime, Tuple, timedelta, operator
from market_data.Symbol_Data import SymbolData
from market_data.price_data_import import fragmented_intraday_import, nonconsecutive_intraday_import
from market_data.add_technicals import intraday_pipeline, add_avwap_by_offset, run_pipeline, Step, _add_intraday_technicals_worker
from market_data import ProcessPoolExecutor, as_completed, tqdm, ThreadPoolExecutor, levene, kruskal, median_test, sp
import numpy as np

        
def _add_intraday_with_avwap0(item):
    symbol, df = item
    # inject the extra step at the end
    steps = intraday_pipeline + [
        Step(add_avwap_by_offset,
                kwargs={'offset': 0},
                needs=['Close', 'Volume'],
                adds=['VWAP_0'])
    ]
    df = run_pipeline(df, steps=steps)
    return symbol, df

def process_symbol_intraday_returns(items):
    """
    Used with measure_intraday_returns.

    Args:
        sym (_type_): _description_

    Returns:
        _type_: _description_
    """
    sym, intraday_signals, daily_df = items
    import os
    print(f"[PID {os.getpid()}] starting {sym!r}")
    # re‐index daily data by date for easy lookup
    daily_by_date = daily_df.copy()
    daily_by_date.index = daily_by_date.index.date
    # discover all signal names for this symbol
    conds = set()
    for df in intraday_signals:
        conds.update([c for c in df.columns if df[c].eq(1).any()])
    # prepare storage
    metrics = {}
    for cond in conds:
        metrics[cond] = {
            'count': 0,
            'max_within': [],
            'min_within': [],
            'ret_close0': []
        }
        for k in range(1, 6):
            metrics[cond][f'ret_high_{k}d'] = []
            metrics[cond][f'ret_low_{k}d'] = []
            metrics[cond][f'ret_close_{k}d'] = []
    # loop through each intraday frame and collect returns
    for df in intraday_signals:
        # group by calendar day
        for day, intraday in df.groupby(df.index.date):
            for cond in conds:
                times = intraday.index[intraday[cond] == 1]
                for t in times:
                    price0 = intraday.at[t, 'Close']
                    # within‐day slice
                    sub = intraday.loc[t:]
                    metrics[cond]['count'] += 1
                    metrics[cond]['max_within'].append((sub['High'].max()  - price0) / price0 * 100)
                    metrics[cond]['min_within'].append((sub['Low'].min()   - price0) / price0 * 100)
                    metrics[cond]['ret_close0'].append((sub['Close'].iloc[-1] - price0) / price0 * 100)
                    # next 1–5 business days
                    d = day
                    for k in range(1, 6):
                        d = IntradaySignalProcessing.next_business_day(d)
                        if d in daily_by_date.index:
                            row = daily_by_date.loc[d]
                            metrics[cond][f'ret_high_{k}d'].append((row['High']  - price0) / price0 * 100)
                            metrics[cond][f'ret_low_{k}d'].append((row['Low']   - price0) / price0 * 100)
                            metrics[cond][f'ret_close_{k}d'].append((row['Close'] - price0) / price0 * 100)
    # aggregate into a DataFrame
    df_ret = pd.DataFrame(index=sorted(conds))
    for cond, vals in metrics.items():
        df_ret.at[cond, 'count']            = vals['count']
        df_ret.at[cond, 'mean_max_within']  = (sum(vals['max_within'])  / len(vals['max_within']))  if vals['max_within']  else float('nan')
        df_ret.at[cond, 'mean_min_within']  = (sum(vals['min_within'])  / len(vals['min_within']))  if vals['min_within']  else float('nan')
        df_ret.at[cond, 'mean_ret_close0']  = (sum(vals['ret_close0'])  / len(vals['ret_close0']))  if vals['ret_close0']  else float('nan')
        for k in range(1, 6):
            for m in ('high','low','close'):
                key      = f'ret_{m}_{k}d'
                mean_key = f'mean_{key}'
                lst      = vals[key]
                df_ret.at[cond, mean_key] = (sum(lst) / len(lst)) if lst else float('nan')
    # return sym, df_ret
    return df_ret, metrics

def process_symbol_conditions(items):
    sym, frames, conditions = items
    result = []
    for df in frames:
        # signals_df = pd.DataFrame(index=df.index)
        # signals_df['Close'] = df['Close']
        # signals_df['High'] = df['High']
        # signals_df['Low'] = df['Low']
        # signals_df['Open'] = df['Open']
        
        # signals_df['close_over_ema5'] = ((df['Close'] > df['ema5']) & 
        #                     (df['Close'].shift(1) <= df['ema5'].shift(1))).astype(int)        
        if not conditions:
            base_conditions = {
                'close_over_ema5': df['Close'] > df['ema5'],
                'close_over_ema9': df['Close'] > df['ema9'],
                'close_over_ema20': df['Close'] > df['ema20'],
                'atrs_traded_over_1': df['ATRs_Traded'] > 1,
                'atrs_traded_over_1_5': df['ATRs_Traded'] > 1.5,
                'atrs_traded_over_2': df['ATRs_Traded'] > 2,
                'close_over_u_band': df['Close'] > df['u_band'],
                'close_under_l_band': df['Close'] < df['l_band'],
                'relative_band_dist_decreasing': df['relative_band_dist'] < df['relative_band_dist'].shift(1),
                'relative_band_dist_increasing': df['relative_band_dist'] > df['relative_band_dist'].shift(1),
                'emacd59_over_0': df['eMACD59'] > 0,
                'emacd59_under_0': df['eMACD59'] < 0,
                'relative_macd59_diff_over_1': df['relative_macd59_diff'] > 1,
                'emacd520_over_0': df['eMACD520'] > 0,
                'emacd520_under_0': df['eMACD520'] < 0,
                'relative_macd520_diff_over_1': df['relative_macd520_diff'] > 1,
                'emacd920_over_0': df['eMACD920'] > 0,
                'emacd920_under_0': df['eMACD920'] < 0,
                'relative_macd920_diff_over_1': df['relative_macd920_diff'] > 1,
                'close_over_vwap': df['Close'] > df['VWAP'],
                'close_over_vwap_uband_1std_byday': df['Close'] > df['vwap_uband_1std_byday'],
                'close_over_vwap_uband_2std_byday': df['Close'] > df['vwap_uband_2std_byday'],
                'close_over_vwap_uband_3std_byday': df['Close'] > df['vwap_uband_3std_byday'],
                'close_under_vwap_lband_1std_byday': df['Close'] < df['vwap_lband_1std_byday'],
                'close_under_vwap_lband_2std_byday': df['Close'] < df['vwap_lband_2std_byday'],
                'close_under_vwap_lband_3std_byday': df['Close'] < df['vwap_lband_3std_byday'],
                'close_over_close_uband_1std_byday': df['Close'] > df['close_uband_1std_byday'],
                'close_over_close_uband_2std_byday': df['Close'] > df['close_uband_2std_byday'],
                'close_over_close_uband_3std_byday': df['Close'] > df['close_uband_3std_byday'],
                'close_under_close_lband_1std_byday': df['Close'] < df['close_lband_1std_byday'],
                'close_under_close_lband_2std_byday': df['Close'] < df['close_lband_2std_byday'],
                'close_under_close_lband_3std_byday': df['Close'] < df['close_lband_3std_byday'],
                'close_over_high_5ema_byday': df['Close'] > df['high_5ema_byday'],
                'close_over_high_9ema_byday': df['Close'] > df['high_9ema_byday'],
                'close_over_high_20ema_byday': df['Close'] > df['high_20ema_byday'],
                'close_under_high_5ema_byday': df['Close'] < df['high_5ema_byday'],
                'close_under_high_9ema_byday': df['Close'] < df['high_9ema_byday'],
                'close_under_high_20ema_byday': df['Close'] < df['high_20ema_byday'],
                'close_over_close_5ema_byday': df['Close'] > df['close_5ema_byday'],
                'close_over_close_9ema_byday': df['Close'] > df['close_9ema_byday'],
                'close_over_close_20ema_byday': df['Close'] > df['close_20ema_byday'],
                'close_under_close_5ema_byday': df['Close'] < df['close_5ema_byday'],
                'close_under_close_9ema_byday': df['Close'] < df['close_9ema_byday'],
                'close_under_close_20ema_byday': df['Close'] < df['close_20ema_byday'],
                'close_over_low_5ema_byday': df['Close'] > df['low_5ema_byday'],
                'close_over_low_9ema_byday': df['Close'] > df['low_9ema_byday'],
                'close_over_low_20ema_byday': df['Close'] > df['low_20ema_byday'],
                'close_under_low_5ema_byday': df['Close'] < df['low_5ema_byday'],
                'close_under_low_9ema_byday': df['Close'] < df['low_9ema_byday'],
                'close_under_low_20ema_byday': df['Close'] < df['low_20ema_byday'],
                'high_5ema_byday_over_close_5ema_byday': df['high_5ema_byday'] > df['close_5ema_byday'],
                'high_5ema_byday_under_close_5ema_byday': df['high_5ema_byday'] < df['close_5ema_byday'],
                'high_5ema_byday_over_low_5ema_byday': df['high_5ema_byday'] > df['low_5ema_byday'],
                'high_5ema_byday_under_low_5ema_byday': df['high_5ema_byday'] < df['low_5ema_byday'],
                'close_5ema_byday_over_low_5ema_byday': df['close_5ema_byday'] > df['low_5ema_byday'],
                'close_5ema_byday_under_low_5ema_byday': df['close_5ema_byday'] < df['low_5ema_byday'],
                'high_9ema_byday_over_close_9ema_byday': df['high_9ema_byday'] > df['close_9ema_byday'],
                'high_9ema_byday_under_close_9ema_byday': df['high_9ema_byday'] < df['close_9ema_byday'],
                'high_9ema_byday_over_low_9ema_byday': df['high_9ema_byday'] > df['low_9ema_byday'],
                'high_9ema_byday_under_low_9ema_byday': df['high_9ema_byday'] < df['low_9ema_byday'],
                'close_9ema_byday_over_low_9ema_byday': df['close_9ema_byday'] > df['low_9ema_byday'],
                'close_9ema_byday_under_low_9ema_byday': df['close_9ema_byday'] < df['low_9ema_byday'],
                'high_20ema_byday_over_close_20ema_byday': df['high_20ema_byday'] > df['close_20ema_byday'],
                'high_20ema_byday_under_close_20ema_byday': df['high_20ema_byday'] < df['close_20ema_byday'],
                'high_20ema_byday_over_low_20ema_byday': df['high_20ema_byday'] > df['low_20ema_byday'],
                'high_20ema_byday_under_low_20ema_byday': df['high_20ema_byday'] < df['low_20ema_byday'],
                'close_20ema_byday_over_low_20ema_byday': df['close_20ema_byday'] > df['low_20ema_byday'],
                'close_20ema_byday_under_low_20ema_byday': df['close_20ema_byday'] < df['low_20ema_byday'],
            }
            conditions = base_conditions
        else:
            _ops = {
                '>':  operator.gt,
                '<':  operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '==': operator.eq,
                '!=': operator.ne,
            }
            # compare = lambda x, y, op: _ops[op](x, y)
            # conditions = {k: compare(df[v[0]], df[v[1]], v[2]) for k, v in conditions.items()}
            new_conditions: dict[str, pd.Series] = {}
            for name, desc in conditions.items():
                # if desc[0] is itself a list/tuple, interpret as multiple comparisons
                if isinstance(desc[0], (list, tuple)):
                    # e.g. desc = [['ema9','VWAP','>'], ['ema5','ema9','>']]
                    combined = None
                    for col1, col2, op in desc:
                        this_cmp = _ops[op](df[col1], df[col2])
                        combined = this_cmp if combined is None else (combined & this_cmp)
                    new_conditions[name] = combined
                else:
                    # single comparison: desc = ['ema9','VWAP','>']
                    col1, col2, op = desc
                    new_conditions[name] = _ops[op](df[col1], df[col2])

            _conditions = new_conditions
        # detect only the points where each condition turns true (preceded by false)
        for name, cond in _conditions.items():
        #     prev = cond.shift(1).fillna(False) 
        #     event = cond & ~prev
            # signals_df.loc[cond, name] = 1
            df.loc[cond, name] = 1            
        result.append(df)
    return sym, result

@dataclass(slots=True)
class IntradaySignalProcessing:
    """_summary_

    
    The dataframe in interday_signals was originally intended to be the frame object from sf.condition_statistics.
    It could be any dataframe  with columns of boolean values in which True represents an identified signal.
    
    
    
    Returns:
        _type_: _description_
    """
    
    symbols: dict[str, SymbolData]
    interday_signals: dict[str, pd.DataFrame|pd.Series]
    signal_dates: dict[str, dict[str, list[datetime.date|Tuple[datetime.date, datetime.date]]]] = None
    _intraday_frames: dict[str, list[pd.DataFrame]] = None
    intraday_frames: dict[str, list[pd.DataFrame]] = None
    consecutive_signals: bool = False
    conditions: list[str] = None
    intraday_signals: dict[str, list[pd.DataFrame]] = None
    intraday_returns_raw: dict[str, dict[str, dict[str, list[int|float]]]] = None
    intraday_returns: dict[str, pd.DataFrame] = None
    ev_by_symbol: dict[str, dict[str, dict[str, list[int|float]]]] = None
    ev_agg: dict[str, dict[str, dict[str, list[int|float]]]] = None
    
    # def __post_init__(self):
    #     if isinstance(self.interday_signals[list(self.interday_signals.keys())[0]], pd.DataFrame):
    #         self.signal_dates = {sym: {signal: self.interday_signals[sym].loc[self.interday_signals[sym][signal] == 1, signal].index for signal in self.interday_signals[sym].columns} for sym in self.interday_signals}
    #     elif isinstance(self.interday_signals[list(self.interday_signals.keys())[0]], pd.Series):
    #         self.signal_dates = {sym: self.interday_signals[sym].loc[self.interday_signals[sym] == 1].index for sym in self.interday_signals}

    @staticmethod
    def next_business_day(d: datetime.date) -> datetime.date:
        """
        Return the next business (weekday) date after d,
        skipping Saturdays and Sundays.
        """
        d += timedelta(days=1)
        # 0 = Monday, …, 4 = Friday, 5 = Saturday, 6 = Sunday
        while d.weekday() >= 5:
            d += timedelta(days=1)
        return d

    def is_consecutive_business_days(dates: List[datetime.date]) -> bool:
        """
        Return True if:
        - dates is empty or has a single element, or
        - for every i > 0, dates[i] == next_business_day(dates[i-1])
            and the list is strictly increasing.
        Otherwise, return False.
        """
        if len(dates) < 2:
            return True

        for prev, curr in zip(dates, dates[1:]):
            # must be strictly increasing
            if curr <= prev:
                return False
            # must be exactly the next business day
            if curr != type(self).next_business_day(prev):
                return False

        return True

    
    def identify_signal_dates(self):
        """
        Populate self.signal_dates based on self.interday_signals and self.consecutive_signals.
        """
        result: dict[str, dict[str, list[datetime.date] | list[tuple[datetime.date, datetime.date]]]] = {}
        for sym, signals in self.interday_signals.items():
            sig_dict: dict[str, list] = {}
            # Handle DataFrame of signals
            if isinstance(signals, pd.DataFrame):
                for col in signals.columns:
                    idx = signals.index[signals[col] == 1]
                    # collect dates as datetime.date
                    dates: list[datetime.date] = []
                    for d in idx:
                        if hasattr(d, "date"):
                            dates.append(d.date())
                        else:
                            dates.append(d)
                    if self.consecutive_signals:
                        runs: list[tuple[datetime.date, datetime.date]] = []
                        if dates:
                            start = dates[0]
                            prev = dates[0]
                            for curr in dates[1:]:
                                # check for consecutive business day
                                if curr == type(self).next_business_day(prev):
                                    prev = curr
                                else:
                                    runs.append((start, prev))
                                    start = curr
                                    prev = curr
                            runs.append((start, prev))
                        sig_dict[col] = runs
                    else:
                        sig_dict[col] = dates
            # Handle Series of signals
            elif isinstance(signals, pd.Series):
                name = signals.name if signals.name is not None else "signal"
                idx = signals.index[signals == 1]
                dates: list[datetime.date] = []
                for d in idx:
                    if hasattr(d, "date"):
                        dates.append(d.date())
                    else:
                        dates.append(d)
                if self.consecutive_signals:
                    runs: list[tuple[datetime.date, datetime.date]] = []
                    if dates:
                        start = dates[0]
                        prev = dates[0]
                        for curr in dates[1:]:
                            if curr == type(self).next_business_day(prev):
                                prev = curr
                            else:
                                runs.append((start, prev))
                                start = curr
                                prev = curr
                        runs.append((start, prev))
                    sig_dict[name] = runs
                else:
                    sig_dict[name] = dates
            result[sym] = sig_dict
        self.signal_dates = result


    def import_intraday_data(self, resample: str = '3min', timespan: str = 'second', multiplier: int = 10, limit: int = 50000):
        """
        Import intraday price data for each symbol based on self.signal_dates.
        Uses nonconsecutive_intraday_import when consecutive_signals is False,
        otherwise uses fragmented_intraday_import. Results are stored in
        self._intraday_frames as a dict[symbol, list[pd.DataFrame]].
        """
        # Build a mapping from symbol to list of dates or date‐ranges
        dates_dict: dict[str, list] = {}
        for sym, sig_dict in self.signal_dates.items():
            if self.consecutive_signals:
                # Flatten runs of consecutive dates into [(start, end), …]
                periods: list[tuple[datetime.date, datetime.date]] = []
                for runs in sig_dict.values():
                    periods.extend(runs)
                dates_dict[sym] = periods
            else:
                # Flatten individual dates into [date, …]
                days: list[datetime.date] = []
                for dates in sig_dict.values():
                    days.extend(dates)
                dates_dict[sym] = days

        # Call the appropriate import function
        if self.consecutive_signals:
            # fragmented_intraday_import expects date‐range tuples
            self._intraday_frames = fragmented_intraday_import(
                dates_dict,
                resample=resample,
                timespan=timespan,
                multiplier=multiplier,
                limit=limit
            )
        else:
            # nonconsecutive_intraday_import expects single dates
            self._intraday_frames = nonconsecutive_intraday_import(
                dates_dict,
                resample=resample,
                timespan=timespan,
                multiplier=multiplier,
                limit=limit
            )


    def add_intraday_technicals(self):
        """
        Add technical indicators to the intraday frames.
        """

        
        if self.consecutive_signals:
            worker = _add_intraday_with_avwap0
        else:
            worker = _add_intraday_technicals_worker
        
        #*Add technicals.
        self.intraday_frames = {sym: [] for sym in self._intraday_frames}
        items = [(sym, frame) for sym, frames in self._intraday_frames.items() for frame in frames]
        # Tune the number of workers and chunk size
        num_workers = 8#1  # adjust as needed
        chunksize = 7#1    # adjust chunk size as needed
        #! Temporary fix for ProcessPoolExecutor not being available. Change back to ProcessPoolExecutor when available.
        # with ThreadPoolExecutor(max_workers=num_workers) as executor:
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            for symbol, df in tqdm(
                executor.map(worker, items, chunksize=chunksize),
                total=len(items),
                desc="Adding technicals"
            ):
                # re‐attach the processed DataFrame back to SymbolData
                self.intraday_frames[symbol].append(df)


    def condition_statistics(self, conditions: None|dict[str, list[str]]=None):
        """
        Calculate the condition statistics for each symbol.
        """
        # result = {}
        
        # for sym, frames in tqdm(self.intraday_frames.items(), 
        #                         total=len(self.intraday_frames), 
        #                         desc="Calculating condition statistics"):
        #     result[sym] = []
        #     for df in frames:
        #         signals_df = pd.DataFrame(index=df.index)
        #         signals_df['Close'] = df['Close']
        #         signals_df['High'] = df['High']
        #         signals_df['Low'] = df['Low']
        #         signals_df['Open'] = df['Open']
                
        #         # define base conditions for each signal
        #         #! Added the if condition because the relative_macd59_diff column was not being created.
        #         #! Try removing it after restarting the kernel.
        #         # if 'relative_macd59_diff' in df.columns:
        if conditions == None:
            self.conditions = [
                'close_over_ema5',
                'close_over_ema9',
                'close_over_ema20',
                'atrs_traded_over_1',
                'atrs_traded_over_1_5',
                'atrs_traded_over_2',
                'close_over_u_band',
                'close_under_l_band',
                'relative_band_dist_decreasing',
                'relative_band_dist_increasing',
                'emacd59_over_0',
                'emacd59_under_0',
                'relative_macd59_diff_over_1',
                'emacd520_over_0',
                'emacd520_under_0',
                'relative_macd520_diff_over_1',
                'emacd920_over_0',
                'emacd920_under_0',
                'relative_macd920_diff_over_1',
                'close_over_vwap',
                'close_over_vwap_uband_1std_byday',
                'close_over_vwap_uband_2std_byday',
                'close_over_vwap_uband_3std_byday',
                'close_under_vwap_lband_1std_byday',
                'close_under_vwap_lband_2std_byday',
                'close_under_vwap_lband_3std_byday',
                'close_over_close_uband_1std_byday',
                'close_over_close_uband_2std_byday',
                'close_over_close_uband_3std_byday',
                'close_under_close_lband_1std_byday',
                'close_under_close_lband_2std_byday',
                'close_under_close_lband_3std_byday',
                'close_over_high_5ema_byday',
                'close_over_high_9ema_byday',
                'close_over_high_20ema_byday',
                'close_under_high_5ema_byday',
                'close_under_high_9ema_byday',
                'close_under_high_20ema_byday',
                'close_over_close_5ema_byday',
                'close_over_close_9ema_byday',
                'close_over_close_20ema_byday',
                'close_under_close_5ema_byday',
                'close_under_close_9ema_byday',
                'close_under_close_20ema_byday',
                'close_over_low_5ema_byday',
                'close_over_low_9ema_byday',
                'close_over_low_20ema_byday',
                'close_under_low_5ema_byday',
                'close_under_low_9ema_byday',
                'close_under_low_20ema_byday',
                'high_5ema_byday_over_close_5ema_byday',
                'high_5ema_byday_under_close_5ema_byday',
                'high_5ema_byday_over_low_5ema_byday',
                'high_5ema_byday_under_low_5ema_byday',
                'close_5ema_byday_over_low_5ema_byday',
                'close_5ema_byday_under_low_5ema_byday',
                'high_9ema_byday_over_close_9ema_byday',
                'high_9ema_byday_under_close_9ema_byday',
                'high_9ema_byday_over_low_9ema_byday',
                'high_9ema_byday_under_low_9ema_byday',
                'close_9ema_byday_over_low_9ema_byday',
                'close_9ema_byday_under_low_9ema_byday',
                'high_20ema_byday_over_close_20ema_byday',
                'high_20ema_byday_under_close_20ema_byday',
                'high_20ema_byday_over_low_20ema_byday',
                'high_20ema_byday_under_low_20ema_byday',
                'close_20ema_byday_over_low_20ema_byday',
                'close_20ema_byday_under_low_20ema_byday',
            ]
        else:
            self.conditions = conditions
        #         # else:
        #         #     base_conditions = {
        #         #         'close_over_ema5': df['Close'] > df['ema5'],
        #         #         'close_over_ema9': df['Close'] > df['ema9'],
        #         #         'close_over_ema20': df['Close'] > df['ema20'],
        #         #         'atrs_traded_over_1': df['ATRs_Traded'] > 1,
        #         #         'atrs_traded_over_1_5': df['ATRs_Traded'] > 1.5,
        #         #         'atrs_traded_over_2': df['ATRs_Traded'] > 2,
        #         #         'close_over_u_band': df['Close'] > df['u_band'],
        #         #         'close_under_l_band': df['Close'] < df['l_band'],
        #         #         'relative_band_dist_decreasing': df['relative_band_dist'] < df['relative_band_dist'].shift(1),
        #         #         'relative_band_dist_increasing': df['relative_band_dist'] > df['relative_band_dist'].shift(1),
        #         #         'emacd59_over_0': df['eMACD59'] > 0,
        #         #         'emacd59_under_0': df['eMACD59'] < 0,
        #         #         # 'relative_macd59_diff_over_1': df['relative_macd59_diff'] > 1,
        #         #         'emacd520_over_0': df['eMACD520'] > 0,
        #         #         'emacd520_under_0': df['eMACD520'] < 0,
        #         #         # 'relative_macd520_diff_over_1': df['relative_macd520_diff'] > 1,
        #         #         'emacd920_over_0': df['eMACD920'] > 0,
        #         #         'emacd920_under_0': df['eMACD920'] < 0,
        #         #         # 'relative_macd920_diff_over_1': df['relative_macd920_diff'] > 1,
        #         #         'close_over_vwap': df['Close'] > df['VWAP'],
        #         #         'close_over_vwap_uband_1std_byday': df['Close'] > df['vwap_uband_1std_byday'],
        #         #         'close_over_vwap_uband_2std_byday': df['Close'] > df['vwap_uband_2std_byday'],
        #         #         'close_over_vwap_uband_3std_byday': df['Close'] > df['vwap_uband_3std_byday'],
        #         #         'close_under_vwap_lband_1std_byday': df['Close'] < df['vwap_lband_1std_byday'],
        #         #         'close_under_vwap_lband_2std_byday': df['Close'] < df['vwap_lband_2std_byday'],
        #         #         'close_under_vwap_lband_3std_byday': df['Close'] < df['vwap_lband_3std_byday'],
        #         #         'close_over_close_uband_1std_byday': df['Close'] > df['close_uband_1std_byday'],
        #         #         'close_over_close_uband_2std_byday': df['Close'] > df['close_uband_2std_byday'],
        #         #         'close_over_close_uband_3std_byday': df['Close'] > df['close_uband_3std_byday'],
        #         #         'close_under_close_lband_1std_byday': df['Close'] < df['close_lband_1std_byday'],
        #         #         'close_under_close_lband_2std_byday': df['Close'] < df['close_lband_2std_byday'],
        #         #         'close_under_close_lband_3std_byday': df['Close'] < df['close_lband_3std_byday'],
        #         #         'close_over_high_5ema_byday': df['Close'] > df['high_5ema_byday'],
        #         #         'close_over_high_9ema_byday': df['Close'] > df['high_9ema_byday'],
        #         #         'close_over_high_20ema_byday': df['Close'] > df['high_20ema_byday'],
        #         #         'close_under_high_5ema_byday': df['Close'] < df['high_5ema_byday'],
        #         #         'close_under_high_9ema_byday': df['Close'] < df['high_9ema_byday'],
        #         #         'close_under_high_20ema_byday': df['Close'] < df['high_20ema_byday'],
        #         #         'close_over_close_5ema_byday': df['Close'] > df['close_5ema_byday'],
        #         #         'close_over_close_9ema_byday': df['Close'] > df['close_9ema_byday'],
        #         #         'close_over_close_20ema_byday': df['Close'] > df['close_20ema_byday'],
        #         #         'close_under_close_5ema_byday': df['Close'] < df['close_5ema_byday'],
        #         #         'close_under_close_9ema_byday': df['Close'] < df['close_9ema_byday'],
        #         #         'close_under_close_20ema_byday': df['Close'] < df['close_20ema_byday'],
        #         #         'close_over_low_5ema_byday': df['Close'] > df['low_5ema_byday'],
        #         #         'close_over_low_9ema_byday': df['Close'] > df['low_9ema_byday'],
        #         #         'close_over_low_20ema_byday': df['Close'] > df['low_20ema_byday'],
        #         #         'close_under_low_5ema_byday': df['Close'] < df['low_5ema_byday'],
        #         #         'close_under_low_9ema_byday': df['Close'] < df['low_9ema_byday'],
        #         #         'close_under_low_20ema_byday': df['Close'] < df['low_20ema_byday'],
        #         #         'high_5ema_byday_over_close_5ema_byday': df['high_5ema_byday'] > df['close_5ema_byday'],
        #         #         'high_5ema_byday_under_close_5ema_byday': df['high_5ema_byday'] < df['close_5ema_byday'],
        #         #         'high_5ema_byday_over_low_5ema_byday': df['high_5ema_byday'] > df['low_5ema_byday'],
        #         #         'high_5ema_byday_under_low_5ema_byday': df['high_5ema_byday'] < df['low_5ema_byday'],
        #         #         'close_5ema_byday_over_low_5ema_byday': df['close_5ema_byday'] > df['low_5ema_byday'],
        #         #         'close_5ema_byday_under_low_5ema_byday': df['close_5ema_byday'] < df['low_5ema_byday'],
        #         #         'high_9ema_byday_over_close_9ema_byday': df['high_9ema_byday'] > df['close_9ema_byday'],
        #         #         'high_9ema_byday_under_close_9ema_byday': df['high_9ema_byday'] < df['close_9ema_byday'],
        #         #         'high_9ema_byday_over_low_9ema_byday': df['high_9ema_byday'] > df['low_9ema_byday'],
        #         #         'high_9ema_byday_under_low_9ema_byday': df['high_9ema_byday'] < df['low_9ema_byday'],
        #         #         'close_9ema_byday_over_low_9ema_byday': df['close_9ema_byday'] > df['low_9ema_byday'],
        #         #         'close_9ema_byday_under_low_9ema_byday': df['close_9ema_byday'] < df['low_9ema_byday'],
        #         #         'high_20ema_byday_over_close_20ema_byday': df['high_20ema_byday'] > df['close_20ema_byday'],
        #         #         'high_20ema_byday_under_close_20ema_byday': df['high_20ema_byday'] < df['close_20ema_byday'],
        #         #         'high_20ema_byday_over_low_20ema_byday': df['high_20ema_byday'] > df['low_20ema_byday'],
        #         #         'high_20ema_byday_under_low_20ema_byday': df['high_20ema_byday'] < df['low_20ema_byday'],
        #         #         'close_20ema_byday_over_low_20ema_byday': df['close_20ema_byday'] > df['low_20ema_byday'],
        #         #         'close_20ema_byday_under_low_20ema_byday': df['close_20ema_byday'] < df['low_20ema_byday'],
        #         #     }
                    
        #         # detect only the points where each condition turns true (preceded by false)
        #         for name, cond in base_conditions.items():
        #             prev = cond.shift(1).fillna(False) 
        #             event = cond & ~prev
        #             signals_df.loc[event, name] = 1
        #         result[sym].append(signals_df)
        # self.intraday_signals = result
        from multiprocessing import Pool, cpu_count
        
        # Create a pool of workers
        num_workers = min(8, len(self.intraday_frames))  # Use up to 8 workers or number of symbols, whichever is smaller
        chunk_size = max(1, len(self.intraday_frames) // (num_workers * 4))

        # # Create tasks
        # tasks = [(sym, frames) for sym, frames in self.intraday_frames.items()]        
        # with Pool(num_workers) as pool:            
        #     # Process symbols in parallel with progress bar
        #     results = {}
        #     for sym, result in tqdm(
        #         pool.starmap(process_symbol_conditions, tasks, chunksize=chunk_size),
        #         total=len(tasks),
        #         desc="Calculating condition statistics"
        #     ):
        #         results[sym] = result

        # Create tasks
        tasks = [(sym, frames, conditions) for sym, frames in self.intraday_frames.items()]
        
        results = {}        
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # Process symbols in parallel with progress bar
            for sym, result in tqdm(
                executor.map(process_symbol_conditions, tasks, chunksize=chunk_size),
                total=len(tasks),
                desc="Calculating condition statistics"
            ):
                results[sym] = result        

        self.intraday_signals = results


    def measure_intraday_returns(self):
        """Compute return metrics for each symbol and each signal condition."""
        from multiprocessing import Pool
        print("-> Starting measure_intraday_returns")
        results = {}
        results_raw = {}


        # run symbol‐level computations in parallel
        #! Tried running ThreadPoolExecutor and ProcessPoolExecutor.
        #! Temporary fix for ProcessPoolExecutor not being available. Change back to ProcessPoolExecutor when available.
        # with ThreadPoolExecutor() as executor:      
        # num_workers = 8#1  # adjust as needed
        # chunksize = 7#1    # adjust chunk size as needed  
        # # with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # #     futures = {executor.submit(process_symbol_intraday_returns, sym, self.intraday_signals[sym], self.symbols[sym],chunksize=chunksize): sym for sym in self.intraday_signals}
        # #     for fut in tqdm(as_completed(futures), 
        # #                     total=len(futures), 
        # #                     desc="Computing intraday returns"
        # #                     ):
        # #         sym, df_r = fut.result()
        # #         results[sym] = df_r
        # with Pool(num_workers) as pool:
        #     #Create tasks
        #     tasks = [
        #         (sym, self.intraday_signals[sym], self.symbols[sym], chunksize)
        #         for sym in self.intraday_signals
        #     ]
        #     # Map tasks to pool and track progress
        #     for sym, df_r in tqdm(
        #         pool.starmap(process_symbol_intraday_returns, tasks),
        #         total=len(tasks),
        #         desc="Computing intraday returns"
        #     ):
        #         results[sym] = df_r
        
        # # compute and store in the instance
        # self.intraday_returns = results    
        
        # num_workers = min(8, len(self.intraday_signals))
        # chunk_size  = max(1, len(self.intraday_signals) // (num_workers * 4))        
        
        
        # tasks = [
        #         (sym, self.intraday_signals[sym], self.symbols[sym].df.copy())
        #         for sym in self.intraday_signals
        #     ]
        # print(f"→ Dispatching {len(tasks)} symbols to the pool")
        # with Pool(processes=num_workers) as pool:
        #     for sym, df_r in tqdm(
        #         pool.imap_unordered(process_symbol_intraday_returns, tasks, chunksize=chunk_size),
        #         total=len(tasks),
        #         desc="Computing intraday returns"
        #     ):
        #         results[sym] = df_r

    
        syms = list(self.intraday_signals)
        signals = [self.intraday_signals[s] for s in syms]
        daily_dfs = [self.symbols[s].df.copy() for s in syms]
        max_workers = min(8, len(syms))

        print(f"→ Dispatching {len(syms)} symbols to the pool")
        with ProcessPoolExecutor(max_workers=max_workers) as exe:
            futures = {
                exe.submit(process_symbol_intraday_returns, (sym, sig, df)): sym
                for sym, sig, df in zip(syms, signals, daily_dfs)
            }
            print(f"→ Waiting for {len(futures)} futures to complete")
            for fut in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="Computing intraday returns"
            ):
                sym = futures[fut]
                results[sym], results_raw[sym] = fut.result()
            
        self.intraday_returns = results
        self.intraday_returns_raw = results_raw
        
        # return results

    def perform_kruskal(self,
        metric: str = 'max_within',
        return_full: bool = False
    ) -> tuple[list[str], float] | dict:
        """
        Perform Levene's test and the Kruskal–Wallis H-test across signals on a specified return metric.

        Args:
            metric: the key in intraday_returns_raw[sym][signal] containing the list of returns
            return_full: if False (default), returns (signals, kruskal_pvalue);
                            if True, returns a dict with:
                            {
                                'signals': [...],
                                'levene':   (stat, pvalue),
                                'kruskal':  (H_stat, pvalue),
                                'groups':   { signal: [values, ...], ... }
                            }

        Returns:
            Either a tuple (signals, kruskal_pvalue) or the full-results dict.

        Raises:
            ValueError: if fewer than 2 signals have data to compare.
        """
        # 1) Aggregate non-null returns across all symbols per signal
        groups = {
            sig: [
                v
                for sym_metrics in self.intraday_returns_raw.values()
                for v in sym_metrics.get(sig, {}).get(metric, [])
                if pd.notna(v)
            ]
            for sig in (self.conditions or [])
        }
        # 2) Filter out any constant-valued groups
        groups = {
            sig: vals
            for sig, vals in groups.items()
            if len(vals) >= 2 and not np.allclose(vals, vals[0])
        }

        if len(groups) < 2:
            raise ValueError("Need at least two signals with data to perform Kruskal–Wallis.")

        signals = list(groups.keys())
        data = [groups[s] for s in signals]

        # 2) Levene's test for homogeneity of variances
        lev_stat, lev_p = levene(*data)

        # 3) Kruskal–Wallis H-test (non-parametric ANOVA alternative)
        h_stat, kruskal_p = kruskal(*data)

        # 6) Fallback if p is still nan
        if np.isnan(kruskal_p):
            med_stat, med_p, _, _ = median_test(*data)
            results['median_test'] = (med_stat, med_p)    
            if return_full:
                return {
                    'signals': signals,
                    'levene':   (lev_stat, lev_p),
                    'kruskal':  (med_stat, med_p),
                    'groups':   groups
                }

            return signals, med_p
        
        else:
            if return_full:
                return {
                    'signals': signals,
                    'levene':   (lev_stat, lev_p),
                    'kruskal':  (h_stat, kruskal_p),
                    'groups':   groups
                }

            return signals, kruskal_p

    def perform_dunn(self,
        metric: str = 'max_within',
        p_adjust: str = 'bonferroni'
    ) -> pd.DataFrame:
        """
        Perform Dunn’s post-hoc pairwise comparisons on the groups defined in self.conditions.

        Args:
            metric:   which return‐series to use (e.g. 'ret_close0')
            p_adjust: method for multiple-testing correction (e.g. 'bonferroni', 'holm', 'fdr_bh')

        Returns:
            A DataFrame whose (i, j) entry is the adjusted p-value for comparing group i vs. j.
        """

        # 1) Build & clean groups (same as perform_kruskal did)
        raw = {
            sig: [
                v
                for sym in self.intraday_returns_raw.values()
                for v in sym.get(sig, {}).get(metric, [])
            ]
            for sig in (self.conditions or [])
        }
        groups = {
            sig: [v for v in vals if pd.notna(v) and np.isfinite(v)]
            for sig, vals in raw.items()
        }
        # drop empty or constant groups
        groups = {
            sig: vals
            for sig, vals in groups.items()
            if len(vals) > 1 and not np.allclose(vals, vals[0])
        }
        if len(groups) < 2:
            raise ValueError(
                f"Need at least two non-constant signals for Dunn’s test; "
                f"found only {len(groups)}."
            )

        # 2) Melt into a DataFrame of (group, value)
        df = pd.DataFrame(
            {
                'group': np.repeat(list(groups.keys()), [len(v) for v in groups.values()]),
                'value': np.concatenate(list(groups.values()))
            }
        )

        # 3) Run Dunn’s test
        #    this returns a symmetric DataFrame of adjusted p-values
        pvals = sp.posthoc_dunn(
            df,
            val_col='value',
            group_col='group',
            p_adjust=p_adjust
        )

        return pvals        

    def prior_day_stop_loss(self,
                            df: pd.DataFrame,
                            signal_date: datetime.date|str,
                            bias: str = 'long'):
        ts = pd.to_datetime(signal_date)

        # Find the previous business‐day:
        prev_day = ts - pd.Timedelta(days=1)
        while prev_day.weekday() >= 5:  # Sat/Sun
            prev_day -= pd.Timedelta(days=1)

        # Grab yesterday’s low (or high)
        prev_low  = df.at[prev_day, 'Low']
        prev_high = df.at[prev_day, 'High']

        # Now call with stop_target=prev_low for a long bias:
        ev_sym, ev_agg = isp.compute_expected_signal_values(
            stop_target=prev_low,
            bias=bias
        )

        # Or, if you wanted to use yesterday’s high in a short bias:
        ev_sym, ev_agg = isp.compute_expected_signal_values(
            stop_target=prev_high,
            bias=bias
        )
        return ev_sym, ev_agg
    def compute_expected_signal_values(self, stop_target: float = None, bias: str = 'long'):
        """
        Compute expected values for each signal within-day and up to 5 days after signal.
        Results stored in self.expected_values_by_symbol and self.expected_values.
        """
        ev_per_symbol: dict[str, dict[str, dict[int, float]]] = {}
        agg_metrics = {sig: {'count': 0, 'sum_ev': {h: 0.0 for h in range(6)}} for sig in (self.conditions or [])}
        for sym, dfs in self.intraday_signals.items():
            ev_per_symbol[sym] = {sig: {} for sig in (self.conditions or [])}
            daily_df = self.symbols[sym].df
            for df in dfs:
                for date, intraday in df.groupby(df.index.date):
                    for sig in (self.conditions or []):
                        if sig not in intraday.columns:
                            continue
                        times = intraday.index[intraday[sig] == 1]
                        if len(times) == 0:
                            continue
                        t0 = times.min()
                        price_sig = intraday.at[t0, 'Close']
                        pre = intraday.loc[:t0]
                        if stop_target is None:
                            if bias == 'long':
                                stop_price = pre['Low'].min()
                            else:
                                stop_price = pre['High'].max()
                        else:
                            stop_price = stop_target
                        post = intraday.loc[t0:]
                        if bias == 'long':
                            peak0 = post['High'].max()
                            stop_cross0 = (post['Close'] <= stop_price).any()
                            ret_ns0 = (peak0 - price_sig) / price_sig
                            ret_s0 = (stop_price - price_sig) / price_sig
                        else:
                            peak0 = post['Low'].min()
                            stop_cross0 = (post['Close'] >= stop_price).any()
                            ret_ns0 = (price_sig - peak0) / price_sig
                            ret_s0 = (price_sig - stop_price) / price_sig
                        ev0 = ret_ns0 if not stop_cross0 else ret_s0
                        ev_per_symbol[sym][sig][0] = ev0
                        agg_metrics[sig]['count'] += 1
                        agg_metrics[sig]['sum_ev'][0] += ev0
                        for h in range(1, 6):
                            d = date
                            days = [d]
                            for _ in range(h):
                                d = type(self).next_business_day(d)
                                days.append(d)
                            mask = daily_df.index.map(lambda x: x.date() in days)
                            period = daily_df.loc[mask]
                            if period.empty:
                                continue
                            if bias == 'long':
                                peak = period['High'].max()
                                stop_cross = (period['Close'] <= stop_price).any()
                                ret_ns = (peak - price_sig) / price_sig
                                ret_s = (stop_price - price_sig) / price_sig
                            else:
                                peak = period['Low'].min()
                                stop_cross = (period['Close'] >= stop_price).any()
                                ret_ns = (price_sig - peak) / price_sig
                                ret_s = (price_sig - stop_price) / price_sig
                            evh = ret_ns if not stop_cross else ret_s
                            ev_per_symbol[sym][sig][h] = evh
                            agg_metrics[sig]['sum_ev'][h] += evh
        ev_agg = {sig: {h: agg_metrics[sig]['sum_ev'][h] / agg_metrics[sig]['count']
                        for h in agg_metrics[sig]['sum_ev']}
                  for sig in agg_metrics if agg_metrics[sig]['count'] > 0}
        self.expected_values_by_symbol = ev_per_symbol
        self.expected_values = ev_agg
        return ev_per_symbol, ev_agg

    def compute_expected_signal_ev(self,
                                stop_target: float = None,
                                bias: str = 'long'):
        """
        For each symbol → signal → horizon (0–5), compute:
            P_pos = prob(return ≥ 0)
            R_pos = avg(return | return ≥ 0)
            P_neg = prob(return < 0)
            R_neg = avg(return | return < 0)
            EV    = [P_pos, P_neg] · [R_pos, R_neg]
        Stores:
            self.ev_by_symbol[sym][sig][h] = EV
            self.ev_agg      [sig][h]        = EV across all symbols
        """
        # prepare containers
        ev_lists_sym = {
            sym: {
                sig: {h: {'pos': [], 'neg': []} for h in range(6)}
                for sig in self.conditions
            }
            for sym in self.symbols
        }
        ev_lists_agg = {
            sig: {h: {'pos': [], 'neg': []} for h in range(6)}
            for sig in self.conditions
        }

        for sym, frames in self.intraday_signals.items():
            daily_df = self.symbols[sym].df
            for intraday in frames:
                for date, day_df in intraday.groupby(intraday.index.date):
                    for sig in self.conditions:
                        times = day_df.index[day_df.get(sig, 0) == 1]
                        if times.empty:
                            continue
                        t0      = times.min()
                        price0  = day_df.at[t0, 'Close']
                        pre     = day_df.loc[:t0]

                        # decide stop_price
                        stop_price = (
                            stop_target if stop_target is not None
                            else (pre['Low'].min() if bias == 'long'
                                    else pre['High'].max())
                        )

                        for h in range(6):
                            # intraday
                            if h == 0:
                                post = day_df.loc[t0:]
                            else:
                                # build mask for signal-day + next h business days
                                days = [date]
                                d = date
                                for _ in range(h):
                                    d = type(self).next_business_day(d)
                                    days.append(d)
                                mask = np.isin(daily_df.index.date, days)
                                post = daily_df.loc[mask]
                            if post.empty:
                                continue

                            if bias == 'long':
                                peak    = post['High'].max()
                                stopped = (post['Close'] <= stop_price).any()
                                ret     = ((peak - price0) / price0
                                            if not stopped
                                            else (stop_price - price0) / price0)
                            else:
                                trough  = post['Low'].min()
                                stopped = (post['Close'] >= stop_price).any()
                                ret     = ((price0 - trough) / price0
                                            if not stopped
                                            else (price0 - stop_price) / price0)

                            bucket = 'pos' if ret >= 0 else 'neg'
                            ev_lists_sym[sym][sig][h][bucket].append(ret)
                            ev_lists_agg[sig][h][bucket].append(ret)

        # now compute EV via dot product
        ev_by_symbol = {}
        for sym in ev_lists_sym:
            ev_by_symbol[sym] = {}
            for sig in ev_lists_sym[sym]:
                ev_by_symbol[sym][sig] = {}
                for h, data in ev_lists_sym[sym][sig].items():
                    pos, neg = data['pos'], data['neg']
                    total    = len(pos) + len(neg)
                    if total == 0:
                        ev = np.nan
                    else:
                        P_pos = len(pos) / total
                        P_neg = len(neg) / total
                        R_pos = np.mean(pos) if pos else 0.0
                        R_neg = np.mean(neg) if neg else 0.0
                        ev    = np.dot([P_pos, P_neg], [R_pos, R_neg])
                    ev_by_symbol[sym][sig][h] = ev

        ev_agg = {}
        for sig in ev_lists_agg:
            ev_agg[sig] = {}
            for h, data in ev_lists_agg[sig].items():
                pos, neg = data['pos'], data['neg']
                total    = len(pos) + len(neg)
                if total == 0:
                    ev = np.nan
                else:
                    P_pos = len(pos) / total
                    P_neg = len(neg) / total
                    R_pos = np.mean(pos) if pos else 0.0
                    R_neg = np.mean(neg) if neg else 0.0
                    ev    = np.dot([P_pos, P_neg], [R_pos, R_neg])
                ev_agg[sig][h] = ev

        self.ev_by_symbol = ev_by_symbol
        self.ev_agg       = ev_agg
        return ev_by_symbol, ev_agg
    
    