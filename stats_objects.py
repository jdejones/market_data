from market_data import date, timedelta, List, dataclass, pd, datetime, Tuple
from market_data.Symbol_Data import SymbolData
from market_data.price_data_import import fragmented_intraday_import, nonconsecutive_intraday_import
from market_data.add_technicals import intraday_pipeline, add_avwap_by_offset, run_pipeline, Step, _add_technicals_worker
from market_data import ProcessPoolExecutor, as_completed, tqdm



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
    intraday_signals: dict[str, list[pd.DataFrame]] = None
    intraday_returns: dict[str, pd.DataFrame] = None
    
    def __post_init__(self):
        if isinstance(self.interday_signals, pd.DataFrame):
            self.signal_dates = {sym: {signal: self.interday_signals[sym].loc[self.interday_signals[sym][signal] == 1, signal].index for signal in self.interday_signals[sym].columns} for sym in self.interday_signals}
        elif isinstance(self.interday_signals, pd.Series):
            self.signal_dates = {sym: self.interday_signals.index.tolist() for sym in self.interday_signals.index}


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


    def import_intraday_data(self, timespan: str = 'second', multiplier: int = 10, limit: int = 50000):
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
                timespan=timespan,
                multiplier=multiplier,
                limit=limit
            )
        else:
            # nonconsecutive_intraday_import expects single dates
            self._intraday_frames = nonconsecutive_intraday_import(
                dates_dict,
                timespan=timespan,
                multiplier=multiplier,
                limit=limit
            )


    def add_intraday_technicals(self):
        """
        Add technical indicators to the intraday frames.
        """
        
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
        
        if self.consecutive_signals:
            worker = _add_intraday_with_avwap0
        else:
            worker = _add_technicals_worker
        
        #*Add technicals.
        items = [(sym, frame) for sym, frames in self._intraday_frames.items() for frame in frames]
        # Tune the number of workers and chunk size
        num_workers = 8  # adjust as needed
        chunksize = 7    # adjust chunk size as needed
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            for symbol, df in tqdm(
                executor.map(_add_technicals_worker, items, chunksize=chunksize),
                total=len(items),
                desc="Adding technicals"
            ):
                # re‐attach the processed DataFrame back to SymbolData
                self.intraday_frames[symbol].append(df)

    def condition_statistics(self):
        """
        Calculate the condition statistics for each symbol.
        """
        result = {}
        for sym, frames in self.intraday_frames.items():
            result[sym] = []
            for df in frames:
                signals_df = pd.DataFrame(index=df.index)
                # define base conditions for each signal
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
                # detect only the points where each condition turns true (preceded by false)
                for name, cond in base_conditions.items():
                    event = cond & ~cond.shift(1)
                    signals_df.loc[event, name] = 1
                result[sym].append(signals_df)
        self.intraday_signals = result


    def measure_intraday_returns(intraday_signals):
        """Compute return metrics for each symbol and each signal condition."""
        results = {}
        def process_symbol(sym):
            daily_df = self.symbols[sym].df.copy()
            # re‐index daily data by date for easy lookup
            daily_by_date = daily_df.copy()
            daily_by_date.index = daily_by_date.index.date
            # discover all signal names for this symbol
            conds = set()
            for df in intraday_signals[sym]:
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
            for df in intraday_signals[sym]:
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
                                d = type(self).next_business_day(d)
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
            return sym, df_ret

        # run symbol‐level computations in parallel
        with ProcessPoolExecutor() as executor:
            futures = {executor.submit(process_symbol, sym): sym for sym in intraday_signals}
            for fut in as_completed(futures):
                sym, df_r = fut.result()
                results[sym] = df_r
        return results

    # compute and store in the instance
    self.intraday_returns = measure_intraday_returns(self.intraday_signals)