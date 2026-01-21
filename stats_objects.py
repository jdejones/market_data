from market_data import date, timedelta, List, dataclass, pd, datetime, Tuple, timedelta, operator, Union
from market_data.Symbol_Data import SymbolData
from market_data.price_data_import import fragmented_intraday_import, nonconsecutive_intraday_import, intraday_import
from market_data.add_technicals import intraday_pipeline, add_avwap_by_offset, run_pipeline, Step, _add_intraday_technicals_worker, SMA, EMA
from market_data import ProcessPoolExecutor, as_completed, tqdm, ThreadPoolExecutor, levene, kruskal, median_test, sp, Union, Dict, List, partial, find_peaks, linregress
import numpy as np


def _add_intraday_with_avwap0(item):
    """
    Apply the standard intraday technicals pipeline plus a 0-offset AVWAP column.

    This helper is designed for use inside multiprocessing executors. It takes a
    ``(symbol, df)`` pair, appends an `add_avwap_by_offset` step with ``offset=0``
    to the global ``intraday_pipeline``, runs the combined pipeline via
    :func:`run_pipeline`, and returns the updated pair.

    Parameters
    ----------
    item : tuple[str, pd.DataFrame]
        Two-tuple ``(symbol, df)`` where ``df`` is an intraday OHLCV DataFrame
        indexed by a DatetimeIndex and containing at least ``'Close'`` and
        ``'Volume'`` columns.

    Returns
    -------
    tuple[str, pd.DataFrame]
        The same symbol together with a new DataFrame that includes all
        columns produced by ``intraday_pipeline`` plus a ``'VWAP_0'`` column
        from :func:`add_avwap_by_offset`.
    """
    symbol, df = item
    # Clone the global intraday pipeline and append a 0-offset AVWAP step.
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
    Compute within-day and 1–5 day forward return metrics for all intraday signals of a symbol.
 
    This is a worker function intended for use with :class:`IntradaySignalProcessing`
    and is typically invoked by :meth:`IntradaySignalProcessing.measure_intraday_returns`.

    Given a tuple ``(symbol, intraday_signals, daily_df)``, it scans each intraday
    DataFrame for 0/1 signal columns, and for every ``1`` event computes:

    - Within-day max/min excursions in percent from the signal price.
    - Close-to-close same-day return.
    - Max/Min/Close returns over the next 1–5 business days based on the
      daily OHLC data in ``daily_df``.

    Parameters
    ----------
    items : tuple
        Three-tuple ``(sym, intraday_signals, daily_df)`` where:
        - ``sym`` is the symbol string.
        - ``intraday_signals`` is a list of intraday DataFrames with
          OHLC columns and one or more 0/1 signal columns.
        - ``daily_df`` is a daily OHLC DataFrame indexed by date or
          DatetimeIndex (will be reindexed by ``.index.date``).

    Returns
    -------
    tuple[pd.DataFrame, dict]
        - A summary DataFrame indexed by signal name with columns like
          ``'count'``, ``'mean_max_within'``, ``'mean_min_within'``,
          ``'mean_ret_close0'``, and ``'mean_ret_{high,low,close}_{1..5}d'``.
        - A nested metrics dictionary of the raw lists used to compute
          those means for each signal.
    """
    sym, intraday_signals, daily_df = items
    import os
    print(f"[PID {os.getpid()}] starting {sym!r}")
    # Reindex daily data by plain date so intraday date slices can look up forward days.
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
        # Iterate calendar days inside each intraday DataFrame.
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
    """
    Produce intraday signal columns for a symbol given comparison-based conditions.

    This worker is used by :meth:`IntradaySignalProcessing.condition_statistics`.
    For each intraday DataFrame, it either:

    - Builds a fixed library of boolean conditions (``base_conditions``) when
      ``conditions`` is falsy, or
    - Interprets the user-provided mapping of condition names to comparison
      descriptions and evaluates them.

    The resulting boolean Series are written back into each DataFrame as
    0/1 columns with the condition names.

    Parameters
    ----------
    items : tuple
        Three-tuple ``(sym, frames, conditions)`` where:
        - ``sym`` is the symbol string.
        - ``frames`` is a list of intraday OHLCV DataFrames with technical
          columns (e.g. EMAs, bands, VWAP) already attached.
        - ``conditions`` is either:
          - ``None`` / empty, in which case built-in ``base_conditions`` are used.
          - A mapping ``{name: desc}`` where:
            * ``desc = [col1, col2, op]`` encodes a single comparison
              ``df[col1] (op) df[col2]``; or
            * ``desc = [[col1, col2, op], ...]`` encodes an AND-combination
              of multiple comparisons.

    Returns
    -------
    tuple[str, list[pd.DataFrame]]
        The symbol and a list of the same DataFrames, each now containing
        one column per condition with integer 1 where the condition is true.
    """
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
            # Map textual comparison operators to the corresponding numpy/pandas-aware functions.
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
                # Support multi-clause conditions by AND-ing multiple (col1, col2, op) comparisons.
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
        # Write boolean conditions directly into the intraday DataFrame as 0/1 signal columns.
        for name, cond in _conditions.items():
        #     prev = cond.shift(1).fillna(False) 
        #     event = cond & ~prev
            # signals_df.loc[cond, name] = 1
            df.loc[cond, name] = 1            
        result.append(df)
    return sym, result

def calculate_symbol_rvol(symbol_data_tuple, target_date=None, lookback_days=20):
    """
    Calculate intraday relative volume (RVol) for a single symbol on a target date.

    RVol is defined here as the ratio of the current day's **cumulative intraday
    volume** at each timestamp to the average cumulative volume at the **same
    clock time** over the previous ``lookback_days`` trading days.

    Parameters
    ----------
    symbol_data_tuple : tuple[str, pd.DataFrame]
        Two-tuple ``(symbol, df)`` where ``df`` is intraday OHLCV data indexed
        by a DatetimeIndex and containing at least a ``'Volume'`` column.
    target_date : datetime.date or str, optional
        Calendar date for which RVol should be computed. If ``None``, the most
        recent date present in ``df`` is used.
    lookback_days : int, default 20
        Number of prior trading days used to compute the average cumulative
        volume profile.

    Returns
    -------
    tuple[str, pd.DataFrame | None]
        - The symbol string.
        - A DataFrame indexed by intraday timestamps on ``target_date`` with
          columns:
          * ``'cumulative_volume'``
          * ``'avg_historical_volume'``
          * ``'intraday_rvol'``
        or ``None`` if there is insufficient data.
    """
    symbol, df = symbol_data_tuple
    
    if df is None or df.empty:
        return symbol, None
    
    try:
        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            df = df.copy()
            df.index = pd.to_datetime(df.index)
        
        df = df.sort_index()
        idx = df.index
        date = idx.date
        time = idx.time
        
        
        # Group by date and calculate cumulative sum within each day
        cumvol = df["Volume"].groupby(date).cumsum()
        
        # Get unique dates and ensure we have enough data
        unique_dates = sorted(df['date'].unique())
        
        if len(unique_dates) < lookback_days + 1:
            return symbol, None
        
        # Get the target date data
        target_mask = (date == target_date)
        target_date_data = pd.DataFrame(
            {"time": time[target_mask], "cumulative_volume": cumvol[target_mask]},
            index=idx[target_mask],
        )
        
        if target_date_data.empty:
            return symbol, None
        
        # Get historical data (previous lookback_days)
        historical_dates = unique_dates[-(lookback_days + 1):-1]  # Exclude target date
        historical_data = df[df['date'].isin(historical_dates)].copy()
        
        if historical_data.empty:
            return symbol, None
        
        # Calculate average cumulative volume by time across historical days
        avg_cumulative_by_time = historical_data.groupby('time')['cumulative_volume'].mean()
        
        # Calculate RVol for target date
        target_date_data['avg_historical_volume'] = target_date_data['time'].map(avg_cumulative_by_time)
        target_date_data['intraday_rvol'] = (
            target_date_data['cumulative_volume'] / target_date_data['avg_historical_volume']
        ).fillna(0)
        
        # Clean up and return relevant columns
        result_data = target_date_data[['cumulative_volume', 'avg_historical_volume', 'intraday_rvol']].copy()
        
        return symbol, result_data
        
    except Exception as e:
        print(f"Error calculating RVol for {symbol}: {e}")
        return symbol, None

@dataclass(slots=True)
class IntradaySignalProcessing:
    """
    Orchestrate intraday data import, signal construction, and return/EV analysis.
    
    The dataframe in interday_signals was originally intended to be the frame object from sf.condition_statistics.
    It could be any dataframe  with columns of boolean values in which True represents an identified signal.

    This class ties together:
    - A mapping of daily price data per symbol (:class:`SymbolData` instances).
    - Interday signal definitions (e.g. daily pattern flags).
    - Intraday imports around those signal dates (fragmented or nonconsecutive).
    - Computation of intraday technical indicators and 0/1 intraday signals.
    - Aggregation of within-day and multi-day return statistics and EV metrics.

    Attributes
    ----------
    symbols : dict[str, SymbolData]
        Mapping from symbol to :class:`SymbolData` containing daily OHLCV
        DataFrames in ``.df``.
    interday_signals : dict[str, pd.DataFrame | pd.Series]
        For each symbol, either a DataFrame of daily 0/1 signal columns or a
        single signal Series indexed by date/DatetimeIndex.
    signal_dates : dict[str, dict[str, list[datetime.date] | list[tuple[datetime.date, datetime.date]]]] | None
        Populated by :meth:`identify_signal_dates`. For each symbol and signal
        name, stores either a list of individual dates or a list of
        (start_date, end_date) tuples when ``consecutive_signals=True``.
    _intraday_frames : dict[str, list[pd.DataFrame]] | None
        Raw intraday OHLCV frames fetched around ``signal_dates``.
    intraday_frames : dict[str, list[pd.DataFrame]] | None
        Intraday frames after technical indicator pipelines have been applied.
    consecutive_signals : bool
        If True, treat runs of consecutive business-day signals as multi-day
        periods and import intraday data via :func:`fragmented_intraday_import`.
        Otherwise, treat each signal date independently.
    conditions : list[str] | dict | None
        Set of intraday condition names (or explicit definitions) used to
        create 0/1 intraday signal columns in :meth:`condition_statistics`.
    intraday_signals : dict[str, list[pd.DataFrame]] | None
        For each symbol, the intraday frames with signal columns written in.
    intraday_returns_raw : dict[str, dict[str, dict[str, list[float]]]] | None
        Raw per-symbol, per-signal return lists produced by
        :meth:`measure_intraday_returns`.
    intraday_returns : dict[str, pd.DataFrame] | None
        Per-symbol summary DataFrames of average intraday / multi-day returns
        by signal condition.
    ev_by_symbol, ev_agg, expected_values_by_symbol, expected_values
        Containers used by :meth:`compute_expected_signal_values` and
        :meth:`compute_expected_signal_ev` for intraday EV analytics.
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
        Return the next weekday (Mon–Fri) strictly after the given date.

        The function skips Saturdays and Sundays but does not account for
        exchange holidays. It is used throughout this module to advance
        from a signal date to future trading days when computing returns.
        #TODO Add exchange holiday handling.

        Parameters
        ----------
        d : datetime.date
            Anchor date.

        Returns
        -------
        datetime.date
            The next calendar date such that ``weekday() < 5``.
        """
        d += timedelta(days=1)
        # 0 = Monday, …, 4 = Friday, 5 = Saturday, 6 = Sunday
        while d.weekday() >= 5:
            d += timedelta(days=1)
        return d

    def is_consecutive_business_days(dates: List[datetime.date]) -> bool:
        """
        Check whether a sequence of dates forms a strictly increasing run of business days.

        A list is considered consecutive if:
        - It has 0 or 1 element, or
        - For every adjacent pair ``(prev, curr)``:
          * ``curr > prev``, and
          * ``curr`` equals :meth:`next_business_day(prev)`.

        Parameters
        ----------
        dates : list[datetime.date]
            Ordered sequence of dates to check.

        Returns
        -------
        bool
            True if the list is empty, length-one, or a contiguous chain of
            business days; False otherwise.
        """
        if len(dates) < 2:
            return True

        # Enforce strictly increasing dates and exact 1-business-day spacing.
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
        Derive per-symbol signal dates (or date ranges) from `interday_signals`.

        For each symbol:
        - If ``interday_signals[sym]`` is a DataFrame, each column is treated
          as a separate 0/1 signal series.
        - If it is a Series, its ``name`` (or the fallback ``"signal"``) is
          used as the signal key.

        Dates are extracted wherever the signal equals 1. When
        ``self.consecutive_signals`` is:

        - False: each signal produces a flat list of :class:`datetime.date`
          objects.
        - True: each signal produces a list of ``(start_date, end_date)``
          tuples representing contiguous runs of business days (using
          :meth:`next_business_day`).

        The result is stored in ``self.signal_dates`` as a nested mapping
        ``{symbol: {signal_name: dates_or_runs}}``.
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
        Import intraday OHLCV data around each signal date or date-range.

        This method consumes :attr:`signal_dates` and constructs a per-symbol
        lookup list passed into either:

        - :func:`fragmented_intraday_import` when ``self.consecutive_signals``
          is True, using ``[(start_date, end_date), ...]`` periods; or
        - :func:`nonconsecutive_intraday_import` when False, using a flat list
          of individual dates.

        The resulting intraday frames are stored in :attr:`_intraday_frames`
        as ``{symbol: [df1, df2, ...]}``.

        Parameters
        ----------
        resample : str, default '3min'
            Pandas-style resampling rule applied to the raw intraday data.
        timespan : str, default 'second'
            Base timespan passed through to the data import layer.
        multiplier : int, default 10
            Timespan multiplier forwarded to the import functions.
        limit : int, default 50000
            Max number of intraday bars per request (API-dependent).
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
        Enrich imported intraday frames with technical indicators (and optional AVWAP).

        For each symbol and intraday DataFrame in :attr:`_intraday_frames`, this
        method:

        - Selects a worker function:
          * ``_add_intraday_with_avwap0`` when ``consecutive_signals=True``,
            which appends a 0-offset AVWAP column in addition to the standard
            intraday pipeline.
          * ``_add_intraday_technicals_worker`` otherwise.
        - Dispatches all ``(symbol, df)`` pairs to a :class:`ProcessPoolExecutor`.
        - Collects and stores the processed frames in :attr:`intraday_frames`.

        The input DataFrames are not modified in-place; the processed copies
        are stored in a new mapping.

        Notes
        -----
        This step can be CPU-intensive; ``num_workers`` and ``chunksize`` can
        be tuned for your environment.
        """

        
        if self.consecutive_signals:
            worker = _add_intraday_with_avwap0
        else:
            worker = _add_intraday_technicals_worker
        
        #*Add technicals.
        self.intraday_frames = {sym: [] for sym in self._intraday_frames}
        # Flatten symbol → [frames] into a list of (symbol, frame) tasks.
        items = [(sym, frame) for sym, frames in self._intraday_frames.items() for frame in frames]
        # Tune the number of workers and chunk size
        num_workers = 8#1  # adjust as needed
        chunksize = 7#1    # adjust chunk size as needed
        #! Temporary fix for ProcessPoolExecutor not being available. Change back to ProcessPoolExecutor when available.
        # Parallelize technical indicator computation across symbols and frames.
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
        Construct 0/1 intraday signal columns for each symbol according to conditions.

        This method operates on :attr:`intraday_frames`, which are intraday OHLCV
        DataFrames already enriched with technical indicators. It then:

        - Determines the set of condition names:
          * If ``conditions is None``, uses a built-in list of boolean
            expressions over existing columns (e.g. ``'close_over_ema5'``).
          * Otherwise, uses the supplied mapping of condition definitions.
        - Dispatches each ``(symbol, frames, conditions)`` triplet to
          :func:`process_symbol_conditions` using a :class:`ProcessPoolExecutor`.
        - Collects the resulting frames with 0/1 signal columns into
          :attr:`intraday_signals`.

        Parameters
        ----------
        conditions : dict[str, list] or None, optional
            Optional mapping defining custom conditions. See
            :func:`process_symbol_conditions` for the supported formats.
            If None, a fixed library of conditions is used.

        Side Effects
        ------------
        Populates :attr:`conditions` and :attr:`intraday_signals`.
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

        # Package each symbol's intraday frames with the shared conditions for parallel evaluation.
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
        """
        Compute within-day and 1–5 day return distributions for all intraday signals.

        This method assumes :attr:`intraday_signals` is populated with, for each
        symbol, a list of intraday DataFrames that include both OHLC columns and
        0/1 signal columns (as created in :meth:`condition_statistics`).

        For each symbol, it submits a job to :func:`process_symbol_intraday_returns`,
        passing the symbol, its list of intraday frames, and a copy of the
        symbol's daily OHLCV DataFrame (from :attr:`symbols`). The worker
        computes:

        - ``'max_within'`` / ``'min_within'``: intraday peak and trough moves.
        - ``'ret_close0'``: signal-close to close-of-day return.
        - ``'ret_{high,low,close}_{1..5}d'``: multi-day forward returns based on
          daily bars and a business-day calendar.

        Results are aggregated into:

        - :attr:`intraday_returns`: per-symbol summary DataFrames by signal.
        - :attr:`intraday_returns_raw`: nested dicts of raw lists per symbol,
          signal, and metric key.

        Returns
        -------
        None
            The results are stored on the instance attributes described above.
        """
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

        # Prepare parallel jobs: one per symbol, each with its intraday signals and daily data.
        syms = list(self.intraday_signals)
        signals = [self.intraday_signals[s] for s in syms]
        daily_dfs = [self.symbols[s].df.copy() for s in syms]
        max_workers = min(8, len(syms))

        print(f"→ Dispatching {len(syms)} symbols to the pool")
        # Run symbol-level return computations in parallel using a process pool.
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
        return_full: bool = False) -> tuple[list[str], float] | dict:
        """
        Compare distributions of a chosen return metric across signals using non-parametric tests.

        The input groups are built from :attr:`intraday_returns_raw` by extracting
        the list ``intraday_returns_raw[sym][signal][metric]`` for each signal
        name in :attr:`conditions`. Typical metric keys include:

        - ``'max_within'``, ``'min_within'``, ``'ret_close0'``
        - ``'ret_{high,low,close}_{1..5}d'``

        After filtering out empty and constant groups, the function applies:

        - Levene's test for equal variances across groups.
        - The Kruskal–Wallis H-test (or, if its p-value is NaN, a median test).

        Parameters
        ----------
        metric : str, default 'max_within'
            Key into each signal's raw returns dictionary.
        return_full : bool, default False
            If True, return a dictionary with statistics and per-group data.
            Otherwise, return a tuple ``(signals, p_value)``.

        Returns
        -------
        tuple[list[str], float] or dict
            Either:
            - ``(signals, kruskal_pvalue)`` where ``signals`` is the list of
              group labels and ``kruskal_pvalue`` is the p-value; or
            - A dict with keys ``'signals'``, ``'levene'``, ``'kruskal'``,
              and ``'groups'`` when ``return_full=True``.
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
        p_adjust: str = 'bonferroni') -> pd.DataFrame:
        """
        Perform Dunn's post-hoc pairwise comparisons on intraday return groups.

        Groups are built from :attr:`intraday_returns_raw` in the same way as
        :meth:`perform_kruskal`, using ``metric`` as the key into each
        signal's raw returns list. Empty and constant groups are discarded.

        Parameters
        ----------
        metric : str, default 'max_within'
            Which return series to analyze (e.g. ``'ret_close0'``).
        p_adjust : str, default 'bonferroni'
            Multiple-testing correction method passed to
            :func:`scikit_posthocs.posthoc_dunn`.

        Returns
        -------
        pd.DataFrame
            A symmetric DataFrame whose rows and columns are signal names and
            whose entries are adjusted p-values for each pairwise comparison.
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
                f"Need at least two non-constant signals for Dunn's test; "
                f"found only {len(groups)}."
            )

        # 2) Melt into a DataFrame of (group, value)
        df = pd.DataFrame(
            {
                'group': np.repeat(list(groups.keys()), [len(v) for v in groups.values()]),
                'value': np.concatenate(list(groups.values()))
            }
        )

        # 3) Run Dunn's test
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
        """
        Convenience wrapper to compute EV using a prior-day high/low as stop target.

        Given a daily OHLCV DataFrame and a signal date, this method:

        - Locates the previous business day (Mon–Fri) before ``signal_date``.
        - Reads that day's ``Low`` and ``High``.
        - Calls :meth:`compute_expected_signal_values` twice using each as the
          ``stop_target``, with the specified trading ``bias``.

        Parameters
        ----------
        df : pd.DataFrame
            Daily OHLCV price data indexed by date/DatetimeIndex.
        signal_date : datetime.date or str
            Signal date for which the prior-day stop is derived.
        bias : {'long', 'short'}, default 'long'
            Trading bias used when interpreting EV calculations.

        Returns
        -------
        tuple[dict, dict]
            The pair of dictionaries returned by
            :meth:`compute_expected_signal_values` for each stop choice.
        """
        ts = pd.to_datetime(signal_date)

        # Find the previous business‐day:
        prev_day = ts - pd.Timedelta(days=1)
        while prev_day.weekday() >= 5:  # Sat/Sun
            prev_day -= pd.Timedelta(days=1)

        # Grab yesterday's low (or high)
        prev_low  = df.at[prev_day, 'Low']
        prev_high = df.at[prev_day, 'High']

        # Now call with stop_target=prev_low for a long bias:
        ev_sym, ev_agg = isp.compute_expected_signal_values(
            stop_target=prev_low,
            bias=bias
        )

        # Or, if you wanted to use yesterday's high in a short bias:
        ev_sym, ev_agg = isp.compute_expected_signal_values(
            stop_target=prev_high,
            bias=bias
        )
        return ev_sym, ev_agg
    def compute_expected_signal_values(self, stop_target: float = None, bias: str = 'long'):
        """
        Compute horizon-based expected values (EV) per signal using a hard stop level.

        For every symbol and intraday signal in :attr:`intraday_signals`, this
        method:

        - Locates the earliest signal time within each day.
        - Defines a stop price either from ``stop_target`` or from the pre-signal
          intraday range (min Low for long, max High for short).
        - For horizon ``h = 0``:
          * Uses intraday data from signal time to the end of that day.
        - For horizons ``h = 1..5``:
          * Uses daily data from the signal date plus the next ``h`` business
            days (via :meth:`next_business_day`).
        - Computes the realized return assuming:
          * If the stop is never hit, take the best favourable move (High for
            long, Low for short).
          * If the stop is hit, take the stop price instead.
        - Aggregates these returns into an EV per signal and horizon.

        Parameters
        ----------
        stop_target : float, optional
            Explicit stop price to use for all signals. If None, a dynamic stop
            is taken from the pre-signal intraday range as described above.
        bias : {'long', 'short'}, default 'long'
            Direction of the trade, determining whether peaks or troughs
            represent favourable moves.

        Returns
        -------
        tuple[dict, dict]
            - ``ev_per_symbol``: mapping ``{symbol: {signal: {horizon: EV}}}``.
            - ``ev_agg``: mapping ``{signal: {horizon: EV}}`` aggregated
              across all symbols.
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
        Compute EV per signal and horizon from empirical positive/negative return buckets.

        For each symbol, signal, and horizon ``h ∈ {0..5}``, this method:

        - Collects realized returns under a stop rule (similar to
          :meth:`compute_expected_signal_values`).
        - Splits them into:
          * ``pos``: returns ``>= 0``
          * ``neg``: returns ``< 0``
        - Computes:
          * ``P_pos``, ``P_neg``: empirical frequencies of each bucket.
          * ``R_pos``, ``R_neg``: mean return within each bucket.
        - Defines EV as ``EV = P_pos * R_pos + P_neg * R_neg``.

        Parameters
        ----------
        stop_target : float, optional
            Explicit stop price to use for all signals. If None, a dynamic stop
            is computed per signal as in :meth:`compute_expected_signal_values`.
        bias : {'long', 'short'}, default 'long'
            Trade direction determining whether highs or lows are favourable.

        Returns
        -------
        tuple[dict, dict]
            - ``ev_by_symbol``: nested ``{symbol: {signal: {h: EV}}}``.
            - ``ev_agg``: nested ``{signal: {h: EV}}`` aggregated over symbols.
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
    
#Interday EV objects
def exit_stop_rel_entry(df, entry_signal, l_operand, r_operand, bias='long'):
    """
    Describing this function was difficult to articulate so I kept my description and added the LLM generated description:
    My description:
    This function is intended to be used with the compute_expected_interday_values function
    and is only needed if the exit and stop signals are the same conditions, and the
    only difference is the relationship between their occurrence and the value of 
    the entry signal.
    For example, the function was originally used to find EV with episodic pivots.
    In the case of the episodic pivot the l_operand would be the Close price and 
    the r_operand would be the 20DMA. The entry signal would be selected prior to
    running this function.
    
    It is used to compute the exit and stop signals for a given entry signal, l_operand, r_operand, and bias.
    The entry signal is the signal that triggers the entry into a trade.
    The l_operand is the left operand of the comparison.
    The r_operand is the right operand of the comparison.
    The bias is the long or short bias of the trade.
    The function returns the exit and stop signals for the given entry signal, l_operand, r_operand, and bias.
    
    LLM description:
    Derive exit and stop signals based on how price evolves after an entry condition.

    This helper is intended for interday EV analysis when the **same** structural
    condition (e.g. ``Close < 20DMA``) can represent either an exit or a stop,
    depending on whether price moved favourably before the condition was met.

    For each row where ``df[entry_signal]`` is True:

    - Let ``start_close`` be the closing price on the entry day.
    - Scan forward in time for the first day where the comparison
      ``df[l_operand] < df[r_operand]`` (for long bias) or the opposite
      inequality (for short bias) holds:
      * If the comparison holds and ``df[l_operand]`` is **favourable**
        vs ``start_close`` (above for long, below for short), mark an exit.
      * If the comparison holds and ``df[l_operand]`` is **unfavourable``
        vs ``start_close``, mark a stop.

    Parameters
    ----------
    df : pd.DataFrame
        Daily price/indicator DataFrame indexed by date and containing:
        ``'Close'`` plus columns referenced by ``l_operand`` and ``r_operand``.
    entry_signal : str
        Column name that is True/1 on entry dates.
    l_operand : str
        Column name for the left-hand side of the comparison (e.g. 'Close').
    r_operand : str
        Column name for the right-hand side (e.g. '20DMA').
    bias : {'long', 'short'}, default 'long'
        Trading bias; determines which side of ``start_close`` is favourable.

    Returns
    -------
    tuple[pd.Series, pd.Series]
        Two 0/1 Series indexed like ``df``:
        - ``exit_signals``: first favourable comparison date per entry.
        - ``stop_signals``: first unfavourable comparison date per entry.
    """
    exit_signals = pd.Series(0, index=df.index)
    stop_signals = pd.Series(0, index=df.index)

    # Get all start dates
    start_dates = df.loc[df[entry_signal] == True].index
    for start_date in start_dates:
        # Get the close price at start
        start_close = df.loc[start_date, 'Close']
        
        # Create mask for subsequent dates where:
        # 1. Close < 20DMA (trend broken)
        if bias == 'long':
            mask_exit = (df.index > start_date) & \
                (df[l_operand] < df[r_operand]) & \
                (df[l_operand] > start_close)        
            mask_stop = (df.index > start_date) & \
                (df[l_operand] < df[r_operand]) & \
                (df[l_operand] < start_close)
        else:
            mask_exit = (df.index > start_date) & \
                (df[l_operand] > df[r_operand]) & \
                (df[l_operand] < start_close)
            mask_stop = (df.index > start_date) & \
                (df[l_operand] > df[r_operand]) & \
                (df[l_operand] > start_close) 
        if mask_exit.any():
            first_exit_date = mask_exit.idxmax()
            exit_signals[first_exit_date] = 1
        if mask_stop.any():
            first_stop_date = mask_stop.idxmax()
            stop_signals[first_stop_date] = 1
    return exit_signals, stop_signals

def exit_stop_rel_signal_price(
    df: pd.DataFrame, 
    entry_signal: str,
    signal_day_col: str,
    stop_day_col: str,
    _operator: Union[str, callable],
    result_col: str
    ):
    """
    Describing this function was difficult to articulate so I kept my description and added the LLM generated description:
    My description:
    This is for use with the compute_interday_expected_values function.
    It is needed if the stop price is dependent on a price from the day the signal occurs.
    For example, if the stop price is greater than the high of the the day the day the signal occurs
    the level would be 'High' and _operator would be '>'.
    Add a column `stop_signal` marking the first day AFTER each `entry_signal` where
    the comparison between the current row's `stop_day_col` and the ENTRY-DAY value of
    `signal_day_col` is satisfied. The scan stops at the next entry if one occurs.

    LLM description:
    Mark the first post-entry day where a stop condition relative to the entry-day price is met.

    This function is designed for interday EV workflows where the stop threshold
    is defined in terms of a price observed on the **entry day**. For each
    occurrence of ``entry_signal == 1``:

    - Capture the entry-day value ``entry_price = df.at[entry_date, signal_day_col]``.
    - Scan strictly after ``entry_date`` (and before any subsequent entry) for
      the first row where the comparison
      ``op(df[stop_day_col], entry_price)`` is True, where ``op`` is defined by
      ``_operator``.
    - Mark that date with a 1 in ``result_col``.

    Parameters
    ----------
    df : pd.DataFrame
        Daily OHLCV DataFrame containing the specified columns.
    entry_signal : str
        Column name that is 1/True on entry dates.
    signal_day_col : str
        Column whose entry-day value defines the reference price (e.g. 'High').
    stop_day_col : str
        Column compared on subsequent days against the entry-day reference
        (e.g. 'Low').
    _operator : str or callable
        One of {'>', '<', '>=', '<=', '==', '!='} or a binary callable
        ``(current, reference) -> bool``.
    result_col : str
        Name of the new 0/1 column to write stop signals into.

    Returns
    -------
    pd.DataFrame
        The same DataFrame with a new integer column ``result_col`` set to 1
        on the first stop date per entry, and 0 elsewhere.
        
    """
    # Validate inputs and work on a copy
    required_cols = [entry_signal, signal_day_col, stop_day_col]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in DataFrame")

    df[result_col] = 0

    # Resolve operator
    _ops = {
        '>':  operator.gt,
        '<':  operator.lt,
        '>=': operator.ge,
        '<=': operator.le,
        '==': operator.eq,
        '!=': operator.ne,
    }
    if callable(_operator):
        op_func = _operator
    else:
        if _operator not in _ops:
            raise ValueError(f"Unsupported operator '{_operator}'. Use one of {list(_ops.keys())} or a callable.")
        op_func = _ops[_operator]

    # Identify entry dates
    entry_mask = (df[entry_signal] == 1) | (df[entry_signal] == True)
    entry_dates = df.index[entry_mask]
    if len(entry_dates) == 0:
        return df

    # For each entry, find the first date after it (and before the next entry) where
    # op(df[stop_day_col], entry_day_signal_price) is True.
    for i, start_date in enumerate(entry_dates):
        try:
            reference_price = df.at[start_date, signal_day_col]
        except KeyError:
            continue
        if pd.isna(reference_price):
            continue

        # Compute end boundary (exclusive): the next entry date, if any
        end_date = entry_dates[i + 1] if (i + 1) < len(entry_dates) else None

        window_mask = df.index > start_date
        if end_date is not None:
            window_mask &= df.index < end_date

        cmp_mask = window_mask & op_func(df[stop_day_col], reference_price)
        if cmp_mask.any():
            # First occurrence in the window
            first_stop_date = cmp_mask.idxmax()
            df.at[first_stop_date, result_col] = 1

    return df

def signal_statistics(
    dataframes: dict[str, pd.DataFrame],
    signal_column: str,
    bias: str = 'long',
    lookback: int = 2000
    ) -> Tuple[Dict[int, Dict], Dict]:
    """
    Calculate horizon-based return statistics for a given 0/1 signal across symbols.

    For each symbol's daily OHLCV DataFrame:

    - Restrict to the last ``lookback`` rows.
    - Identify all dates where ``df[signal_column] == 1``.
    - For each signal date:
      * Compute same-day open→close return (``'1day'``).
      * For each horizon in {5, 10, 20} calendar bars:
        - Take the slice from the signal index to ``signal_idx + days``.
        - For long bias:
          + If the maximum High exceeds the signal-day Close, use that High as
            the exit; otherwise use the minimum Low.
        - For short bias, invert the logic using Low/High.
        - Express the resultant exit vs signal-day Close as a percentage return.

    All returns are aggregated per symbol and across all symbols.

    Parameters
    ----------
    dataframes : dict[str, pd.DataFrame]
        Mapping from symbol to OHLCV DataFrame including the signal column.
    signal_column : str
        Name of the 0/1 signal column.
    bias : {'long', 'short'}, default 'long'
        Direction in which favourable moves are measured.
    lookback : int, default 2000
        Number of most recent rows to retain per symbol before analysis.

    Returns
    -------
    symbol_stats : dict
        Mapping ``{symbol: {...}}`` with keys:
        - ``'num_signals'``
        - ``'returns'``: {'1day', '5days', '10days', '20days'} → list[float]
        - ``'mean_returns'``: same keys → float
        - ``'signal_dates'``: list of dates.
    aggregate_stats : dict
        Same structure as a single ``symbol_stats`` entry but aggregated across
        all symbols.
    """
    
    symbol_stats = {}
    all_returns = {'1day': [], '5days': [], '10days': [], '20days': []}
    all_signal_dates = []
    total_signals = 0
    
    for sym, df in dataframes.items():
        # Limit to lookback period
        df = df[-lookback:].copy()
        
        # Get signal dates
        signal_dates = df.loc[df[signal_column] == 1].index
        num_signals = len(signal_dates)
        total_signals += num_signals
        
        returns = {'1day': [], '5days': [], '10days': [], '20days': []}
        signal_dates_list = list(signal_dates)
        all_signal_dates.extend(signal_dates_list)
        
        for signal_date in signal_dates:
            close_price = df.loc[signal_date, 'Close']
            open_price = df.loc[signal_date, 'Open']
            if bias == 'long':
                day_ret = ((close_price - open_price) / open_price) * 100
            else:
                day_ret = ((open_price - close_price) / open_price) * 100
            returns['1day'].append(day_ret)
            all_returns['1day'].append(day_ret)
            
            signal_idx = df.index.get_loc(signal_date)
            
            # Calculate returns for each period
            for days in [5, 10, 20]:
                key = f'{days}days'
                
                # Determine end index for the period
                end_idx = min(signal_idx + days + 1, len(df))
                period_data = df.iloc[signal_idx:end_idx]
                
                if len(period_data) <= 1:
                    continue
                    
                max_val = period_data['High'].max()
                min_val = period_data['Low'].min()                    
                if bias == 'long':                    
                    if max_val > close_price:
                        exit_price = max_val
                    else:
                        exit_price = min_val
                        
                elif bias == 'short':                    
                    if min_val < close_price:
                        exit_price = min_val
                    else:
                        exit_price = max_val
                
                # Calculate return
                if bias == 'long':
                    return_pct = ((exit_price - close_price) / close_price) * 100
                else:
                    return_pct = ((close_price - exit_price) / close_price) * 100
                    
                returns[key].append(return_pct)
                all_returns[key].append(return_pct)
        
        # Calculate mean returns for this symbol
        mean_returns = {}
        for key in returns:
            if returns[key]:
                mean_returns[key] = sum(returns[key]) / len(returns[key])
            else:
                mean_returns[key] = 0.0
        
        symbol_stats[sym] = {
            'num_signals': num_signals,
            'returns': returns,
            'mean_returns': mean_returns,
            'signal_dates': signal_dates_list
        }
    
    # Calculate aggregate statistics
    aggregate_mean_returns = {}
    for key in all_returns:
        if all_returns[key]:
            aggregate_mean_returns[key] = sum(all_returns[key]) / len(all_returns[key])
        else:
            aggregate_mean_returns[key] = 0.0
    
    aggregate_stats = {
        'num_signals': total_signals,
        'returns': all_returns,
        'mean_returns': aggregate_mean_returns,
        'signal_dates': all_signal_dates
    }
    
    return symbol_stats, aggregate_stats
    
def signal_statistics_optimized(
    dataframes: Dict[str, pd.DataFrame],
    indicator: str = 'SMA',
    base_col: str = 'Close',
    left_operand: int = 10,
    right_operand: int = 20,
    variance: int = 5,
    op: str = '>',
    bias: str = 'long',
    lookback: int = 2000) -> Tuple[Dict[Tuple[int, int], Dict[str, Dict]], Dict[Tuple[int, int], Dict]]:
    """
    Compare two *instances of the same technical indicator* with different
    lookback windows (e.g. ``10DMA > 20DMA``, ``9DMA > 19DMA``) across a
    range of periods, and compute return statistics via :func:`signal_statistics`.

    Instead of scanning a broad min/max window, this function explores a
    *local neighbourhood* around two anchor periods:

        left_period  ∈ [left_operand  - variance, left_operand  + variance]
        right_period ∈ [right_operand - variance, right_operand + variance]

    For each pair of periods ``(p_fast, p_slow)`` with
    ``p_fast`` taken from the left range, ``p_slow`` from the right range,
    and ``p_fast < p_slow``:

    - The requested moving average (currently ``SMA`` or ``EMA``) of ``base_col``
      is computed for both ``p_fast`` and ``p_slow`` if not already present.
    - A 0/1 **signal column** is created in the function namespace named::

          signal_{indicator_lower}_{base_col}_{p_fast}{op}{p_slow}

      e.g. ``signal_sma_Close_10>20``.
    - :func:`signal_statistics` is called with that signal column.
    - Aggregate mean, max and min returns are collected for 1/5/10/20 day horizons.

    Parameters
    ----------
    dataframes : dict[str, pd.DataFrame]
        Mapping of symbol -> price/indicator DataFrame.
    indicator : {'SMA', 'EMA'}, default 'SMA'
        Which moving-average function from :mod:`add_technicals` to use.
    base_col : str, default 'Close'
        Column over which the moving average is computed.
    left_operand : int, default 10
        Center of the period range for the "fast" moving average.
    right_operand : int, default 20
        Center of the period range for the "slow" moving average.
    variance : int, default 5
        Radius around each operand that defines the search window.
        For example, ``left_operand=10, right_operand=20, variance=5``
        yields:

            p_fast  ∈ [5, 15]
            p_slow  ∈ [15, 25]

        and the function evaluates all pairs with ``p_fast < p_slow``.
    op : {'>', '<', '>=', '<=', '==', '!='}, default '>'
        Comparison operator applied between the two MAs for each (p_fast, p_slow) pair.
    bias : {'long', 'short'}, default 'long'
        Direction of the trade, forwarded to :func:`signal_statistics`.
    lookback : int, default 2000
        Lookback window (in rows) forwarded to :func:`signal_statistics`.

    Returns
    -------
    pair_symbol_stats : dict
        Mapping ``(fast_period, slow_period) -> symbol_stats`` where
        ``symbol_stats`` has the same structure as :func:`signal_statistics`
        but is augmented with:
        
        - ``min_returns`` : dict[horizon -> float]
        - ``max_returns`` : dict[horizon -> float]
        
        and ``num_signals`` is an integer.
    pair_aggregate_stats : dict
        Mapping ``(fast_period, slow_period) -> aggregate_stats`` where
        ``aggregate_stats`` has the same base structure as
        :func:`signal_statistics` and is also augmented with
        ``min_returns`` / ``max_returns`` dictionaries of floats.

    Notes
    -----
    - The original input DataFrames are **not** modified; copies are created
      before adding moving-average and signal columns.
    - If no signals are generated for a given pair, the max/min statistics
      for that horizon are set to ``NaN``.
    """
    if left_operand <= 0 or right_operand <= 0:
        raise ValueError("left_operand and right_operand must be positive integers.")
    if variance < 0:
        raise ValueError("variance must be a non-negative integer.")

    # Map indicator name to the underlying implementation in add_technicals.py
    ma_funcs = {
        'SMA': SMA,
        'EMA': EMA,
    }
    if indicator not in ma_funcs:
        raise ValueError(f"Unsupported indicator {indicator!r}. Supported: {list(ma_funcs.keys())}.")
    ma_func = ma_funcs[indicator]

    # Operator mapping reused from elsewhere in this module
    _ops = {
        '>':  operator.gt,
        '<':  operator.lt,
        '>=': operator.ge,
        '<=': operator.le,
        '==': operator.eq,
        '!=': operator.ne,
    }
    if op not in _ops:
        raise ValueError(f"Unsupported operator {op!r}. Valid operators are {list(_ops.keys())}.")

    # Work on copies so we do not mutate the caller's data
    ma_frames: Dict[str, pd.DataFrame] = {sym: df.copy() for sym, df in dataframes.items()}

    # Determine the relevant period ranges around each operand
    left_start = max(1, left_operand - variance)
    left_end   = left_operand + variance
    right_start = max(1, right_operand - variance)
    right_end   = right_operand + variance

    left_range  = range(left_start, left_end + 1)
    right_range = range(right_start, right_end + 1)

    # Pre-compute the moving-average columns for all needed periods
    all_periods = sorted(set(list(left_range) + list(right_range)))
    for sym, df in ma_frames.items():
        if base_col not in df.columns:
            raise KeyError(f"Base column {base_col!r} not found in DataFrame for symbol {sym!r}.")
        for p in all_periods:
            col_name = f'{indicator}_{base_col}_{p}'
            if col_name not in df.columns:
                # ma_func mutates df in-place
                ma_func(df, base=base_col, target=col_name, period=p)

    horizons = ['1day', '5days', '10days', '20days']
    indicator_lower = indicator.lower()
    pair_symbol_stats: Dict[Tuple[int, int], Dict[str, Dict]] = {}
    pair_aggregate_stats: Dict[Tuple[int, int], Dict] = {}

    # Evaluate all ordered pairs p_fast < p_slow drawn from the two ranges.
    # Explicitly skip any pairs where the periods are equal to avoid
    # comparing identical windows (which can distort statistics).
    for p_fast in left_range:
        for p_slow in right_range:
            if p_fast == p_slow:
                continue  # never compare equal-period MAs
            if p_fast > p_slow:
                continue  # enforce "fast" MA has shorter lookback

            left_col = f'{indicator}_{base_col}_{p_fast}'
            right_col = f'{indicator}_{base_col}_{p_slow}'
            # Signal column lives purely inside this function's namespace;
            # its name is deterministic but not user-configurable.
            signal_col = f'signal_{indicator_lower}_{base_col}_{p_fast}{op}{p_slow}'

            # Create / overwrite the signal column on the MA-enriched frames
            for sym, df in ma_frames.items():
                cmp_result = _ops[op](df[left_col], df[right_col])
                df[signal_col] = cmp_result.astype(int)

            symbol_stats, aggregate_stats = signal_statistics(
                ma_frames,
                signal_column=signal_col,
                bias=bias,
                lookback=lookback
            )
            
            # Augment per-symbol stats with min/max per horizon
            for sym, stats in symbol_stats.items():
                min_ret: Dict[str, float] = {}
                max_ret: Dict[str, float] = {}
                for h in horizons:
                    vals = stats['returns'].get(h, [])
                    if vals:
                        min_ret[h] = float(min(vals))
                        max_ret[h] = float(max(vals))
                    else:
                        min_ret[h] = float('nan')
                        max_ret[h] = float('nan')
                stats['min_returns'] = min_ret
                stats['max_returns'] = max_ret
            
            # Augment aggregate stats with min/max per horizon
            agg_min: Dict[str, float] = {}
            agg_max: Dict[str, float] = {}
            for h in horizons:
                vals = aggregate_stats['returns'].get(h, [])
                if vals:
                    agg_min[h] = float(min(vals))
                    agg_max[h] = float(max(vals))
                else:
                    agg_min[h] = float('nan')
                    agg_max[h] = float('nan')
            aggregate_stats['min_returns'] = agg_min
            aggregate_stats['max_returns'] = agg_max
            
            key = (int(p_fast), int(p_slow))
            pair_symbol_stats[key] = symbol_stats
            pair_aggregate_stats[key] = aggregate_stats

    return pair_symbol_stats, pair_aggregate_stats

#These functions should be tested.
#*What will this function do if the exit/stop signals do not occur after the entry signal?
def compute_interday_expected_values(
    interday_signals: Dict[str, pd.DataFrame],
    price_data: Dict[str, pd.DataFrame],
    entry_signal: str,
    exit_signal: str,
    stop_signal: Union[str, List[str]],
    bias: str = 'long') -> Tuple[Dict[str, float], float]:
    """
    Compute per-symbol and aggregate expected values (EV) on a daily timeframe.

    For each symbol:

    - Iterate all dates in ``interday_signals[sym]``.
    - Whenever ``entry_signal`` is True:
      * Record the entry-day closing price.
      * Consider only future rows strictly after the entry date, up until the
        next entry (if any).
      * Find the earliest date where ``exit_signal`` is True.
      * Find the earliest date where any of the ``stop_signal`` columns is True.
      * If an exit occurs before or on the same date as the first stop, treat
        it as a positive outcome; otherwise, treat the first stop as a negative
        outcome.
      * Compute the realized return based on ``bias`` and append it to either
        ``pos_returns`` or ``neg_returns``.

    EV per symbol is defined as:

    .. math::
        EV = P_{pos} R_{pos} + P_{neg} R_{neg}

    using the empirical proportions and means of positive and negative returns.

    Parameters
    ----------
    interday_signals : dict[str, pd.DataFrame]
        Mapping symbol → DataFrame of boolean signals indexed by date.
    price_data : dict[str, pd.DataFrame]
        Mapping symbol → daily OHLCV DataFrame (same index as signals).
    entry_signal : str
        Column name marking entry events.
    exit_signal : str
        Column name marking positive exits.
    stop_signal : str or list[str]
        Column name(s) marking stop-loss events.
    bias : {'long', 'short'}, default 'long'
        Direction of the trade.

    Returns
    -------
    ev_by_symbol : dict[str, float]
        Expected value per symbol.
    ev_aggregate : float
        EV pooled across all symbols.
    """
    # Ensure stop_signals is a list to support multiple stop columns.
    if isinstance(stop_signal, str):
        stop_signals = [stop_signal]

    ev_by_symbol: Dict[str, float] = {}
    all_pos: List[float] = []
    all_neg: List[float] = []

    for sym, signals_df in interday_signals.items():
        price_df = price_data[sym]
        signals = signals_df.sort_index()  # ensure chronological

        pos_returns = []
        neg_returns = []

        for date in signals.index:
            # skip non-entry rows
            if not signals.at[date, entry_signal]:
                continue

            entry_price = price_df.at[date, 'Close']
            # Only consider signals strictly AFTER the entry day to prevent same-day exit/stop
            pos = signals.index.get_loc(date)
            if isinstance(pos, slice) or not np.isscalar(pos):
                # If duplicates exist, start after the last occurrence of this date
                if isinstance(pos, slice):
                    start_i = pos.stop
                else:
                    start_i = int(np.max(pos)) + 1
            else:
                start_i = pos + 1
            future = signals.iloc[start_i:]

            # find first exit
            exits = future.index[future[exit_signal] == True]
            exit_date = exits[0] if len(exits) > 0 else None

            # find first stop
            stops = []
            for stop_col in stop_signals:
                stops.extend(list(future.index[future[stop_col] == True]))
            stop_dates = sorted(set(stops))
            stop_date = stop_dates[0] if stop_dates else None

            # Decide which event comes first
            if exit_date and (not stop_date or exit_date <= stop_date):
                # positive outcome
                price_out = price_df.at[exit_date, 'Close']
                ret = ((price_out - entry_price) / entry_price
                       if bias == 'long'
                       else (entry_price - price_out) / entry_price)
                pos_returns.append(ret)
                all_pos.append(ret)
            elif stop_date:
                # negative outcome
                price_out = price_df.at[stop_date, 'Close']
                ret = ((price_out - entry_price) / entry_price
                       if bias == 'long'
                       else (entry_price - price_out) / entry_price)
                neg_returns.append(ret)
                all_neg.append(ret)
            # else: neither exit nor stop in the future → ignore

        # compute EV for this symbol
        total = len(pos_returns) + len(neg_returns)
        if total > 0:
            P_pos = len(pos_returns) / total
            P_neg = len(neg_returns) / total
            R_pos = np.mean(pos_returns) if pos_returns else 0.0
            R_neg = np.mean(neg_returns) if neg_returns else 0.0
            ev = float(np.dot([P_pos, P_neg], [R_pos, R_neg]))
        else:
            ev = float('nan')

        ev_by_symbol[sym] = ev

    # compute aggregate EV across all symbols
    total_all = len(all_pos) + len(all_neg)
    if total_all > 0:
        P_pos_all = len(all_pos) / total_all
        P_neg_all = len(all_neg) / total_all
        R_pos_all = np.mean(all_pos) if all_pos else 0.0
        R_neg_all = np.mean(all_neg) if all_neg else 0.0
        ev_aggregate = float(np.dot([P_pos_all, P_neg_all], [R_pos_all, R_neg_all]))
    else:
        ev_aggregate = float('nan')

    return ev_by_symbol, ev_aggregate

def conditional_probability(df, condition_col, outcome_col, condition_value=True, outcome_value=True, periods_ahead=1):
    """
    Compute P(outcome_{t+n} = outcome_value | condition_t = condition_value).

    The function looks at each row ``t`` where ``df[condition_col] ==
    condition_value`` and the shifted outcome ``df[outcome_col].shift(-n)``
    is non-NaN, and estimates the conditional probability that the shifted
    outcome equals ``outcome_value``.

    Parameters
    ----------
    df : pd.DataFrame
        Input data containing the condition and outcome columns.
    condition_col : str
        Column representing the condition at time t.
    outcome_col : str
        Column representing the outcome at time t.
    condition_value :
        Value treated as "condition holds". Default is True.
    outcome_value :
        Value treated as "outcome occurs". Default is True.
    periods_ahead : int, default 1
        How many rows ahead to look for the outcome.

    Returns
    -------
    float
        Estimated conditional probability in [0, 1], or NaN if the condition
        never occurs with a valid future outcome.
    """
    
    # Validate inputs
    if condition_col not in df.columns:
        raise ValueError(f"Condition column '{condition_col}' not found in DataFrame")
    if outcome_col not in df.columns:
        raise ValueError(f"Outcome column '{outcome_col}' not found in DataFrame")
    
    # Create shifted outcome column for future periods
    outcome_shifted = df[outcome_col].shift(-periods_ahead)
    
    # Find where condition is met
    condition_mask = df[condition_col] == condition_value
    
    # Count total condition occurrences (excluding last n periods where we can't check outcome)
    valid_condition_mask = condition_mask & ~outcome_shifted.isna()
    total_conditions = valid_condition_mask.sum()
    
    if total_conditions == 0:
        return float('nan')
    
    # Count where both condition is met AND outcome occurs in next period(s)
    both_occur = valid_condition_mask & (outcome_shifted == outcome_value)
    favorable_outcomes = both_occur.sum()
    
    # Calculate conditional probability
    probability = favorable_outcomes / total_conditions
    
    return float(probability)


#Intraday RVol objects
def intraday_rvol(symbols_list, date=None, lookback_days=20, 
                  timespan='second', multiplier=30, market_open_only=True):
    """
    Calculate intraday relative volume (RVol) profiles for a list of symbols.

    For each symbol, this function:

    - Imports intraday OHLCV data over a window ending at ``date`` (or the
      most recent trading day soif None).
    - Computes cumulative volume by day and timestamp.
    - For the target date, divides cumulative volume at each timestamp by the
      average cumulative volume at the same clock time across the previous
      ``lookback_days`` trading days.

    Parameters
    ----------
    symbols_list : list[str]
        List of stock symbols to analyze.
    date : str or datetime, optional
        Target date. If None, inferred from the most recent intraday data.
    lookback_days : int, default 20
        Number of prior trading days used to build the average volume profile.
    timespan : str, default 'second'
        Base timespan for intraday imports.
    multiplier : int, default 30
        Timespan multiplier for intraday imports.
    market_open_only : bool, default True
        Whether to restrict imports to regular market hours.

    Returns
    -------
    dict[str, pd.DataFrame]
        Mapping from symbol to a DataFrame (as returned by
        :func:`calculate_symbol_rvol`) containing cumulative volume and
        intraday RVol columns for the target date.
    """
    # from market_data.price_data_import import intraday_import
    # from concurrent.futures import ProcessPoolExecutor
    # import pandas as pd
    # import numpy as np
    # from datetime import datetime, timedelta
    
    # Handle date input
    if date is None:
        target_date = datetime.datetime.now().date()
    elif isinstance(date, str):
        target_date = pd.to_datetime(date).date()
    else:
        target_date = date.date() if hasattr(date, 'date') else date
    
    # Calculate date range (target date + previous lookback_days)
    start_date = target_date - timedelta(days=lookback_days + 10)  # Add buffer for weekends/holidays
    end_date = target_date
    
    # Import intraday data for all symbols
    print(f"Importing intraday data for {len(symbols_list)} symbols...")
    intraday_data = intraday_import(
        wl=symbols_list,
        from_date=start_date.strftime('%Y-%m-%d'),
        to_date=end_date.strftime('%Y-%m-%d'),
        timespan=timespan,
        multiplier=multiplier,
        market_open_only=market_open_only
    )
    
    if date is None:
        sample = next((df for df in intraday_data.values() if df is not None and not df.empty), None)
        if sample is None:
            raise ValueError("No intraday data found for any symbol")
        target_date = sample.index.date[-1]
    
    # Prepare data for multiprocessing
    symbol_data_tuples = [(symbol, intraday_data.get(symbol)) for symbol in symbols_list]
    
    # Create a partial function with the additional arguments
    calculate_rvol_partial = partial(calculate_symbol_rvol, target_date=target_date, lookback_days=lookback_days)
    
    # Use multiprocessing to calculate RVol for all symbols
    print("Calculating intraday RVol...")
    results = {}
    
    with ProcessPoolExecutor() as executor:
        for symbol, rvol_data in tqdm(executor.map(calculate_rvol_partial, symbol_data_tuples), 
                                      total=len(symbol_data_tuples), 
                                      desc="Calculating RVol"):
            if rvol_data is not None:
                results[symbol] = rvol_data
    
    print(f"Completed intraday RVol calculation for {len(results)} symbols")
    return results



#Seasonality/Cyclicality objects
def compute_cycle_strength(price: pd.Series,
                           trend_penalty_weight: float = 1.0) -> float:
    """
    Quantify how "cyclical" a price series is, penalizing strong trends.

    The score is higher when:

    - Peaks and troughs (after detrending) have relatively consistent sizes
      (low coefficient of variation), and
    - The underlying linear trend slope is small relative to the average price.

    The algorithm:

    - Fits and removes a linear trend.
    - Finds peaks and troughs in the detrended series.
    - Computes the coefficients of variation of peak and trough heights.
    - Forms a cycle-consistency score from their inverse.
    - Penalizes this score by an increasing function of the residual trend.

    Parameters
    ----------
    price : pd.Series
        Price series indexed by time.
    trend_penalty_weight : float, default 1.0
        Multiplier on the trend-strength penalty term.

    Returns
    -------
    float
        Scalar cycle-strength score (rounded to two decimals) or NaN if the
        series is too short or lacks sufficient peaks/troughs.
    """
    # 1) Clean up
    price = price.dropna()
    if len(price) < 10:
        return np.nan

    # 2) Fit & remove linear trend
    x = np.arange(len(price))
    slope, intercept, _, _, _ = linregress(x, price.values)
    trend = slope * x + intercept
    detrended = price.values - trend

    # 3) Find peaks and troughs
    peaks, _   = find_peaks(detrended)
    troughs, _ = find_peaks(-detrended)

    if len(peaks) < 2 or len(troughs) < 2:
        return np.nan

    peak_vals   = detrended[peaks]
    trough_vals = detrended[troughs]

    # 4) How much do those peak/trough heights *vary*?
    #    We use coefficient of variation (std/mean_abs) so it's scale-invariant
    cv_peaks   = np.std(peak_vals)   / (np.mean(np.abs(peak_vals))   + 1e-8)
    cv_troughs = np.std(trough_vals) / (np.mean(np.abs(trough_vals)) + 1e-8)

    cycle_consistency = 1.0 / (cv_peaks + cv_troughs + 1e-8)

    # 5) Penalize any remaining trend relative to price scale
    mean_price    = np.mean(price.values)
    trend_strength = abs(slope) / (mean_price + 1e-8)

    # 6) Final score
    return round(cycle_consistency / (1.0 + trend_penalty_weight * trend_strength), 2)

def rank_symbols_by_cycle_strength(data_dict: dict[str, pd.Series],
                                   trend_penalty_weight: float = 1.0
                                   ) -> pd.DataFrame:
    """
    Rank symbols by the cycle strength of their price series.

    Parameters
    ----------
    data_dict : dict[str, pd.Series]
        Mapping from symbol to price series.
    trend_penalty_weight : float, default 1.0
        Passed through to :func:`compute_cycle_strength`.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
        - ``'symbol'``
        - ``'cycle_strength'``

        Sorted in descending order by ``cycle_strength``.
    """
    scores = {}
    for sym, series in data_dict.items():
        try:
            score = compute_cycle_strength(series, trend_penalty_weight=trend_penalty_weight)
            if not np.isnan(score):
                scores[sym] = score
        except Exception as e:
            print(f"⚠️ Error processing {sym}: {e}")

    df = (
        pd.DataFrame.from_dict(scores, orient='index', columns=['cycle_strength'])
          .sort_values('cycle_strength', ascending=False)
          .reset_index()
          .rename(columns={'index': 'symbol'})
    )
    return df


def drawdown_current(df: pd.DataFrame, start: str|int):
    """
    Compute the current absolute drawdown since a given starting point.

    Starting from the row indicated by ``start``, this function:

    - Finds the maximum High price observed in ``df['High']``.
    - Compares it to the most recent closing price.
    - Returns the absolute price difference (peak minus last Close).

    Parameters
    ----------
    df : pd.DataFrame
        Daily OHLCV DataFrame with at least ``'High'`` and ``'Close'`` columns.
    start : str or int
        Either:
        - A label key (e.g. date string) usable with ``df.loc[start:]``, or
        - A positional index usable with ``df.loc[start:]`` when ``start`` is
          an integer.

    Returns
    -------
    float
        Absolute drawdown in price units from the maximum High since ``start``
        to the latest Close.
    """
    if isinstance(start, str):
        high = df['High'].loc[start:].max()
        low = df['Close'].iloc[-1]
        return high - low
    if isinstance(start, int):
        high = df['High'].loc[start:].max()
        low = df['Close'].iloc[-1]
        return high - low
    
def drawdown_max(df: pd.DataFrame, start: str|int|datetime.date):
    """
    Compute the maximum historical drawdown and its start/end dates.

    For all dates on or after ``start``, this function:

    - Scans all pairs of days ``(i, j)`` with ``j > i``.
    - For each pair where ``Low_j < High_i``, computes the drop
      ``High_i - Low_j``.
    - Tracks the largest such drop and records the corresponding
      start and end dates.

    Parameters
    ----------
    df : pd.DataFrame
        Daily OHLCV DataFrame with columns ``'High'`` and ``'Low'`` and a
        date-like index.
    start : str or int or datetime.date
        Starting point for the analysis. May be:
        - A label key (e.g. date string) for ``df.loc[start:]``,
        - A :class:`datetime.date`, or
        - A positional index integer.

    Returns
    -------
    dict
        A dict mapping ``"start_date:end_date"`` (concatenated string key) to
        the maximum drawdown magnitude over that period. If no valid pairs are
        found, the dict will be empty.
    """
    if isinstance(start, str):
        df = df[['High', 'Low']].loc[start:]
        df['Date'] = df.index
        # initialize max diff and dates
        max_diff = 0
        start_date = ''
        end_date = ''
        max_diff_dict = {}
        # loop through each row and compare the current high to all lows that come after it
        for i in range(len(df)):
            for j in range(i+1, len(df)):
                if df['Low'][j] < df['High'][i]:
                    diff = df['High'][i] - df['Low'][j]
                    if diff > max_diff:
                        max_diff = diff
                        start_date = df['Date'][i]
                        end_date = df['Date'][j]
                        max_diff_dict[start_date + ':'+ end_date] = max_diff

        return max_diff_dict
    
    elif isinstance(start, datetime.date):
        df = df[['High', 'Low']].loc[start:]
        df['Date'] = df.index
        # initialize max diff and dates
        max_diff = 0
        start_date = ''
        end_date = ''
        max_diff_dict = {}
        # loop through each row and compare the current high to all lows that come after it
        for i in range(len(df)):
            for j in range(i+1, len(df)):
                if df['Low'][j] < df['High'][i]:
                    diff = df['High'][i] - df['Low'][j]
                    if diff > max_diff:
                        max_diff = diff
                        start_date = df['Date'][i]
                        end_date = df['Date'][j]
                        max_diff_dict[start_date + ':'+ end_date] = max_diff
        return max_diff_dict
    
    elif isinstance(start, int):
        df = df[['High', 'Low']].loc[start:]
        df['Date'] = df.index
        # initialize max diff and dates
        max_diff = 0
        start_date = ''
        end_date = ''
        max_diff_dict = {}
        # loop through each row and compare the current high to all lows that come after it
        for i in range(len(df)):
            for j in range(i+1, len(df)):
                if df['Low'][j] < df['High'][i]:
                    diff = df['High'][i] - df['Low'][j]
                    if diff > max_diff:
                        max_diff = diff
                        start_date = df['Date'][i]
                        end_date = df['Date'][j]
                        max_diff_dict[start_date + ':'+ end_date] = max_diff
        return max_diff_dict
    else:
        raise ValueError(f"Invalid type for start: {type(start)}")

def price_volume_analysis(
    symbols: dict[str, pd.DataFrame],
    target: str = 'Close') -> dict[str, float]:
    """
    Statistical price-volume analysis via correlation of log changes.

    For each symbol, computes:
    - log return of `target`: diff(log(price))
    - log change in Volume: diff(log(volume))
    Then returns their Pearson correlation.

    Parameters
    ----------
    symbols : dict[str, pd.DataFrame]
        Mapping of ticker -> OHLCV DataFrame. Must contain `target` and `Volume`.
    target : str, default 'Close'
        Column name used as the price series for log returns.

    Returns
    -------
    dict[str, float]
        Dictionary sorted descending by correlation (NaNs last).
    """
    results: dict[str, float] = {}

    for sym, df in symbols.items():
        if df is None or getattr(df, "empty", True):
            results[sym] = np.nan
            continue

        if target not in df.columns or 'Volume' not in df.columns:
            results[sym] = np.nan
            continue

        # Ensure numeric and avoid invalid log inputs.
        price = pd.to_numeric(df[target], errors='coerce').astype('float64')
        vol = pd.to_numeric(df['Volume'], errors='coerce').astype('float64')

        price = price.where(price > 0)
        vol = vol.where(vol > 0)

        log_ret = np.log(price).diff()
        log_vol_chg = np.log(vol).diff()

        aligned = pd.concat(
            [log_ret.rename('log_ret'), log_vol_chg.rename('log_vol_chg')],
            axis=1
        ).dropna()

        if len(aligned) < 2:
            results[sym] = np.nan
        else:
            results[sym] = float(aligned['log_ret'].corr(aligned['log_vol_chg']))

    # Descending sort by correlation, placing NaNs last.
    return dict(
        sorted(
            results.items(),
            key=lambda kv: (pd.isna(kv[1]), -(kv[1] if not pd.isna(kv[1]) else 0.0)),
        )
    )
