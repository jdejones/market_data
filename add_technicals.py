from __future__ import annotations
from dataclasses import dataclass, field
from functools  import partial
from typing     import Callable, Dict, Any, List
from market_data import pd, datetime



def SMA(df, base, target, period):
    """
    Function to compute Simple Moving Average (SMA)
    
    Args :
        df : Pandas DataFrame which contains ['date', 'open', 'high', 'low', 'close', 'volume'] columns
        base : String indicating the column name from which the SMA needs to be computed from
        target : String indicates the column name to which the computed data needs to be stored
        period : Integer indicates the period of computation in terms of number of candles
        
    Returns :
        df : Pandas DataFrame with new column added with name 'target'
    """
    df[target] = df[base].rolling(window=period).mean().fillna(0)
    return df


def AVWAP_by_date(df, date):
    df['CxV']=df['Close'] * df['Volume']
    #df['CxV']= (df.iloc[-days:, df.columns.get_level_values(0)=='Volume'].fillna(0)*df.iloc[-days:, df.columns.get_level_values(0)=='Close']).fillna(0, inplace=True)
    df['Sum CxV']=df.loc[date:, df.columns.get_level_values(0)=='CxV'].cumsum().fillna(0)
    df['Sum Volume'] = df.loc[date:, df.columns.get_level_values(0)=='Volume'].cumsum()
    #df['VWAP']=(df.iloc[-days:, df.columns.get_level_values(0)=='Sum CxV']/df.iloc[-days:, df.columns.get_level_values(0)=='Volume'].cumsum()).fillna(0)
    df['VWAP %s' % date]=df['Sum CxV']/df['Sum Volume']
    
    return df
    
def AVWAP_rolling(df, days):
    df['CxV']=df['Close']*df['Volume']
    #df['CxV']= (df.iloc[-days:, df.columns.get_level_values(0)=='Volume'].fillna(0)*df.iloc[-days:, df.columns.get_level_values(0)=='Close']).fillna(0, inplace=True)
    df['Sum CxV']=df.iloc[:, df.columns.get_level_values(0)=='CxV'].rolling(days).sum()
    df['Sum Volume'] = df.iloc[:, df.columns.get_level_values(0)=='Volume'].rolling(days).sum()
    #df['VWAP']=(df.iloc[-days:, df.columns.get_level_values(0)=='Sum CxV']/df.iloc[-days:, df.columns.get_level_values(0)=='Volume'].cumsum()).fillna(0)
    df['VWAP_%s' % days]=df['Sum CxV']/df['Sum Volume']
    
    return df


def ATR(df, period, ohlc=['Open', 'High', 'Low', 'Close']):
    """
    Function to compute Average True Range (ATR)
    
    Args :
        df : Pandas DataFrame which contains ['date', 'open', 'high', 'low', 'close', 'volume'] columns
        period : Integer indicates the period of computation in terms of number of candles
        ohlc: List defining OHLC Column names (default ['Open', 'High', 'Low', 'Close'])
        
    Returns :
        df : Pandas DataFrame with new columns added for 
            True Range (TR)
            ATR (ATR_$period)
    """
    atr = 'ATR_' + str(period)
    # Compute true range only if it is not computed and stored earlier in the df
    if not 'TR' in df.columns:
        tr = pd.concat(
            [
                (df[ohlc[1]] - df[ohlc[2]]),
                (df[ohlc[1]] - df[ohlc[3]].shift()).abs(),
                (df[ohlc[2]] - df[ohlc[3]].shift()).abs(),
            ],
            axis=1,
        ).max(axis=1)
        df["TR"] = tr
    # Compute EMA of true range using ATR formula after ignoring first row
    EMA(df, 'TR', atr, period, alpha=True)
    
    return df

def EMA(df, base, target, period, alpha=False):
    """
    Function to compute Exponential Moving Average (EMA)
    
    Args :
        df : Pandas DataFrame which contains ['date', 'open', 'high', 'low', 'close', 'volume'] columns
        base : String indicating the column name from which the EMA needs to be computed from
        target : String indicates the column name to which the computed data needs to be stored
        period : Integer indicates the period of computation in terms of number of candles
        alpha : Boolean if True indicates to use the formula for computing EMA using alpha (default is False)
        
    Returns :
        df : Pandas DataFrame with new column added with name 'target'
    """
    con = pd.concat([df[:period][base].rolling(window=period).mean(), df[period:][base]])
    
    if (alpha == True):
        # (1 - alpha) * previous_val + alpha * current_val where alpha = 1 / period
        df[target] = con.ewm(alpha=1 / period, adjust=False).mean().fillna(0)
    else:
        # ((current_val - previous_val) * coeff) + previous_val where coeff = 2 / (period + 1)
        df[target] = con.ewm(span=period, adjust=False).mean().fillna(0)
    
    return df

def RSI(df, base="Close", period=21):
    """
    Function to compute Relative Strength Index (RSI)
    
    Args :
        df : Pandas DataFrame which contains ['date', 'open', 'high', 'low', 'close', 'volume'] columns
        base : String indicating the column name from which the MACD needs to be computed from (Default Close)
        period : Integer indicates the period of computation in terms of number of candles
        
    Returns :
        df : Pandas DataFrame with new columns added for 
            Relative Strength Index (RSI_$period)
    """
 
    delta = df[base].diff()
    up, down = delta.copy(), delta.copy()

    up[up < 0] = 0
    down[down > 0] = 0
    
    rUp = up.ewm(com=period - 1,  adjust=False).mean()
    rDown = down.ewm(com=period - 1, adjust=False).mean().abs()

    df['RSI_' + str(period)] = 100 - (100 / (1 + (rUp / rDown))).fillna(0)

    return df

def Stochastics(df, k, d):
    df['n_high'] = df['High'].rolling(k).max()
    df['n_low'] = df['Low'].rolling(k).min()
    df['%k'] = (df['Close'] - df['n_low']) * 100 / (df['n_high'] - df['n_low'])
    df['%d'] = df['%k'].rolling(d).mean()
    df.drop(['n_high'], axis=1)
    df.drop(['n_low'], axis=1)
    
    return df

def get_adx(df, high, low, close, lookback):
    plus_dm = df[high].diff()
    minus_dm = df[low].diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm > 0] = 0
    
    tr1 = pd.DataFrame(df[high] - df[low])
    tr2 = pd.DataFrame(abs(df[high] - df[close].shift(1)))
    tr3 = pd.DataFrame(abs(df[low] - df[close].shift(1)))
    frames = [tr1, tr2, tr3]
    tr = pd.concat(frames, axis = 1, join = 'inner').max(axis = 1)
    atr = tr.rolling(lookback).mean()
    
    plus_di = 100 * (plus_dm.ewm(alpha = 1/lookback).mean() / atr)
    minus_di = abs(100 * (minus_dm.ewm(alpha = 1/lookback).mean() / atr))
    dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
    adx = ((dx.shift(1) * (lookback - 1)) + dx) / lookback
    adx_smooth = adx.ewm(alpha = 1/lookback).mean()
    df['+DI'] = plus_di
    df['-DI'] = minus_di
    df['ADX'] = adx_smooth
    
    return df

def MACD(df, base, short_period, long_period, ma_type='simple'):
    if ma_type == 'simple':
        #df['MACD'] = SMA(df, base = base, target = 'MACD_Short', period = short_period) - SMA(df, base = base, target = 'MACD_Long',period = long_period)
        #df['Short_SMA'] = SMA(df, base, target=str(short_period) + 'SMA', period = short_period)
        #df['Long_SMA'] = SMA(df, base, target = str(long_period) + 'SMA', period = long_period)
        #df['MACD'] = float(df[base].rolling(window=short_period).mean().fillna(0, inplace=True)) - float(df[base].rolling(window=long_period).mean().fillna(0, inplace=True))
        df[str(short_period) + 'SMA'] = df[base].rolling(window=short_period).mean().fillna(0)
        df[str(long_period) + 'SMA'] = df[base].rolling(window=long_period).mean().fillna(0)
        df[f'sMACD{short_period}{long_period}'] = df[str(short_period) + 'SMA'].sub(df[str(long_period) + 'SMA'])
        
        return df
    elif ma_type == 'exponential':
        # df[f'e{str(short_period)}'] = 
        EMA(df, base, f'ema{short_period}', short_period)
        # df[f'e{str(long_period)}'] = 
        EMA(df, base, f'ema{long_period}', long_period)
        # df[f'eMACD{short_period}{long_period}'] = df[f'e{str(short_period)}'].sub(df[f'e{str(long_period)}'])
        df[f'eMACD{short_period}{long_period}'] =  df[f'ema{short_period}'].sub(df[f'ema{long_period}'])
        
        return df

def relative_macd(df, base):
    df[f'relative_macd_{base}'] = df[base] / df['ATR_14']
    
    return df
    
def add_avwap_by_offset(df, offset: int|str):
    # if len(df) > offset:
    if isinstance(offset, int):
        date_idx = df.index[-offset]
    elif isinstance(offset, str):
        date_idx = offset
        # col = f'VWAP_{offset}D' I believe this is unnecessary and added by a.i.
        # call your existing AVWAP_by_date(df, date_idx) which writes the column
    AVWAP_by_date(df, date_idx)
        # df.rename(columns={f'VWAP {date_idx}': col}, inplace=True) I believe this is unnecessary and added by a.i.
    # else:
    #     df[f'VWAP {date_idx}'] = np.nan
    return df

def atrs_from(df, base: str|int):
    if isinstance(base, str):
        df[f'ATRs_from_{base}'] = (df['Close'] - df[base]) / df['ATR_14']
    elif isinstance(base, int):#I added this to try and handle the AVWAPs in the second pipeline.append().
        date_idx = df.index[-base]
        df[f'ATRs_from_AVWAP_{date_idx}'] = (df['Close'] - df[f'VWAP {date_idx}']) / df['ATR_14']
    
    return df

def diff_from_signal(df, base, signal):
    df[f'{base}_diff'] = df[base] - df[signal]
    return df

def gap(df, base):
    df['Gap'] = (((df['Open'] - df['Close'].shift(1)) / df['Close'].shift(1)) * 100)
    
    return df

def RVol(df, base):
    df['RVol'] = (df['Volume'] / df['AvgV20'])
    
    return df

def dollar_volume(df, base):
    df['Dollar_Volume'] = df['Volume'] * df['Close']
    
    return df

def percent_change(df, base):
    df['Percent_Change'] = ((df['Close'] - df['Close'].shift(1)) / df['Close'].shift(1)) * 100
    df['C-O'] = ((df['Close'] - df['Open']) / df['Open']) * 100
    
    return df

def atrs_traded_ex_gap(df):
    df['ATRs Traded_ex_gap'] = (df['Close'] - df['Open'].shift(1)) / df['ATR_14']
    
    return df

def relative_atr(df):
    df['Relative_ATR'] = (df['ATR_14'] / df['Close']) * 100
    
    return df

def atrs_traded(df):
    df['H-L'] = df['High'] - df['Low']
    df['H-Cp'] = abs(df['High'] - df['Close'].shift(1))
    df['L-Cp'] = abs(df['Low'] - df['Close'].shift(1)) 
    df['ATRs_Traded'] = df[['H-L', 'H-Cp', 'L-Cp']].max(axis=1) / df['ATR_14']
    return df

def bollinger_bands(df, base: pd.Series, window: int=20, std: int=2, rbd: bool=True, ema: bool=False):
    if ema:
        ma_series = df[base].ewm(span=window, adjust=False).mean()
    else:
        ma_series = df[base].rolling(window=window).mean()
    rolling_std = df[base].rolling(window=window).std()
    df[f'u_band'] = ma_series + (std * rolling_std)
    df[f'l_band'] = ma_series - (std * rolling_std)
    if rbd == True:
        df['relative_band_dist'] = ((df['u_band'] - df['l_band']) / df['ATR_14'])
    return df

def relative_diff_from_signal(df, base, target):
    df[f'relative_{target}_diff'] = df[base] / df['ATR_14']
    return df

# Intraday groupby functions
def VWAP_(group):
    group = group.copy()
    group['DxV'] = group['Volume'] * group['Close']
    group['Sumb DxV'] = group['DxV'].cumsum()
    group['VWAP'] = group['Sumb DxV'] / group['Volume'].cumsum()
    
    return group

def vwap_bands(group):
    group = group.copy()
    ma_series = group['VWAP'].ewm(span=9, adjust=False).mean()
    rolling_std = group['VWAP'].rolling(window=9).std()
    group['vwap_uband_1std_byday'] = ma_series + rolling_std
    group['vwap_uband_2std_byday'] = ma_series + (2 * rolling_std)
    group['vwap_uband_3std_byday'] = ma_series + (3 * rolling_std)
    
    group['vwap_lband_1std_byday'] = ma_series - rolling_std
    group['vwap_lband_2std_byday'] = ma_series - (2 * rolling_std)
    group['vwap_lband_3std_byday'] = ma_series - (3 * rolling_std)
    
    return group

def byday_emas(group):
    group = group.copy()
    group['close_5ema_byday'] = group['Close'].ewm(span=5, adjust=False).mean()
    group['close_9ema_byday'] = group['Close'].ewm(span=9, adjust=False).mean()
    group['close_20ema_byday'] = group['Close'].ewm(span=20, adjust=False).mean()
    
    group['high_5ema_byday'] = group['High'].ewm(span=5, adjust=False).mean()
    group['high_9ema_byday'] = group['High'].ewm(span=9, adjust=False).mean()
    group['high_20ema_byday'] = group['High'].ewm(span=20, adjust=False).mean()
    
    group['low_5ema_byday'] = group['Low'].ewm(span=5, adjust=False).mean()
    group['low_9ema_byday'] = group['Low'].ewm(span=9, adjust=False).mean()
    group['low_20ema_byday'] = group['Low'].ewm(span=20, adjust=False).mean()
    
    return group

def byday_bands(group):
    group = group.copy()
    ma_series = group['Close'].ewm(span=9, adjust=False).mean()
    rolling_std = group['Close'].rolling(window=9).std()
    group['close_uband_1std_byday'] = ma_series + rolling_std
    group['close_uband_2std_byday'] = ma_series + (2 * rolling_std)
    group['close_uband_3std_byday'] = ma_series + (3 * rolling_std)
    
    group['close_lband_1std_byday'] = ma_series - rolling_std
    group['close_lband_2std_byday'] = ma_series - (2 * rolling_std)
    group['close_lband_3std_byday'] = ma_series - (3 * rolling_std)

    return group


@dataclass(slots=True)
class Step:
    func: Callable              # e.g. add_sma
    kwargs: Dict[str, Any] = field(default_factory=dict)
    needs: List[str] = field(default_factory=list)   # columns required before running
    adds:  List[str] = field(default_factory=list)   # columns the step promises to create

    def __call__(self, df):
        # quick dependency check (optional but handy while refactoring)
        missing = [c for c in self.needs if c not in df.columns]
        if missing:
            raise KeyError(f"Missing prerequisites {missing} for {self.func.__name__}")
        return self.func(df, **self.kwargs)


pipeline = [
    Step(SMA, kwargs={'base': 'Close', 'target': '5DMA', 'period': 5}, needs=['Close'], adds=['5DMA']),    
    Step(SMA, kwargs={'base': 'Close', 'target': '10DMA', 'period': 10}, needs=['Close'], adds=['10DMA']),    
    Step(SMA, kwargs={'base': 'Close', 'target': '20DMA', 'period': 20}, needs=['Close'], adds=['20DMA']),    
    Step(SMA, kwargs={'base': 'Close', 'target': '50DMA', 'period': 50}, needs=['Close'], adds=['50DMA']),    
    Step(SMA, kwargs={'base': 'Close', 'target': '200DMA', 'period': 200}, needs=['Close'], adds=['200DMA']),    
    Step(SMA, kwargs={'base': 'Volume', 'target': 'AvgV20', 'period': 20}, needs=['Volume'], adds=['AvgV20']),
    Step(RVol, kwargs={'base': 'Volume'}, needs=['Volume', 'AvgV20'], adds=['RVol']),
    Step(dollar_volume, kwargs={'base': 'Volume'}, needs=['Volume', 'Close'], adds=['Dollar_Volume']),
    Step(SMA, kwargs={'base': 'Dollar_Volume', 'target': 'AvgDV20', 'period': 20}, needs=['Dollar_Volume'], adds=['AvgDV20']),
    Step(ATR, kwargs={'period': 14}, needs=['High', 'Low', 'Close'], adds=['ATR_14']),
    Step(atrs_from, kwargs={'base': '5DMA'}, needs=['Close', 'ATR_14'], adds=['ATRs_from_5DMA']),
    Step(atrs_from, kwargs={'base': '10DMA'}, needs=['Close', 'ATR_14'], adds=['ATRs_from_10DMA']),
    Step(atrs_from, kwargs={'base': '20DMA'}, needs=['Close', 'ATR_14'], adds=['ATRs_from_20DMA']),
    Step(atrs_from, kwargs={'base': '50DMA'}, needs=['Close', 'ATR_14'], adds=['ATRs_from_50DMA']),
    Step(atrs_from, kwargs={'base': '200DMA'}, needs=['Close', 'ATR_14'], adds=['ATRs_from_200DMA']),
    Step(EMA, kwargs={'base': 'ATR_14', 'target': 'ATR_14_signal', 'period': 9}, needs=['ATR_14'], adds=['ATR_14_signal']),
    Step(RSI, kwargs={'period': 14}, needs=['Close'], adds=['RSI_14']),
    Step(EMA, kwargs={'base': 'RSI_14', 'target': 'RSI_14_signal', 'period': 9}, needs=['RSI_14'], adds=['RSI_14_signal']),
    Step(diff_from_signal, kwargs={'base': 'RSI_14', 'signal': 'RSI_14_signal'}, needs=['RSI_14', 'RSI_14_signal'], adds=['RSI_14_diff']),
    Step(Stochastics, kwargs={'k': 14, 'd': 3}, needs=['High', 'Low', 'Close'], adds=['n_high', 'n_low', '%k', '%d']),
    Step(diff_from_signal, kwargs={'base': '%k', 'signal': '%d'}, needs=['%k', '%d'], adds=['%k_diff']),
    Step(get_adx, kwargs={'high': 'High', 'low': 'Low', 'close': 'Close', 'lookback': 14}, needs=['High', 'Low', 'Close'], adds=['+DI', '-DI', 'ADX']),
    Step(EMA, kwargs={'base': 'ADX', 'target': 'ADX_signal', 'period': 10}, needs=['ADX'], adds=['ADX_signal']),
    Step(diff_from_signal, kwargs={'base': 'ADX', 'signal': 'ADX_signal'}, needs=['ADX', 'ADX_signal'], adds=['ADX_diff']),
    Step(MACD, kwargs={'base': 'Close','short_period': 12, 'long_period': 26, 'ma_type': 'exponential'}, needs=['Close'], adds=['ema12', 'ema26', 'eMACD1226']),
    Step(relative_macd, kwargs={'base': 'eMACD1226'}, needs=['eMACD1226', 'ATR_14'], adds=['relative_macd_eMACD1226']),
    Step(EMA, kwargs={'base': 'eMACD1226', 'target': 'eMACD1226_signal', 'period': 9}, needs=['eMACD1226'], adds=['eMACD1226_signal']),
    Step(diff_from_signal, kwargs={'base': 'eMACD1226', 'signal': 'eMACD1226_signal'}, needs=['eMACD1226', 'eMACD1226_signal'], adds=['eMACD1226_diff']),
    Step(relative_diff_from_signal, kwargs={'base': 'eMACD1226_diff', 'target':'macd'}, needs=['eMACD1226_diff', 'ATR_14'], adds=['relative_macd_diff']),
    Step(MACD, kwargs={'base': 'Close', 'short_period': 10, 'long_period': 20, 'ma_type': 'exponential'}, needs=['Close'], adds=['ema10', 'ema20', 'eMACD1020']),
    Step(EMA, kwargs={'base': 'eMACD1020', 'target': 'eMACD1020_signal', 'period': 9}, needs=['eMACD1020'], adds=['eMACD1020_signal']),
    Step(diff_from_signal, kwargs={'base': 'eMACD1020', 'signal': 'eMACD1020_signal'}, needs=['eMACD1020', 'eMACD1020_signal'], adds=['eMACD1020_diff']),
    Step(relative_macd, kwargs={'base': 'eMACD1020'}, needs=['eMACD1020', 'ATR_14'], adds=['relative_macd_eMACD1020']),
    Step(AVWAP_rolling, kwargs={'days': 5}, needs=['Close', 'Volume'], adds=['VWAP_5']),
    Step(AVWAP_rolling, kwargs={'days': 10}, needs=['Close', 'Volume'], adds=['VWAP_10']),
    Step(AVWAP_rolling, kwargs={'days': 20}, needs=['Close', 'Volume'], adds=['VWAP_20']),
    Step(atrs_from, kwargs={'base': 'VWAP_5'}, needs=['Close', 'ATR_14'], adds=['ATRs_from_VWAP_5']),
    Step(atrs_from, kwargs={'base': 'VWAP_10'}, needs=['Close', 'ATR_14'], adds=['ATRs_from_VWAP_10']),
    Step(atrs_from, kwargs={'base': 'VWAP_20'}, needs=['Close', 'ATR_14'], adds=['ATRs_from_VWAP_20']),
    Step(gap, kwargs={'base': 'Close'}, needs=['Open', 'Close'], adds=['Gap']),
    Step(percent_change, kwargs={'base': 'Close'}, needs=['Close'], adds=['Percent_Change']),
    Step(atrs_traded_ex_gap, needs=['Close', 'Open', 'ATR_14'], adds=['ATRs Traded_ex_gap']),
    Step(relative_atr, needs=['ATR_14'], adds=['Relative_ATR']),
    Step(atrs_traded, needs=['High', 'Low', 'Close'], adds=['H-L', 'H-Cp', 'L-Cp', 'ATRs_Traded']),
    Step(bollinger_bands, kwargs={'base': 'Close', 'window': 20, 'std': 2, 'rbd': True}, needs=['Close', 'ATR_14'], adds=['u_band', 'l_band', 'relative_band_dist'])
]

for off in (5, 10, 20):
    pipeline.append(
        Step(add_avwap_by_offset, kwargs=dict(offset=off),
             needs=['Close', 'Volume'], adds=[f'VWAP_{off}'])
    )
    pipeline.append(
        Step(atrs_from, kwargs={'base': off}, needs=['Close', 'ATR_14'], adds=[f'ATRs_from_VWAP_{off}'])
    )
pipeline.append(
    Step(add_avwap_by_offset, kwargs={'offset': datetime.datetime(datetime.datetime.today().year, 1, 1).strftime('%Y-%m-%d')}, needs=['Close', 'Volume'], adds=[f'ATRs_from_VWAP_{datetime.datetime(datetime.datetime.today().year, 1, 1).strftime('%Y-%m-%d')}'])
)


intraday_pipeline = [
    Step(EMA, kwargs={'base': 'Close', 'target': 'ema5', 'period': 5}, needs=['Close'], adds=['ema5']),
    Step(EMA, kwargs={'base': 'Close', 'target': 'ema9', 'period': 9}, needs=['Close'], adds=['ema9']),
    Step(EMA, kwargs={'base': 'Close', 'target': 'ema20', 'period': 20}, needs=['Close'], adds=['ema20']),
    Step(ATR, kwargs={'period': 14}, needs=['High', 'Low', 'Close'], adds=['ATR_14']),
    Step(atrs_traded, needs=['High', 'Low', 'Close'], adds=['H-L', 'H-Cp', 'L-Cp', 'ATRs_Traded']),
    Step(bollinger_bands, kwargs={'base': 'Close', 'window': 20, 'std': 2, 'rbd': True, 'ema': True}, needs=['Close', 'ATR_14'], adds=['u_band', 'l_band', 'relative_band_dist']),
    Step(MACD, kwargs={'base': 'Close', 'short_period': 5, 'long_period': 9, 'ma_type': 'exponential'}, needs=['Close'], adds=['ema5', 'ema9', 'eMACD59']),
    Step(EMA, kwargs={'base': 'eMACD59', 'target': 'eMACD59_signal', 'period': 9}, needs=['eMACD59'], adds=['eMACD59_signal']),
    Step(diff_from_signal, kwargs={'base': 'eMACD59', 'signal': 'eMACD59_signal'}, needs=['eMACD59', 'eMACD59_signal'], adds=['eMACD59_diff']),
    Step(relative_diff_from_signal, kwargs={'base': 'eMACD59_diff', 'target':'macd59'}, needs=['eMACD59_diff', 'ATR_14'], adds=['relative_macd59_diff']),
    Step(MACD, kwargs={'base': 'Close', 'short_period': 5, 'long_period': 20, 'ma_type': 'exponential'}, needs=['Close'], adds=['ema5', 'ema20', 'eMACD520']),
    Step(EMA, kwargs={'base': 'eMACD520', 'target': 'eMACD520_signal', 'period': 9}, needs=['eMACD520'], adds=['eMACD520_signal']),
    Step(diff_from_signal, kwargs={'base': 'eMACD520', 'signal': 'eMACD520_signal'}, needs=['eMACD520', 'eMACD520_signal'], adds=['eMACD520_diff']),
    Step(relative_diff_from_signal, kwargs={'base': 'eMACD520_diff', 'target':'macd520'}, needs=['eMACD520_diff', 'ATR_14'], adds=['relative_macd520_diff']),
    Step(MACD, kwargs={'base': 'Close', 'short_period': 9, 'long_period': 20, 'ma_type': 'exponential'}, needs=['Close'], adds=['ema9', 'ema20', 'eMACD920']),
    Step(EMA, kwargs={'base': 'eMACD920', 'target': 'eMACD920_signal', 'period': 9}, needs=['eMACD920'], adds=['eMACD920_signal']),
    Step(diff_from_signal, kwargs={'base': 'eMACD920', 'signal': 'eMACD920_signal'}, needs=['eMACD920', 'eMACD920_signal'], adds=['eMACD920_diff']),
    Step(relative_diff_from_signal, kwargs={'base': 'eMACD920_diff', 'target':'macd920'}, needs=['eMACD920_diff', 'ATR_14'], adds=['relative_macd920_diff']),
    Step(VWAP_, needs=['Close', 'Volume'], adds=['VWAP', 'DxV', 'Sumb DxV']),
    Step(vwap_bands, needs=['VWAP'], adds=['vwap_uband_1std_byday', 'vwap_uband_2std_byday', 'vwap_uband_3std_byday', 'vwap_lband_1std_byday', 'vwap_lband_2std_byday', 'vwap_lband_3std_byday']),
    Step(byday_emas, needs=['Close', 'High', 'Low'], adds=['close_5ema_byday', 'close_9ema_byday', 'close_20ema_byday', 'high_5ema_byday', 'high_9ema_byday', 'high_20ema_byday', 'low_5ema_byday', 'low_9ema_byday', 'low_20ema_byday']),
    Step(byday_bands, needs=['Close'], adds=['close_uband_1std_byday', 'close_uband_2std_byday', 'close_uband_3std_byday', 'close_lband_1std_byday', 'close_lband_2std_byday', 'close_lband_3std_byday']),
]



def run_pipeline(df: pd.DataFrame, steps: List[Step] = pipeline) -> pd.DataFrame:
    #*I commented out some original code to modify the dataframe in place.
    #*This should allow for easier access to processed dataframes.
    # out = df.copy(deep=False)   # shallow copy keeps memory down; we mutate in‑place
    # for step in steps:
    #     # out = step(df)         # dataclass is callable
    #     step(df)
    # # return out
    
    
    out = df
    for step in steps:
        out = step(out)
    return out

def _add_technicals_worker(item: tuple[str, pd.DataFrame]) -> tuple[str, pd.DataFrame]:
    # symbol, df = item
    # run_pipeline(df)
    # return symbol, df
    
    
    symbol, df = item
    df = run_pipeline(df)
    return symbol, df

def _add_intraday_technicals_worker(item: tuple[str, pd.DataFrame]) -> tuple[str, pd.DataFrame]:
    # symbol, df = item
    # run_pipeline(df, steps=intraday_pipeline)
    # return symbol, df

    
    symbol, df = item
    df = run_pipeline(df, steps=intraday_pipeline)
    return symbol, df

