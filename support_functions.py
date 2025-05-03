import yfinance as yf
from market_data.price_data_import import api_import
from market_data.add_technicals import RSI
from market_data.watchlist_filters import Technical_Score_Calculator
import market_data.fundamentals as fu
from market_data import pd, sa, fu, np, datetime, re







def relative_strength(symbols, lookback=-252, bm=None):
    #TODO This function is minimally viable. It currently only finds the relative strength for a given look back period. It should be able to find
    #TODO the relative strength since IPO.
    #Get Data
    if bm is None:
        df = api_import(['SPY'])['SPY']['Close'][lookback:]#.reset_index(drop=True)
        df.name = 'SPY'
        bm = df
    else:
        bm_df = bm
    for sym in symbols:
        if sym in symbols.keys():
            target = symbols[sym].df['Close']
            target.name = sym
        else:
            target = yf.download(sym, period='max', interval='1d')
        #Process Data
        df = pd.concat([df.reset_index(drop=True), target[lookback:].reset_index(drop=True)], axis=1)
        df['relative_strength'] = df[sym]/df['SPY']#!There was an error after updating yfinance. I had to add the subscript for ['Close'] and the second [bm]
        RSI(df, base='relative_strength')
        df = df.drop([sym, 'relative_strength'], axis=1)
        df = df.rename(columns={'RSI_21': sym})
    
    return sorted({k: df[k].iloc[-1] for k in symbols}.items(), key=lambda x: x[1])



def create_index(symbols, level='sector'):    
    def indexer(dict_of_frames):
        """Indexer accepts a dictionary of dataframes and returns an OHLC time series dataframe with cumulative market cap values
        obtained from the dataframes in the input dictionary. It can be used as a stand alone function; however, it is intended 
        to be used with another function that will separate stocks into respective sectors/industries place them into a dictioanry
        obejct and input them into indexer.
        Args:
            dict_of_frames (dictioary): A dictionary of OHLC time series dataframes that contain price data.
        Returns:
            pd.DataFrame: OHLC time series Pandas Dataframe with cumulative market cap values
        """
        # if sa == None:
        #     sa = seeking_alpha_api()
        #     sa.key_data_load()
        # if sa.key_data == None:
        #     sa.load_all()
        for sym in dict_of_frames.keys():
            try:#!The seeking alpha api is returning empty responses for a few symbols key data resulting in key errrors for subscripts of sa.key_data
                dict_of_frames[sym] = dict_of_frames[sym].assign(cap_open = dict_of_frames[sym]['Open'] * sa.key_data[sym]['data'][0]['attributes']['shares'],
                    cap_high = dict_of_frames[sym]['High'] * sa.key_data[sym]['data'][0]['attributes']['shares'],
                    cap_low = dict_of_frames[sym]['Low'] * sa.key_data[sym]['data'][0]['attributes']['shares'],
                    cap_close = dict_of_frames[sym]['Close'] * sa.key_data[sym]['data'][0]['attributes']['shares'])
            except KeyError as ke:
                #TODO print(sym, 'key error:\n', ke) update error handling
                continue
            except TypeError as te:
                #TODO print(sym, 'type error:\n', te) update error handling
                continue
        boo_open = pd.DataFrame()
        boo_high = pd.DataFrame()
        boo_low = pd.DataFrame()
        boo_close = pd.DataFrame()
        boo_volume = pd.DataFrame()
        for sym in list(dict_of_frames.keys()):
            try:
                boo_open = pd.concat([boo_open, dict_of_frames[sym]['cap_open']], axis=1)
                boo_high = pd.concat([boo_high, dict_of_frames[sym]['cap_high']], axis=1)
                boo_low = pd.concat([boo_low, dict_of_frames[sym]['cap_low']], axis=1)
                boo_close = pd.concat([boo_close, dict_of_frames[sym]['cap_close']], axis=1)
                boo_volume = pd.concat([boo_volume, dict_of_frames[sym]['Volume']], axis=1)
            except KeyError:
                print(sym)
                continue
        boo_open = boo_open.sort_index()
        boo_high = boo_high.sort_index()
        boo_low = boo_low.sort_index()
        boo_close = boo_close.sort_index()
        boo_volume = boo_volume.sort_index()
        boo_open['open_summed'] = boo_open.sum(axis=1)
        boo_high['high_summed'] = boo_high.sum(axis=1)
        boo_low['low_summed'] = boo_low.sum(axis=1)
        boo_close['close_summed'] = boo_close.sum(axis=1)
        boo_volume['volume_summed'] = boo_volume.sum(axis=1)
        final_ohlc_df = pd.concat([boo_open['open_summed'], boo_high['high_summed'], boo_low['low_summed'], boo_close['close_summed'], boo_volume['volume_summed']], keys=['Open', 'High', 'Low', 'Close', 'Volume'], axis=1)
        final_ohlc_df.index = pd.to_datetime(final_ohlc_df.index).strftime('%Y-%m-%d')
        return final_ohlc_df
    # if self.watchlista == None:
    #     raise TypeError('sector_industry_studies.watchlista is NoneType')
    # if fu == None:
    #     self.fundamentals()
    # fu.get_sym_sector_industry(lst)
    # sec_ind_dict_ori = fu.sector_industry_dict_saved
    sec_ind_dict_ori = fu.get_sym_sector_industry(list(symbols.keys()))
    sec_ind_dict_new = {}
    for k, v in sec_ind_dict_ori.items():
        try:
            if v[level] in sec_ind_dict_new:
                sec_ind_dict_new[v[level]].append(k)
            else:
                sec_ind_dict_new[v[level]] = [k]
        except KeyError as ke:
            #TODO print(k, ': ', ke) update error handling
            continue
    multidexer_dict_of_frames = {}
    for k, v in sec_ind_dict_new.items():
        for sym in v:
            try:
                multidexer_dict_of_frames[k].update({sym: symbols[sym].df})
            except:
                multidexer_dict_of_frames[k] = {sym: symbols[sym].df}
    final_dict = {}
    for k,v in multidexer_dict_of_frames.items():
        final_dict[k] = indexer(v)
    return final_dict


def condition_statistics(sym:str, symbols:dict, lookback:int=2000):
    #Set conditions
    conditions = {
    'dma_5over10': (symbols[sym].df['5DMA'][-lookback:] > symbols[sym].df['10DMA'][-lookback:]) & (symbols[sym].df['5DMA'][-lookback:].shift(1) < symbols[sym].df['10DMA'][-lookback:].shift(1)),
    'dma_5over20': (symbols[sym].df['5DMA'][-lookback:] > symbols[sym].df['20DMA'][-lookback:]) & (symbols[sym].df['5DMA'][-lookback:].shift(1) < symbols[sym].df['20DMA'][-lookback:].shift(1)),
    'dma_10over20': (symbols[sym].df['10DMA'][-lookback:] > symbols[sym].df['20DMA'][-lookback:]) & (symbols[sym].df['10DMA'][-lookback:].shift(1) < symbols[sym].df['20DMA'][-lookback:].shift(1)),
    'dma_20over50': (symbols[sym].df['20DMA'][-lookback:] > symbols[sym].df['50DMA'][-lookback:]) & (symbols[sym].df['20DMA'][-lookback:].shift(1) < symbols[sym].df['50DMA'][-lookback:].shift(1)),
    'dma_50over200': (symbols[sym].df['50DMA'][-lookback:] > symbols[sym].df['200DMA'][-lookback:]) & (symbols[sym].df['50DMA'][-lookback:].shift(1) < symbols[sym].df['200DMA'][-lookback:].shift(1)),
    'atr_over_signal': (symbols[sym].df['ATR_14'][-lookback:] > symbols[sym].df['ATR_14_signal'][-lookback:]) & (symbols[sym].df['ATR_14'][-lookback:].shift(1) < symbols[sym].df['ATR_14_signal'][-lookback:].shift(1)),
    'di_over_di': (symbols[sym].df['+DI'][-lookback:] > symbols[sym].df['-DI'][-lookback:]) & (symbols[sym].df['+DI'][-lookback:].shift(1) < symbols[sym].df['-DI'][-lookback:].shift(1)),
    'rsi_over_signal': (symbols[sym].df['RSI_14'][-lookback:] > symbols[sym].df['RSI_14_signal'][-lookback:]) & (symbols[sym].df['RSI_14'][-lookback:].shift(1) < symbols[sym].df['RSI_14_signal'][-lookback:].shift(1)),
    'stoch_over_signal': (symbols[sym].df['%k'][-lookback:] > symbols[sym].df['%d'][-lookback:]) & (symbols[sym].df['%k'][-lookback:].shift(1) < symbols[sym].df['%d'][-lookback:].shift(1)),
    'adx_over_signal': (symbols[sym].df['ADX'][-lookback:] > symbols[sym].df['ADX_signal'][-lookback:]) & (symbols[sym].df['ADX'][-lookback:].shift(1) < symbols[sym].df['ADX_signal'][-lookback:].shift(1)),
    'close_overuband': (symbols[sym].df['Close'][-lookback:] > symbols[sym].df['u_band'][-lookback:]) & (symbols[sym].df['Close'][-lookback:].shift(1) < symbols[sym].df['u_band'][-lookback:].shift(1)),
    'close_underlband': (symbols[sym].df['Close'][-lookback:] > symbols[sym].df['l_band'][-lookback:]) & (symbols[sym].df['Close'][-lookback:].shift(1) < symbols[sym].df['l_band'][-lookback:].shift(1))
    }#TODO Need to add MACD indicators
    for name, condition in conditions.items():
        symbols[sym].df[name] = np.nan
        symbols[sym].df[name][-lookback:].loc[condition] = 1

    #n signals
    num_signals = {signal: len(symbols[sym].df.loc[pd.notnull(symbols[sym].df[signal])]) for signal in conditions}


    #Returns over time
    returns = {}
    for condition in conditions:
        returns[condition] = {}
        indicies = symbols[sym].df[condition].loc[pd.notnull(symbols[sym].df[condition])].index[-lookback:]#Type Index
        for idx in indicies:
            index = indicies.get_loc(idx)
            df = symbols[sym].df[-lookback:]
            close = df.Close.loc[idx]
            if (index + 5 < len(indicies)) and (df.index.get_loc(idx) + 20):# or (len(indicies) <= 5):
                close_plus5 = df.Close.loc[idx: df.index[df.index.get_loc(idx) + 5]].max()
                if close_plus5 == close:
                    close_plus5 = df.Close.loc[idx: df.index[df.index.get_loc(idx) + 5]].min()
            else:
                close_plus5 = df.Close.loc[idx:].max()
                if close_plus5 == close:
                    close_plus5 = df.Close.loc[idx:].min()
            if (index + 10 < len(indicies)) and (df.index.get_loc(idx) + 20):# or (len(indicies) <= 10):
                close_plus10 = df.Close.loc[idx: df.index[df.index.get_loc(idx) + 10]].max()
                if close_plus10 == close:
                    close_plus10 = df.Close.loc[idx: df.index[df.index.get_loc(idx) + 10]].min()
            else:
                close_plus10 = df.Close.loc[idx:].max()
                if close_plus10 == close:
                    close_plus10 = df.Close.loc[idx:].min()
            if (index + 20 < len(indicies)) and (df.index.get_loc(idx) + 20):# or (len(indicies) <= 20):#Check if there enough indicies remaining and check if the number of indicies to call is <= the total number if indicies.
                close_plus20 = df.Close.loc[idx: df.index[df.index.get_loc(idx) + 20]].max()
                if close_plus20 == close:
                    close_plus20 = df.Close.loc[idx: df.index[df.index.get_loc(idx) + 20]].min()
            else:
                close_plus20 = df.Close.loc[idx:].max()
                if close_plus20 == close:
                    close_plus20 = df.Close.loc[idx:].min()
            if '5days' in list(returns[condition].keys()):
                returns[condition]['5days'].append(((close_plus5 - close) / close) * 100)
            else:
                returns[condition]['5days'] = [(((close_plus5 - close) / close) * 100)]
            if '10days' in list(returns[condition].keys()):
                returns[condition]['10days'].append(((close_plus10 - close) / close) * 100)
            else:
                returns[condition]['10days'] = [(((close_plus10 - close) / close) * 100)]
            if '20days' in list(returns[condition].keys()):
                returns[condition]['20days'].append(((close_plus20 - close) / close) * 100)
            else:
                returns[condition]['20days'] = [(((close_plus20 - close) / close) * 100)]



    #Descriptive Statistics
    mean_returns = {}
    for condition in conditions:
        mean_returns[condition] = {}
        for days in returns[condition]:
            mean_returns[condition][days] = sum([val for val in returns[condition][days]]) / len([val for val in returns[condition][days]])
    return {'num_signals': num_signals, 'returns': returns, 'mean_returns': mean_returns}

def perf_since_earnings(symbols, earnings_season_start=None, sort=True):
    if earnings_season_start == None:
        raise ValueError('Earnings season start date must be specified')
    perf_since_earnings_dict = {}
    for sym in symbols:
        try:
            earnings_date = datetime.datetime.strptime(sa.earnings_dict[sym]['revenue_actual']['0'][0]['effectivedate'].split('T')[0], '%Y-%m-%d')
            if earnings_date > datetime.datetime.strptime(earnings_season_start, '%Y-%m-%d'):
                perf_since_earnings_dict[sym] = ((symbols[sym].df.iloc[-1]['Close'] - symbols[sym].df.loc[earnings_date.strftime('%Y-%m-%d')]['Close']) / symbols[sym].df.loc[earnings_date.strftime('%Y-%m-%d')]['Close']) * 100
        except Exception as e:
            #TODO print(sym, e, sep=': ') update error handling
            continue
    if sort == True:
        return sorted(perf_since_earnings_dict.items(), key=lambda x: x[1])
    else:
        return perf_since_earnings_dict

def sec_ind_activity_by_tss_plots(tsc: Technical_Score_Calculator):
    for col in tsc.df_byrvol_positive.columns:
        try:
            fu.watchlist_composition([item[0] for item in tsc.df_byrvol_positive[col].dropna()], level='sector', name=f'Positive TSS-{col}')
        except ValueError as ve:
            if len(tsc.df_byrvol_positive[col].dropna()) == 0:
                print(f'df_byrvol_positive[{col}] is empty')
            else:
                raise ValueError(ve)
    for col in tsc.df_byrvol_positive.columns:
        try:
            fu.watchlist_composition([item[0] for item in tsc.df_byrvol_positive[col].dropna()], level='industry', name=f'Positive TSS-{col}')
        except ValueError as ve:
            if len(tsc.df_byrvol_positive[col].dropna()) == 0:
                print(f'df_byrvol_positive[{col}] is empty')
            else:
                raise ValueError(ve)
    for col in tsc.df_byrvol_negative.columns:
        try:
            fu.watchlist_composition([item[0] for item in tsc.df_byrvol_negative[col].dropna()], level='sector', name=f'Negative TSS-{col}')
        except ValueError as ve:
            if len(tsc.df_byrvol_negative[col].dropna()) == 0:
                print(f'df_byrvol_positive[{col}] is empty')
            else:
                raise ValueError(ve)
    for col in tsc.df_byrvol_positive.columns:
        try:
            fu.watchlist_composition([item[0] for item in tsc.df_byrvol_positive[col].dropna()], level='industry', name=f'Negative TSS-{col}')
        except ValueError as ve:
            if len(tsc.df_byrvol_negative[col].dropna()) == 0:
                print(f'df_byrvol_positive[{col}] is empty')
            else:
                raise ValueError(ve)
            
def relationship_to_indicator(symbols: dict, secondary_indicator: str, primary_indicator='Close') -> float:
    total_symbols = len(symbols)
    syms_above_indicator = 0
    for sym,v in symbols.items():
        try:
            if symbols[sym].df[primary_indicator].iloc[-1] > symbols[sym].df[secondary_indicator].iloc[-1]:
                syms_above_indicator += 1
        except KeyError as ke:
            #TODO print(sym, ke) update error handling
            continue
    return (syms_above_indicator / total_symbols) * 100

def primary_statistics(**watchlist: dict):
    # !Symbols from IWM are not returning AVWAP data. Some of the symbols look like datetime
    # !objects. It's only IWM. I'm concerned that if I change something in watchlista 
    # !only for this situation it could break my code.
    # TODO Some columns may require creating additional functions. They may be identified in the
    # TODO next line.
    # TODO Columns to add: % Elevated RVol
    vwaps = []
    for v in watchlist.values():
        for sym,df in v.items():
            columns = v[sym].df.columns
            break
    for item in columns:
        if len(re.findall('VWAP ', item)):
            if len(item) > 4:
                vwaps.append(item)
    for v in watchlist.values():
        columns=['% Above 5DMA', '% Above 10DMA', '% Above 20DMA', '% Above 50DMA', '% Above 200DMA',
                    '% Above '+vwaps[0], '% Above '+vwaps[1], '% Above '+vwaps[2], '% Above '+vwaps[3]]
    df = pd.DataFrame(columns=columns)
    for k,v in watchlist.items():
        df2 = (pd.DataFrame({'% Above 5DMA': relationship_to_indicator(v, secondary_indicator='5DMA'),
                            '% Above 10DMA': relationship_to_indicator(v, secondary_indicator='10DMA'),
                            '% Above 20DMA': relationship_to_indicator(v, secondary_indicator='20DMA'),
                            '% Above 50DMA': relationship_to_indicator(v, secondary_indicator='50DMA'),
                            '% Above 200DMA': relationship_to_indicator(v, secondary_indicator='200DMA'),
                            '% Above '+vwaps[0]: relationship_to_indicator(v, secondary_indicator=vwaps[0]),
                            '% Above '+vwaps[1]: relationship_to_indicator(v, secondary_indicator=vwaps[1]),
                            '% Above '+vwaps[2]: relationship_to_indicator(v, secondary_indicator=vwaps[2]),
                            '% Above '+vwaps[3]: relationship_to_indicator(v, secondary_indicator=vwaps[3])},
                        index=[k]))
        df = pd.concat([df, df2], names=columns)
    return df

def secondary_statistics(**watchlist):
    # TODO Explode watchlist to a dictionary. The keys will be the name of the watchlist whose data is on
    # TODO the row with the index named after it.
    columns=['% Expanding ATR', '+DI>-DI', '+MACD 5-10', '+MACD 10-20', '+MACD 20-50', '+MACD 50-200']
    df = pd.DataFrame(columns=columns)
    for k,v in watchlist.items():
        df2 = (pd.DataFrame({'% Expanding ATR': relationship_to_indicator(v, primary_indicator='TR', secondary_indicator='ATR_14'),
                            '+DI>-DI': relationship_to_indicator(v, primary_indicator='+DI', secondary_indicator='-DI'),
                            '+MACD 5-10': relationship_to_indicator(v, primary_indicator='5DMA', secondary_indicator='10DMA'),
                            '+MACD 10-20': relationship_to_indicator(v, primary_indicator='10DMA', secondary_indicator='20DMA'),
                            '+MACD 20-50': relationship_to_indicator(v, primary_indicator='20DMA', secondary_indicator='50DMA'),
                            '+MACD 50-200': relationship_to_indicator(v, primary_indicator='50DMA', secondary_indicator='200DMA')},
                        index=[k]))
        df = pd.concat([df, df2], names=columns)
    return df

def close_over_vwap_ratio(symbols: dict) -> int:
    count = [0,0]
    for sym in symbols:
        try:
            close_over_vwap = bool(symbols[sym].df.iloc[-1].Close > symbols[sym].df.iloc[-1].VWAP)
            if close_over_vwap == True:
                count[0] += 1
                count[1] += 1
            else:
                count[1] += 1
        except KeyError as ke:
            if ke == sym:
                continue
    if count[1] == 0:
        return 0.0
    else:
        ratio = (count[0]/count[1]) * 100
        return ratio
