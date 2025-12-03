import yfinance as yf
from market_data.price_data_import import api_import
from market_data.add_technicals import RSI
from market_data.watchlist_filters import Technical_Score_Calculator
import market_data.fundamentals as fu
from market_data import pd, sa, fu, np, datetime, re, scoreatpercentile
from market_data.Symbol_Data import SymbolData
import market_data.watchlists_locations as wl





class relative_strength:
    
    def __init__(self, symbols: dict, lookback: int=-252, bm: pd.Series=None):
        self.symbols = symbols
        self.lookback = lookback
        self.bm = bm
        self.errors = {}
    
    def __call__(self) -> list[tuple[str, float]]:
        if self.bm is None:
            bm = api_import(['SPY'])['SPY']['Close'][self.lookback:]
            bm.name = 'SPY'
        else:
            bm = self.bm
        
        # Initialize self.df as a DataFrame with SPY
        self.df = pd.DataFrame(index=bm.index)
        self.df['SPY'] = bm
        
        for sym in self.symbols:
            if sym in self.symbols.keys():
                target = self.symbols[sym].df['Close'][self.lookback:]
            else:
                target = yf.download(sym, period='max', interval='1d')['Close'][self.lookback:]
            
            try:
                try:
                    # Check if target index exactly matches bm index, or if target index matches the end of bm index
                    if not (target.index.equals(bm.index) or target.index.equals(bm.index[-len(target.index):])):
                        raise IndexError(f'{sym} index does not match SPY index')
                except IndexError as ie:
                    self.errors[sym] = ie
                    continue
            except Exception as e:
                self.errors[sym] = e
                continue
            # Use a temporary DataFrame to avoid cumulative issues
            temp_df = pd.DataFrame({'SPY': bm, sym: target})
            temp_df['relative_strength'] = temp_df[sym] / temp_df['SPY']
            
            # Calculate RSI on the temporary DataFrame
            RSI(temp_df, base='relative_strength')
            
            # Store the RSI result in self.df
            self.df[sym] = round(temp_df['RSI_21'], 3)
        
        # Return the sorted list of (symbol, RSI) tuples (only for symbols that were successfully processed)
        return sorted({k: round(self.df[k].iloc[-1].item(), 3) for k in self.df.columns[1:]}.items(), key=lambda x: x[1])
    
    def __getitem__(self, key):
        return self.df[key]

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
        all_symbols_list = wl.make_watchlist(wl.all_symbols)
        for sym in dict_of_frames.keys():
            try:#!The seeking alpha api is returning empty responses for a few symbols key data resulting in key errrors for subscripts of sa.key_data
                dict_of_frames[sym] = dict_of_frames[sym].assign(cap_open = dict_of_frames[sym]['Open'] * sa.key_data[sym]['data'][0]['attributes']['shares'],
                    cap_high = dict_of_frames[sym]['High'] * sa.key_data[sym]['data'][0]['attributes']['shares'],
                    cap_low = dict_of_frames[sym]['Low'] * sa.key_data[sym]['data'][0]['attributes']['shares'],
                    cap_close = dict_of_frames[sym]['Close'] * sa.key_data[sym]['data'][0]['attributes']['shares'])
            except KeyError as ke:
                #! Handles symbols that do not have data in sa.key_data. I could resolve this by using share data from sec-api.io.
                if sym in all_symbols_list:
                    continue
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
                #! Handles symbols that do not have data in sa.key_data. I could resolve this by using share data from sec-api.io.
                if sym in all_symbols_list:
                    continue
                # print(sym)
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

def condition_statistics(df: pd.DataFrame, lookback:int=2000):
    #Set conditions
    conditions = {
    'dma_5over10': (df['5DMA'][-lookback:] > df['10DMA'][-lookback:]) & (df['5DMA'][-lookback:].shift(1) < df['10DMA'][-lookback:].shift(1)),
    'dma_5over20': (df['5DMA'][-lookback:] > df['20DMA'][-lookback:]) & (df['5DMA'][-lookback:].shift(1) < df['20DMA'][-lookback:].shift(1)),
    'dma_10over20': (df['10DMA'][-lookback:] > df['20DMA'][-lookback:]) & (df['10DMA'][-lookback:].shift(1) < df['20DMA'][-lookback:].shift(1)),
    'dma_20over50': (df['20DMA'][-lookback:] > df['50DMA'][-lookback:]) & (df['20DMA'][-lookback:].shift(1) < df['50DMA'][-lookback:].shift(1)),
    'dma_50over200': (df['50DMA'][-lookback:] > df['200DMA'][-lookback:]) & (df['50DMA'][-lookback:].shift(1) < df['200DMA'][-lookback:].shift(1)),
    'atr_over_signal': (df['ATR_14'][-lookback:] > df['ATR_14_signal'][-lookback:]) & (df['ATR_14'][-lookback:].shift(1) < df['ATR_14_signal'][-lookback:].shift(1)),
    'di_over_di': (df['+DI'][-lookback:] > df['-DI'][-lookback:]) & (df['+DI'][-lookback:].shift(1) < df['-DI'][-lookback:].shift(1)),
    'rsi_over_signal': (df['RSI_14'][-lookback:] > df['RSI_14_signal'][-lookback:]) & (df['RSI_14'][-lookback:].shift(1) < df['RSI_14_signal'][-lookback:].shift(1)),
    'stoch_over_signal': (df['%k'][-lookback:] > df['%d'][-lookback:]) & (df['%k'][-lookback:].shift(1) < df['%d'][-lookback:].shift(1)),
    'adx_over_signal': (df['ADX'][-lookback:] > df['ADX_signal'][-lookback:]) & (df['ADX'][-lookback:].shift(1) < df['ADX_signal'][-lookback:].shift(1)),
    'close_overuband': (df['Close'][-lookback:] > df['u_band'][-lookback:]) & (df['Close'][-lookback:].shift(1) < df['u_band'][-lookback:].shift(1)),
    'close_underlband': (df['Close'][-lookback:] > df['l_band'][-lookback:]) & (df['Close'][-lookback:].shift(1) < df['l_band'][-lookback:].shift(1))
    }#TODO Need to add MACD indicators
    for name, condition in conditions.items():
        #* I've updated this to comply with changes in pandas 3.0. It seems to be working.
        df = df[-lookback:]
        df.loc[condition, name] = 1

    #n signals
    num_signals = {signal: len(df.loc[pd.notnull(df[signal])]) for signal in conditions}

    #Returns over time
    returns = {}
    for condition in conditions:
        returns[condition] = {}
        indicies_condition = df[condition].loc[pd.notnull(df[condition])].index[-lookback:]#Type Index
        indicies_all = df.index[-lookback:]
        for idx in indicies_condition:
            index = indicies_all.get_loc(idx)
            df = df[-lookback:]
            close = df.Close.loc[idx]
            if (index + 5 <= len(indicies_all)) and (idx + datetime.timedelta(days=5) <= indicies_all[-1]):# or (len(indicies) <= 5):
                close_plus5 = df.Close.iloc[df.index.get_loc(idx):df.index.get_loc(idx) + 6].max()
                if close_plus5 == close:
                    close_plus5 = df.Close.iloc[df.index.get_loc(idx):df.index.get_loc(idx) + 6].min()
            else:
                close_plus5 = df.Close.loc[idx:].max()
                if close_plus5 == close:
                    close_plus5 = df.Close.loc[idx:].min()
            if (index + 10 <= len(indicies_all)) and (idx + datetime.timedelta(days=10) <= indicies_all[-1]):# or (len(indicies) <= 10):
                close_plus10 = df.Close.iloc[df.index.get_loc(idx):df.index.get_loc(idx) + 11].max()
                if close_plus10 == close:
                    close_plus10 = df.Close.iloc[df.index.get_loc(idx):df.index.get_loc(idx) + 11].min()
            else:
                close_plus10 = df.Close.loc[idx:].max()
                if close_plus10 == close:
                    close_plus10 = df.Close.loc[idx:].min()
            if (index + 20 <= len(indicies_all)) and (idx + datetime.timedelta(days=20) <= indicies_all[-1]):# or (len(indicies) <= 20):#Check if there enough indicies remaining and check if the number of indicies to call is <= the total number if indicies.
                close_plus20 = df.Close.iloc[df.index.get_loc(idx):df.index.get_loc(idx) + 21].max()
                if close_plus20 == close:
                    close_plus20 = df.Close.iloc[df.index.get_loc(idx):df.index.get_loc(idx) + 21].min()
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
    
    #I added this feature becuase there are some time series with discrepant data. It is easier to remove
    #the outliers caused than to find the correct prices.
    #Remove outliers using interdecile range (remove upper and lower 10%)
    for condition in conditions:
        total_removed = 0
        for days in returns[condition]:
            if len(returns[condition][days]) >= 10:  # Only apply if we have enough data points
                # Calculate 10th and 90th percentiles using scipy
                p10, p90 = scoreatpercentile(returns[condition][days], [10, 90])
                
                # Create boolean mask to identify outliers
                mask = [(p10 <= val <= p90) for val in returns[condition][days]]
                
                # Count removed values
                num_removed = sum(not m for m in mask)
                total_removed += num_removed
                
                # Filter returns while preserving order
                returns[condition][days] = [val for val, keep in zip(returns[condition][days], mask) if keep]
        
        # Subtract total removed values from signal count for this condition
        num_signals[condition] -= total_removed

    #Descriptive Statistics
    mean_returns = {}
    for condition in conditions:
        mean_returns[condition] = {}
        for days in returns[condition]:
            mean_returns[condition][days] = sum([val for val in returns[condition][days]]) / len([val for val in returns[condition][days]])

    # Calculate expected value (EV) for each condition
    ev = {}
    for condition in conditions:
        if condition not in returns or not returns[condition]:
            ev[condition] = {'5days': 0.0, '10days': 0.0, '20days': 0.0}
            continue
            
        ev[condition] = {}
        for days in returns[condition]:
            if not returns[condition][days]:  # empty list
                ev[condition][days] = 0.0
                continue
            
            #Obtain returns and convert to numpy array
            returns_array = np.array(returns[condition][days])
            n_total = len(returns_array)
            
            if n_total == 0:
                ev[condition][days] = 0.0
                continue
            
            # Count positive and negative returns
            positive_mask = returns_array > 0
            negative_mask = returns_array < 0
            
            n_positive = np.sum(positive_mask)
            n_negative = np.sum(negative_mask)
            
            # Calculate probabilities
            prob_positive = n_positive / n_total if n_total > 0 else 0
            prob_negative = n_negative / n_total if n_total > 0 else 0
            
            # Calculate average returns for each outcome using numpy mean
            avg_positive = np.mean(returns_array[positive_mask]) if n_positive > 0 else 0
            avg_negative = np.mean(returns_array[negative_mask]) if n_negative > 0 else 0
            
            # Expected value calculation using numpy dot product
            probabilities = np.array([prob_positive, prob_negative])
            outcomes = np.array([avg_positive, avg_negative])
            expected_value = np.dot(probabilities, outcomes)
            
            ev[condition][days] = round(expected_value, 4)

    return {'num_signals': num_signals, 'returns': returns, 'mean_returns': mean_returns, 'frame': df[list(conditions.keys())]}

def perf_since_earnings(symbols, earnings_season_start=None, sort=True):
    if earnings_season_start == None:
        raise ValueError('Earnings season start date must be specified')
    perf_since_earnings_dict = {}
    for sym in symbols:
        try:
            earnings_date = datetime.datetime.strptime(sa.earnings_dict[sym]['revenue_actual']['0'][0]['effectivedate'].split('T')[0], '%Y-%m-%d')
            if earnings_date > datetime.datetime.strptime(earnings_season_start, '%Y-%m-%d'):
                perf_since_earnings_dict[sym] = round((((symbols[sym].df.iloc[-1]['Close'] - symbols[sym].df.loc[earnings_date.strftime('%Y-%m-%d')]['Close']) / symbols[sym].df.loc[earnings_date.strftime('%Y-%m-%d')]['Close']) * 100).item(), 3)
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
    return round((syms_above_indicator / total_symbols) * 100, 3)

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
        return round(ratio, 3)



def trend_bias(symbol: SymbolData):
    
    #Variables
    long_term_low = str(symbol.df.Lo3.loc[symbol.df.Lo3.notnull()].index[-1]).split(' ')[0]
    long_term_high = str(symbol.df.Hi3.loc[symbol.df.Hi3.notnull()].index[-1]).split(' ')[0]
    mid_term_low = str(symbol.df.Lo2.loc[symbol.df.Lo2.notnull()].index[-1]).split(' ')[0]
    mid_term_high = str(symbol.df.Hi2.loc[symbol.df.Hi2.notnull()].index[-1]).split(' ')[0]
    short_term_low = str(symbol.df.Lo1.loc[symbol.df.Lo1.notnull()].index[-1]).split(' ')[0]
    short_term_high = str(symbol.df.Hi1.loc[symbol.df.Hi1.notnull()].index[-1]).split(' ')[0]
    most_recent_close = symbol.df.Close.iloc[-1]
    
    #If then logic
    if datetime.datetime.strptime(long_term_low, "%Y-%m-%d") > datetime.datetime.strptime(long_term_high, "%Y-%m-%d"):
        long_term_bias = 'bullish'
    else:
        long_term_bias = 'bearish'
    if datetime.datetime.strptime(mid_term_low, "%Y-%m-%d") > datetime.datetime.strptime(mid_term_high, "%Y-%m-%d"):
        mid_term_bias = 'bullish'
    else:
        mid_term_bias = 'bearish'
    if mid_term_bias == 'bullish':
        if datetime.datetime.strptime(short_term_low, "%Y-%m-%d") > datetime.datetime.strptime(short_term_high, "%Y-%m-%d"):
            if (most_recent_close > symbol.df.loc[short_term_low].Low) & (most_recent_close < symbol.df.loc[short_term_high].High):
                short_term_bias = 'testing'
            elif (most_recent_close < symbol.df.loc[short_term_low].Low):
                short_term_bias = 'retracement'
            elif (most_recent_close > symbol.df.loc[short_term_high].High):
                short_term_bias = 'continuation'
        elif datetime.datetime.strptime(short_term_low, "%Y-%m-%d") < datetime.datetime.strptime(short_term_high, "%Y-%m-%d"):
            if (most_recent_close > symbol.df.loc[short_term_low].Low) & (most_recent_close < symbol.df.loc[short_term_high].High):
                short_term_bias = 'retracement'
            elif (most_recent_close > symbol.df.loc[short_term_high].High):
                short_term_bias = 'continuation'
            elif (most_recent_close < symbol.df.loc[short_term_low].Low):
                short_term_bias = 'correction'
    elif mid_term_bias == 'bearish':
        if datetime.datetime.strptime(short_term_low, "%Y-%m-%d") < datetime.datetime.strptime(short_term_high, "%Y-%m-%d"):
            if (most_recent_close > symbol.df.loc[short_term_low].Low) & (most_recent_close < symbol.df.loc[short_term_high].High):
                short_term_bias = 'testing'
            elif (most_recent_close < symbol.df.loc[short_term_low].Low):
                short_term_bias = 'continuation'
            elif (most_recent_close > symbol.df.loc[short_term_high].High):
                short_term_bias = 'retracement'
        elif datetime.datetime.strptime(short_term_low, "%Y-%m-%d") > datetime.datetime.strptime(short_term_high, "%Y-%m-%d"):
            if (most_recent_close > symbol.df.loc[short_term_low].Low) & (most_recent_close < symbol.df.loc[short_term_high].High):
                short_term_bias = 'retracement'
            elif (most_recent_close < symbol.df.loc[short_term_low].Low):
                short_term_bias = 'continuation'
            elif (most_recent_close > symbol.df.loc[short_term_high].High):
                short_term_bias = 'correction'
    return (long_term_bias, mid_term_bias, short_term_bias)

def watchlist_suggestions(tb):
    #* THere is also a stock suggestions function that could be added here. I'm not adding it now because the suggestions
    #* are conditions need to be improved and it is consistently returning errors. It may be worth revisiting later.
    """These watchlist suggestions represent the first watchlists to review. They are not intended to be
    comprehensive.
    They are suggested by the current start of the market trend and are not forward looking in that they 
    do not account for probablistic changes in the current trend.
    Returns:
        List: Keys of the watchlists dictionary named after the watchlists that are suggested.
    """
    
    if tb[0] == 'bullish':
        if tb[1] == 'bullish':
            if tb[2] == 'testing':
                watchlists = {'relative_strength_results': wl.make_watchlist(wl.relative_strength_tc2000_favorites),
                                'high_quant_results': wl.make_watchlist(wl.high_quant),
                                'uptrend_retracement_results': wl.make_watchlist(wl.uptrend_retracement)
                                }
                return list(watchlists.keys())
            elif tb[2] == 'continuation':
                watchlists = {'relative_strength_results': wl.make_watchlist(wl.relative_strength_tc2000_favorites),
                                'high_quant_results': wl.make_watchlist(wl.high_quant)
                                }
                return list(watchlists.keys())
            elif tb[2] == 'retracement':
                watchlists = {'relative_strength_results': wl.make_watchlist(wl.relative_strength_tc2000_favorites),
                                'high_quant_results': wl.make_watchlist(wl.high_quant)
                                }
                return list(watchlists.keys())
            elif tb[2] == 'correction':
                watchlists = {'uptrend_retracement_results': wl.make_watchlist(wl.uptrend_retracement),
                                'uptrend_accumulation': wl.make_watchlist(wl.uptrend_accumulation),
                                'uptrend': wl.make_watchlist(wl.uptrend),
                                'accumulation': wl.make_watchlist(wl.accumulation)
                                }
                return list(watchlists.keys())
        elif tb[1] == 'bearish':
            if tb[2] == 'testing':
                watchlists = {'relative_weakness_results': wl.make_watchlist(wl.relative_weakness_tc2000_favorites),
                                'downtrend_retracement': wl.make_watchlist(wl.downtrend_retracement),
                                'distribution': wl.make_watchlist(wl.distribution),
                                'downtrend_distribution': wl.make_watchlist(wl.downtrend_distribution)
                                }
                return (list(watchlists.keys()))
            elif tb[2] == 'continuation':
                watchlists = {'relative_strength_results': wl.make_watchlist(wl.relative_strength_tc2000_favorites),
                                'distribution': wl.make_watchlist(wl.distribution),
                                'distribution_breakdown': wl.make_watchlist(wl.distribution_breakdown),
                                'downtrend': wl.make_watchlist(wl.downtrend)                        
                                }
                return (list(watchlists.keys()))
            elif tb[2] == 'retracement':
                watchlists = {'seller_capitulation': wl.make_watchlist(wl.seller_capitulation),
                                'downtrend_retracement': wl.make_watchlist(wl.downtrend_retracement),
                                'downtrend':  wl.make_watchlist(wl.downtrend),
                                'downtrend_distribution': wl.make_watchlist(wl.downtrend_distribution)
                                }
                return (list(watchlists.keys()))
            elif tb[2] == 'correction':
                watchlists = {'relative_strength_results': wl.make_watchlist(wl.relative_strength_tc2000_favorites),
                                'high_quant_results': wl.make_watchlist(wl.high_quant),
                                'uptrend_accumulation': wl.make_watchlist(wl.uptrend_accumulation),
                                'accumulation_breakout': wl.make_watchlist(wl.accumulation_breakout),
                                'uptrend': wl.make_watchlist(wl.uptrend),
                                'accumulation': wl.make_watchlist(wl.accumulation)
                                }
                return (list(watchlists.keys()))
    elif tb[0] == 'bearish':
        if tb[1] == 'bullish':
            if tb[2] == 'testing':
                watchlists = {'distribution': wl.make_watchlist(wl.distribution),
                                'downtrend_retracement': wl.make_watchlist(wl.downtrend_retracement),
                                'distribution_breakdown': wl.make_watchlist(wl.distribution_breakdown),
                                'downtrend': wl.make_watchlist(wl.downtrend),
                                'seller_capitulation': wl.make_watchlist(wl.seller_capitulation),
                                'downtrend_distribution': wl.make_watchlist(wl.downtrend_distribution)
                                }
                return list(watchlists.keys())
            elif tb[2] == 'continuation':
                watchlists = {'relative_strength_results': wl.make_watchlist(wl.relative_strength_tc2000_favorites),
                                'high_quant_results': wl.make_watchlist(wl.high_quant),
                                'accumulation_breakout': wl.make_watchlist(wl.accumulation_breakout),
                                'uptrend_accumulation': wl.make_watchlist(wl.uptrend_accumulation)
                                }
                return list(watchlists.keys())
            elif tb[2] == 'retracement':
                watchlists = {'seller_capitulation': wl.make_watchlist(wl.seller_capitulation),
                                'distribution': wl.make_watchlist(wl.distribution),
                                'distribution_breakdown': wl.make_watchlist(wl.distribution_breakdown)
                                }
                return list(watchlists.keys())
            elif tb[2] == 'correction':
                watchlists = {'downtrend': wl.make_watchlist(wl.downtrend),
                                'distribution_breakdown': wl.make_watchlist(wl.distribution_breakdown),
                                'downtrend_distribution': wl.make_watchlist(wl.downtrend_distribution)
                                }
                return list(watchlists.keys())
        elif tb[1] == 'bearish':
            if tb[2] == 'testing':
                watchlists = {'downtrend': wl.make_watchlist(wl.downtrend),
                                'downtrend_distribution': wl.make_watchlist(wl.downtrend_distribution),
                                'distribution_breakdown': wl.make_watchlist(wl.distribution_breakdown),
                                'relative_weakness_results': wl.make_watchlist(wl.relative_weakness_tc2000_favorites)
                                }
                return list(watchlists.keys())
            elif tb[2] == 'continuation':
                watchlists = {'downtrend': wl.make_watchlist(wl.downtrend),
                                'downtrend_distribution': wl.make_watchlist(wl.downtrend_distribution),
                                'relative_weakness_results': wl.make_watchlist(wl.relative_weakness_tc2000_favorites)
                                }
                return list(watchlists.keys())
            elif tb[2] == 'retracement':
                watchlists = {'seller_capitulation': wl.make_watchlist(wl.seller_capitulation),
                                'downtrend': wl.make_watchlist(wl.downtrend),
                                'downtrend_retracement': wl.make_watchlist(wl.downtrend_retracement),
                                'downtrend_distribution': wl.make_watchlist(wl.downtrend_distribution)
                                }
                return list(watchlists.keys())

def timeframe_optimizer():
    pass
