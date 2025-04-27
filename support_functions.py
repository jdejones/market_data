import yfinance as yf
from market_data.price_data_import import api_import
from market_data.add_technicals import RSI
from market_data import pd







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



def create_index(symbols):    
    def indexer(self, dict_of_frames):
        """Indexer accepts a dictionary of dataframes and returns an OHLC time series dataframe with cumulative market cap values
        obtained from the dataframes in the input dictionary. It can be used as a stand alone function; however, it is intended 
        to be used with another function that will separate stocks into respective sectors/industries place them into a dictioanry
        obejct and input them into indexer.
        Args:
            dict_of_frames (dictioary): A dictionary of OHLC time series dataframes that contain price data.
        Returns:
            pd.DataFrame: OHLC time series Pandas Dataframe with cumulative market cap values
        """
        if self.saa == None:
            self.saa = seeking_alpha_api()
            self.saa.key_data_load()
        if self.saa.key_data == None:
            self.saa.load_all()
        for sym in dict_of_frames.keys():
            try:#!The seeking alpha api is returning empty responses for a few symbols key data resulting in key errrors for subscripts of self.saa.key_data
                dict_of_frames[sym] = dict_of_frames[sym].assign(cap_open = dict_of_frames[sym]['Open'] * self.saa.key_data[sym]['data'][0]['attributes']['shares'],
                    cap_high = dict_of_frames[sym]['High'] * self.saa.key_data[sym]['data'][0]['attributes']['shares'],
                    cap_low = dict_of_frames[sym]['Low'] * self.saa.key_data[sym]['data'][0]['attributes']['shares'],
                    cap_close = dict_of_frames[sym]['Close'] * self.saa.key_data[sym]['data'][0]['attributes']['shares'])
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
    def multidexer(self, lst, level='sector'):
        if self.watchlista == None:
            raise TypeError('sector_industry_studies.watchlista is NoneType')
        if self.fundamentals_analyzer == None:
            self.fundamentals()
        self.fundamentals_analyzer.get_sym_sector_industry(lst)
        sec_ind_dict_ori = self.fundamentals_analyzer.sector_industry_dict_saved
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
                    multidexer_dict_of_frames[k].update({sym: self.watchlista.saved_dict[sym]})
                except:
                    multidexer_dict_of_frames[k] = {sym: self.watchlista.saved_dict[sym]}
        final_dict = {}
        for k,v in multidexer_dict_of_frames.items():
            final_dict[k] = self.indexer(v)
        return final_dict