import yfinance as yf
import pandas as pd
from market_data.price_data_import import api_import
from market_data.add_technicals import RSI







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



