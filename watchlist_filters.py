import pandas as pd
from datetime import date, timedelta, datetime
import numpy as np
from tqdm import tqdm
from dataclasses import dataclass, field
from typing import List
from market_data.watchlists_locations import *
import market_data.fundamentals as fu
import matplotlib.pyplot as plt
import requests
from finvizfinance.screener.overview import Overview
import math
import plotly.express as px
import market_data.seeking_alpha as sa
import logging
import market_data.support_functions as sf
import market_data.watchlists_locations as wl
import inspect
import importlib
import sys
from market_data.api_keys import seeking_alpha_api_key

@dataclass
class Technical_Score_Calculator:
    sent_dict: dict = field(default_factory=dict)
    positive_sent_dict_stats: pd.DataFrame = field(default_factory=pd.DataFrame)
    negative_sent_dict_stats: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_byrvol_positive: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_byrvol_negative: pd.DataFrame = field(default_factory=pd.DataFrame)
    review_symbols_status: List = field(default_factory=list)
    ts_elevated_results: List = field(default_factory=list)
    ts_depressed_results: List = field(default_factory=list)

    def technical_score_calculator(self, saved_dict: dict) -> None:
        """Compute technical sentiment scores and store in attributes."""
        def symbol_removal():
            try:
                for sym in list(saved_dict.keys()):
                    if len(saved_dict[sym].df) < 4:
                        del saved_dict[sym]
            except:
                symbol_removal()
        symbol_removal()
        keys_list = list(saved_dict.keys())
        z = 0
        for sym, b in tqdm(saved_dict.items(), desc='Technical Score Calculator'):
            try:
                b = b.df
                self.sent_dict[keys_list[z]] = 0
                if b['5DMA'].iloc[-3] < b['5DMA'].iloc[-2] < b['5DMA'].iloc[-1]:
                    self.sent_dict[keys_list[z]] += 1
                if b['5DMA'].iloc[-3] > b['5DMA'].iloc[-2] > b['5DMA'].iloc[-1]:
                    self.sent_dict[keys_list[z]] -= 1
                if b['Close'].iloc[-1] > b['5DMA'].iloc[-1]:
                    self.sent_dict[keys_list[z]] += 1
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['Close'].iloc[-1] > b['10DMA'].iloc[-1]:
                    self.sent_dict[keys_list[z]] += 1
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['Close'].iloc[-1] > b['20DMA'].iloc[-1]:
                    self.sent_dict[keys_list[z]] += 1
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['Close'].iloc[-1] > b['50DMA'].iloc[-1]:
                    self.sent_dict[keys_list[z]] += 1
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['5DMA'].iloc[-1] > b['10DMA'].iloc[-1]:
                    self.sent_dict[keys_list[z]] += 1
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['10DMA'].iloc[-1] > b['20DMA'].iloc[-1]:
                    self.sent_dict[keys_list[z]] += 1
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['20DMA'].iloc[-1] > b['50DMA'].iloc[-1]:
                    self.sent_dict[keys_list[z]] += 1
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['High'].iloc[-1] > b['Close'].iloc[-10:].max():
                    self.sent_dict[keys_list[z]] += 1
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['Close'].iloc[-1] > b['High'].iloc[-10:].max():
                    self.sent_dict[keys_list[z]] += 2
                if b['Close'].iloc[-1] > b['Open'].iloc[-1]:
                    self.sent_dict[keys_list[z]] += 1
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['Close'].iloc[-1] > b['Close'].iloc[-2]:
                    if b['Volume'].iloc[-1] > b['Volume'].iloc[-2]:
                        self.sent_dict[keys_list[z]] += 2
                    else:
                        self.sent_dict[keys_list[z]] -= 1
                if b['Close'].iloc[-1] < b['Close'].iloc[-2]:
                    if b['Volume'].iloc[-1] < b['Volume'].iloc[-2]:
                        self.sent_dict[keys_list[z]] += 2
                    else:
                        self.sent_dict[keys_list[z]] -= 2
                if b['Close'].iloc[-1] < b['Open'].iloc[-1]:
                    if b['Volume'].iloc[-1] > b['Volume'].iloc[-2]:
                        self.sent_dict[keys_list[z]] -= 2
                    else:
                        self.sent_dict[keys_list[z]] += 1
                if b['Gap'].iloc[-10:].max(0) > 10:
                    if b['Gap'].iloc[-5:].max(0) > 10:
                        self.sent_dict[keys_list[z]] += 3
                    else:
                        self.sent_dict[keys_list[z]] += 2
                if b['Gap'].iloc[-10:].max(0) > 5:
                    if b['Gap'].iloc[-5:].max(0) > 5:
                        self.sent_dict[keys_list[z]] += 2
                    else:
                        self.sent_dict[keys_list[z]] += 1.5
                if b['ATR_14'].iloc[-1] > b['ATR_14 9_EMA'].iloc[-1]:
                    self.sent_dict[keys_list[z]] += 1
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['ATR_14'].iloc[-1] > b['ATR_14'].iloc[-2] > b['ATR_14'].iloc[-3]:
                    self.sent_dict[keys_list[z]] += 2
                elif b['ATR_14'].iloc[-1] > b['ATR_14'].iloc[-2]:
                    self.sent_dict[keys_list[z]] += 1
                else:
                    pass
                if b['RSI_14'].iloc[-1] > 70:
                    self.sent_dict[keys_list[z]] += 2
                if b['RSI_14'].iloc[-1] > 90:
                    self.sent_dict[keys_list[z]] += 3
                if b['RSI_14'].iloc[-1] > b['RSI_14'].iloc[-2] > b['RSI_14'].iloc[-3]:
                    self.sent_dict[keys_list[z]] += 1
                if b['%k'].iloc[-1] > 90:
                    self.sent_dict[keys_list[z]] += 3
                if b['%k'].iloc[-1] > 80:
                    self.sent_dict[keys_list[z]] += 2
                if b['%k'].iloc[-1] > b['%k'].iloc[-2] > b['%k'].iloc[-3]:
                    self.sent_dict[keys_list[z]] += 1
                if b['+DI'].iloc[-1] > b['-DI'].iloc[-1]:
                    self.sent_dict[keys_list[z]] += 1
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['ADX'].iloc[-1] > 25:
                    self.sent_dict[keys_list[z]] += 1
                if b['ADX'].iloc[-1] > 35:
                    self.sent_dict[keys_list[z]] += 2
                if b['ADX'].iloc[-1] > b['ADX'].iloc[-2] > b['ADX'].iloc[-3]:
                    self.sent_dict[keys_list[z]] += 1
                if b['sMACD1226'].iloc[-1] > 0:
                    self.sent_dict[keys_list[z]] += 1
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['sMACD1226'].iloc[-1] > b['MACD 9_EMA'].iloc[-1]:
                    self.sent_dict[keys_list[z]] += 2
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['sMACD1226'].iloc[-1] > b['sMACD1226'].iloc[-2] > b['sMACD1226'].iloc[-3]:
                    self.sent_dict[keys_list[z]] += 2
                if b['sMACD1226'].iloc[-1] < b['sMACD1226'].iloc[-2] < b['sMACD1226'].iloc[-3]:
                    self.sent_dict[keys_list[z]] += 2
                if 1 <= b['RVol'].iloc[-1] <= 1.25:
                    if b['Close'].iloc[-1] > b['Open'].iloc[-1]:
                        self.sent_dict[keys_list[z]] += 1
                    else:
                        self.sent_dict[keys_list[z]] -= 1
                if 1.25 <= b['RVol'].iloc[-1] <= 1.5:
                    if b['Close'].iloc[-1] > b['Open'].iloc[-1]:               
                        self.sent_dict[keys_list[z]] += 2
                    else:
                        self.sent_dict[keys_list[z]] -= 2
                if 1.5 <= b['RVol'].iloc[-1] <= 2.0:
                    elevated_rvol.append(sym)
                    if b['Close'].iloc[-1] > b['Open'].iloc[-1]:                
                        self.sent_dict[keys_list[z]] += 3
                    else:
                        self.sent_dict[keys_list[z]] -= 3
                if b['RVol'].iloc[-1] >= 2.0:
                    elevated_rvol.append(sym)
                    if b['Close'].iloc[-1] > b['Open'].iloc[-1]:                
                        self.sent_dict[keys_list[z]] += 4
                    else:
                        self.sent_dict[keys_list[z]] -= 4
                if b['Dollar Volume'].iloc[-1] > 5000000:
                    self.sent_dict[keys_list[z]] += 1
                else:
                    self.sent_dict[keys_list[z]] -= 1
                if b['AvgDV'].iloc[-1] > 5000000:
                    self.sent_dict[keys_list[z]] += 1
                if b['AvgDV'].iloc[-1] > 10000000:
                    self.sent_dict[keys_list[z]] += 2
                if b['AvgDV'].iloc[-1] < 1000000:
                    self.sent_dict[keys_list[z]] -= 3
                if b['ATRs Traded_ex-gap'].iloc[-1] > 1:
                    self.sent_dict[keys_list[z]] += 1
                if b['ATRs Traded_ex-gap'].iloc[-1] > 2:
                    self.sent_dict[keys_list[z]] += 2
                z += 1                
            except KeyError as ke:
                z += 1
                self.review_symbols_status.append(sym)
                continue                    
        positive_values = [v for v in self.sent_dict.values() if v > 0]
        negative_values = [v for v in self.sent_dict.values() if v < 0]
        self.positive_sent_dict_stats = pd.DataFrame(np.array(positive_values), columns=['Values']).describe()
        self.negative_sent_dict_stats = pd.DataFrame(np.array(negative_values), columns=['Values']).describe()
        elevated = {k:v for k,v in self.sent_dict.items() if v > self.positive_sent_dict_stats.loc['mean','Values']}
        depressed = {k:v for k,v in self.sent_dict.items() if v < self.negative_sent_dict_stats.loc['mean','Values']}
        rvol2_pos = sorted({k:v for k,v in elevated.items() if saved_dict[k].df ['RVol'].iloc[-1] > 2.0}.items(), key=lambda item: item[1], reverse=True)
        rvol2_neg = sorted({k:v for k,v in depressed.items() if saved_dict[k].df ['RVol'].iloc[-1] > 2.0}.items(), key=lambda item: item[1])
        rvol1_5to2_pos = sorted({k:v for k,v in elevated.items() if 1.5 < saved_dict[k].df ['RVol'].iloc[-1] < 2.0}.items(), key=lambda item: item[1], reverse=True)
        rvol1_5to2_neg = sorted({k:v for k,v in depressed.items() if 1.5 < saved_dict[k].df ['RVol'].iloc[-1] < 2.0}.items(), key=lambda item: item[1])
        rvol1to1_5_pos = sorted({k:v for k,v in elevated.items() if 1.0 < saved_dict[k].df['RVol'].iloc[-1] < 1.5}.items(), key=lambda item: item[1], reverse=True)
        rvol1to1_5_neg = sorted({k:v for k,v in depressed.items() if 1.0 < saved_dict[k].df['RVol'].iloc[-1] < 1.5}.items(), key=lambda item: item[1])
        rvollessthan1_pos = sorted({k:v for k,v in elevated.items() if saved_dict[k].df['RVol'].iloc[-1] < 1.0}.items(), key=lambda item: item[1], reverse=True)
        rvollessthan1_neg = sorted({k:v for k,v in depressed.items() if saved_dict[k].df['RVol'].iloc[-1] < 1.0}.items(), key=lambda item: item[1])
        s1_positive = pd.Series(rvol2_pos, name='RVol > 2', dtype=object)
        s2_positive = pd.Series(rvol1_5to2_pos, name='RVol 1.5-2', dtype=object)
        s3_positive = pd.Series(rvol1to1_5_pos, name='RVol 1-1.5', dtype=object)
        s4_positive = pd.Series(rvollessthan1_pos, name='RVol < 1', dtype=object)
        s1_negative = pd.Series(rvol2_neg, name='RVol > 2', dtype=object)
        s2_negative = pd.Series(rvol1_5to2_neg, name='RVol 1.5-2', dtype=object)
        s3_negative = pd.Series(rvol1to1_5_neg, name='RVol 1-1.5', dtype=object)
        s4_negative = pd.Series(rvollessthan1_neg, name='RVol < 1', dtype=object)
        self.df_byrvol_positive = pd.concat([s1_positive, s2_positive, s3_positive, s4_positive], axis=1, ignore_index=False)
        self.df_byrvol_negative = pd.concat([s1_negative, s2_negative, s3_negative, s4_negative], axis=1, ignore_index=False)

    def ts_elevated(self):
        ts_elevated_results = [sym for sym, val in self.sent_dict.items() if val > (self.positive_sent_dict_stats.loc['mean', 'Values'] + (self.positive_sent_dict_stats.loc['std', 'Values'] * 2))]
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\ts_elevated.txt", "w") as f:
            for sym in ts_elevated_results:
                f.write(sym + '\n')

    def ts_depressed(self):
        ts_depressed_results = [sym for sym, val in self.sent_dict.items() if val < (self.negative_sent_dict_stats.loc['mean', 'Values'] - (self.negative_sent_dict_stats.loc['std', 'Values'] * 2))]
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\ts_depressed.txt", "w") as f:
            for sym in ts_depressed_results:
                f.write(sym + '\n')


#Run the trend_fanned script
#@staticmethod
def fanned_up(symbols, save=True):
    #from Fundamental_Analysis import Fundamentals_Analyzer
    def symbol_removal():
        try:
            for sym in symbols:
                if len(symbols[sym].df) < 4:
                    del symbols[sym].df
                else:
                    continue
        except:
            symbol_removal()
    symbol_removal()
    fanned=[]
    for a,b in symbols.items():
        b = b.df
        if (b.iloc[-1]['10DMA'] > b.iloc[-1]['20DMA'] > b.iloc[-1]['50DMA'] > b.iloc[-1]['200DMA']):
            fanned.append(a)
    BM = []
    CC = []
    CD = []
    CS = []
    E = []
    FS = []
    H = []
    I = []
    RE = []
    T = []
    U = []
    # Fundamentals_Analyzer.get_sym_sector_industry(fanned)
    # foo = Fundamentals_Analyzer()
    # foo.load_data()
    # foo.get_sym_sector_industry_saved()
    # fu.get_sym_sector_industry_saved()
    for sym in fanned:
        try:
            if fu.sectors_industries[sym]['sector'] == 'Basic Materials':
                BM.append(sym)
                continue
            elif fu.sectors_industries[sym]['sector'] == 'Consumer Cyclical':
                CC.append(sym)
                continue
            elif fu.sectors_industries[sym]['sector'] == 'Consumer Defensive':
                CD.append(sym)
                continue
            elif fu.sectors_industries[sym]['sector'] == 'Communication Services':
                CS.append(sym)
                continue
            elif fu.sectors_industries[sym]['sector'] == 'Energy':
                E.append(sym)
                continue
            elif fu.sectors_industries[sym]['sector'] == 'Financial Services':
                FS.append(sym)
                continue
            elif fu.sectors_industries[sym]['sector'] == 'Healthcare':
                H.append(sym)
                continue
            elif fu.sectors_industries[sym]['sector'] == 'Industrials':
                I.append(sym)
                continue
            elif fu.sectors_industries[sym]['sector'] == 'Real Estate':
                RE.append(sym)
                continue
            elif fu.sectors_industries[sym]['sector'] == 'Technology':
                T.append(sym)
                continue
            elif fu.sectors_industries[sym]['sector'] == 'Utilities':
                U.append(sym)
                continue
            else:
                continue
        except Exception as e:
            #TODO print(sym, e) update error handling
            continue
    fanned_up_results = fanned
    if save == True:
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\Fanned_up_all.txt", "w") as f:
            for item in fanned:
                f.writelines(item + "\n")
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\Fanned_Basic_Materials.txt", "w") as f:
            for item in BM:
                f.writelines(item + "\n")
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\Fanned_Consumer_Cyclical.txt", "w") as f:
            for item in CC:
                f.writelines(item + "\n")                
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\Fanned_Consumer_Defensive.txt", "w") as f:
            for item in CD:
                f.writelines(item + "\n")
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\Fanned_Communication_Services.txt", "w") as f:
            for item in CS:
                f.writelines(item + "\n")
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\Fanned_Energy.txt", "w") as f:
            for item in E:
                f.writelines(item + "\n")
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\Fanned_Financial_Services.txt", "w") as f:
            for item in FS:
                f.writelines(item + "\n")
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\Fanned_Healthcare.txt", "w") as f:
            for item in H:
                f.writelines(item + "\n")
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\Fanned_Industrials.txt", "w") as f:
            for item in I:
                f.writelines(item + "\n")
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\Fanned_Real Estate.txt", "w") as f:
            for item in RE:
                f.writelines(item + "\n")
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\Fanned_Technology.txt", "w") as f:
            for item in T:
                f.writelines(item + "\n")
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\Fanned_Utilities.txt", "w") as f:
            for item in U:
                f.writelines(item + "\n")

def mid_to_long_term_fanned_up(symbols):
    def symbol_removal():
        try:
            for sym in symbols:
                if len(symbols[sym].df) < 4:
                    del symbols[sym].df
                else:
                    continue
        except:
            symbol_removal()
    symbol_removal()
    fanned=[]
    for a,b in symbols.items():
        b = b.df
        if (b.iloc[-1]['20DMA'] > b.iloc[-1]['50DMA'] > b.iloc[-1]['200DMA']):
            fanned.append(a)
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\mid_long_term_fanned_up.txt", "w") as f:
        for sym in fanned:
            f.write(sym + '\n')

#Run the long list systematic script
#@staticmethod
def technical_long_list_systematic(symbols, save=True):
    technical_long_list = []
    for a,b in symbols.items():
        b = b.df
        try:
            c_50dma = b['Close'].iloc[-1] > b['50DMA'].iloc[-1]
            dma20_50 = b['20DMA'].iloc[-1] > b['50DMA'].iloc[-1]
            dma50_200 = b['50DMA'].iloc[-1] > b['200DMA'].iloc[-1]
            dma50_ytdavwap = b['50DMA'].iloc[-1] > b['VWAP 2023-01-03'].iloc[-1]
            if c_50dma and dma20_50 and dma50_200 and dma50_ytdavwap:
                technical_long_list.append(a)
        except:
            continue
    technical_long_list = technical_long_list
    technical_long_list_quant_over_4 = fu.sa_fundamental_data['Symbol'].loc[(fu.sa_fundamental_data['quantRating'] > 4) & (fu.sa_fundamental_data['Symbol'].isin(technical_long_list))]        
    if save == True:
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\technical_long_list.txt", "w") as f:
            for item in technical_long_list:
                f.write(item +'\n')
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\technical_long_list_quant_over_4.txt", "w") as f:
            for item in technical_long_list_quant_over_4:
                f.write(item + '\n')


#Run the short list systematic script
#@staticmethod
def technical_short_list_systematic(symbols, save=True):
    technical_short_list=[]
    for a,b in symbols.items():
        b = b.df
        try:
            fanned = b.iloc[-1]['10DMA'] < b.iloc[-1]['20DMA'] < b.iloc[-1]['50DMA'] < b.iloc[-1]['200DMA']
            #dma10_50 = b.iloc[-1]['10DMA'] < b.iloc[-1]['50DMA']
            dma50_ytdavwap = b.iloc[-1]['50DMA'] < b.iloc[-1]['VWAP 2023-01-03']
            dma50_200 = b.iloc[-1]['50DMA'] < b.iloc[-1]['200DMA']
            if fanned or dma10_50 or dma50_ytdavwap or dma50_200:
                technical_short_list.append(a)
        except:
            continue
    technical_short_list = technical_short_list
    technical_short_list_quant_under_1half = fu.sa_fundamental_data.Symbol.loc[(fu.sa_fundamental_data['quantRating'] < 1.5) & (fu.sa_fundamental_data.Symbol.isin(technical_short_list))]
    if save == True:
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\technical_short_list.txt", "w") as f:
            for item in technical_short_list:
                f.write(item +'\n')
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\technical_short_list_quant_under_1half.txt", "w") as f:
            for item in technical_short_list_quant_under_1half:
                f.write(item + '\n')
        
#@staticmethod
def tenets(symbols):
    #I changed the data_dict to symbolsg
    daily_percent_change = sorted({k:v.df['Percent_Change'].iloc[-1].round(2) for k,v in symbols.items() if v.df['Percent_Change'].iloc[-1].round(2) > 3 or v.df['Percent_Change'].iloc[-1].round(2) < -3}.items(), key= lambda item: item[1], reverse=True)
    weekly_percent_change = sorted({k:(((v.df['Close'].resample('W').last().iloc[-1] - v.df['Open'].resample('W').first()).iloc[-1] / v.df['Close'].resample('W').last().iloc[-1]) * 100).round(2) for k,v in symbols.items() if (((v.df['Close'].resample('W').last().iloc[-1] - v.df['Open'].resample('W').first()).iloc[-1] / v.df['Close'].resample('W').last().iloc[-1]) * 100).round(2) > 5  or (((v.df['Close'].resample('W').last().iloc[-1] - v.df['Open'].resample('W').first()).iloc[-1] / v.df['Close'].resample('W').last().iloc[-1]) * 100).round(2) < 5}.items(), key= lambda item: item[1], reverse=True)
    monthly_percent_change = sorted({k:(((v.df['Close'].resample('M').last().iloc[-1] - v.df['Open'].resample('M').first()).iloc[-1] / v.df['Close'].resample('M').last().iloc[-1]) * 100).round(2) for k,v in symbols.items() if (((v.df['Close'].resample('M').last().iloc[-1] - v.df['Open'].resample('M').first()).iloc[-1] / v.df['Close'].resample('M').last().iloc[-1]) * 100).round(2) > 10 or (((v.df['Close'].resample('M').last().iloc[-1] - v.df['Open'].resample('M').first()).iloc[-1] / v.df['Close'].resample('M').last().iloc[-1]) * 100).round(2) < 10}.items(), key= lambda item: item[1], reverse=True)
    quarterly_percent_change = sorted({k:(((v.df['Close'].resample('Q').last().iloc[-1] - v.df['Open'].resample('Q').first()).iloc[-1] / v.df['Close'].resample('Q').last().iloc[-1]) * 100).round(2) for k,v in symbols.items() if (((v.df['Close'].resample('Q').last().iloc[-1] - v.df['Open'].resample('Q').first()).iloc[-1] / v.df['Close'].resample('Q').last().iloc[-1]) * 100).round(2) > 10 or (((v.df['Close'].resample('Q').last().iloc[-1] - v.df['Open'].resample('Q').first()).iloc[-1] / v.df['Close'].resample('Q').last().iloc[-1]) * 100).round(2) < 10}.items(), key= lambda item: item[1], reverse=True)
    yearly_percent_change = sorted({k:(((v.df['Close'].resample('Y').last().iloc[-1] - v.df['Open'].resample('Y').first()).iloc[-1] / v.df['Close'].resample('Y').last().iloc[-1]) * 100).round(2) for k,v in symbols.items() if (((v.df['Close'].resample('Y').last().iloc[-1] - v.df['Open'].resample('Y').first()).iloc[-1] / v.df['Close'].resample('Y').last().iloc[-1]) * 100).round(2) > 20 or (((v.df['Close'].resample('Y').last().iloc[-1] - v.df['Open'].resample('Y').first()).iloc[-1] / v.df['Close'].resample('Y').last().iloc[-1]) * 100).round(2) < 20}.items(), key= lambda item: item[1], reverse=True)
    daily_rvol = sorted({k:v.df['RVol'].iloc[-1].round(2) for k,v in symbols.items() if v.df['RVol'].iloc[-1].round(2) > 1.10}.items(), key=lambda item: item[1], reverse=True)
    weekly_rvol = sorted({k:(v.df['Volume'].resample('W').sum().iloc[-1] /v.df['Volume'].resample('W').sum().rolling(window=12).mean().iloc[-1]).round(2) for k,v in symbols.items()}.items(), key= lambda item: item[1], reverse=True)
    monthly_rvol = sorted({k:(v.df['Volume'].resample('M').sum().iloc[-1] / v.df['Volume'].resample('M').sum().rolling(window=12).mean().iloc[-1]).round(2) for k,v in symbols.items()}.items(), key= lambda item: item[1], reverse=True)
    quarterly_rvol = sorted({k:(v.df['Volume'].resample('Q').sum().iloc[-1] / v.df['Volume'].resample('Q').sum().rolling(window=4).mean().iloc[-1]).round(2) for k,v in symbols.items()}.items(), key= lambda item: item[1], reverse=True)
    yearly_rvol = sorted({k:(v.df['Volume'].resample('Y').sum().iloc[-1] / v.df['Volume'].resample('Y').sum().rolling(window=5).mean().iloc[-1]).round(2) for k,v in symbols.items()}.items(), key= lambda item: item[1], reverse=True)
    #I want to make this dataframe a multilevel column dataframe.
    #The following is an attempt to do so.
    # columns = [[('Volatility', 'Daily'), ('Volatility', 'Weekly'), ('Volatility', 'Monthly'), ('Volatility', 'Yearly')], [('Activity', 'Daily'), ('Activity', 'Weekly'), ('Activity', 'Monthly'), ('Activity', 'Yearly')]]
    # df = pd.DataFrame({
    #     'Volatility': 
    #         {
    #         'Daily' : daily_percent_change,
    #         'Weekly' : weekly_percent_change,
    #         'Monthly' : monthly_percent_change,
    #         'Yearly' : yearly_percent_change
    #         },
    #     'Activity' :
    #         {
    #         'Daily' : daily_rvol,
    #         'Weekly' : weekly_rvol,
    #         'Monthly' : monthly_rvol,
    #         'Yearly' : yearly_rvol
    #         }
    # })
    # df.columns = pd.MultiIndex.from_tuples(columns)
    df = pd.DataFrame.from_dict({
            'Volatility Daily' : daily_percent_change,
            'Activity Daily' : daily_rvol,
            'Volatility Weekly' : weekly_percent_change,
            'Activity Weekly' : weekly_rvol,
            'Volatility Monthly' : monthly_percent_change,
            'Activity Monthly' : monthly_rvol,
            'Volatility Quarterly' : quarterly_percent_change,
            'Activity Quarterly' : quarterly_rvol,
            'Volatility Yearly' : yearly_percent_change,
            'Activity Yearly' : yearly_rvol                
            }, orient='index')

    return df


def correlations(symbols, time_horizon, matrix=False):
    #I changed data_dict to symbols
    foo = {k:v.df['Close'][-time_horizon:] for k,v in symbols.items()}
    fas = pd.DataFrame.from_dict(foo).corr()
    correlation_values = fas
    correlation_values_mean = fas.describe().loc['mean'].sort_values(ascending=False)
    if matrix == True:
        cmap = plt.get_cmap('coolwarm')
        plt.matshow(fas, cmap=cmap)
        plt.xticks([item for item in range(len(foo.keys()))], foo.keys(), rotation=90)
        plt.yticks([item for item in range(len(foo.keys()))], foo.keys())
        plt.colorbar()
        return fas
        return plt.show()
    return fas


def correlation_matrix(symbols, time_horizon):
    foo = {k:v.df['Close'][-time_horizon:] for k,v in symbols.items()}
    fas = pd.DataFrame.from_dict(foo).corr()
    cmap = plt.get_cmap('coolwarm')
    plt.matshow(fas, cmap=cmap)
    plt.xticks([item for item in range(len(foo.keys()))], foo.keys(), rotation=90)
    plt.yticks([item for item in range(len(foo.keys()))], foo.keys())
    plt.colorbar()
    return plt.show()

#I should eventually make this function into its own class containing
#multiple report formats.
#! Not working
def standard_report(watchlista_object, corr_timeframe):
    # map saved_dict to symbols
    symbols = watchlista_object.saved_dict
    from Fundamental_Analysis import top_quant
    # from Fundamental_Analysis import extremes_tsc_quant
    # return watchlista_object.price_data_imports(watchlista_object.symbols,'max','1d',transfer=transfer_obj.saved_dict)
    # return watchlista_object.add_all_interday_technicals_watchlista()
    # return watchlista_object.technical_sentiment_calculator()
    # return print(f'{watchlista_object=}.split('=')', ' Tenets\n', watchlista_object.tenets())
    # return print(f'{watchlista_object=}.split('=')', ' Top Quant\n', top_quant(watchlista_object))
    # return print(f'{watchlista_object=}.split('=')', ' Correlations\n', watchlista_object.correlations(corr_timeframe, matrix=True))
    # return print(f'{watchlista_object=}.split('=')', ' Correlation Means\n', watchlista_object.correlation_values_mean)
    # return print(f'{watchlista_object=}.split('=')', ' Top Correlation Means First\n', watchlista_object.correlation_values[watchlista_object.correlation_values_mean[0]].sort_values(ascending=False))
    # return print(f'{watchlista_object=}.split('=')', ' Top Correlation Means Second\n', watchlista_object.correlation_values[watchlista_object.correlation_values_mean[1]].sort_values(ascending=False))
    # functions = [(,watchlista_object.price_data_imports(watchlista_object.symbols,'max','1d',transfer=transfer_obj.saved_dict)),
    #             (,watchlista_object.add_all_interday_technicals_watchlista()), 
    #             (,watchlista_object.technical_sentiment_calculator()),
    #             (print(f'{watchlista_object=}.split('=')', ' Tenets\n'),watchlista_object.tenets()),
    #             (print(f'{watchlista_object=}.split('=')', ' Top Quant\n'),top_quant(watchlista_object)),
    #             ()]
    from tabulate import tabulate
    import inspect
    frame = inspect.currentframe()
    frame = inspect.getouterframes(frame)[1]
    string = inspect.getframeinfo(frame[0]).code_context[0].strip()
    args = string[string.find('(') + 1:-1].split(',')    
    names = []
    for i in args:
        if i.find('=') != -1:
            names.append(i.split('=')[1].strip())      
        else:
            names.append(i)
    watchlista_object.price_data_import(watchlista_object.symbols,'max','1d',transfer=symbols)
    watchlista_object.add_all_interday_technicals_watchlista()
    watchlista_object.technical_sentiment_calculator()
    print('\n' + names[0] + ' Correlations\n')
    print(tabulate(watchlista_object.correlations(corr_timeframe,matrix=True), headers='keys', tablefmt='grid'))
    # .tenets() is not working on every object and is breaking the function.
    print('\n' + names[0] + ' Tenets\n')
    print(tabulate(watchlista_object.tenets()))
    print('\n' + names[0] + ' Top Quant\n')
    print(tabulate(top_quant(watchlista_object)))
    print('\n' + names[0] + ' Correlations Mean\n')
    print(watchlista_object.correlation_values_mean)
    print('\n' + names[0] + ' Correlation Means First\n')
    print(watchlista_object.correlation_values[watchlista_object.correlation_values_mean.index[0]].sort_values(ascending=False))
    print('\n' + names[0] + ' Correlation Means Second\n')
    print(watchlista_object.correlation_values[watchlista_object.correlation_values_mean.index[1]].sort_values(ascending=False))

def atrs_from(symbols) -> pd.DataFrame:
    atrsfrom10dma = pd.Series(sorted({k:abs(symbols[k].df['ATRs_from_10DMA'].iloc[-1].round(2)) for k in symbols if symbols[k].df['ATRs_from_10DMA'].iloc[-1] < 1.5}.items(), key=lambda  item: item[1]), name='ATRs_from_10DMA')
    atrsfrom20dma = pd.Series(sorted({k:abs(symbols[k].df['ATRs_from_20DMA'].iloc[-1].round(2)) for k in symbols if symbols[k].df['ATRs_from_20DMA'].iloc[-1] < 1.5}.items(), key=lambda  item: item[1]), name='ATRs_from_20DMA')
    atrsfrom50dma = pd.Series(sorted({k:abs(symbols[k].df['ATRs_from_50DMA'].iloc[-1].round(2)) for k in symbols if symbols[k].df['ATRs_from_50DMA'].iloc[-1] < 1.5}.items(), key=lambda  item: item[1]), name='ATRs_from_50DMA')
    atrsfrom5davwap = pd.Series(sorted({k:abs(symbols[k].df['ATRs_from_VWAP_5'].iloc[-1].round(2)) for k in symbols if symbols[k].df['ATRs_from_VWAP_5'].iloc[-1] < 1.5}.items(), key=lambda  item: item[1]), name='ATRs_from_VWAP_5')
    atrsfrom10davwap = pd.Series(sorted({k:abs(symbols[k].df['ATRs_from_VWAP_10'].iloc[-1].round(2)) for k in symbols if symbols[k].df['ATRs_from_VWAP_10'].iloc[-1] < 1.5}.items(), key=lambda  item: item[1]), name='ATRs_from_VWAP_10')
    atrsfrom20davwap = pd.Series(sorted({k:abs(symbols[k].df['ATRs_from_VWAP_20'].iloc[-1].round(2)) for k in symbols if symbols[k].df['ATRs_from_VWAP_20'].iloc[-1] < 1.5}.items(), key=lambda  item: item[1]), name='ATRs_from_VWAP_20')
    df = pd.concat([atrsfrom10dma, atrsfrom20dma, atrsfrom50dma, atrsfrom5davwap, atrsfrom10davwap, atrsfrom20davwap], axis=1)
    return df

def episodic_pivots(symbols, start_date, gap_threshold=7.9, save=True, print_results=False):
    # Iterate through each symbol to identify episodic pivots
    ep_gap_day = {}
    episodic_pivots_results = []
    episodic_pivot_start = []
    for sym in tqdm(symbols, desc='Episodic Pivots'):
        for day in symbols[sym].df.loc[start_date:].index:
            if symbols[sym].df['Gap'].loc[day] > gap_threshold:
                ep_gap_day[sym] = day
                if symbols[sym].df['RVol'].loc[day] < 1:
                    continue
                for day2 in symbols[sym].df.loc[day:].index:
                    if symbols[sym].df['Close'].loc[day2] > symbols[sym].df['20DMA'].loc[day2]:
                        if day2 == symbols[sym].df.loc[day:].index[-1]:
                            episodic_pivots_results.append(sym)
                            episodic_pivot_start.append((sym, day))
                            break
                    elif symbols[sym].df['Close'].loc[day2] < symbols[sym].df['20DMA'].loc[day2]:
                        break
    # finalize and dedupe results
    episodic_pivots_results = list(set(episodic_pivots_results))
    # filter by high quant
    episodic_pivots_quant_over_4 = fu.sa_fundamental_data.Symbol.loc[
        (fu.sa_fundamental_data['quantRating'] > 4) &
        (fu.sa_fundamental_data.Symbol.isin(episodic_pivots_results))
    ]
    if save == True:
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\episodic_pivots.txt", 'w') as f:
            for item in episodic_pivots_results:
                f.write(item + '\n')
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\episodic_pivots_quant_over_4.txt", 'w') as f:
            for item in episodic_pivots_quant_over_4:
                f.write(item + '\n')
    if print_results == True:
        return episodic_pivots_results

def gapped_no_income(symbols, look_back=5):
    gapped_no_income_results = []
    for n in range(look_back):          
        for item in fu.sa_fundamental_data['Symbol'].loc[(fu.sa_fundamental_data['Symbol'].isin([sym for sym in symbols if symbols[sym].df['Gap'].iloc[-n] > 5])) & (fu.sa_fundamental_data['NET Income TTM'] < 0)].to_list():
            gapped_no_income_results.append(item)
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\no_income_gapped.txt", 'w') as f:
        for item in gapped_no_income_results:
            f.write(item +'\n')

def no_income():
    no_income_results = fu.sa_fundamental_data.Symbol.loc[fu.sa_fundamental_data['NET Income TTM'] < 0].to_list()
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\no_income.txt", 'w') as f:
        for item in no_income_results:
            f.write(str(item) + '\n')

def high_quant():
    high_quant_results = fu.sa_fundamental_data.Symbol.loc[fu.sa_fundamental_data['quantRating'] > 4].to_list()
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\high_quant.txt", 'w') as f:
        for item in high_quant_results:
            f.write(str(item) + '\n')

def low_quant():
    low_quant_results = fu.sa_fundamental_data.Symbol.loc[fu.sa_fundamental_data['quantRating'] < 2].to_list()
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\low_quant.txt", 'w') as f:
        for item in low_quant_results:
            f.write(item + '\n')

#! Not working.
def high_short_interest(symbols, short_int=10, length=100, avgv_min=5000000):
    high_short_interest_results = []
    page = 1
    while len(high_short_interest_results) < length:
        url = "https://seeking-alpha.p.rapidapi.com/screeners/get-results"
        querystring = {"page":page,"per_page":"100","type":"stock"}
        payload = {
            'short_interest_percent_of_float': {'gte': short_int}#'disabled': False, #The preceeding was removed because an error began to be returned from the API.
        }
        headers = {
            "content-type": "application/json",
            "X-RapidAPI-Key": f"{seeking_alpha_api_key}",
            "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
        }
        response = requests.request("POST", url, json=payload, headers=headers, params=querystring).json()
        for item in response['data']:
            try:
                if symbols[item['attributes'].df['name']].AvgDV.iloc[-1] > avgv_min:
                    high_short_interest_results.append(item['attributes']['name'])
            except KeyError:
                continue
        page += 1
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\high_short_interest.txt", 'w') as f:
        for item in high_short_interest_results:
            f.write(item + '\n')

def finviz_horizontal_support_resistance():
    screener = Overview()
    screener.set_filter(signal='Horizontal S/R')
    results = screener.screener_view(order='Market Cap.')
    finviz_horizontal_support_resistance_results = results.Ticker.loc[results.Industry != 'Exchange Traded Fund'].to_list()
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\finviz_horizontal_s_r.txt", "w") as f:
        for item in finviz_horizontal_support_resistance_results:
            f.write(item + '\n')

def finviz_tl_support():
    screener = Overview()
    screener.set_filter(signal='TL Support')
    results = screener.screener_view(order='Market Cap.')
    finviz_tl_support_results = results.Ticker.loc[results.Industry != 'Exchange Traded Fund'].to_list()
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\finviz_tl_support.txt", "w") as f:
        for item in finviz_tl_support_results:
            f.write(item + '\n')

def finviz_tl_resistance():
    screener = Overview()
    screener.set_filter(signal='TL Resistance')
    results = screener.screener_view(order='Market Cap.')
    finviz_tl_resistance_results = results.Ticker.loc[results.Industry != 'Exchange Traded Fund'].to_list()
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\finviz_tl_resistance.txt", "w") as f:
        for item in finviz_tl_resistance_results:
            f.write(item + '\n')

def finviz_high_short_interest():
    screener = Overview()
    screener.set_filter(filters_dict={'Float Short': 'Over 20%'})
    results = screener.screener_view(order='Market Cap.')
    finviz_high_short_interest_results = results.Ticker.loc[results.Industry != 'Exchange Traded Fund'].to_list()
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\finviz_high_short_interest.txt", "w") as f:
        for item in finviz_high_short_interest_results:
            f.write(item + '\n')

#! Refactor incomplete. Needs sector/inustry functions. This may be easier to refactor when I'm writing objects to execute trade protocols.
def general_long_save(no_of_results=20):
    general_long_results = []
    def general_long(self, list_length, include_ind_tss=True):
        general_long_list = []
        industry_to_access = 0
        quant_rating = []
        analyst_rating = []
        eps_beat = []
        eps_improve = []
        revenue_beat = []
        revenue_improve= []
        while len(general_long_list) < list_length:
            try:
                if include_ind_tss == True:
                    industry_accessed = sorted(self.watchlista_ind.elevated_score_dict.items(), key=lambda item: item[1], reverse=True)[industry_to_access][0]
                    industry_to_access += 1
                    symbols = self.fundamentals_analyzer.sym_by_level([item for item in self.watchlista.symbols if self.watchlista.sent_dict[item] > 0], industry_accessed)
                elif include_ind_tss == False:
                    symbols = [k for k,v in self.watchlista.sent_dict.items() if v > 0]
                for sym in symbols:
                    try:
                        if sa.ratings_dict == None:
                            sa.ratings_dict.get_ratings_load()
                        if round(sa.ratings_dict[sym]['data'][0]['attributes']['ratings']['quantRating'], 2) > 4:
                            quant_rating.append(sym)
                            pass
                        else:
                            continue
                        if sa.analyst_ratings_dict == None:
                            sa.analyst_ratings_load()
                        if sa.analyst_ratings_dict[sym]['data']['attributes']['average_rate'] != None:
                            if round(float(sa.analyst_ratings_dict[sym]['data']['attributes']['average_rate']), 2) > 4:
                                analyst_rating.append(sym)
                                pass
                            else:
                                continue
                        else:
                            continue
                        if float(sa.earnings_dict[sym]['estimates']['revenue_actual']['0'][0]['dataitemvalue']) >= float(sa.earnings_dict[sym]['estimates']['revenue_consensus_mean']['0'][0]['dataitemvalue']):
                            revenue_beat.append(sym)
                            pass
                        else:
                            continue
                        if float(sa.earnings_dict[sym]['estimates']['revenue_actual']['0'][0]['dataitemvalue']) <= float(sa.earnings_dict[sym]['estimates']['revenue_consensus_mean']['1'][0]['dataitemvalue']):
                            revenue_improve.append(sym)
                            pass
                        else:
                            continue
                        if round(float(sa.earnings_dict[sym]['estimates']['eps_gaap_actual']['0'][0]['dataitemvalue']), 2) >= round(float(sa.earnings_dict[sym]['estimates']['eps_gaap_consensus_mean']['0'][0]['dataitemvalue']), 2):
                            eps_beat.append(sym)
                            pass
                        else:
                            continue
                        if round(float(sa.earnings_dict[sym]['estimates']['eps_gaap_actual']['0'][0]['dataitemvalue']), 2) <= round(float(sa.earnings_dict[sym]['estimates']['eps_gaap_consensus_mean']['1'][0]['dataitemvalue']), 2):
                            eps_improve.append(sym)
                            pass
                        else:
                            continue
                        general_long_list.append(sym)
                        self.general_long_symbols.append(sym)
                    except KeyError as ke:
                        self.missing_symbols[sym] = ke
                        continue
                    except IndexError as ie:
                        self.check_symbol_data.append(sym)
                        continue
                if include_ind_tss == False:
                    break
            except IndexError as ie:
                traceback.print_exc()
                print(ie, ':\n', 'Symbols/Industries avaialable may have been exhausted.')
                #return general_long_list
                break
        loopiter = 1
        print('eps improve', len(eps_improve))
        print('eps beat', len(eps_beat))
        print('revenue improve', len(revenue_improve))
        print('revenue beat', len(revenue_beat))
        print('analyst rating', len(analyst_rating))
        print('quant rating', len(quant_rating))
        while len(set(general_long_list)) <= list_length:
            if loopiter == 1:
                general_long_list.extend(eps_improve)
                loopiter += 1
                continue
            if loopiter == 2:
                general_long_list.extend(eps_beat)
                loopiter += 1
                continue
            if loopiter == 3:
                general_long_list.extend(revenue_improve)
                loopiter += 1
                continue
            if loopiter == 4:
                general_long_list.extend(revenue_beat)
                loopiter += 1
                continue
            if loopiter == 5:
                general_long_list.extend(analyst_rating)
                loopiter += 1
                continue
            if loopiter == 6:
                general_long_list.extend(quant_rating)
                loopiter += 1 
                continue
            else:
                self.general_long_list = general_long_list
                return set(general_long_list)
        self.general_long_list = general_long_list
        if len(set(general_long_list)) == 0:
            self.general_long(list_length, include_ind_tss=False)
        else:
            return set(general_long_list)    
    general_long_results.append(strat.general_long(no_of_results))
    if len(general_long_results) < no_of_results:
        general_long_results = list(strat.general_long(20, include_ind_tss=False))
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\general_long.txt", "w") as f:
        for item in general_long_results:
            f.write(item + '\n')

def no_income_price_above_target(symbols) -> None:
    """Filters for symbols with income <= 0 and saved them to a file.

    Raises:
        ValueError: Raised if no value for no_income_results()
    """
    results = []
    # errors = {}
    no_income_results = open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\no_income.txt", "r").read().splitlines()
    if len(no_income_results) == 0:
        raise ValueError('no_income_results list empty. Try calling no_income()')
    for sym in no_income_results:
        try:
            if symbols[sym].df['Close'].iloc[-1] > float(sa.analyst_price_targets_dict[sym]['estimates'][sa.meta_data[sym]['data']['id']]['target_price']['0'][0]['dataitemvalue']):
                results.append(sym)
        except Exception as e:
            # errors[sym] = e
            continue
    # if len(errors) > 0:
        # console = logging.StreamHandler()
        # console.setLevel(logging.INFO)
        # formatter = logging.Formatter('%(levelname)s - %(message)s')
        # console.setFormatter(formatter)
        # logging.getLogger('').addHandler(console)
        # logging.warning(str(len(errors.keys())) + ' exceptions were raised. Check log at "E:\Programming\Python\Programs\Error_Logs\Watchlista\\no_income_above_target\error_log.txt"')
        # logger.warning(str(len(errors.keys())) + ' exceptions were raised. Check log at "E:\Programming\Python\Programs\Error_Logs\Watchlista\\no_income_above_target\error_log.txt"')
    # logging.shutdown()
    no_income_price_above_target_results = results
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\no_income_price_above_target.txt", "w") as f:
        for sym in no_income_price_above_target_results:
            f.write(sym + '\n')

#! Refactor incomplete. I may not be getting expected results because of few passing symbols and the limited number of symbols in the watchlist.
def high_short_interest_price_above_target(symbols):
    high_short_interest_price_above_target_results = []
    high_short_interest_results = open(wl.high_short_interest).read().splitlines()
    finviz_high_short_interest_results = open(wl.finviz_high_short_interest).read().splitlines()
    logger = logging.getLogger('high_short_interest_price_above_target_logger')
    if not logger.handlers:
        f_handler = logging.FileHandler(r"E:\Programming\Python\Programs\Error_Logs\Watchlista\high_short_interest_price_above_target\error_log.txt")        
        c_handler = logging.StreamHandler()
        f_handler.setLevel(logging.INFO)
        c_handler.setLevel(logging.WARNING)
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)
        c_handler.setFormatter(c_format)
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)
    errors = {}
    if (len(high_short_interest_results) < 0) or (len(finviz_high_short_interest_results) < 0):
        raise ValueError('HIGH SHORT INTEREST CONTAINERS ARE EMPTY. RUN HIGH SHORT INTEREST FUNCTIONS')
    high_short_interest_cumulative = high_short_interest_results + finviz_high_short_interest_results
    for sym in high_short_interest_cumulative:
        try:
            if symbols[sym].df['Close'].iloc[-1] > float(sa.analyst_price_targets_dict[sym]['estimates'][sa.meta_data[sym]['data']['id']]['target_price']['0'][0]['dataitemvalue']):
                high_short_interest_price_above_target_results.append(sym)
        except Exception as e:
            errors[sym] = e
            logger.info('%s : %s', sym, str(e))
            continue
    if len(errors) > 0:
        logger.warning(str(len(errors.keys())) + ' exceptions were raised. Check log at "E:\Programming\Python\Programs\Error_Logs\Watchlista\\high_short_interest_price_above_target\error_log.txt"')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\high_short_interest_price_above_target.txt", "w") as f:
        for sym in high_short_interest_price_above_target_results:
            f.write(sym + '\n')

def relative_strength(symbols, threshold=70, no_of_results=5, bm=None, lookback=-252, file=r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\relative_strength_tc2000_favorites.txt"):
    # #TODO This function is minimally viable. It currently only finds the relative strength for a given look back period. It should be able to find
    # #TODO the relative strength since IPO.
    # #Get Data
    rel_stren = sf.relative_strength(symbols, lookback=-252, bm=bm)
    threshold_passed = []  
    
    def results(threshold=threshold):
        # threshold_passed = []
        for sym in rel_stren:
            if (sym[1] > threshold) and (sym[0] not in threshold_passed):
                threshold_passed.append(sym[0])
        if len(threshold_passed) >= no_of_results:
            pass
        else:
            results(threshold=threshold-5)   
    results(threshold=threshold)     
    with open(file, "w") as f:
        for sym in threshold_passed:
            f.write(sym + '\n')


def relative_weakness(symbols, threshold=20, no_of_results=5, bm=None, file=r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\relative_weakness_tc2000_favorites.txt"):
    rel_weak = sf.relative_strength(symbols, lookback=-252, bm=bm)
    threshold_passed = []
    def results(threshold=threshold):
        for sym in rel_weak:
            if (sym[1] < threshold) and (sym[0] not in threshold_passed):
                threshold_passed.append(sym[0])
        if len(threshold_passed) >= no_of_results:
            pass
        else:
            results(threshold=threshold+5)
    results(threshold)  
    with open(file, "w") as f:
        for sym in threshold_passed:
            f.write(sym + '\n')

def earnings_performance(symbols):
    earnings_beats = []
    earnings_misses = []
    for sym in symbols:
        try:
            # if (fu.sa_fundamental_data['EPS Surprise'].loc[fu.sa_fundamental_data.Symbol == sym].values[0] > 1) & (fu.sa_fundamental_data['Revenue Surprise'].loc[fu.sa_fundamental_data.Symbol == sym].values[0] > 1):
            if (float(sa.earnings_dict[sym]['eps_gaap_actual']['0'][0]['dataitemvalue']) > float(sa.earnings_dict[sym]['eps_gaap_consensus_mean']['0'][0]['dataitemvalue'])) and (float(sa.earnings_dict[sym]['revenue_actual']['0'][0]['dataitemvalue']) > float(sa.earnings_dict[sym]['revenue_consensus_mean']['0'][0]['dataitemvalue'])):
                earnings_beats.append(sym)
            # elif (fu.sa_fundamental_data['EPS Surprise'].loc[fu.sa_fundamental_data.Symbol == sym].values[0] < 1) & (fu.sa_fundamental_data['Revenue Surprise'].loc[fu.sa_fundamental_data.Symbol == sym].values[0] < 1):
            elif (float(sa.earnings_dict[sym]['eps_gaap_actual']['0'][0]['dataitemvalue']) < float(sa.earnings_dict[sym]['eps_gaap_consensus_mean']['0'][0]['dataitemvalue'])) and (float(sa.earnings_dict[sym]['revenue_actual']['0'][0]['dataitemvalue']) < float(sa.earnings_dict[sym]['revenue_consensus_mean']['0'][0]['dataitemvalue'])):
                earnings_misses.append(sym)
        except:
            continue
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\earnings_beats.txt", "w") as f:
        for sym in earnings_beats:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\earnings_misses.txt", "w") as f:
        for sym in earnings_misses:
            f.write(sym + '\n')        

def active_volatile(symbols=None, plot=False):
    if symbols == None:
        symbols = symbols
    df = pd.DataFrame({'Symbol': symbols.keys(), 'AvgDV20': [symbols[sym].df['AvgDV20'].iloc[-1] for sym in symbols], 'Relative_ATR': [symbols[sym].df['Relative_ATR'].iloc[-1] for sym in symbols]})
    df['AvgDV20_std_from_mean'] = df.AvgDV20.apply(lambda x: (x - df.AvgDV20.mean()) / df.AvgDV20.std())
    df['Relative_ATR_std_from_mean'] = df.Relative_ATR.apply(lambda x: (x - df.Relative_ATR.mean()) / df.Relative_ATR.std())
    df['dist_from_origin'] = df.apply(lambda x: math.dist([x['AvgDV20_std_from_mean'], x['Relative_ATR_std_from_mean']], [0, 0]), axis=1)
    active_volatile_df = df
    active_volatile_quadrant1 = df.Symbol.loc[(df.AvgDV20_std_from_mean > 0) & (df.Relative_ATR_std_from_mean > 0)].to_list()
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\active_volatile_quadrant1.txt", "w") as f:
        for sym in active_volatile_quadrant1:
            f.write(sym + '\n')
    if plot == True:
        return px.scatter(df, x=df.AvgDV20_std_from_mean, y=df.Relative_ATR_std_from_mean, hover_data=['Symbol'])

def price_above_target(symbols=None, save=True):
    price_above_target_results = []
    if symbols == None:
        symbols=symbols
    errors = {}
    for sym in symbols:
        try:
            if symbols[sym].df['Close'].iloc[-1] > float(sa.analyst_price_targets_dict[sym]['estimates'][sa.meta_data[sym]['data']['id']]['target_price']['0'][0]['dataitemvalue']):
                price_above_target_results.append(sym)
        except Exception as e:
            errors[sym] = e
            continue
    if save == True:
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\price_above_target.txt", "w") as f:
            for sym in price_above_target_results:
                f.write(sym + '\n')

def price_above_high(symbols=None, save=True):
    price_above_high_results = []
    if symbols == None:
        symbols=symbols
    errors = {}
    for sym in symbols:
        try:
            if symbols[sym].df['Close'].iloc[-1] > float(sa.analyst_price_targets_dict[sym]['estimates'][sa.meta_data[sym]['data']['id']]['target_price_high']['0'][0]['dataitemvalue']):
                price_above_high_results.append(sym)
        except Exception as e:
            errors[sym] = e
            continue
    if save == True:
        with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\price_above_high.txt", "w") as f:
            for sym in price_above_high_results:
                f.write(sym + '\n')


def regime_watchlists(symbols):
    bull_bo_252 = []
    bear_bo_252 = []
    bull_bo_50 = []
    bear_bo_50 = []
    bull_bo_20 = []
    bear_bo_20 = []
    bull_bo_10 = []
    bear_bo_10 = []
    bull_bo_5 = []
    bear_bo_5 = []
    bull_turtle_5020 = []
    bear_turtle_5020 = []
    bull_turtle_2010 = []
    bear_turtle_2010 = []
    bull_turtle_105 = []
    bear_turtle_105 = []
    bull_sma50200 = []
    bear_sma50200 = []
    bull_ema50200 = []
    bear_ema50200 = []
    bull_ema2050 = []
    bear_ema2050 = []
    bull_ema1020 = []
    bear_ema1020 = []
    bull_regime_crossing_ma_fanned_2050200 = []
    bear_regime_crossing_ma_fanned_2050200 = []
    bull_regime_crossing_ma_crossed_2050200 = []
    bear_regime_crossing_ma_crossed_2050200 = []
    bull_regime_crossing_ma_fanned_102050 = []
    bear_regime_crossing_ma_fanned_102050 = []
    bull_regime_crossing_ma_crossed_102050 = []
    bear_regime_crossing_ma_crossed_102050 = []
    bull_triple_ma2050200 = []
    bear_triple_ma2050200 = []
    bull_rg = []
    bear_rg = []
    bull_act_vol_rg = []
    bear_act_vol_rg = []
    neutral_act_vol_rg = []
    active_volatile_quadrant1 = open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\active_volatile_quadrant1.txt", "r").read().splitlines()
    for sym in symbols:
        try:
            if symbols[sym].df.bo_252.iloc[-1] > 0:
                bull_bo_252.append(sym)
            elif symbols[sym].df.bo_252.iloc[-1] < 0:
                bear_bo_252.append(sym)
            if symbols[sym].df.bo_50.iloc[-1] > 0:
                bull_bo_50.append(sym)
            elif symbols[sym].df.bo_50.iloc[-1] < 0:
                bear_bo_50.append(sym)
            if symbols[sym].df.bo_20.iloc[-1] > 0:
                bull_bo_20.append(sym)
            elif symbols[sym].df.bo_20.iloc[-1] < 0:
                bear_bo_20.append(sym)
            if symbols[sym].df.bo_10.iloc[-1] > 0:
                bull_bo_10.append(sym)
            elif symbols[sym].df.bo_10.iloc[-1] < 0:
                bear_bo_10.append(sym)
            if symbols[sym].df.bo_5.iloc[-1] > 0:
                bull_bo_5.append(sym)
            elif symbols[sym].df.bo_5.iloc[-1] < 0:
                bear_bo_5.append(sym)
            if symbols[sym].df.turtle50_20.iloc[-1] > 0:
                bull_turtle_5020.append(sym)
            elif symbols[sym].df.turtle50_20.iloc[-1] < 0:
                bear_turtle_5020.append(sym)
            if symbols[sym].df.turtle20_10.iloc[-1] > 0:
                bull_turtle_2010.append(sym)
            elif symbols[sym].df.turtle20_10.iloc[-1] < 0:
                bear_turtle_2010.append(sym)
            if symbols[sym].df.turtle10_5.iloc[-1] > 0:
                bull_turtle_105.append(sym)
            elif symbols[sym].df.turtle10_5.iloc[-1] < 0:
                bear_turtle_105.append(sym)
            if symbols[sym].df.sma50200.iloc[-1] > 0:
                bull_sma50200.append(sym)
            elif symbols[sym].df.sma50200.iloc[-1] < 0:
                bear_sma50200.append(sym)
            if symbols[sym].df.ema50200.iloc[-1] > 0:
                bull_ema50200.append(sym)
            elif symbols[sym].df.ema50200.iloc[-1] < 0:
                bear_ema50200.append(sym)
            if symbols[sym].df.ema2050.iloc[-1] > 0:
                bull_ema2050.append(sym)
            elif symbols[sym].df.ema2050.iloc[-1] < 0:
                bear_ema2050.append(sym)
            if symbols[sym].df.ema1020.iloc[-1] > 0:
                bull_ema1020.append(sym)
            elif symbols[sym].df.ema1020.iloc[-1] < 0:
                bear_ema1020.append(sym)
            if symbols[sym].df.regime_crossing_ma_long_2050200.iloc[-1] > 0:
                bull_regime_crossing_ma_fanned_2050200.append(sym)
            elif symbols[sym].df.regime_crossing_ma_long_2050200.iloc[-1] < 0:
                bull_regime_crossing_ma_crossed_2050200.append(sym)
            if symbols[sym].df.regime_crossing_ma_short_2050200.iloc[-1] > 0:
                bear_regime_crossing_ma_fanned_2050200.append(sym)
            elif symbols[sym].df.regime_crossing_ma_short_2050200.iloc[-1] < 0:
                bear_regime_crossing_ma_crossed_2050200.append(sym)
            if symbols[sym].df.regime_crossing_ma_long_102050.iloc[-1] > 0:
                bull_regime_crossing_ma_fanned_102050.append(sym)
            elif symbols[sym].df.regime_crossing_ma_long_102050.iloc[-1] < 0:
                bull_regime_crossing_ma_crossed_102050.append(sym)
            if symbols[sym].df.regime_crossing_ma_short_102050.iloc[-1] > 0:
                bear_regime_crossing_ma_fanned_102050.append(sym)
            elif symbols[sym].df.regime_crossing_ma_short_102050.iloc[-1] < 0:
                bear_regime_crossing_ma_crossed_102050.append(sym)
            if symbols[sym].df.triple_ma2050200.iloc[-1] > 0:
                bull_triple_ma2050200.append(sym)
            elif symbols[sym].df.triple_ma2050200.iloc[-1] < 0:
                bear_triple_ma2050200.append(sym)
            if symbols[sym].df.rg.iloc[-1] > 0:
                bull_rg.append(sym)
            elif symbols[sym].df.rg.iloc[-1] < 0:
                bear_rg.append(sym)
        except Exception as e:
            #TODO print(regime_watchlists.__name__, '\n', sym, e) update error handling
            continue
    for sym in active_volatile_quadrant1:
            try:
                if symbols[sym].df.rg.iloc[-1] > 0:
                    bull_act_vol_rg.append(sym)
                elif symbols[sym].df.rg.iloc[-1] < 0:
                    bear_act_vol_rg.append(sym)
                elif symbols[sym].df.rg.iloc[-1] == 0:
                    neutral_act_vol_rg.append(sym)
            except:
                continue
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_bo_252.txt", "w") as f:
        for sym in bull_bo_252:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_bo_252.txt", "w") as f:
        for sym in bear_bo_252:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_bo_50.txt", "w") as f:
        for sym in bull_bo_50:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_bo_50.txt", "w") as f:
        for sym in bear_bo_50:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_bo_20.txt", "w") as f:
        for sym in bull_bo_20:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_bo_20.txt", "w") as f:
        for sym in bear_bo_20:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_bo_10.txt", "w") as f:
        for sym in bull_bo_10:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_bo_10.txt", "w") as f:
        for sym in bear_bo_10:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_bo_5.txt", "w") as f:
        for sym in bull_bo_5:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_bo_5.txt", "w") as f:
        for sym in bear_bo_5:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_turtle_5020.txt", "w") as f:
        for sym in bull_turtle_5020:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_turtle_5020.txt", "w") as f:
        for sym in bear_turtle_5020:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_turtle_2010.txt", "w") as f:
        for sym in bull_turtle_2010:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_turtle_2010.txt", "w") as f:
        for sym in bear_turtle_2010:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_turtle_105.txt", "w") as f:
        for sym in bull_turtle_105:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_turtle_105.txt", "w") as f:
        for sym in bear_turtle_105:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_sma50200.txt", "w") as f:
        for sym in bull_sma50200:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_sma50200.txt", "w") as f:
        for sym in bear_sma50200:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_ema50200.txt", "w") as f:
        for sym in bull_ema50200:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_ema50200.txt", "w") as f:
        for sym in bear_ema50200:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_ema2050.txt", "w") as f:
        for sym in bull_ema2050:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_ema2050.txt", "w") as f:
        for sym in bear_ema2050:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_ema1020.txt", "w") as f:
        for sym in bull_ema1020:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_ema1020.txt", "w") as f:
        for sym in bear_ema1020:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_regime_crossing_ma_fanned_2050200.txt", "w") as f:
        for sym in bull_regime_crossing_ma_fanned_2050200:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_regime_crossing_ma_crossed_2050200.txt", "w") as f:
        for sym in bull_regime_crossing_ma_crossed_2050200:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_regime_crossing_ma_fanned_2050200.txt", "w") as f:
        for sym in bear_regime_crossing_ma_fanned_2050200:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_regime_crossing_ma_crossed_2050200.txt", "w") as f:
        for sym in bear_regime_crossing_ma_crossed_2050200:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_regime_crossing_ma_fanned_102050.txt", "w") as f:
        for sym in bull_regime_crossing_ma_fanned_102050:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_regime_crossing_ma_crossed_102050.txt", "w") as f:
        for sym in bull_regime_crossing_ma_crossed_102050:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_regime_crossing_ma_fanned_102050.txt", "w") as f:
        for sym in bear_regime_crossing_ma_fanned_102050:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_regime_crossing_ma_crossed_102050.txt", "w") as f:
        for sym in bear_regime_crossing_ma_crossed_102050:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_triple_ma2050200.txt", "w") as f:
        for sym in bull_triple_ma2050200:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_triple_ma2050200.txt", "w") as f:
        for sym in bear_triple_ma2050200:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_rg.txt", "w") as f:
        for sym in bull_rg:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_rg.txt", "w") as f:
        for sym in bear_rg:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bull_act_vol_rg.txt", "w") as f:
        for sym in bull_act_vol_rg:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\bear_act_vol_rg.txt", "w") as f:
        for sym in bear_act_vol_rg:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Regimes Watchlists\neutral_act_vol_rg.txt", "w") as f:
        for sym in neutral_act_vol_rg:
            f.write(sym + '\n')

#! Refactor incomplete. I may not be getting expected results because of few passing symbols and the limited number of symbols in the watchlist.
def no_income_price_above_high(symbols):
    no_income_results = open(wl.no_income).read().splitlines()
    no_income_price_above_high_results = []
    if len(no_income_results) == 0:
        raise ValueError('no_income_results list empty. Try calling no_income()')
    for sym in no_income_results:
        try:
            if symbols[sym].df['Close'].iloc[-1] > float(sa.analyst_price_targets_dict[sym]['estimates'][sa.meta_data[sym]['data']['id']]['target_price_high']['0'][0]['dataitemvalue']):
                no_income_price_above_high_results.append(sym)
        except Exception as e:
            #TODO print(no_income_price_above_high.__name__, sym, e) update error handling
            continue
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\no_income_price_above_high.txt", "w") as f:
        for sym in no_income_price_above_high_results:
            f.write(sym + '\n')

def seller_capitulation(symbols):
    seller_capitulation_results = []
    for sym in symbols:
        try:
            if (symbols[sym].df.RSI_14.iloc[-1] >= 69.5) & (symbols[sym].df.Close.iloc[-1] > symbols[sym].df.u_band.iloc[-1]):
                seller_capitulation_results.append(sym)
        except Exception as e:
            #TODO print(seller_capitulation.__name__, sym, e, sep=': ') update error handling
            continue
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\seller_capitulation.txt", "w") as f:
        for sym in seller_capitulation_results:
            f.write(sym + '\n')

def buyer_capitulation(symbols):
    buyer_capitulation_results = []
    for sym in symbols:
        try:
            if (symbols[sym].df.RSI_14.iloc[-1] <= 30.4) & (symbols[sym].df.Close.iloc[-1] < symbols[sym].df.l_band.iloc[-1]):
                buyer_capitulation_results.append(sym)
        except Exception as e:
            #TODO print(buyer_capitulation.__name__, sym, e, sep=': ') update error handling
            continue
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\buyer_capitulation.txt", "w") as f:
        for sym in buyer_capitulation_results:
            f.write(sym + '\n')

def uptrend_retracement(symbols):
    uptrend_retracement_results = []
    for sym in symbols:
        try:
            if (symbols[sym].df.ADX.iloc[-10:] > symbols[sym].df.ADX_EMA.iloc[-10:]).any() and (symbols[sym].df.ADX.iloc[-10:] >= 34.5).any() and (symbols[sym].df.ADX.iloc[-1] < symbols[sym].df.ADX_EMA.iloc[-1]) and (symbols[sym].df['+DI'].iloc[-1] > symbols[sym].df['-DI'].iloc[-1]):
                uptrend_retracement_results.append(sym)
        except Exception as e:
            #TODO print(uptrend_retracement.__name__, sym, e, sep=': ') update error handling
            continue
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\uptrend_retracement.txt", "w") as f:
        for sym in uptrend_retracement_results:
            f.write(sym + '\n')
    
def downtrend_retracement(symbols):
    downtrend_retracement_results = []
    for sym in symbols:
        try:
            if (symbols[sym].df.ADX.iloc[-10:] > symbols[sym].df.ADX_EMA.iloc[-10:]).any() and (symbols[sym].df.ADX.iloc[-10:] >= 34.5).any() and (symbols[sym].df.ADX.iloc[-1] < symbols[sym].df.ADX_EMA.iloc[-1]) and (symbols[sym].df['+DI'].iloc[-1] < symbols[sym].df['-DI'].iloc[-1]):
                downtrend_retracement_results.append(sym)
        except Exception as e:
            #TODO print(downtrend_retracement.__name__, sym, e, sep=': ') update error handling
            continue
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\downtrend_retracement.txt", "w") as f:
        for sym in downtrend_retracement_results:
            f.write(sym + '\n')

def finviz_high_p_e(value=40):
    overview = Overview()
    overview.set_filter(filters_dict={'P/E': f'Over {value}'})
    results = overview.screener_view().Ticker.to_list()
    finviz_high_p_e_results = [sym for sym in results if sym in symbols]
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\finviz_high_p_e.txt", "w") as f:
        for sym in finviz_high_p_e_results:
            f.write(sym + '\n')

def trends(symbols):
    distribution = []
    accumulation = []
    accumulation_breakout = []
    distribution_breakdown = []
    uptrend_accumulation = []
    downtrend_distribution = []
    uptrend = []
    downtrend = []
    for sym in tqdm(symbols, desc=f'Trends first loop'):
        try:
            symbols[sym].df['consolidation'] = np.nan
            symbols[sym].df['consolidation'].loc[symbols[sym].df.ADX < 15] = 1
            consolidation_firstday = symbols[sym].df.consolidation.loc[(symbols[sym].df.consolidation == 1) & (symbols[sym].df.consolidation.shift(1).isna())].index
            consolidation_lastday = symbols[sym].df.consolidation.loc[(symbols[sym].df.consolidation == 1) & (symbols[sym].df.consolidation.shift(-1).isna())].index
            consolidation_ranges = [(item[0], item[1]) for item in zip(consolidation_firstday, consolidation_lastday)]
            symbols[sym].df['acc_dist'] = np.nan
            for dates in consolidation_ranges:
                try:
                    if dates == consolidation_ranges[0]:
                        if (symbols[sym].df.ADX.loc[:dates[0]].max() > 35):
                            if (symbols[sym].df['+DI'].loc[:dates[0]].loc[symbols[sym].df.ADX.loc[:dates[0]] == symbols[sym].df.ADX.loc[:dates[0]].max()] > symbols[sym].df['-DI'].loc[:dates[0]].loc[symbols[sym].df.ADX.loc[:dates[0]] == symbols[sym].df.ADX.loc[:dates[0]].max()]).values[0]:
                                symbols[sym].df.acc_dist.loc[dates[0]:dates[1]] = 1
                            else:
                                symbols[sym].df.acc_dist.loc[dates[0]:dates[1]] = 2
                        else:
                            continue
                    elif dates == consolidation_ranges[-1]:
                        index = consolidation_ranges.index(dates)
                        if (symbols[sym].df.ADX.loc[consolidation_ranges[index-1][1]: dates[0]].max() > 35):
                            if (symbols[sym].df['+DI'].loc[consolidation_ranges[index-1][1]: dates[0]].loc[symbols[sym].df.ADX.loc[consolidation_ranges[index-1][1]: dates[0]] == symbols[sym].df.ADX.loc[consolidation_ranges[index-1][1]: dates[0]].max()] > symbols[sym].df['-DI'].loc[consolidation_ranges[index-1][1]: dates[0]].loc[symbols[sym].df.ADX.loc[consolidation_ranges[index-1][1]: dates[0]] == symbols[sym].df.ADX.loc[consolidation_ranges[index-1][1]: dates[0]].max()]).values[0]:
                                symbols[sym].df.acc_dist.loc[dates[0]:dates[1]] = 1
                            else:
                                symbols[sym].df.acc_dist.loc[dates[0]:dates[1]] = 2
                        else:
                            continue
                    else:
                        index = consolidation_ranges.index(dates)
                        if (symbols[sym].df.ADX.loc[consolidation_ranges[index-1][1]: dates[0]].max() > 35):
                            if (symbols[sym].df['+DI'].loc[consolidation_ranges[index-1][1]: dates[0]].loc[symbols[sym].df.ADX.loc[consolidation_ranges[index-1][1]: dates[0]] == symbols[sym].df.ADX.loc[consolidation_ranges[index-1][1]: dates[0]].max()] > symbols[sym].df['-DI'].loc[consolidation_ranges[index-1][1]: dates[0]].loc[symbols[sym].df.ADX.loc[consolidation_ranges[index-1][1]: dates[0]] == symbols[sym].df.ADX.loc[consolidation_ranges[index-1][1]: dates[0]].max()]).values[0]:
                                symbols[sym].df.acc_dist.loc[dates[0]:dates[1]] = 1
                            else:
                                symbols[sym].df.acc_dist.loc[dates[0]:dates[1]] = 2
                        else:
                            continue
                except Exception as e:
                    if dates == consolidation_ranges[0]:
                        #TODO print(sym, e, symbols[sym].df.ADX.loc[:dates[0]].max(), sep=': ') update error handling
                        continue
                    elif dates == consolidation_ranges[-1]:
                        #TODO print(sym, e, symbols[sym].df.ADX.loc[dates[0]:].max(), sep=': ') update error handling
                        continue
                    else:
                        #TODO print(sym, e, symbols[sym].df.ADX.loc[consolidation_ranges[index-1][1]: dates[0]].max(), sep=': ') update error handling
                        continue
                    continue
            symbols[sym].df['w_acc_dist'] = np.nan
            symbols[sym].df.w_acc_dist.loc[(symbols[sym].df.consolidation == 1) & (symbols[sym].df.Close > symbols[sym].df.Open)] = 1
            symbols[sym].df.w_acc_dist.loc[(symbols[sym].df.consolidation == 1) & (symbols[sym].df.Close < symbols[sym].df.Open)] = -1
            symbols[sym].df.w_acc_dist = symbols[sym].df.loc[(pd.notnull(symbols[sym].df.w_acc_dist))].apply(lambda x: x.w_acc_dist * x.Volume, axis=1)
            for dates in consolidation_ranges:
                try:
                    symbols[sym].df.w_acc_dist.loc[dates[0]:dates[1]] = symbols[sym].df.w_acc_dist.loc[dates[0]:dates[1]].cumsum()
                except Exception as e:
                    #TODO print(e) update error handling
                    continue
        except Exception as e:
            #TODO print(sym, e, sep=': ') update error handling
            continue

    for sym in tqdm(symbols, desc=f'Trends second loop'):
        try:
            try:
                if symbols[sym].df.w_acc_dist.iloc[-1] < 0:
                    distribution.append(sym)
                elif symbols[sym].df.w_acc_dist.iloc[-1] > 0:
                    accumulation.append(sym)
            except Exception as e:
                #TODO print(sym, e, sep=': ') update error handling
                pass
            try:
                consolidation_most_recent = symbols[sym].df.consolidation.loc[(symbols[sym].df.consolidation == 1) & (symbols[sym].df.consolidation.shift(1).isna())].index[-1]
                if symbols[sym].df.consolidation.iloc[-2] == 1:
                    if symbols[sym].df.Close.iloc[-1] > (symbols[sym].df.loc[consolidation_most_recent:].iloc[:-1].Close).max():
                        accumulation_breakout.append(sym)
                    elif symbols[sym].df.Close.iloc[-1] < (symbols[sym].df.loc[consolidation_most_recent:].iloc[:-1].Close).min():
                        distribution_breakdown.append(sym)
            except Exception as e:
                #TODO print(sym, e, sep=': ') update error handling
                pass
            try:
                if symbols[sym].df.acc_dist.iloc[-1] == 1:
                    uptrend_accumulation.append(sym)
                elif symbols[sym].df.acc_dist.iloc[-1] == 2:
                    downtrend_distribution.append(sym)
            except Exception as e:
                #TODO print(sym, e, sep=': ') update error handling
                pass
            try:
                if (symbols[sym].df.ADX.iloc[-1] > 35) and (symbols[sym].df['+DI'].iloc[-1] > symbols[sym].df['-DI'].iloc[-1]):
                    uptrend.append(sym)
                elif (symbols[sym].df.ADX.iloc[-1] > 35) and (symbols[sym].df['+DI'].iloc[-1] < symbols[sym].df['-DI'].iloc[-1]):
                    downtrend.append(sym)
            except Exception as e:
                #TODO print(sym, e, sep=': ') update error handling
                pass
        except Exception as e:
            #TODO print(sym, e, sep=': ') update error handling
            continue
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\distribution.txt", "w") as f:
        for sym in distribution:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\accumulation.txt", "w") as f:
        for sym in accumulation:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\accumulation_breakout.txt", "w") as f:
        for sym in accumulation_breakout:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\distribution_breakdown.txt", "w") as f:
        for sym in distribution_breakdown:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\uptrend.txt", "w") as f:
        for sym in uptrend:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\downtrend.txt", "w") as f:
        for sym in downtrend:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\uptrend_accumulation.txt", "w") as f:
        for sym in uptrend_accumulation:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\downtrend_distribution.txt", "w") as f:
        for sym in downtrend_distribution:
            f.write(sym + '\n')

def ma_overlap_long(symbols):
    overlap10_long = []
    overlap20_long = []
    overlap50_long = []
    for sym in symbols:
        try:
            open_ = symbols[sym].df.Open.iloc[-1]
            close = symbols[sym].df.Close.iloc[-1]
            low = symbols[sym].df.Low.iloc[-1]
            dma10 = symbols[sym].df['10DMA'].iloc[-1]
            dma20 = symbols[sym].df['20DMA'].iloc[-1]
            dma50 = symbols[sym].df['50DMA'].iloc[-1]
            if (close > dma10) and (low < dma10) and (open_ > dma10):
                overlap10_long.append(sym)
            if (close > dma20) and (low < dma20) and (open_ > dma20):
                overlap20_long.append(sym)
            if (close > dma50) and (low < dma50) and (open_ > dma50):
                overlap50_long.append(sym)
        except:
            continue
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\overlap10_long.txt", "w") as f:
        for sym in overlap10_long:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\overlap20_long.txt", "w") as f:
        for sym in overlap20_long:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\overlap50_long.txt", "w") as f:
        for sym in overlap50_long:
            f.write(sym + '\n') 

def ma_overlap_short(symbols):
    overlap10_short = []
    overlap20_short = []
    overlap50_short = []
    for sym in symbols:
        try:
            open_ = symbols[sym].df.Open.iloc[-1]
            close = symbols[sym].df.Close.iloc[-1]
            high = symbols[sym].df.High.iloc[-1]
            dma10 = symbols[sym].df['10DMA'].iloc[-1]
            dma20 = symbols[sym].df['20DMA'].iloc[-1]
            dma50 = symbols[sym].df['50DMA'].iloc[-1]
            if (close < dma10) and (high > dma10) and (open_ < dma10):
                overlap10_short.append(sym)
            if (close < dma20) and (high > dma20) and (open_ < dma20):
                overlap20_short.append(sym)
            if (close < dma50) and (high > dma50) and (open_ < dma50):
                overlap50_short.append(sym)
        except:
            continue
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\overlap10_short.txt", "w") as f:
        for sym in overlap10_short:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\overlap20_short.txt", "w") as f:
        for sym in overlap20_short:
            f.write(sym + '\n')
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\overlap50_short.txt", "w") as f:
        for sym in overlap50_short:
            f.write(sym + '\n')

def ma_pullback(symbols:dict, ma: str='20', alert_pass_threshold: float=0.5, alert_keep_threshold: float=1):
    ma_pullback_alerts_10 = []
    ma_pullback_alerts_20 = []
    ma_pullback_alerts_50 = []
    if ma == '10':
        ma_pullback_alerts = ma_pullback_alerts_10
    elif ma =='20':
        ma_pullback_alerts = ma_pullback_alerts_20
    elif ma == '50':
        ma_pullback_alerts = ma_pullback_alerts_50
    episodic_pivots_results = [item.replace('\n', '') for item in open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\episodic_pivots.txt").readlines()]
    fanned_up_results = [item.replace('\n', '') for item in open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\fanned_up_all.txt").readlines()]
    mid_long_term_fanned_up_results = [item.replace('\n', '') for item in open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\mid_long_term_fanned_up.txt").readlines()]
    syms = episodic_pivots_results + fanned_up_results + mid_long_term_fanned_up_results
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\\"+ma+"dma_pullback.txt") as f:
        for line in f:
            ma_pullback_alerts.append(line.replace('\n', ''))
    if len(ma_pullback_alerts) > 0:
        all_symbols_list = wl.make_watchlist(wl.all_symbols)
        for sym in ma_pullback_alerts:
            try:
                if -1 > symbols[sym].df['ATRs_from_'+ma+'DMA'].iloc[-1] > alert_keep_threshold:
                    ma_pullback_alerts.remove(sym)
            except KeyError as ke:
                if sym in all_symbols_list:#Continue if symbol is filter out by liquidity filter.
                    continue
                print(ma_pullback.__name__, sym, sep=': ')
                continue
    for sym in syms:
        if (sym in symbols) and (-0.5 < symbols[sym].df['ATRs_from_'+ma+'DMA'].iloc[-1] < alert_pass_threshold):
            ma_pullback_alerts.append(sym)
    ma_pullback_alerts = list(set(ma_pullback_alerts))
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\\"+ma+"dma_pullback.txt", "w") as f:
        for sym in ma_pullback_alerts:
            f.write(sym + '\n')

def run_all(symbols):
    """
    Manually invoke each filter function in this module in sequence.
    """
    # get this module object by its name
    mod = sys.modules['market_data.watchlist_filters']
    # Trend-based watchlists
    mod.fanned_up(symbols, save=True)
    mid_to_long_term_fanned_up(symbols)
    mod.technical_long_list_systematic(symbols, save=True)
    mod.technical_short_list_systematic(symbols, save=True)
    mod.regime_watchlists(symbols)
    mod.trends(symbols)
    # Price and volume activity
    mod.price_above_target(symbols)
    mod.price_above_high(symbols)
    mod.active_volatile(symbols)
    mod.ma_overlap_long(symbols)
    mod.ma_overlap_short(symbols)
    mod.ma_pullback(symbols, ma='10')
    mod.ma_pullback(symbols, ma='20')
    mod.ma_pullback(symbols, ma='50')
    # Relative strength and earnings
    mod.relative_strength(symbols)
    mod.relative_weakness(symbols)
    mod.earnings_performance(symbols)
    # Gap and income filters
    mod.gapped_no_income(symbols)
    mod.no_income()
    mod.no_income_price_above_target(symbols)
    mod.no_income_price_above_high(symbols)
    # Quant and short interest
    mod.high_quant()
    mod.low_quant()
    # mod.high_short_interest(symbols)
    mod.finviz_high_short_interest()
    mod.high_short_interest_price_above_target(symbols)
    mod.finviz_horizontal_support_resistance()
    mod.finviz_tl_support()
    mod.finviz_tl_resistance()  
    # Technical tenets and atr
    mod.tenets(symbols)
    mod.atrs_from(symbols)
    #Technical score
    tsc = mod.Technical_Score_Calculator()
    tsc.technical_score_calculator(symbols)
    tsc.ts_elevated()
    tsc.ts_depressed()
    return

