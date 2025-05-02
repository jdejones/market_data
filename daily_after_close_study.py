if __name__ == "__main__":
    import warnings
    # Suppress all warnings
    warnings.filterwarnings('ignore')
    #I added the try/except block as a quick fix to avoid conflicts with the
    #interactive interpreter and the github repo. There should be a
    #cleaner way to do this.
    try:
        from market_data.Symbol_Data import SymbolData
    except ModuleNotFoundError:
        import sys
        sys.path.insert(0, r"C:\Users\jdejo\Market_Data_Processing")
        from market_data.Symbol_Data import SymbolData
    from market_data.price_data_import import *
    from market_data.add_technicals import *
    from market_data.watchlists_locations import make_watchlist, hadv, sp500, iwm, mdy, etfs
    from market_data.watchlist_filters import Technical_Score_Calculator
    import market_data.watchlist_filters as wf
    import market_data.watchlists_locations as wl
    import market_data.seeking_alpha as sa
    import market_data.regimes as rg
    import market_data.support_functions as sf
    import market_data.fundamentals as fu
    import market_data.episodic_pivots as ep
    
    #hadv == high average dollar volume
    hadv = make_watchlist(hadv)
    symbols = {k: SymbolData(k, v) for k,v in api_import(hadv).items()}
    for symbol in tqdm(symbols, desc='Adding technicals'):        
        run_pipeline(symbols[symbol].df)
    r = rg.Regimes(symbols)
    r.run_all_combos()
    #TODO Correct the errors in ma_pullback.
    wf.run_all(symbols)

    #Sector and industry indices
    #TODO Correct the errors resulting in several printed symbols.
    sec = {k: SymbolData(k,v) for k,v in sf.create_index(symbols).items()}
    ind = {k: SymbolData(k,v) for k,v in sf.create_index(symbols, level='industry').items()}
    for symbol in tqdm(sec, desc='Adding technicals'):
        run_pipeline(sec[symbol].df)
    for symbol in tqdm(ind, desc='Adding technicals'):
        try:
            run_pipeline(ind[symbol].df)
        except Exception as e:
            print(f"Error adding technicals for {symbol}: {e}")

    #Market Cap weighted ETFs
    sp500 = {k: SymbolData(k,v) for k,v in api_import(make_watchlist(sp500), transfer={k:v.df for k,v in symbols.items()}).items()}
    mdy = {k: SymbolData(k,v) for k,v in api_import(make_watchlist(mdy), transfer={k:v.df for k,v in symbols.items()}).items()}
    iwm = {k: SymbolData(k,v) for k,v in api_import(make_watchlist(iwm), transfer={k:v.df for k,v in symbols.items()}).items()}

    for sym in tqdm(sp500, desc='Adding technicals'):
        if len(sp500[sym].df.columns) <= 10:
            run_pipeline(sp500[sym].df)
    for sym in tqdm(mdy, desc='Adding technicals'):
        if len(mdy[sym].df.columns) <= 10:
            run_pipeline(mdy[sym].df)
    for sym in tqdm(iwm, desc='Adding technicals'):
        if len(iwm[sym].df.columns) <= 10:
            run_pipeline(iwm[sym].df)            
            
    #All ETFs
    etfs = {k: SymbolData(k,v) for k,v in api_import(make_watchlist(etfs)).items()}
    for sym in tqdm(etfs, desc='Adding technicals'):
        run_pipeline(etfs[sym].df)
    r_etfs = rg.Regimes(etfs)
    r_etfs.run_all_combos()
    
    #Stock Stats
    stock_stats = {sym: sf.condition_statistics(sym) for sym in tqdm(symbols, desc='Calculating Stock Stats')}
    
    sector_close_vwap_count = {sector: [0, 0] for sector in sec}
    industry_close_vwap_count = {industry: [0, 0] for industry in ind}
    for sym in tqdm(symbols, desc=f'Close Over VWAP Ratio'):
        try:
            sec_ind = fu.sectors_industries[sym]
            close_over_vwap = bool(symbols[sym].df.iloc[-1].Close > symbols[sym].df.iloc[-1].VWAP)
        except Exception as e:
            continue
        try:
            for k in sector_close_vwap_count.keys():
                if k == sec_ind['sector']:
                    if close_over_vwap == True:
                        sector_close_vwap_count[k][0] += 1
                        sector_close_vwap_count[k][1] += 1
                    else:
                        sector_close_vwap_count[k][1] += 1
        except Exception as e:
            pass
        try:
            for k in industry_close_vwap_count.keys():
                if k == sec_ind['industry']:
                    if close_over_vwap == True:
                        industry_close_vwap_count[k][0] += 1
                        industry_close_vwap_count[k][1] += 1
                    else:
                        industry_close_vwap_count[k][1] += 1
        except Exception as e:
            pass
    sector_close_vwap_ratio = {sector: (sector_close_vwap_count[sector][0]/sector_close_vwap_count[sector][1]) * 100 for sector in sec}
    industry_close_vwap_ratio = {industry: (industry_close_vwap_count[industry][0]/industry_close_vwap_count[industry][1]) * 100 for industry in ind}

    #Episodic Pivots
    #!There are errors in the durations functions.
    ep.load_all()
    ep_curdur = {}
    for sym in tqdm(ep.current_duration_dict, desc=f'Episodic Pivots Current Duration'):
        try:
            ep_curdur[sym] = [ep.current_duration_dict[sym], fu.sa_fundamental_data['quantRating'].loc[fu.sa_fundamental_data.Symbol == sym].values[0]]
        except Exception as e:
            #TODO print(sym, ': ', e) update error handling
            continue
    ep_rr = {}
    for sym in tqdm([item[0] for item in sorted(ep.reward_risk_dict.items(), key=lambda x: x[1])], desc=f'Episodic Pivots Reward Risk'):
        try:
            ep_rr[sym] = [ep.reward_risk_dict[sym], int(fu.sa_fundamental_data['quantRating'].loc[fu.sa_fundamental_data.Symbol == sym].values[0])]
        except Exception as e:
            #TODO print(sym, e, sep=': ') update error handling
            continue    



    rel_stren = sf.relative_strength(symbols)
    prev_perf_since_earnings = sf.perf_since_earnings(symbols, earnings_season_start='2025-01-15')
    perf_since_earnings = sf.perf_since_earnings(symbols, earnings_season_start='2025-04-11')
    days_elevated_rvol = {}
    days_range_expansion = {}
    for sym in symbols:
        try:
            n = 0
            if symbols[sym].df.RVol.iloc[-1] > 1:
                elevated_rvol = True
                rvol_rev = symbols[sym].df.RVol.iloc[::-1]
                while elevated_rvol:
                    if rvol_rev[n] > 1:
                        n+=1
                    else:
                        elevated_rvol = False
                days_elevated_rvol[sym] = n
            n = 0
            if symbols[sym].df['ATRs_traded'].iloc[-1] > 1:
                atrs_traded = True
                atrs_traded_rev = symbols[sym].df['ATRs_traded'].iloc[::-1]
                while atrs_traded:
                    if atrs_traded_rev[n] > 1:
                        n+=1
                    else:
                        atrs_traded = False
                days_range_expansion[sym] = n
        except Exception as e:
            print(sym, e, sep=': ')
    from finvizfinance.screener.custom import Custom
    custom = Custom()
    cols = [col for col in custom.get_columns()]
    try:
        results_finvizsearch = custom.screener_view(limit=-1, select_page=None, verbose=1, ascend=True, columns=cols, sleep_sec=1)
    except:
        results_finvizsearch = custom.screener_view(limit=-1, select_page=None, verbose=1, ascend=True, columns=cols, sleep_sec=1)
    results_finvizsearch['DV'] = pd.to_numeric(results_finvizsearch['Previous Close'], errors='coerce').astype(float) * results_finvizsearch.Volume
    results_finvizsearch['Market Cap.'] = pd.to_numeric(results_finvizsearch['Market Cap.'].str.replace('.', '').str.replace('B', '0000000').str.replace('M', '0000'), errors='coerce').astype(float)
    results_finvizsearch['DV_Cap'] = results_finvizsearch['DV'] / results_finvizsearch['Market Cap.']
    dv_cap = results_finvizsearch[['Ticker', 'DV_Cap']].dropna().loc[results_finvizsearch.DV > 5_000_000].sort_values('DV_Cap')
    results_finvizsearch['Performance (YearToDate)'] = pd.to_numeric(results_finvizsearch['Performance (YearToDate)'].str.replace('.', '').str.replace('%', ''), errors='coerce').astype(float) / 100
    results_finvizsearch['perf_dvcap_dist'] = results_finvizsearch.apply(lambda x: np.linalg.norm(np.array([x['Performance (YearToDate)'], x['DV_Cap']])), axis=1)
    perf_dvcap_dist = results_finvizsearch[['Ticker', 'perf_dvcap_dist']].dropna().loc[results_finvizsearch.DV > 5_000_000]
    columns_with_percent = [col for col in results_finvizsearch.columns if (results_finvizsearch[col].astype(str).str.contains('%').any()) and (col != 'Company')]
    results_finvizsearch = results_finvizsearch.rename({col: f'{col}(%)' for col in columns_with_percent}, axis=1)
    for col in columns_with_percent:
            results_finvizsearch[f'{col}(%)'] = results_finvizsearch[f'{col}(%)'].str.replace('%', '')
            results_finvizsearch[f'{col}(%)'] = pd.to_numeric(results_finvizsearch[f'{col}(%)'], errors='coerce')
            results_finvizsearch[f'{col}(%)'] = results_finvizsearch[f'{col}(%)'] / 100
    top_rstren = [item[0] for item in rel_stren if (item[1] > 70) and (hadv.saved_dict[item[0]]['Relative ATR'].iloc[-1] > 4)]
    top_prevperfearn = [item[0] for item in prev_perf_since_earnings if (item[1] > 50) and (hadv.saved_dict[item[0]]['Relative ATR'].iloc[-1] > 4)]
    top_perfearn = [item[0] for item in perf_since_earnings if (item[1] > 30) and (hadv.saved_dict[item[0]]['Relative ATR'].iloc[-1] > 4)]
    long_list = top_rstren + top_prevperfearn + top_perfearn
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\long_list.txt", "w") as f:
        for sym in long_list:
            f.write(sym + '\n')

    bottom_rweak = [item[0] for item in rel_stren if (item[1] < 30) and (hadv.saved_dict[item[0]]['Relative ATR'].iloc[-1] > 4)]
    bottom_prevperfearn = [item[0] for item in prev_perf_since_earnings if (item[1] < -30) and (hadv.saved_dict[item[0]]['Relative ATR'].iloc[-1] > 4)]
    bottom_perfearn = [item[0] for item in perf_since_earnings if (item[1] < -20) and (hadv.saved_dict[item[0]]['Relative ATR'].iloc[-1] > 4)]
    short_list = bottom_rweak + bottom_prevperfearn + bottom_perfearn
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\short_list.txt", "w") as f:
        for sym in short_list:
            f.write(sym + '\n')
    shortable = [sym.replace('\n', '') for sym in open(r"E:\Market Research\Studies\Sector Studies\Watchlists\shortable.txt").readlines()]
    hist_short_int = pd.read_csv(r"E:\Market Research\Dataset\Fundamental Data\historic_short_interest.txt")