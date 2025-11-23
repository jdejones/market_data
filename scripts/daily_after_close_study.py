if __name__ == "__main__":
    import warnings
    # Suppress all warnings
    warnings.filterwarnings('ignore')
    # I added the try/except block as a quick fix to avoid conflicts with the
    # interactive interpreter and the github repo. There should be a
    # cleaner way to do this.
    try:
        from market_data.Symbol_Data import SymbolData
    except ModuleNotFoundError:
        import sys
        sys.path.insert(0, r"C:\Users\jdejo\Market_Data_Processing")
        from market_data.Symbol_Data import SymbolData
    from market_data.price_data_import import *
    from market_data.add_technicals import *
    from market_data.add_technicals import _add_technicals_worker
    from market_data.watchlists_locations import make_watchlist, hadv, sp500, iwm, mdy, etfs
    from market_data.watchlist_filters import Technical_Score_Calculator
    import market_data.watchlist_filters as wf
    import market_data.watchlists_locations as wl
    import market_data.seeking_alpha as sa
    import market_data.regimes as rg
    import market_data.support_functions as sf
    import market_data.fundamentals as fu
    import market_data.stats_objects as so
    import market_data.anchored_vwap as av
    from market_data.episodic_pivots import Episodic_Pivots
    from market_data import operator, np, ProcessPoolExecutor, as_completed, pickle, threading
    from market_data.stats_objects import IntradaySignalProcessing as isp
    from market_data import create_engine, text, DateTime, pymysql, redis, json, gzip
    from market_data.api_keys import database_password, seeking_alpha_api_key
    from market_data.interest_list import InterestList as il
    
    #*Import price data.
    #hadv == high average dollar volume
    hadv = make_watchlist(hadv)
    data = api_import(hadv)
    symbols = {k: SymbolData(k, v) for k,v in data.items()}
    
    #########################################################################
    #Connect to database
    #*Temporarily commented out while fixing MySQL bugs
    # url = f"mysql+pymysql://root:{database_password}@127.0.0.1:3306/daily_sa"
    # engine = create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


    # # query a database -> DataFrame
    # daily_quant_rating_df = pd.read_sql("SELECT * FROM quant_rating", con=engine)
    daily_quant_rating_df = pd.read_csv(r"E:\Market Research\temporary.csv", index_col='Unnamed: 0')
    
    # if len(daily_quant_rating_df.columns) > 1000:
    #     warnings.warn("Number of columns is greater than 1000. Limit is 1017.")
        
    # #*Temporarily commented out while fixing MySQL bugs
    # # daily_quant_rating_df.set_index('index', inplace=True)
    # #Concatenate new column
    # daily_quant_rating_df = pd.concat([daily_quant_rating_df, pd.DataFrame({datetime.datetime.today().date(): []})], axis=1)

    # #Request and add new rows
    # #list of symbols
    # syms = list(symbols.keys())

    # #api requests
    # #assign counters
    # i=0
    # j=50
    # while True:
    #         #break when list is exhausted
    #         if i > len(syms):
    #             break
    #         #assign None near end of list
    #         if j > len(syms):
    #             j = None
    #         #API call
    #         url = "https://seeking-alpha.p.rapidapi.com/symbols/get-metrics"

    #         querystring = {"symbols":f"{','.join(syms[i:j])}","fields":"quant_rating"}

    #         headers = {
    #             "x-rapidapi-key": f"{seeking_alpha_api_key}",
    #             "x-rapidapi-host": "seeking-alpha.p.rapidapi.com"
    #         }

    #         response_request = requests.get(url, headers=headers, params=querystring)

    #         if response_request.status_code != 200:
    #             if response_request.status_code == 504:
    #                 print('request status code is 504: Gateway Timeout; sleeping for 30 seconds and retrying')
    #                 time.sleep(30)
    #                 response_request = requests.get(url, headers=headers, params=querystring)
    #                 response = response_request.json()
    #             else:
    #                 print(f'request status code is {response_request.status_code}: retrying request after 30 seconds')
    #                 time.sleep(30)
    #                 response_request = requests.get(url, headers=headers, params=querystring)
    #                 response = response_request.json()

    #         response = response_request.json()
                    
    #         if response_request.status_code != 200:
    #             break
            
    #         #Container for symbol and respective rating
    #         quant_ratings_errors = {}
    #         try:
    #             sym_ratings = {sa.sym_by_id[_['id'].strip('[]').split(',')[0]]:_['attributes']['value'] for _ in response['data']}
    #         except:
    #             sym_ratings = {}
    #             for _ in response['data']:
    #                 try:
    #                     sym_ratings[sa.sym_by_id[_['id'].strip('[]').split(',')[0]]] = _['attributes']['value']
    #                 except Exception as e:
    #                     quant_ratings_errors[f'{i}:{j}'] = ((i,j), _, e)
    #                     continue
    #             pass
    #         if len(quant_ratings_errors) > 3000:
    #             break
            
    #         #Concatenate rating to dataframe
    #         for sym in sym_ratings:
    #             daily_quant_rating_df.loc[sym, datetime.datetime.today().date()] = sym_ratings[sym]
                    
    #         #Increment counters
    #         i += 50
    #         if j == None:
    #             break
    #         j += 50
    #         time.sleep(2)

    # #*Temporarily commented out while fixing MySQL bugs
    # # daily_quant_rating_df.reset_index(inplace=True)
    # # # # write back, replace existing table
    # # daily_quant_rating_df.to_sql("quant_rating", 
    # #         con=engine, 
    # #         if_exists="replace", 
    # #         index=False, 
    # #         method="multi",
    # #         chunksize=200,
    # #         dtype={'date': DateTime})
    # # daily_quant_rating_df.set_index('index', inplace=True)
    # # daily_quant_rating_df.index.name = 'Symbol'
    # daily_quant_rating_df.to_csv(r"E:\Market Research\temporary.csv")
    daily_quant_rating_df['diff'] = daily_quant_rating_df[daily_quant_rating_df.columns[-1]] - daily_quant_rating_df[daily_quant_rating_df.columns[-2]]
    #########################################################################
    
    #*Add technicals.
    items = [(sd.symbol, sd.df) for sd in symbols.values()]
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
            symbols[symbol].df = df
    
    #TODO Add concurrency for regimes               
    r = rg.Regimes(symbols)
    r.run_all_combos()
    wf.run_all(symbols)

    #Sector and industry indices
    #TODO Add concurrency for sector and industry indices instantiation.
    sec = {k: SymbolData(k,v) for k,v in sf.create_index(symbols).items() if len(v) > 0}#TODO: Add error handling for symbols that do not have data in sa.key_data.
    ind = {k: SymbolData(k,v) for k,v in sf.create_index(symbols, level='industry').items() if len(v) > 0}#TODO: Add error handling for symbols that do not have data in sa.key_data.
    #TODO Add concurrency for sector and industry indices technicals.
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

    for sym in tqdm(sp500, desc='Adding technicals to SPY stocks'):
        if len(sp500[sym].df.columns) <= 10:
            run_pipeline(sp500[sym].df)
    for sym in tqdm(mdy, desc='Adding technicals to MDY stocks'):
        if len(mdy[sym].df.columns) <= 10:
            run_pipeline(mdy[sym].df)
    for sym in tqdm(iwm, desc='Adding technicals to IWM stocks'):
        if len(iwm[sym].df.columns) <= 10:
            run_pipeline(iwm[sym].df)            
            
    #All ETFs
    etfs = {k: SymbolData(k,v) for k,v in api_import(make_watchlist(etfs)).items()}
    for sym in tqdm(etfs, desc='Adding technicals to ETFs'):
        run_pipeline(etfs[sym].df)
    r_etfs = rg.Regimes(etfs)
    r_etfs.run_all_combos()
    
    #Stock Stats
    stock_stats = {}
    symbols_list = list(symbols.keys())
    dfs         = [symbols[sym].df for sym in symbols_list]
    with ProcessPoolExecutor(max_workers=5) as executor:
        for sym, stats in tqdm(
            zip(symbols_list, executor.map(sf.condition_statistics, dfs, chunksize=1)),
            total=len(symbols_list),
            desc="Calculating Stock Stats"
        ):
            stock_stats[sym] = stats
    
    # Calculate expected value (EV) for each condition across all symbols
    ev = {}
    
    # First, collect all returns for each condition across all symbols
    all_returns = {}
    for sym in stock_stats:
        if 'returns' not in stock_stats[sym]:
            continue
            
        for condition in stock_stats[sym]['returns']:
            if condition not in all_returns:
                all_returns[condition] = {'5days': [], '10days': [], '20days': []}
            
            for days in ['5days', '10days', '20days']:
                if days in stock_stats[sym]['returns'][condition] and stock_stats[sym]['returns'][condition][days]:
                    all_returns[condition][days].extend(stock_stats[sym]['returns'][condition][days])
    
    # Calculate expected value for each condition
    for condition in tqdm(all_returns, desc="Calculating Expected Values by Condition"):
        ev[condition] = {}
        
        for days in ['5days', '10days', '20days']:
            if not all_returns[condition][days]:
                ev[condition][days] = 0.0
                continue
            
            # Convert returns to numpy array
            returns_array = np.array(all_returns[condition][days])
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
            
            # Calculate average returns for each outcome
            avg_positive = np.mean(returns_array[positive_mask]) if n_positive > 0 else 0
            avg_negative = np.mean(returns_array[negative_mask]) if n_negative > 0 else 0
            
            # Expected value calculation using numpy dot product
            probabilities = np.array([prob_positive, prob_negative])
            outcomes = np.array([avg_positive, avg_negative])
            expected_value = np.dot(probabilities, outcomes)
            
            ev[condition][days] = round(expected_value, 4)
        
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
    ep = Episodic_Pivots(symbols)
    ep.load_all()
    ep_curdur = {}
    #TODO Add concurrency for to compute the following two loops in parallel?
    for sym in tqdm(ep.current_duration_dict, desc=f'Episodic Pivots Current Duration'):
        try:
            ep_curdur[sym] = [ep.current_duration_dict[sym], round(fu.sa_fundamental_data['quantRating'].loc[fu.sa_fundamental_data.Symbol == sym].values[0].item(), 3)]
        except Exception as e:
            #TODO print(sym, ': ', e) update error handling
            continue
    ep_rr = {}
    for sym in tqdm([item[0] for item in sorted(ep.reward_risk_dict.items(), key=lambda x: x[1])], desc=f'Episodic Pivots Reward Risk'):
        try:
            ep_rr[sym] = [round(ep.reward_risk_dict[sym].item(), 3), round(fu.sa_fundamental_data['quantRating'].loc[fu.sa_fundamental_data.Symbol == sym].values[0].item(), 1)]
        except Exception as e:
            #TODO print(sym, e, sep=': ') update error handling
            continue


    rel_stren_obj = sf.relative_strength(symbols)
    rel_stren = rel_stren_obj()
    prev_perf_since_earnings = sf.perf_since_earnings(symbols, earnings_season_start='2025-04-11')
    perf_since_earnings = sf.perf_since_earnings(symbols, earnings_season_start='2025-07-15')
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
            if symbols[sym].df['ATRs_Traded'].iloc[-1] > 1:
                atrs_traded = True
                atrs_traded_rev = symbols[sym].df['ATRs_Traded'].iloc[::-1]
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
    except ConnectionError:
        results_finvizsearch = custom.screener_view(limit=-1, select_page=None, verbose=1, ascend=True, columns=cols, sleep_sec=1)
    results_finvizsearch['DV'] = pd.to_numeric(results_finvizsearch['Previous Close'], errors='coerce').astype(float) * results_finvizsearch.Volume
    results_finvizsearch['Market Cap.'] = pd.to_numeric(results_finvizsearch['Market Cap.'].str.replace('.', '').str.replace('B', '0000000').str.replace('M', '0000'), errors='coerce').astype(float)
    results_finvizsearch['DV_Cap'] = results_finvizsearch['DV'] / results_finvizsearch['Market Cap.']
    dv_cap = results_finvizsearch[['Ticker', 'DV_Cap']].dropna().loc[results_finvizsearch.DV > 5_000_000].round(3).sort_values('DV_Cap')
    results_finvizsearch['Performance (YearToDate)'] = pd.to_numeric(results_finvizsearch['Performance (YearToDate)'].str.replace('.', '').str.replace('%', ''), errors='coerce').astype(float) / 100
    results_finvizsearch['perf_dvcap_dist'] = results_finvizsearch.apply(lambda x: np.linalg.norm(np.array([x['Performance (YearToDate)'], x['DV_Cap']])), axis=1)
    perf_dvcap_dist = results_finvizsearch[['Ticker', 'perf_dvcap_dist']].dropna().loc[results_finvizsearch.DV > 5_000_000].round(3)
    columns_with_percent = [col for col in results_finvizsearch.columns if (results_finvizsearch[col].astype(str).str.contains('%').any()) and (col != 'Company')]
    results_finvizsearch = results_finvizsearch.rename({col: f'{col}(%)' for col in columns_with_percent}, axis=1)
    for col in columns_with_percent:
            results_finvizsearch[f'{col}(%)'] = results_finvizsearch[f'{col}(%)'].str.replace('%', '')
            results_finvizsearch[f'{col}(%)'] = pd.to_numeric(results_finvizsearch[f'{col}(%)'], errors='coerce')
            results_finvizsearch[f'{col}(%)'] = results_finvizsearch[f'{col}(%)'] / 100
    top_rstren = [item[0] for item in rel_stren if (item[1] > 70) and (symbols[item[0]].df['Relative_ATR'].iloc[-1] > 4)]
    top_prevperfearn = [item[0] for item in prev_perf_since_earnings if (item[1] > 50) and (symbols[item[0]].df['Relative_ATR'].iloc[-1] > 4)]
    top_perfearn = [item[0] for item in perf_since_earnings if (item[1] > 30) and (symbols[item[0]].df['Relative_ATR'].iloc[-1] > 4)]
    long_list = top_rstren + top_prevperfearn + top_perfearn
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\long_list.txt", "w") as f:
        for sym in long_list:
            f.write(sym + '\n')

    bottom_rweak = [item[0] for item in rel_stren if (item[1] < 30) and (symbols[item[0]].df['Relative_ATR'].iloc[-1] > 4)]
    bottom_prevperfearn = [item[0] for item in prev_perf_since_earnings if (item[1] < -30) and (symbols[item[0]].df['Relative_ATR'].iloc[-1] > 4)]
    bottom_perfearn = [item[0] for item in perf_since_earnings if (item[1] < -20) and (symbols[item[0]].df['Relative_ATR'].iloc[-1] > 4)]
    short_list = bottom_rweak + bottom_prevperfearn + bottom_perfearn
    with open(r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists\short_list.txt", "w") as f:
        for sym in short_list:
            f.write(sym + '\n')
    shortable = [sym.replace('\n', '') for sym in open(r"E:\Market Research\Studies\Sector Studies\Watchlists\shortable.txt").readlines()]
    hist_short_int = pd.read_csv(r"E:\Market Research\Dataset\Fundamental Data\historic_short_interest.txt")

    tsc = Technical_Score_Calculator()
    tsc.technical_score_calculator(symbols)
    tsc_sec = Technical_Score_Calculator()
    tsc_sec.technical_score_calculator(sec)
    tsc_ind = Technical_Score_Calculator()
    tsc_ind.technical_score_calculator(ind)
    #TODO I believe the following are printing symbols that are not passing the liquidity filter.
    #TODO Add error handling for illiquid symbols.
    sector_member_mappings = {sector: fu.sector_industry_member_search(sector, level='sector') for sector in sec}
    industry_member_mappings = {industry: fu.sector_industry_member_search(industry, level='industry') for industry in ind}
    #It may be more pythonic to use the following dictionary comprehension.
    #The python kernel crashed before I would test the following dictionary comprehensions.
    #These could be added directory to the line tha prints sector/industry close over vwap
    # close_over_vwap_dict = {sector: close_over_vwap_ratio({sym: symbols[sym]}) for sector in sector_member_mappings for sym in sector_member_mappings[sector]}
    # close_over_vwap_dict = {industry: close_over_vwap_ratio({sym: symbols[sym]}) for industry in industry_member_mappings for sym in industry_member_mappings[industry]}
    def close_over_vwap_dict(mapping):
        fas = {}
        all_symbols_list = wl.make_watchlist(wl.all_symbols)
        for sector in mapping:
            #A dictionary mapping all symbols in an industry to their symbols data
            temp = {}
            try:
                for sym in mapping[sector]:
                    try:
                        if len(symbols[sym].df) == 0:
                            temp[sym] = symbols[sym]
                        else:
                            temp.update({sym: symbols[sym]})
                    except KeyError as ke:
                        if sym in all_symbols_list:
                            continue
                    except Exception as e:
                        print(close_over_vwap_dict.__name__, sym, e, sep=': ')
                        continue
            except Exception as e:
                print(close_over_vwap_dict.__name__, sym, e, sep=': ')
                continue
            fas[sector] = sf.close_over_vwap_ratio(temp)    
        return fas
    
    #Factors: Fundamental
    #Expected Revenue Growth +1Q and +4Q
    qplus1 = {}
    qplus4 = {}
    for sym in sa.earnings_dict:
        try:
            qplus1[sym] = (
                round((float(sa.earnings_dict[sym]['revenue_consensus_mean']['1'][0]['dataitemvalue'])
                - float(sa.earnings_dict[sym]['revenue_actual']['0'][0]['dataitemvalue'])
            ) / float(sa.earnings_dict[sym]['revenue_actual']['0'][0]['dataitemvalue']) * 100, 2)
            )
            qplus4[sym] = (
                round((float(sa.earnings_dict[sym]['revenue_consensus_mean']['4'][0]['dataitemvalue'])
                - float(sa.earnings_dict[sym]['revenue_actual']['0'][0]['dataitemvalue'])
            ) / float(sa.earnings_dict[sym]['revenue_actual']['0'][0]['dataitemvalue']) * 100, 2)
            )
        except:
            continue

    interest_list_long = il(source_symbols=symbols)
    interest_list_long.value_filter(rel_stren, 70, '>=', 'Technical', 'Long', 'rel_stren')
    interest_list_long.value_filter(prev_perf_since_earnings, 50, '>=', 'Technical', 'Long', 'prev_perf_since_earnings')
    interest_list_long.value_filter(perf_since_earnings, 30, '>=', 'Technical', 'Long', 'perf_since_earnings')
    interest_list_long.value_filter(tsc.sent_dict.items(), 
                                    (tsc.positive_sent_dict_stats.loc['mean'].values[0] + 
                                    (tsc.positive_sent_dict_stats.loc['std'].values[0] * 2)), 
                                    '>=', 'Technical', 'Long', 'tsc')

    interest_list_long.value_filter(qplus1, 50, '>=', 'Fundamental', 'Long', 'qplus1')
    interest_list_long.value_filter(qplus4, 100, '>=', 'Fundamental', 'Long', 'qplus4')
    quant_rating_interest = daily_quant_rating_df[daily_quant_rating_df.columns[-2]].reset_index()
    quant_rating_interest = [(sym, val) for sym, val in zip(quant_rating_interest['index'], quant_rating_interest[quant_rating_interest.columns[-1]])]
    interest_list_long.value_filter(quant_rating_interest, 4.9, '>=', 'Fundamental', 'Long', 'daily_quant_rating')
    
    with open(fr"{wl.systematic_watchlists_root}\interest_list_long.txt", "w") as f:
        for sym in interest_list_long.interest_list:
            f.write(sym + '\n')

    #Pickling most used objects, so I don't have to rerun the script.
    def save_snapshots(obj, name):
        base = r"E:\Market Research\Dataset\daily_after_close_study"
        with gzip.open(fr"{base}\{name}.pkl.gz", "wb", compresslevel=5) as f:
            pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
            
    t = []
    variables = ((symbols,'symbols'), 
            (sec, 'sec'),
            (ind, 'ind'),
            (sp500, 'sp500'),
            (mdy, 'mdy'),
            (iwm, 'iwm'),
            (etfs, 'etfs'),
            (stock_stats, 'stock_stats'),
            (ev, 'ev'),
            (all_returns, 'all_returns'),
            (sector_close_vwap_ratio, 'sector_close_vwap_ratio'),
            (industry_close_vwap_ratio, 'industry_close_vwap_ratio'),
            (ep_curdur, 'ep_curdur'),
            (ep_rr, 'ep_rr'),
            (rel_stren, 'rel_stren'),
            (prev_perf_since_earnings, 'prev_perf_since_earnings'),
            (perf_since_earnings, 'perf_since_earnings'),
            (days_elevated_rvol, 'days_elevated_rvol'),
            (days_range_expansion, 'days_range_expansion'),
            (results_finvizsearch, 'results_finvizsearch'),
            (tsc, 'tsc'),
            (tsc_sec, 'tsc_sec'),
            (tsc_ind, 'tsc_ind'),
            (qplus1, 'qplus1'),
            (qplus4, 'qplus4'),
            (interest_list_long.interest_list, 'interest_list_long'))
    for _ in variables:
        t.append(threading.Thread(
            target=save_snapshots,
            args=(_[0], _[1]),
            daemon=False,
        ))
    for _ in t:
        _.start()

    for _ in t:
        _.join()

    # #Pickling most used objects, so I don't have to rerun the script.
    # def save_snapshots(symbols, sec, ind, sp500, mdy, iwm, etfs, stock_stats):
    #     base = r"E:\Market Research\Dataset\daily_after_close_study"
    #     with open(fr"{base}\symbols.pkl", "wb") as f:
    #         pickle.dump(symbols, f, protocol=pickle.HIGHEST_PROTOCOL)
    #     with open(fr"{base}\sec.pkl", "wb") as f:
    #         pickle.dump(sec, f, protocol=pickle.HIGHEST_PROTOCOL)
    #     with open(fr"{base}\ind.pkl", "wb") as f:
    #         pickle.dump(ind, f, protocol=pickle.HIGHEST_PROTOCOL)
    #     with open(fr"{base}\sp500.pkl", "wb") as f:
    #         pickle.dump(sp500, f, protocol=pickle.HIGHEST_PROTOCOL)
    #     with open(fr"{base}\mdy.pkl", "wb") as f:
    #         pickle.dump(mdy, f, protocol=pickle.HIGHEST_PROTOCOL)
    #     with open(fr"{base}\iwm.pkl", "wb") as f:
    #         pickle.dump(iwm, f, protocol=pickle.HIGHEST_PROTOCOL)
    #     with open(fr"{base}\etfs.pkl", "wb") as f:
    #         pickle.dump(etfs, f, protocol=pickle.HIGHEST_PROTOCOL)
    #     with open(fr"{base}\stock_stats.pkl", "wb") as f:
    #         pickle.dump(stock_stats, f, protocol=pickle.HIGHEST_PROTOCOL)

    # t = threading.Thread(
    #     target=save_snapshots,
    #     args=(symbols, sec, ind, sp500, mdy, iwm, etfs, stock_stats),
    #     daemon=True,
    # )
    # t.start()

    #TODO Remove the following code once the previous threading is working.
    #Pickling most used objects, so I don't have to rerun the script.
    # with open(r"E:\Market Research\Dataset\daily_after_close_study\symbols.pkl", "wb") as f:
    #     pickle.dump(symbols, f)

    # with open(r"E:\Market Research\Dataset\daily_after_close_study\sec.pkl", "wb") as f:
    #     pickle.dump(sec, f)

    # with open(r"E:\Market Research\Dataset\daily_after_close_study\ind.pkl", "wb") as f:
    #     pickle.dump(ind, f)

    # with open(r"E:\Market Research\Dataset\daily_after_close_study\sp500.pkl", "wb") as f:
    #     pickle.dump(sp500, f)

    # with open(r"E:\Market Research\Dataset\daily_after_close_study\mdy.pkl", "wb") as f:
    #     pickle.dump(mdy, f)

    # with open(r"E:\Market Research\Dataset\daily_after_close_study\iwm.pkl", "wb") as f:
    #     pickle.dump(iwm, f)

    # with open(r"E:\Market Research\Dataset\daily_after_close_study\etfs.pkl", "wb") as f:
    #     pickle.dump(etfs, f)

    # with open(r"E:\Market Research\Dataset\daily_after_close_study\stock_stats.pkl", "wb") as f:
    #     pickle.dump(stock_stats, f)


    
    #Redis storage
    #Symbols
    # r = redis.Redis(host="localhost", port=6379, decode_responses=True, 
    #                 socket_keepalive=True, retry_on_timeout=True) 
    # i=0
    # j=1000
    # while True:
    #     symbols_redis = {}
    #     for sym in list(symbols.keys())[i:j]:
    #         symbols_redis[sym] = symbols[sym].to_redis()
    #     from itertools import islice
    #     def batched(iterable, n):
    #         it = iter(iterable)
    #         while True:
    #             batch = list(islice(it, n))
    #             if not batch:
    #                 break
    #             yield batch
    #     BATCH_SIZE = 50  # or even 20 if needed
    #     for batch in batched(list(symbols_redis.items()), BATCH_SIZE):
    #         pipe = r.pipeline(transaction=False)
    #         for sym, data in batch:
    #             pipe.hset(f"symbols", sym, json.dumps(data, default=str))
    #         pipe.execute()
    #     i += 1000
    #     if i > len(list(symbols.keys())):
    #         break
    #     if ((j+1000) > len(list(symbols.keys()))):
    #         j = None
    #     if j is not None:
    #         j += 1000
    # #Sec
    # sec_redis = {}
    # for s in sec:
    #     sec_redis[s] = sec[s].to_redis()
    #     r.hset("sec", mapping={s:json.dumps(sec_redis[s], default=str)})
    # #Ind
    # ind_redis = {}
    # for i in ind:
    #     ind_redis[i] = ind[i].to_redis()
    #     r.hset("ind", mapping={i:json.dumps(ind_redis[i], default=str)})
    # #sp500
    # sp500_redis = {}
    # sp500_redis['in_symbols'] = [sym for sym in sp500 if sym in symbols]
    # for sym in sp500:
    #     if sym not in symbols:
    #         sp500_redis[sym] = sp500[sym].to_redis()
    #         r.hset("sp500", mapping={sym:json.dumps(sp500_redis[sym], default=str)})
    # r.hset("sp500", mapping={'in_symbols':json.dumps(sp500_redis['in_symbols'], default=str)})
    # #mdy
    # mdy_redis = {}
    # mdy_redis['in_symbols'] = [sym for sym in mdy if sym in symbols]
    # for sym in mdy:
    #     if sym not in symbols:
    #         mdy_redis[sym] = mdy[sym].to_redis()
    #         r.hset("mdy", mapping={sym:json.dumps(mdy_redis[sym], default=str)})
    # r.hset("mdy", mapping={'in_symbols':json.dumps(mdy_redis['in_symbols'], default=str)})
    # #iwm
    # iwm_redis = {}
    # iwm_redis['in_symbols'] = [sym for sym in iwm if sym in symbols]
    # for sym in iwm:
    #     if sym not in symbols:
    #         iwm_redis[sym] = iwm[sym].to_redis()
    #         r.hset("iwm", mapping={sym:json.dumps(iwm_redis[sym], default=str)})
    # r.hset("iwm", mapping={'in_symbols':json.dumps(iwm_redis['in_symbols'], default=str)})
    # #etfs
    # etfs_redis = {}
    # for sym in etfs:
    #     etfs_redis[sym] = etfs[sym].to_redis()
    #     r.hset("etfs", mapping={sym:json.dumps(etfs_redis[sym], default=str)})        


    from IPython.display import display, HTML
    from pprint import pprint
    print('\ndf_byrvol_positive dataframe descriptive statistics.',
          tsc.positive_sent_dict_stats,
          '\n50 rows of the top performing symbols by relative volume',
          tsc.df_byrvol_positive[:50],
          '\ndf_byrvol_negative dataframe descriptive statistics.',
          tsc.negative_sent_dict_stats,
          '\n50 rows of under performing symbols by relative volume',
          tsc.df_byrvol_negative[:50],
          sf.sec_ind_activity_by_tss_plots(tsc),
          '\nTop Performing Sectors by Relative Volume',
          tsc_sec.df_byrvol_positive,
          #display(HTML(hadv_sec.df_byrvol_positive.to_html())),
          '\nUnder Performing Sectors by Relative Volume',
          tsc_sec.df_byrvol_negative,
          #display(HTML(hadv_sec.df_byrvol_negative.to_html())),
          '\nTop Performing Industries by Relative Volume',
          tsc_ind.df_byrvol_positive,
          #display(HTML(hadv_ind.df_byrvol_positive.to_html())),
          '\nUnder Performing Industries by Relative Volume',
          tsc_ind.df_byrvol_negative,
          #display(HTML(hadv_ind.df_byrvol_negative.to_html())),
          sf.primary_statistics(hadv=symbols, sp500=sp500, mdy=mdy, iwm=iwm),#I will add IWM back when I figure out why it isn't working.
          sf.secondary_statistics(hadv=symbols, sp500=sp500, mdy=mdy, iwm=iwm),
          '\nSP500 Close Over VWAP Ratio',
          sf.close_over_vwap_ratio(sp500),
          '\nMDY Close Over VWAP Ratio',
          sf.close_over_vwap_ratio(mdy),
          '\nIWM Close Over VWAP Ratio',
          sf.close_over_vwap_ratio(iwm),
          '\nSector Close Over VWAP Ratio',
          sorted(close_over_vwap_dict(sector_member_mappings).items(), key=lambda x: x[1], reverse=True),
          '\nIndustry Close Over VWAP Ratio',
          sorted(close_over_vwap_dict(industry_member_mappings).items(), key=lambda x: x[1], reverse=True),
          '\nEpisodic Pivots Reward Risk',
          sorted(ep_rr.items(), key=lambda x: (x[1][1], x[1][0]), reverse=True),
          '\nEpisodic Pivots Current Duration',
          sorted(ep_curdur.items(),key=operator.itemgetter(1,0))[::-1],
          '\nRelative Strength',
          rel_stren[-40:][::-1],
          '\nRelative Weakness',
          rel_stren[:40],
          '\nTop Performers Since Previous Earnings Season Start',
          prev_perf_since_earnings[-100:][::-1],
          '\nUnderperformers Since Previous Earnings Season Start',
          prev_perf_since_earnings[:50],
          '\nTop Performers Since Earnings Season Start',
          perf_since_earnings[-100:][::-1],          
          '\nUnderperformers Since Earnings Season Start',
          perf_since_earnings[:50],
          '\nDays With Elevated RVol',
          sorted(days_elevated_rvol.items(), key=lambda x: x[1], reverse=True),
          '\nDays With Consecutive Range Expansion',
          sorted(days_range_expansion.items(), key=lambda x: x[1], reverse=True),
          '\nRecent Short Interest',
          hist_short_int[['Ticker', hist_short_int.columns[-1]]].sort_values(by=hist_short_int.columns[-1], ascending=False).head(100).set_index('Ticker').to_dict()[hist_short_int.columns[-1]],
          '\nDollar Volume Over Market Cap All',
          dv_cap.tail(30),
          '\nDollar Volume Over Market Cap hadv',
          dv_cap.loc[dv_cap.Ticker.isin(symbols)][-30:],
          '\nEuclidean Distance YTD Perf and DV/Cap',
          perf_dvcap_dist.sort_values('perf_dvcap_dist').tail(30),
          '\nSPY Trend Bias',
          sf.trend_bias(etfs['SPY']), 
          '\nSuggested Watchlists',
          sf.watchlist_suggestions(sf.trend_bias(etfs['SPY'])),
          '\nExpected Revenue Growth +1Q',
          sorted(qplus1.items(), key=lambda x: x[1], reverse=True)[:100],
          '\nExpected Revenue Growth +4Q',
          sorted(qplus4.items(), key=lambda x: x[1], reverse=True)[:100],          
          sep='\n')

    # Revert to default settings
    warnings.filterwarnings('default')
    pd.options.mode.chained_assignment = 'warn'
