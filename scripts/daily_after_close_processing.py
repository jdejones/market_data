if __name__ == "__main__":
    # Windows multiprocessing safety:
    # When running from some interactive environments, __main__.__spec__ may not exist.
    # ProcessPoolExecutor (spawn) expects it to exist (it can be None).
    import __main__ as _main
    if not hasattr(_main, "__spec__"):
        _main.__spec__ = None
    import multiprocessing as _mp
    _mp.freeze_support()

    import warnings
    # Suppress all warnings
    warnings.filterwarnings('ignore')
    # I added the try/except block as a quick fix to avoid conflicts with the
    # interactive interpreter and the github repo. There may be a
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
    from market_data import operator, np, ProcessPoolExecutor, as_completed, pickle, threading, argparse
    from market_data.stats_objects import IntradaySignalProcessing as isp
    from market_data import create_engine, text, DateTime, pymysql, redis, json, gzip, time
    from market_data.api_keys import database_password, seeking_alpha_api_key, seeking_alpha_access_token, polygon_api_key
    from market_data.interest_list import InterestList as il

    parser = argparse.ArgumentParser(description="Run the daily after close study pipeline.")
    parser.add_argument(
        "--skip-daily-storage",
        action="store_true",
        help=(
            "Skip daily storage of variables to MySQL database."
            """Variables skipped:
            daily_quant_rating_df
            results_finvizsearch"""
        ),
    )
    args = parser.parse_args()

    # Timer utilities to measure time between tqdm progress bars
    section_timer_start = time.perf_counter()
    # print_section_time("Starting daily after close study")    

    def print_section_time(label: str) -> None:
        """
        Print elapsed time since the last section timer reset, then reset it.
        Used to track time spent in code segments between tqdm progress bars.
        """
        global section_timer_start
        now = time.perf_counter()
        print(f"{label} took {now - section_timer_start:.2f} seconds")
        section_timer_start = now
    
    
    from polygon.rest import RESTClient
    from polygon.rest.models import (
        MarketHoliday,
    )
    client = RESTClient(polygon_api_key)
    holidays = client.get_market_holidays()
    holiday_dates = [holiday.date for holiday in holidays]
    if datetime.datetime.today().date().strftime("%Y-%m-%d") in holiday_dates:
        raise ValueError("Today is a holiday")

    #*Import price data.
    #hadv == high average dollar volume
    hadv = make_watchlist(hadv)
    data = api_import(hadv)
    symbols = {k: SymbolData(k, v) for k,v in data.items()}
    
    #########################################################################
    #Market as a reminder to monitor daily api usage.
    #Connect to database
    url = f"mysql+pymysql://root:{database_password}@127.0.0.1:3306/stocks"
    engine = create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


    # query database -> DataFrame
    daily_quant_rating_df = pd.read_sql("SELECT * FROM daily_quant_rating", con=engine)
    # daily_quant_rating_df = pd.read_csv(r"E:\Market Research\temporary.csv", index_col='Unnamed: 0')
    
    if not args.skip_daily_storage:
        if len(daily_quant_rating_df.columns) > 1000:
            warnings.warn("Number of columns is greater than 1000. Limit is 1017.")
            
        daily_quant_rating_df.set_index('index', inplace=True)
        #Concatenate new column
        daily_quant_rating_df = pd.concat([daily_quant_rating_df, pd.DataFrame({datetime.datetime.today().date(): []})], axis=1)

        #Request and add new values
        #list of symbols
        syms = list(symbols.keys())

        #api requests
        #assign counters
        i=0
        j=50
        while True:
                #break when list is exhausted
                if i > len(syms):
                    break
                #assign None near end of list
                if j > len(syms):
                    j = None
                #API call
                url = "https://seeking-alpha.p.rapidapi.com/symbols/get-metrics"

                querystring = {"symbols":f"{','.join(syms[i:j])}","fields":"quant_rating"}

                headers = {
                    "x-rapidapi-key": f"{seeking_alpha_api_key}",
                    "x-rapidapi-host": "seeking-alpha.p.rapidapi.com",
                    "accessToken": seeking_alpha_access_token
                }

                response_request = requests.get(url, headers=headers, params=querystring)

                if response_request.status_code != 200:
                    if response_request.status_code == 504:
                        print('request status code is 504: Gateway Timeout; sleeping for 30 seconds and retrying')
                        time.sleep(30)
                        response_request = requests.get(url, headers=headers, params=querystring)
                        response = response_request.json()
                    else:
                        print(f'request status code is {response_request.status_code}: retrying request after 30 seconds')
                        time.sleep(30)
                        response_request = requests.get(url, headers=headers, params=querystring)
                        response = response_request.json()

                response = response_request.json()
                        
                if response_request.status_code != 200:
                    break
                
                if (j is not None) and (len(response['data']) == 0):
                    raise ValueError('No response from Seeking Alpha API')
                
                #Container for symbol and respective rating
                quant_ratings_errors = {}
                try:
                    sym_ratings = {sa.sym_by_id[_['id'].strip('[]').split(',')[0]]:_['attributes']['value'] for _ in response['data']}
                except:
                    sym_ratings = {}
                    for _ in response['data']:
                        try:
                            sym_ratings[sa.sym_by_id[_['id'].strip('[]').split(',')[0]]] = _['attributes']['value']
                        except Exception as e:
                            quant_ratings_errors[f'{i}:{j}'] = ((i,j), _, e)
                            continue
                    pass
                if len(quant_ratings_errors) > 3000:
                    break
                
                #Insert rating to dataframe
                for sym in sym_ratings:
                    daily_quant_rating_df.loc[sym, datetime.datetime.today().date()] = sym_ratings[sym]
                        
                #Increment counters
                i += 50
                if j == None:
                    break
                j += 50
                time.sleep(2)

        daily_quant_rating_df.reset_index(inplace=True)
        # write back, replace existing table
        daily_quant_rating_df.to_sql("daily_quant_rating", 
                con=engine, 
                if_exists="replace", 
                index=False, 
                method="multi",
                chunksize=200,
                dtype={'date': DateTime})
    daily_quant_rating_df.set_index('index', inplace=True)
    daily_quant_rating_df.index.name = 'Symbol'
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
    prev_perf_since_earnings = sf.perf_since_earnings(symbols, earnings_season_start=sa.earnings_dict['JPM']['revenue_actual']['-1'][0]['effectivedate'].split('T')[0])
    perf_since_earnings = sf.perf_since_earnings(symbols, earnings_season_start=sa.earnings_dict['JPM']['revenue_actual']['0'][0]['effectivedate'].split('T')[0])
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
    from finvizfinance.constants import CUSTOM_SCREENER_COLUMNS
    custom = Custom()
    cols = list(CUSTOM_SCREENER_COLUMNS.keys())
    try:
        results_finvizsearch = custom.screener_view(limit=-1, select_page=None, verbose=1, ascend=True, columns=cols, sleep_sec=1)
    except ConnectionError:
        results_finvizsearch = custom.screener_view(limit=-1, select_page=None, verbose=1, ascend=True, columns=cols, sleep_sec=1)
    results_finvizsearch['DV'] = pd.to_numeric(results_finvizsearch['Previous Close'], errors='coerce').astype(float) * results_finvizsearch.Volume
    results_finvizsearch['Market Cap.'] = pd.to_numeric(results_finvizsearch['Market Cap.'].str.replace('.', '').str.replace('B', '0000000').str.replace('M', '0000'), errors='coerce').astype(float)
    results_finvizsearch['Market Cap.'] = results_finvizsearch['Market Cap.'].replace(0, np.nan)
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
    if not args.skip_daily_storage:
        rfs_url = f"mysql+pymysql://root:{database_password}@127.0.0.1:3306/results_finvizsearch"
        rfs_engine = create_engine(rfs_url, pool_pre_ping=True, connect_args={"connect_timeout": 5})
        results_finvizsearch.to_sql(datetime.datetime.today().date().strftime("%Y_%m_%d"),
                                    con=rfs_engine,
                                    if_exists="replace",
                                    index=False,
                                    method="multi",
                                    chunksize=200)
        
    
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
    hist_short_int.set_index('Ticker', inplace=True)


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
    quant_rating_interest = [(sym, val) for sym, val in 
                             zip(quant_rating_interest['Symbol'], quant_rating_interest[quant_rating_interest.columns[-1]])
                             if sym in symbols]
    interest_list_long.value_filter(quant_rating_interest, 4.9, '>=', 'Fundamental', 'Long', 'daily_quant_rating')
    
    with open(fr"{wl.systematic_watchlists_root}\interest_list_long.txt", "w") as f:
        for sym in interest_list_long.interest_list:
            f.write(sym + '\n')
    
    if not args.skip_daily_storage:
        summary_column_map = {
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
            "rvol": "RVol",
            "dollar_volume": "Dollar_Volume",
            "20dma": "20DMA",
            "atr_14": "ATR_14",
            "atr_14_signal": "ATR_14_signal",
            "rsi_14": "RSI_14",
            "rsi_14_signal": "RSI_14_signal",
            "di_plus": "+DI",
            "di_neg": "-DI",
            "adx": "ADX",
            "adx_signal": "ADX_signal",
            "emacd1226": "eMACD1226",
            "emacd1226_signal": "eMACD1226_signal",
            "relative_atr": "Relative_ATR",
            "atrs_traded": "ATRs_Traded",
            "ep": "ep",
        }

        def latest_value(df, column):
            if df.empty or column not in df.columns:
                return np.nan
            value = df[column].iloc[-1]
            if hasattr(value, "item"):
                value = value.item()
            return value

        summary_rows = []
        for sym, symbol_data in tqdm(symbols.items(), desc="Updating stocks.summary"):
            row = {"symbol": sym}
            for summary_column, df_column in summary_column_map.items():
                row[summary_column] = latest_value(symbol_data.df, df_column)
            summary_rows.append(row)

        summary_df = pd.DataFrame(summary_rows)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM summary"))
            summary_df.to_sql(
                "summary",
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=200,
            )
    
    #Pickling most used objects, so I don't have to rerun the script.
    def save_snapshots(obj, name):
        base = r"E:\Market Research\Dataset\daily_after_close_study"
        with gzip.open(fr"{base}\{name}.pkl.gz", "wb", compresslevel=5) as f:
            pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
            
    variables = (
        (symbols,'symbols'),
        (daily_quant_rating_df, 'daily_quant_rating_df'),
        (sec, 'sec'),
        (ind, 'ind'),
        (sp500, 'sp500'),
        (mdy, 'mdy'),
        (iwm, 'iwm'),
        (etfs, 'etfs'),
        (sector_close_vwap_ratio, 'sector_close_vwap_ratio'),
        (industry_close_vwap_ratio, 'industry_close_vwap_ratio'),
        (ep, 'episodic_pivots'),
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
        (interest_list_long.interest_list, 'interest_list_long'),
    )

    # Use tqdm only on the longest-running step (waiting for threads to finish)
    threads = []
    for obj, name in variables:
        threads.append(threading.Thread(
            target=save_snapshots,
            args=(obj, name),
            daemon=False,
        ))

    for thread in threads:
        thread.start()

    for thread in tqdm(threads, desc="Storing Variables"):
        thread.join()        


