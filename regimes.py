from market_data import np, pd, plt
from scipy.signal import find_peaks



class Regimes:
    def __init__(self, symbols):
        self.symbols = symbols
        self._o = None
        self._h = None
        self._l = None
        self._c = None
        self.rt_lo = None
        self.rt_hi = None
        self.slo = None
        self.shi = None
        self.rg = None
        self.clg = None
        self.flr = None
        self.rg_ch = None
        self.st_lo = None
        self.st_hi = None
        self.lt_lo = None
        self.lt_hi = None
        self.ud = None
        self.bs = None
        self.bs_dt = None
        self._rt = None
        self._swg = None
        self.hh_ll = None
        self.hh_ll_dt = None
    
    def graph_regime_combo(ticker,df,_c,rg,lo,hi,slo,shi,clg,flr,rg_ch,
                        ma_st,ma_mt,ma_lt,lt_lo,lt_hi,st_lo,st_hi):
        
        '''
        https://www.color-hex.com/color-names.html
        ticker,df,_c: _c is closing price
        rg: regime -1/0/1 using floor/ceiling method
        lo,hi: small, noisy highs/lows
        slo,shi: swing lows/highs
        clg,flr: ceiling/floor
        
        rg_ch: regime change base
        ma_st,ma_mt,ma_lt: moving averages ST/MT/LT
        lt_lo,lt_hi: range breakout High/Low LT 
        st_lo,st_hi: range breakout High/Low ST 
        '''
        fig = plt.figure(figsize=(20,8))
        ax1 = plt.subplot2grid((1,1), (0,0))
        date = df.index
        close = df[_c]
        ax1.plot_date(df.index, close,'-', color='k',  label=ticker.upper()) 
        try:
            if pd.notnull(rg):  
                base = df[rg_ch]
                regime = df[rg]

                if df[lo].count()>0:
                    ax1.plot(df.index, df[lo],'.' ,color='r', label= 'swing low',alpha= 0.6)
                if df[hi].count()>0:
                    ax1.plot(df.index, df[hi],'.' ,color='g', label= 'swing high',alpha= 0.6)        
                if df[slo].count()>0:
                    ax1.plot(df.index, df[slo],'o' ,color='r', label= 'swing low',alpha= 0.8)
                if df[shi].count()>0:
                    ax1.plot(df.index, df[shi],'o' ,color='g', label= 'swing high',alpha= 0.8)
                if df[flr].count()>0:
                    plt.scatter(df.index, df[flr],c='k',marker='^',label='floor')
                if df[clg].count() >0:
                    plt.scatter(df.index, df[clg],c='k',marker='v',label='ceiling')

                ax1.plot([],[],linewidth=5, label= 'bear', color='m',alpha=0.1)
                ax1.plot([],[],linewidth=5 , label= 'bull', color='b',alpha=0.1)
                ax1.fill_between(date, close, base,where=((regime==1)&(close > base)), facecolor='b', alpha=0.1)
                ax1.fill_between(date, close, base,where=((regime==1)&(close < base)), facecolor='b', alpha=0.4)
                ax1.fill_between(date, close, base,where=((regime==-1)&(close < base)), facecolor='m', alpha=0.1)
                ax1.fill_between(date, close, base,where=((regime==-1)&(close > base)), facecolor='m', alpha=0.4)

            if np.sum(ma_st) >0 :
                ax1.plot(df.index,ma_st,'-' ,color='lime', label= 'ST MA')
                ax1.plot(df.index,ma_mt,'-' ,color='green', label= 'MT MA')
                ax1.plot(df.index,ma_lt,'-' ,color='red', label= 'LT MA')

                if pd.notnull(rg): # floor/ceiling regime present
                    # Profitable conditions
                    ax1.fill_between(date,close, ma_mt,where=((regime==1)&(ma_mt >= ma_lt)&(ma_st>=ma_mt)), 
                                facecolor='green', alpha=0.5) 
                    ax1.fill_between(date,close, ma_mt,where=((regime==-1)&(ma_mt <= ma_lt)&(ma_st <= ma_mt)), 
                                facecolor='red', alpha=0.5)
                    # Unprofitable conditions
                    ax1.fill_between(date,close, ma_mt,where=((regime==1)&(ma_mt>=ma_lt)&(ma_st>=ma_mt)&(close<ma_mt)), 
                                facecolor='darkgreen', alpha=1) 
                    ax1.fill_between(date,close, ma_mt,where=((regime==-1)&(ma_mt<=ma_lt)&(ma_st<=ma_mt)&(close>=ma_mt)), 
                                facecolor='darkred', alpha=1)

                elif pd.isnull(rg): # floor/ceiling regime absent
                    # Profitable conditions
                    ax1.fill_between(date,close, ma_mt,where=((ma_mt >= ma_lt)&(ma_st>=ma_mt)), 
                                facecolor='green', alpha=0.4) 
                    ax1.fill_between(date,close, ma_mt,where=((ma_mt <= ma_lt)&(ma_st <= ma_mt)), 
                                facecolor='red', alpha=0.4)
                    # Unprofitable conditions
                    ax1.fill_between(date,close, ma_mt,where=((ma_mt >= ma_lt)&(ma_st >= ma_mt)&(close < ma_mt)), 
                                facecolor='darkgreen', alpha=1) 
                    ax1.fill_between(date,close, ma_mt,where=((ma_mt <= ma_lt)&(ma_st <= ma_mt)&(close >= ma_mt)), 
                                facecolor='darkred', alpha=1)

            if (np.sum(lt_hi) > 0): # LT range breakout
                ax1.plot([],[],linewidth=5, label= ' LT High', color='m',alpha=0.2)
                ax1.plot([],[],linewidth=5, label= ' LT Low', color='b',alpha=0.2)

                if pd.notnull(rg): # floor/ceiling regime present
                    ax1.fill_between(date, close, lt_lo,
                                    where=((regime ==1) & (close > lt_lo) ), 
                                    facecolor='b', alpha=0.2)
                    ax1.fill_between(date,close, lt_hi,
                                    where=((regime ==-1) & (close < lt_hi)), 
                                    facecolor='m', alpha=0.2)
                    if (np.sum(st_hi) > 0): # ST range breakout
                        ax1.fill_between(date, close, st_lo,
                                        where=((regime ==1)&(close > st_lo) ), 
                                        facecolor='b', alpha=0.3)
                        ax1.fill_between(date,close, st_hi,
                                        where=((regime ==-1) & (close < st_hi)), 
                                        facecolor='m', alpha=0.3)

                elif pd.isnull(rg): # floor/ceiling regime absent           
                    ax1.fill_between(date, close, lt_lo,
                                    where=((close > lt_lo) ), facecolor='b', alpha=0.2)
                    ax1.fill_between(date,close, lt_hi,
                                    where=((close < lt_hi)), facecolor='m', alpha=0.2)
                    if (np.sum(st_hi) > 0): # ST range breakout
                        ax1.fill_between(date, close, st_lo,
                                        where=((close > st_lo) & (st_lo >= lt_lo)), facecolor='b', alpha=0.3)
                        ax1.fill_between(date,close, st_hi,
                                        where=((close < st_hi)& (st_hi <= lt_hi)), facecolor='m', alpha=0.3)

                if (np.sum(st_hi) > 0): # ST range breakout
                    ax1.plot([],[],linewidth=5, label= ' ST High', color='m',alpha=0.3)
                    ax1.plot([],[],linewidth=5, label= ' ST Low', color='b',alpha=0.3)

                ax1.plot(df.index, lt_lo,'-.' ,color='b', label= 'LT low',alpha=0.2)
                ax1.plot(df.index, lt_hi,'-.' ,color='m', label= 'LT high',alpha=0.2)
        except:
            pass
        
        for label in ax1.xaxis.get_ticklabels():
            label.set_rotation(45)
        ax1.grid(True)
        ax1.xaxis.label.set_color('k')
        ax1.yaxis.label.set_color('k')
        plt.xlabel('Date')
        plt.ylabel(str.upper(ticker) + ' Price')
        plt.title(str.upper(ticker))
        plt.legend()

    def reset_variables(self):
        self._o = None
        self._h = None
        self._l = None
        self._c = None
        self.rt_lo = None
        self.rt_hi = None
        self.slo = None
        self.shi = None
        self.rg = None
        self.clg = None
        self.flr = None
        self.rg_ch = None
        self.st_lo = None
        self.st_hi = None
        self.lt_lo = None
        self.lt_hi = None
        self.ud = None
        self.bs = None
        self.bs_dt = None
        self._rt = None
        self._swg = None
        self.hh_ll = None
        self.hh_ll_dt = None

    def regime_breakout_combo(self, single_symbol=False, symbol=None, watchlist=None):
        if watchlist == None:
            watchlist = self.symbols.keys()
        if single_symbol == False:
            for sym in watchlist:
                try:
                    self.regime_breakout(self.symbols[sym].df)
                    self.turtle_trader(self.symbols[sym].df)
                    self.turtle_trader(self.symbols[sym].df, slow=20, fast=10)
                    self.turtle_trader(self.symbols[sym].df, slow=10, fast=5)
                except Exception as e:
                    print(self.regime_breakout_combo.__name__, sym, e, sep=': ')
                    #TODO print(self.regime_breakout_combo.__name__, '\n', sym, e) update error handling
                    continue
        else:
            self.regime_breakout(self.symbols[symbol].df)
            self.turtle_trader(self.symbols[symbol].df)
            self.turtle_trader(self.symbols[symbol].df, slow=20, fast=10)
            self.turtle_trader(self.symbols[symbol].df, slow=10, fast=5)

    def regime_ma_combo(self, single_symbol=False, symbol=None, watchlist=None):
        if watchlist == None:
            watchlist = self.symbols.keys()
        if single_symbol == False:
            for sym in watchlist:
                try:
                    self.regime_sma(self.symbols[sym].df)
                    self.regime_ema(self.symbols[sym].df)
                    self.regime_ema(self.symbols[sym].df, st=20, lt=50)
                    self.regime_ema(self.symbols[sym].df, st=10, lt=20)
                    self.regime_macross(self.symbols[sym].df)
                    self.regime_macross(self.symbols[sym].df, ma=[10,20,50])
                    self.regime_triple_moving_average(self.symbols[sym].df)
                except Exception as e:
                    print(self.regime_ma_combo.__name__, sym, e, sep=': ')
                    #TODO print(self.regime_ma_combo.__name__, '\n', sym, e) update error handling
                    continue
        else:
            self.regime_sma(self.symbols[symbol].df)
            self.regime_ema(self.symbols[symbol].df)
            self.regime_ema(self.symbols[symbol].df, st=20, lt=50)
            self.regime_ema(self.symbols[symbol].df, st=10, lt=20)
            self.regime_macross(self.symbols[symbol].df)
            self.regime_macross(self.symbols[symbol].df, ma=[10,20,50])
            self.regime_triple_moving_average(self.symbols[symbol].df)            

    def floor_ceiling_combo(self, single_symbol=False, symbol=None, watchlist=None):
        if watchlist == None:
            watchlist = self.symbols.keys()
        if single_symbol == False:
            for sym in watchlist:
                try:
                    self.reset_variables()
                    self.lower_upper_OHLC(self.symbols[sym].df)
                    self.regime_args(self.symbols[sym].df)
                    self.historical_swings(self.symbols[sym].df)
                    self.regime_floor_ceiling(self.symbols[sym].df)
                except Exception as e:
                    print(self.floor_ceiling_combo.__name__, sym, e, sep=': ')
                    #TODO print(sym, e) update error handling
                    continue
        else:
            self.lower_upper_OHLC(self.symbols[sym].df)
            self.regime_args(self.symbols[sym].df)
            self.historical_swings(self.symbols[sym].df)
            self.regime_floor_ceiling(self.symbols[sym].df)     

    #Does not require utility functions, can be used as is.
    def regime_breakout(self, df ,_h= 'High',_l= 'Low', window=252):
        hl =  np.where(df[_h] == df[_h].rolling(window).max(), 1, np.where(df[_l] == df[_l].rolling(window).min(), -1,np.nan))
        roll_hl = pd.Series(index= df.index, data= hl).fillna(method= 'ffill')
        df['hi_'+str(window)] = df['High'].rolling(window).max()
        df['lo_'+str(window)] = df['Low'].rolling(window).min()
        df['bo_'+ str(window)]= roll_hl

    def regime_breakout_plot(self, df, window = 252):
        df['hi_'+str(window)] = df['High'].rolling(window).max()
        df['lo_'+str(window)] = df['Low'].rolling(window).min()
        df['bo_'+ str(window)]= regime_breakout(df= df,_h= 'High',_l= 'Low',window= window)
        df[['Close','hi_'+str(window),'lo_'+str(window),'bo_'+ str(window)]].plot(secondary_y= ['bo_'+ str(window)],
                                        figsize=(20,5), style=['k','g:','r:','b-.'],
                                        title = str.upper(ticker)+' '+str(window)+' days high/low')

    #Utility function
    def lower_upper_OHLC(self, df,relative = False):
        if relative==True:
            rel = 'r'
        else:
            rel= ''      
        if 'Open' in df.columns:
            ohlc = [rel+'Open',rel+'High',rel+'Low',rel+'Close']       
        elif 'open' in df.columns:
            ohlc = [rel+'open',rel+'high',rel+'low',rel+'close']
            
        try:
            _o,_h,_l,_c = [ohlc[h] for h in range(len(ohlc))]
        except:
            _o=_h=_l=_c= np.nan
        self._o, self._h, self._l, self._c = _o,_h,_l,_c

    #Utility function. I'm not sure what the value for lvl should be. 1 & 2 work.
    def  regime_args(self, df,lvl=2,relative= False):
        if ('Low' in df.columns) & (relative == False):
            reg_val = ['Lo1','Hi1','Lo'+str(lvl),'Hi'+str(lvl),'rg','clg','flr','rg_ch']
        elif ('low' in df.columns) & (relative == False):
            reg_val = ['lo1','hi1','lo'+str(lvl),'hi'+str(lvl),'rg','clg','flr','rg_ch']
        elif ('Low' in df.columns) & (relative == True):
            reg_val = ['rL1','rH1','rL'+str(lvl),'rH'+str(lvl),'rrg','rclg','rflr','rrg_ch']
        elif ('low' in df.columns) & (relative == True):
            reg_val = ['rl1','rh1','rl'+str(lvl),'rh'+str(lvl),'rrg','rclg','rflr','rrg_ch']
        
        try: 
            rt_lo,rt_hi,slo,shi,rg,clg,flr,rg_ch = [reg_val[s] for s in range(len(reg_val))]
        except:
            rt_lo=rt_hi=slo=shi=rg=clg=flr=rg_ch= np.nan
        self.rt_lo, self.rt_hi, self.slo, self.shi, self.rg, self.clg, self.flr, self.rg_ch = rt_lo,rt_hi,slo,shi,rg,clg,flr,rg_ch

    #Utility function. Needed for graph regime combo. lower_upper_OHLC() must be called first.
    def instantiate_shortterm_longterm_highslows(self, df, bo = [50, 200]):
        self.st_lo, self.lt_lo = [df[self._l].rolling(window = bo[t]).min() for t in range(len(bo))]
        self.st_hi,self.lt_hi = [df[self._h].rolling(window = bo[t]).max() for t in range(len(bo))]

    #Requires utility function: lower_upper_OHLC()
    def turtle_trader(self, df, slow=50, fast=20):
        '''
        _slow: Long/Short direction
        _fast: trailing stop loss
        '''
        if self._h != None:
            _h = self._h
        else:
            _h = 'High'
        if self._l != None:
            _l = self._l
        else:
            _l = 'Low'
        _slow = self.regime_breakout(df,_h,_l,window = slow)
        _fast = self.regime_breakout(df,_h,_l,window = fast)
        
        # turtle = pd. Series(index= df.index, 
        #                     data = np.where(_slow == 1,np.where(_fast == 1,1,0), 
        #                             np.where(_slow == -1, np.where(_fast ==-1,-1,0),0)), name = 'turtle' + str(slow) + '_' + str(fast))
        turtle = pd. Series(index= df.index, 
                            data = np.where(df['bo_' + str(slow)] == 1,np.where(df['bo_' + str(fast)] == 1,1,0), 
                                    np.where(df['bo_' + str(slow)] == -1, np.where(df['bo_' + str(fast)] ==-1,-1,0),0)), name = 'turtle' + str(slow) + '_' + str(fast))
        df['turtle' + str(slow) + '_' + str(fast)] = turtle

    def turtle_trader_plot(self, df, slow=50, fast=20):
        if self._h != None:
            _h = self._h
        else:
            _h = 'High'
        if self._l != None:
            _l = self._l
        else:
            _l = 'Low'
        df['bo_'+ str(slow)] = self.regime_breakout(df,_h,_l,window = slow)
        df['bo_'+ str(fast)] = self.regime_breakout(df,_h,_l,window = fast)
        df['turtle_'+ str(slow)+str(fast)] = self.turtle_trader(df, slow, fast)
        rg_cols = ['bo_'+str(slow),'bo_'+ str(fast),'turtle_'+ str(slow)+str(fast)]

        df[['Close','bo_'+str(slow),'bo_'+ str(fast),'turtle_'+ str(slow)+str(fast)] ].plot(
            secondary_y= rg_cols,figsize=(20,5), style=['k','r','g:','b-.'],
                                        title = str.upper(ticker)+' '+str(rg_cols))

    def regime_sma(self, df, _c='Close', st=50, lt=200):
        '''
        bull +1: sma_st >= sma_lt , bear -1: sma_st <= sma_lt
        '''
        sma_lt = df[_c].rolling(lt).mean()
        sma_st = df[_c].rolling(st).mean()
        rg_sma = np.sign(sma_st - sma_lt)
        df['sma' + str(st) + str(lt)] = rg_sma

    def regime_ema(self, df, _c='Close', st=50, lt=200):
        '''
        bull +1: ema_st >= ema_lt , bear -1: ema_st <= ema_lt
        '''
        ema_st = df[_c].ewm(span=st,min_periods = st).mean()
        ema_lt = df[_c].ewm(span=lt,min_periods = lt).mean()
        rg_ema = np.sign(ema_st - ema_lt)
        df['ema' + str(st) + str(lt)] = rg_ema

    def regime_macross(self, df, ma=[20, 50, 200]):
        ma_st,ma_mt,ma_lt = [df['Close'].rolling(ma[t]).mean() for t in range(len(ma))]
        df['ma_st'] = ma_st
        df['ma_mt'] = ma_mt
        df['ma_lt'] = ma_lt
        s_long = np.where((df['ma_lt'].shift(ma[2]) < df['ma_lt']) & (df['ma_st'] > df['ma_mt']), 1, np.where((df['ma_lt'].shift(ma[2]) < df['ma_lt']) & (df['ma_st'] < df['ma_mt']), -1, np.nan))
        s_short = np.where((df['ma_lt'].shift(ma[2]) > df['ma_lt']) & (df['ma_st'] < df['ma_mt']), 1, np.where((df['ma_lt'].shift(ma[2]) > df['ma_lt']) & (df['ma_st'] > df['ma_mt']), -1, np.nan))
        df['regime_crossing_ma_long_' + str(ma[0]) + str(ma[1]) + str(ma[2])] = pd.Series(index=df.index, data=s_long)
        df['regime_crossing_ma_short_' + str(ma[0]) + str(ma[1]) + str(ma[2])] = pd.Series(index=df.index, data=s_short)

    def regime_triple_moving_average(self, df, ma=[20, 50, 200]):
        ma_st,ma_mt,ma_lt = [df['Close'].rolling(ma[t]).mean() for t in range(len(ma))]
        df['ma_st'] = ma_st
        df['ma_mt'] = ma_mt
        df['ma_lt'] = ma_lt
        s = np.where((df['ma_st'] > df['ma_mt']) & (df['ma_mt'] > df['ma_lt']), 1, np.where((df['ma_st'] < df['ma_mt']) & (df['ma_mt'] < df['ma_lt']), -1, np.nan))
        df['triple_ma' + str(ma[0]) + str(ma[1]) + str(ma[2])] = pd.Series(index=df.index, data=s)

    def historical_swings(self, df, _o='Open', _h='High', _l='Low', _c='Close', dist= None, hurdle= None):
        
        def hilo_alternation(hilo, dist= None, hurdle= None):
            i=0    
            while (np.sign(hilo.shift(1)) == np.sign(hilo)).any(): # runs until duplicates are eliminated

                # removes swing lows > swing highs
                hilo.loc[(np.sign(hilo.shift(1)) != np.sign(hilo)) &  # hilo alternation test
                        (hilo.shift(1)<0) &  # previous datapoint:  high
                        (np.abs(hilo.shift(1)) < np.abs(hilo) )] = np.nan # high[-1] < low, eliminate low 

                hilo.loc[(np.sign(hilo.shift(1)) != np.sign(hilo)) &  # hilo alternation
                        (hilo.shift(1)>0) &  # previous swing: low
                        (np.abs(hilo ) < hilo.shift(1))] = np.nan # swing high < swing low[-1]

                # alternation test: removes duplicate swings & keep extremes
                hilo.loc[(np.sign(hilo.shift(1)) == np.sign(hilo)) & # same sign
                        (hilo.shift(1) < hilo )] = np.nan # keep lower one

                hilo.loc[(np.sign(hilo.shift(-1)) == np.sign(hilo)) & # same sign, forward looking 
                        (hilo.shift(-1) < hilo )] = np.nan # keep forward one

                # removes noisy swings: distance test
                if pd.notnull(dist):
                    hilo.loc[(np.sign(hilo.shift(1)) != np.sign(hilo))&\
                        (np.abs(hilo + hilo.shift(1)).div(dist, fill_value=1)< hurdle)] = np.nan

                # reduce hilo after each pass
                hilo = hilo.dropna().copy() 
                i+=1
                if i == 4: # breaks infinite loop
                    break 
                return hilo
    
        reduction = df[[_o,_h,_l,_c]].copy() 
        reduction['avg_px'] = round(reduction[[_h,_l,_c]].mean(axis=1),2)
        highs = reduction['avg_px'].values
        lows = - reduction['avg_px'].values
        reduction_target =  len(reduction) // 100
        #print(reduction_target )

        n = 0
        while len(reduction) >= reduction_target: 
            highs_list = find_peaks(highs, distance = 1, width = 0)
            lows_list = find_peaks(lows, distance = 1, width = 0)
            hilo = reduction.iloc[lows_list[0]][_l].sub(reduction.iloc[highs_list[0]][_h],fill_value=0)

            # Reduction dataframe and alternation loop
            hilo_alternation(hilo, dist= None, hurdle= None)
            reduction['hilo'] = hilo

            # Populate reduction df
            n += 1        
            reduction[str(_h)[:2]+str(n)] = reduction.loc[reduction['hilo']<0 ,_h]
            reduction[str(_l)[:2]+str(n)] = reduction.loc[reduction['hilo']>0 ,_l]

            # Populate main dataframe
            df[str(_h)[:2]+str(n)] = reduction.loc[reduction['hilo']<0 ,_h]
            df[str(_l)[:2]+str(n)] = reduction.loc[reduction['hilo']>0 ,_l]
            
            # Reduce reduction
            reduction = reduction.dropna(subset= ['hilo'])
            reduction.fillna(method='ffill', inplace = True)
            highs = reduction[str(_h)[:2]+str(n)].values
            lows = -reduction[str(_l)[:2]+str(n)].values
            
            if n >= 9:
                break
                
        def cleanup_latest_swing(df, shi=self.shi, slo=self.slo, rt_hi=self.rt_hi, rt_lo=self.rt_lo): 
            '''
            removes false positives
            '''
            # latest swing
            shi_dt = df.loc[pd.notnull(df[shi]), shi].index[-1]
            s_hi = df.loc[pd.notnull(df[shi]), shi][-1]
            slo_dt = df.loc[pd.notnull(df[slo]), slo].index[-1] 
            s_lo = df.loc[pd.notnull(df[slo]), slo][-1] 
            len_shi_dt = len(df[:shi_dt])
            len_slo_dt = len(df[:slo_dt])
            

            # Reset false positives to np.nan
            for i in range(2):
                
                if (len_shi_dt > len_slo_dt) & ((df.loc[shi_dt:,rt_hi].max()> s_hi) | (s_hi<s_lo)):
                    df.loc[shi_dt, shi] = np.nan
                    len_shi_dt = 0
                elif (len_slo_dt > len_shi_dt) & ((df.loc[slo_dt:,rt_lo].min()< s_lo)| (s_hi<s_lo)):
                    df.loc[slo_dt, slo] = np.nan 
                    len_slo_dt = 0
                else:
                    pass
        cleanup_latest_swing(df)

    def latest_swing_variables(self, df, _h='High', _l='Low', _c='Close'):
        '''
        Latest swings dates & values
        '''
        shi=self.shi
        slo=self.slo
        rt_hi=self.rt_hi
        rt_lo=self.rt_lo
        shi_dt = df.loc[pd.notnull(df[shi]), shi].index[-1]
        slo_dt = df.loc[pd.notnull(df[slo]), slo].index[-1]
        s_hi = df.loc[pd.notnull(df[shi]), shi][-1]
        s_lo = df.loc[pd.notnull(df[slo]), slo][-1]
        
        if slo_dt > shi_dt: 
            swg_var = [1,s_lo,slo_dt,rt_lo,shi, df.loc[slo_dt:,_h].max(), df.loc[slo_dt:, _h].idxmax()]         
        elif shi_dt > slo_dt: 
            swg_var = [-1,s_hi,shi_dt,rt_hi,slo, df.loc[shi_dt:, _l].min(),df.loc[shi_dt:, _l].idxmin()]        
        else: 
            ud = 0
        self.ud, self.bs, self.bs_dt, self._rt, self._swg, self.hh_ll, self.hh_ll_dt = [swg_var[h] for h in range(len(swg_var))]   

    def test_distance(self, ud,bs, hh_ll, dist_vol, dist_pct): 
        ud = self.ud
        bs = self.bs
        hh_ll = self.hh_ll
        # priority: 1. Vol 2. pct 3. dflt
        if (dist_vol > 0):    
            distance_test = np.sign(abs(hh_ll - bs) - dist_vol)
        elif (dist_pct > 0):
            distance_test = np.sign(abs(hh_ll / bs - 1) - dist_pct)
        else:
            distance_test = np.sign(dist_pct)
            
        return int(max(distance_test,0) * ud)

    def average_true_range(self, df, n, _h='High', _l='Low', _c='Close'):
        '''
        http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:average_true_range_atr
        '''
        atr =  (df[_h].combine(df[_c].shift(), max) - df[_l].combine(df[_c].shift(), min)).rolling(window=n).mean()
        return atr

    def retest_swing(self, df, _c='Close'):
        _rt = self._rt
        hh_ll_dt = self.hh_ll_dt
        hh_ll = self.hh_ll
        _swg = self._swg

        dist_vol = round(self.average_true_range(df, _h='High', _l='Low', _c='Close', n=63)[hh_ll_dt] * 2,2)
        dist_pct = 0.05
        _sign = self.test_distance(self.ud, self.bs, self.hh_ll, dist_vol, dist_pct)
        rt_sgmt = df.loc[hh_ll_dt:, _rt] 

        if (rt_sgmt.count() > 0) & (_sign != 0): # Retests exist and distance test met    
            if _sign == 1: # 
                rt_list = [rt_sgmt.idxmax(),rt_sgmt.max(),df.loc[rt_sgmt.idxmax():, _c].cummin()]
                
            elif _sign == -1:
                rt_list = [rt_sgmt.idxmin(), rt_sgmt.min(), df.loc[rt_sgmt.idxmin():, _c].cummax()]
            rt_dt,rt_hurdle, rt_px = [rt_list[h] for h in range(len(rt_list))]

            if str(_c)[0] == 'r':
                df.loc[rt_dt,'rrt'] = rt_hurdle
            elif str(_c)[0] != 'r':
                df.loc[rt_dt,'rt'] = rt_hurdle    

            if (np.sign(rt_px - rt_hurdle) == - np.sign(_sign)).any():
                df.at[hh_ll_dt, _swg] = hh_ll      

    def retracement_swing(self, df, _c='Close', retrace_vol_multiple=2.5, retrace_pct=0.05, dist_vol_multiple=5, dist_pct=0.05):
        hh_ll_dt = self.hh_ll_dt
        hh_ll = self.hh_ll
        vlty = round(self.average_true_range(df=df, _h='High', _l='Low', _c='Close', n=63)[self.hh_ll_dt], 2)
        dist_vol = dist_vol_multiple * vlty
        _sign = self.test_distance(self.ud, self.bs, self.hh_ll, dist_vol, dist_pct)
        retrace_vol = retrace_vol_multiple * vlty
        retrace_pct = 0.05
        self.retest_swing(df)
        _swg = self._swg
        if _sign == 1: #
            retracement = df.loc[hh_ll_dt:, _c].min() - hh_ll

            if (vlty > 0) & (retrace_vol > 0) & ((abs(retracement / vlty) - retrace_vol) > 0):
                df.at[hh_ll_dt, _swg] = hh_ll
            elif (retrace_pct > 0) & ((abs(retracement / hh_ll) - retrace_pct) > 0):
                df.at[hh_ll_dt, _swg] = hh_ll

        elif _sign == -1:
            retracement = df.loc[hh_ll_dt:, _c].max() - hh_ll
            if (vlty > 0) & (retrace_vol > 0) & ((round(retracement / vlty ,1) - retrace_vol) > 0):
                df.at[hh_ll_dt, _swg] = hh_ll
            elif (retrace_pct > 0) & ((round(retracement / hh_ll , 4) - retrace_pct) > 0):
                df.at[hh_ll_dt, _swg] = hh_ll
        else:
            retracement = 0

    def regime_floor_ceiling(self, df, _h='High', _l='Low', _c='Close'):
        stdev = df[_c].rolling(63).std(ddof=0)
        rg_val = ['Lo2','Hi2','flr','clg','rg','rg_ch',1.5]#Lo2 and Hi2 were Lo3 and Hi3. I also swapped their index from the github code.
        slo, shi,flr,clg,rg,rg_ch,threshold = [rg_val[s] for s in range(len(rg_val))]
        # Lists instantiation
        threshold_test,rg_ch_ix_list,rg_ch_list = [],[], []
        floor_ix_list, floor_list, ceiling_ix_list, ceiling_list = [],[],[],[]

        ### Range initialisation to 1st swing
        floor_ix_list.append(df.index[0])
        ceiling_ix_list.append(df.index[0])
        
        ### Boolean variables
        ceiling_found = floor_found = breakdown = breakout = False

        ### Swings lists
        swing_highs = list(df[pd.notnull(df[shi])][shi])
        swing_highs_ix = list(df[pd.notnull(df[shi])].index)
        swing_lows = list(df[pd.notnull(df[slo])][slo])
        swing_lows_ix = list(df[pd.notnull(df[slo])].index)
        loop_size = np.maximum(len(swing_highs),len(swing_lows))

        ### Loop through swings
        for i in range(loop_size): 

            ### asymetric swing list: default to last swing if shorter list
            try:
                s_lo_ix = swing_lows_ix[i]
                s_lo = swing_lows[i]
            except:
                s_lo_ix = swing_lows_ix[-1]
                s_lo = swing_lows[-1]

            try:
                s_hi_ix = swing_highs_ix[i]
                s_hi = swing_highs[i]
            except:
                s_hi_ix = swing_highs_ix[-1]
                s_hi = swing_highs[-1]
        
            # swing_max_ix = np.maximum(s_lo_ix,s_hi_ix) # latest swing index
            if (type(s_lo_ix) != str) and (type(s_hi_ix) != str):
                s_lo_ix = s_lo_ix.strftime('%Y-%m-%d')
                s_hi_ix = s_hi_ix.strftime('%Y-%m-%d')
            swing_max_ix = max([datetime.strptime(s_lo_ix, '%Y-%m-%d'), datetime.strptime(s_hi_ix, '%Y-%m-%d')]).strftime('%Y-%m-%d')#The line above was returning an error because of the types of arguments. This line may have solved the problem.

            ### CLASSIC CEILING DISCOVERY
            if (ceiling_found == False):   
                top = df[floor_ix_list[-1] : s_hi_ix][_h].max()
                ceiling_test = round((s_hi - top) / stdev[s_hi_ix] ,1)  

                ### Classic ceiling test
                if ceiling_test <= -threshold: 
                    ### Boolean flags reset
                    ceiling_found = True 
                    floor_found = breakdown = breakout = False                
                    threshold_test.append(ceiling_test)

                    ### Append lists
                    ceiling_list.append(top)
                    ceiling_ix_list.append(df[floor_ix_list[-1]: s_hi_ix][_h].idxmax())           
                    rg_ch_ix_list.append(s_hi_ix)
                    rg_ch_list.append(s_hi) 

            ### EXCEPTION HANDLING: price penetrates discovery swing
            ### 1. if ceiling found, calculate regime since rg_ch_ix using close.cummin
            elif (ceiling_found == True):
                close_high = df[rg_ch_ix_list[-1] : swing_max_ix][_c].cummax()
                df.loc[rg_ch_ix_list[-1] : swing_max_ix, rg] = np.sign(close_high - rg_ch_list[-1])

                ### 2. if price.cummax penetrates swing high: regime turns bullish, breakout
                if (df.loc[rg_ch_ix_list[-1] : swing_max_ix, rg] >0).any():
                    ### Boolean flags reset
                    floor_found = ceiling_found = breakdown = False
                    breakout = True

            ### 3. if breakout, test for bearish pullback from highest high since rg_ch_ix
            if (breakout == True):
                brkout_high_ix = df.loc[rg_ch_ix_list[-1] : swing_max_ix, _c].idxmax()
                brkout_low = df[brkout_high_ix : swing_max_ix][_c].cummin()
                df.loc[brkout_high_ix : swing_max_ix, rg] = np.sign(brkout_low - rg_ch_list[-1])


            ### CLASSIC FLOOR DISCOVERY        
            if (floor_found == False): 
                bottom = df[ceiling_ix_list[-1] : s_lo_ix][_l].min()
                floor_test = round((s_lo - bottom) / stdev[s_lo_ix],1)

                ### Classic floor test
                if (floor_test >= threshold): 
                    
                    ### Boolean flags reset
                    floor_found = True
                    ceiling_found = breakdown = breakout = False
                    threshold_test.append(floor_test)

                    ### Append lists
                    floor_list.append(bottom)
                    floor_ix_list.append(df[ceiling_ix_list[-1] : s_lo_ix][_l].idxmin())           
                    rg_ch_ix_list.append(s_lo_ix)
                    rg_ch_list.append(s_lo)

            ### EXCEPTION HANDLING: price penetrates discovery swing
            ### 1. if floor found, calculate regime since rg_ch_ix using close.cummin
            elif(floor_found == True):    
                close_low = df[rg_ch_ix_list[-1] : swing_max_ix][_c].cummin()
                df.loc[rg_ch_ix_list[-1] : swing_max_ix, rg] = np.sign(close_low - rg_ch_list[-1])

                ### 2. if price.cummin penetrates swing low: regime turns bearish, breakdown
                if (df.loc[rg_ch_ix_list[-1] : swing_max_ix, rg] <0).any():
                    floor_found = floor_found = breakout = False
                    breakdown = True                

            ### 3. if breakdown,test for bullish rebound from lowest low since rg_ch_ix
            if (breakdown == True):
                brkdwn_low_ix = df.loc[rg_ch_ix_list[-1] : swing_max_ix, _c].idxmin() # lowest low  
                breakdown_rebound = df[brkdwn_low_ix : swing_max_ix][_c].cummax() # rebound
                df.loc[brkdwn_low_ix : swing_max_ix, rg] = np.sign(breakdown_rebound - rg_ch_list[-1])
                #breakdown = False
                #breakout = True  

        ### POPULATE FLOOR,CEILING, RG CHANGE COLUMNS
        df.loc[floor_ix_list[1:], flr] = floor_list
        df.loc[ceiling_ix_list[1:], clg] = ceiling_list
        df.loc[rg_ch_ix_list, rg_ch] = rg_ch_list
        df[rg_ch] = df[rg_ch].fillna(method='ffill')

        ### regime from last swing
        df.loc[swing_max_ix:,rg] = np.where(ceiling_found, # if ceiling found, highest high since rg_ch_ix
                                            np.sign(df[swing_max_ix:][_c].cummax() - rg_ch_list[-1]),
                                            np.where(floor_found, # if floor found, lowest low since rg_ch_ix
                                                    np.sign(df[swing_max_ix:][_c].cummin() - rg_ch_list[-1]),
                                                    np.sign(df[swing_max_ix:][_c].rolling(5).mean() - rg_ch_list[-1]))) 
        df[rg] = df[rg].fillna(method='ffill')
        #df[rg+'_no_fill'] = df[rg]

