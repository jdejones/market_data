"""
Code for processing episodic pivots.
"""

from market_data import datetime, np, tqdm, pd, json
import market_data.seeking_alpha as sa
from market_data.watchlists_locations import make_watchlist, episodic_pivots
from market_data import ThreadPoolExecutor, as_completed
from market_data.price_data_import import api_import

class Episodic_Pivots:
    def __init__(self, symbols: dict) -> None:
        self.symbols = symbols
        self.episodic_pivots_results = make_watchlist(episodic_pivots)
        self.ep_dict = {}
        self.drawdown_dict = {}
        self.returns_dict = {}
        self.max_return_dict = {}
        self.duration_dict = {}
        self.days_til_new_high_dict = {}
        self.current_duration_dict = {}
        self.reward_risk_dict = {}        
    

    def load_all(self, gap_threshold=7.9):
        for sym in tqdm(self.symbols, desc='Loading Episodic Pivots'):
            self.episodic_pivot_finder(sym, gap_threshold)
        self.drawdown()
        self.returns()
        self.max_return()
        self.duration()
        self.days_til_new_high()
        self.current_duration()
        self.risk_reward(risk_level='auto')
        # The following is intended to be used with the intraday_study.py script.
        offset_dates = {sym: [sorted(self.ep_dict[sym].keys())[-1], datetime.datetime.today().date()] for sym in self.ep_dict if len(self.ep_dict[sym]) > 0}
        with open(r"E:\Market Research\Dataset\current_ep_start_end_dates.txt", "w") as f:
            f.write(json.dumps(offset_dates, indent=4, sort_keys=True, default=str))

    def episodic_pivot_finder(self, sym, gap_threshold):
        """Identifies episodic pivots. Assigns a value of 1 for days the symbol passes the episodic pivot filter. Adds the symbol, the date the ep began and the dataframe
            containing the days the symbol was an episodic pivot to a dictionary called ep_dict.
            
            Simplified logic of ep identification:
                EP is the date range between the row with ep == 1 and the row with c_over_under_20DMA == 1.

        Args:
            symbol (str): the stock to find episodic pivots for.
        """
        df = self.symbols[sym].df
        #The first date of each found EP
        self.ep_dict[sym] = {}
      
        ep_initialized = [str(day).split(' ')[0] for day in df.loc[df.Gap > gap_threshold].index]      
        
        #Days when the Closing price is greater than the 20DMA
        df['c_over_under_20DMA'] = np.nan
        df['c_over_under_20DMA'].loc[(df['Close'].shift(1) > df['20DMA'].shift(1)) & (df['Close'] < df['20DMA'])] = 1

        #These for loops may not account for episodic pivots that occur while a previous episodic pivot is still true.
        dates = df['c_over_under_20DMA'].loc[pd.notnull(df['c_over_under_20DMA'])].index
        df['ep'] = np.nan
        # for date in ep_initialized:
        #     try:
        #         self.symbols[sym].df.loc[date: [day for day in dates if pd.to_datetime(date) < day][0], 'ep'] = 1
        #         self.ep_dict[sym].update({date: self.symbols[sym].df.loc[date: [day for day in dates if pd.to_datetime(date) < day][0]]})
        #     except IndexError:
        #         self.symbols[sym].df.loc[date:, 'ep'] = 1
        #         self.ep_dict[sym].update({date: self.symbols[sym].df.loc[date:]})
        local_ep: dict[str, pd.DataFrame] = {}

        def work_one(date_str: str):
            # find the first date after date_str in `dates`
            try:
                next_day = next(day for day in dates
                                if pd.to_datetime(date_str) < day)
                segment = df.loc[date_str:next_day]
            except StopIteration:
                segment = df.loc[date_str:]
            return date_str, segment

        
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(work_one, dt): dt
                       for dt in ep_initialized}
            for future in as_completed(futures):
                date_str, segment = future.result()
                # mark the ep‐column in the slice to 1
                df.loc[segment.index, 'ep'] = 1
                # stash the slice
                local_ep[date_str] = segment.copy()

        self.ep_dict[sym] = local_ep        

    def drawdown(self):
        """Identifies the amount of drawdown from the open in each episodic pivot and adds it to self.drawdown_dict
        """
        #TODO I should have a funtion for drawing down from the initial high.
        for sym in self.ep_dict:
            self.drawdown_dict[sym] = {}
            for ep in self.ep_dict[sym]:
                try:
                    self.drawdown_dict[sym].update({ep: (((self.ep_dict[sym][ep].Low.min() - self.ep_dict[sym][ep].loc[ep].Open) / self.ep_dict[sym][ep].loc[ep].Open) * 100)})
                except Exception as e:
                    #TODO print(sym, e) update error handling
                    continue

    def returns(self):
        for sym in self.ep_dict:
            try:
                self.returns_dict[sym] = {}
                for date in self.ep_dict[sym]:
                    self.returns_dict[sym].update({date: ((self.ep_dict[sym][date].iloc[-1].Close - self.ep_dict[sym][date].iloc[0].Open) / self.ep_dict[sym][date].iloc[0].Open) * 100})
            except Exception as e:
                #TODO print(sym, e) update error handling
                continue

    def max_return(self):
        for sym in self.ep_dict:
            self.max_return_dict[sym] = {}
            for date in self.ep_dict[sym]:
                try:
                    self.max_return_dict[sym].update({date: ((self.ep_dict[sym][date].High.max() - self.ep_dict[sym][date].iloc[0].Open) / self.ep_dict[sym][date].iloc[0].Open) * 100})
                except Exception as e:
                    #TODO print(sym, e) update error handling
                    continue

    def duration(self):
        for sym in self.ep_dict:
            self.duration_dict[sym] = {}
            for date in self.ep_dict[sym]:
                try:
                    self.duration_dict[sym].update({date: (datetime.datetime.strptime(str(self.ep_dict[sym][date].index[-1]).split(' ')[0], '%Y-%m-%d') - datetime.datetime.strptime(str(self.ep_dict[sym][date].index[0]).split(' ')[0], '%Y-%m-%d')).days})
                except Exception as e:
                    #TODO print(sym, e) update error handling
                    continue

    def days_til_new_high(self):
        for sym in self.ep_dict:
            self.days_til_new_high_dict[sym] = {}
            for date in self.ep_dict[sym]:
                try:
                    self.days_til_new_high_dict[sym].update({date: (datetime.datetime.strptime(self.ep_dict[sym][date].loc[self.ep_dict[sym][date].High > self.ep_dict[sym][date].High.iloc[0]].index[0], '%Y-%m-%d') - datetime.datetime.strptime(date, '%Y-%m-%d')).days})
                except Exception as e:
                    #TODO print(sym, e) update error handling
                    continue
     
    def current_duration(self):
        for sym in self.episodic_pivots_results:
            try:
                self.current_duration_dict[sym] = self.duration_dict[sym][sorted(self.duration_dict[sym].keys())[-1]]
            except IndexError:
                if len(self.duration_dict[sym].keys()) == 0:
                    continue
                else:
                    print(self.current_duration.__name__, sym, e, sep='-')
            except Exception as e:
                print(sym, e)# update error handling
                continue
    
    def risk_reward(self, risk_level='low'):
        """Calculates reward/risk for current EPs with the reward being to the price target and the risk to open
        price at the start of the EP. This function could be editedt to allow for customizing reward and risk 
        targets."""
        for sym in self.episodic_pivots_results:
            try:
                if risk_level == 'low':
                    reward = float(sa.analyst_price_targets_dict[sym]['estimates'][sa.meta_data[sym]['data']['id']]['target_price']['0'][0]['dataitemvalue']) - self.symbols[sym].df.Close.iloc[-1]
                    risk = self.symbols[sym].df.Close.iloc[-1] - self.ep_dict[sym][sorted(self.ep_dict[sym])[-1]].Low.min()
                    self.reward_risk_dict[sym] = round(reward / risk, 2)
                elif risk_level == '20DMA':
                    reward = float(sa.analyst_price_targets_dict[sym]['estimates'][sa.meta_data[sym]['data']['id']]['target_price']['0'][0]['dataitemvalue']) - self.symbols[sym].df.Close.iloc[-1]
                    risk = self.symbols[sym].df.Close.iloc[-1] - self.symbols[sym].df['20DMA'].iloc[-1]
                    self.reward_risk_dict[sym] = round(reward / risk, 2)
                elif risk_level == 'auto':
                    low = self.ep_dict[sym][sorted(self.ep_dict[sym])[-1]].Low.min()
                    DMA20 = self.symbols[sym].df['20DMA'].iloc[-1]
                    if low > DMA20:
                        reward = float(sa.analyst_price_targets_dict[sym]['estimates'][sa.meta_data[sym]['data']['id']]['target_price']['0'][0]['dataitemvalue']) - self.symbols[sym].df.Close.iloc[-1]
                        risk = self.symbols[sym].df.Close.iloc[-1] - self.ep_dict[sym][sorted(self.ep_dict[sym])[-1]].Low.min()
                        self.reward_risk_dict[sym] = round(reward / risk, 2)
                    elif DMA20 > low:
                        reward = float(sa.analyst_price_targets_dict[sym]['estimates'][sa.meta_data[sym]['data']['id']]['target_price']['0'][0]['dataitemvalue']) - self.symbols[sym].df.Close.iloc[-1]
                        risk = self.symbols[sym].df.Close.iloc[-1] - self.symbols[sym].df['20DMA'].iloc[-1]
                        self.reward_risk_dict[sym] = round(reward / risk, 2)
            except Exception as e:
                #TODO print(self.__class__, ': ', sym, '-', e) update error handling
                continue
        
    def ep_count(self, include_spy=True):
        df_dict = {}
        for sym in self.symbols:
            df_dict[sym] = symbols[sym].df.ep
        df = pd.DataFrame(df_dict)
        df['total'] = df.sum(axis=1)
        if include_spy:
            spy = api_import(['SPY'])['SPY']['Close']
            ep_spy = pd.concat([df['total'], spy], keys=['EP Count', '$SPY Close'], axis=1)
            return ep_spy.plot()
        df.rename(columns={'total': 'EP Count'}, inplace=True)
        return df['EP Count'].plot()