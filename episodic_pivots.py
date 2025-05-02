"""
Code for processing episodic pivots.
"""

from market_data import datetime
import market_data.seeking_alpha as sa
from watchlists_locations import make_watchlist, episodic_pivots

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
    

    def load_all(self):
        for sym in self.symbols:
            self.episodic_pivot_finder(sym)
        self.drawdown()
        self.returns()
        self.max_return()
        self.duration()
        self.days_til_new_high()
        self.current_duration()
        self.risk_reward(risk_level='auto')

    def episodic_pivot_finder(self, sym):
        """Identifies episodic pivots. Assigns a value of 1 for days the symbol passes the episodic pivot filter. Adds the symbol, the date the ep began and the dataframe
            containing the days the symbol was an episodic pivot to a dictionary called ep_dict

        Args:
            symbol (str): the stock to find episodic pivots for.
        """
        #The first date of each found EP
        self.ep_dict[sym] = {}
        ep_initialized = [str(day).split(' ')[0] for day in self.symbols[sym].df.loc[self.symbols[sym].df.Gap > 7.9].index]
        #Days when the Closing price is greater than the 20DMA

        self.symbols[sym].df['c_over_under_20DMA'] = np.nan
        self.symbols[sym].df['c_over_under_20DMA'].loc[(self.symbols[sym].df['Close'].shift(1) > self.symbols[sym].df['20DMA'].shift(1)) & (self.symbols[sym].df['Close'] < self.symbols[sym].df['20DMA'])] = 1

        #These for loops may not account for episodic pivots that occur while a previous episodic pivot is still true.
        dates = self.symbols[sym].df['c_over_under_20DMA'].loc[pd.notnull(self.symbols[sym].df['c_over_under_20DMA'])].index
        self.symbols[sym].df.ep = np.nan
        for date in ep_initialized:
            try:
                self.symbols[sym].df.loc[date: [day for day in dates if pd.to_datetime(date) < day][0], 'ep'] = 1
                self.ep_dict[sym].update({date: self.symbols[sym].df.loc[date: [day for day in dates if pd.to_datetime(date) < day][0]]})
            except IndexError:
                self.symbols[sym].df.loc[date:, 'ep'] = 1
                self.ep_dict[sym].update({date: self.symbols[sym].df.loc[date:]})

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
                    self.duration_dict[sym].update({date: (datetime.datetime.today() - datetime.datetime.strptime(self.ep_dict[sym][date].index[0], '%Y-%m-%d')).days})
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
                self.current_duration_dict[sym] = self.duration_dict[sym][list(self.duration_dict[sym].keys())[-1]]
            except Exception as e:
                #TODO print(sym, e) update error handling
                continue
    
    def risk_reward(self, risk_level='low'):
        """Calculates reward/risk for current EPs with the reward being to the price target and the risk to open
        price at the start of the EP. This function could be editedt to allow for customizing reward and risk 
        targets."""
        for sym in self.episodic_pivots_results:
            try:
                if risk_level == 'low':
                    reward = float(sa.analyst_price_targets_dict[sym]['estimates'][sa.meta_data[sym]['data']['id']]['target_price']['0'][0]['dataitemvalue']) - self.symbols[sym].df.Close.iloc[-1]
                    risk = self.symbols[sym].df.Close.iloc[-1] - self.ep_dict[sym][list(self.ep_dict[sym])[-1]].Low.min()
                    self.reward_risk_dict[sym] = round(reward / risk, 2)
                elif risk_level == '20DMA':
                    reward = float(sa.analyst_price_targets_dict[sym]['estimates'][sa.meta_data[sym]['data']['id']]['target_price']['0'][0]['dataitemvalue']) - self.symbols[sym].df.Close.iloc[-1]
                    risk = self.symbols[sym].df.Close.iloc[-1] - self.symbols[sym].df['20DMA'].iloc[-1]
                    self.reward_risk_dict[sym] = round(reward / risk, 2)
                elif risk_level == 'auto':
                    low = self.ep_dict[sym][list(self.ep_dict[sym])[-1]].Low.min()
                    DMA20 = self.symbols[sym].df['20DMA'].iloc[-1]
                    if low > DMA20:
                        reward = float(sa.analyst_price_targets_dict[sym]['estimates'][sa.meta_data[sym]['data']['id']]['target_price']['0'][0]['dataitemvalue']) - self.symbols[sym].df.Close.iloc[-1]
                        risk = self.symbols[sym].df.Close.iloc[-1] - self.ep_dict[sym][list(self.ep_dict[sym])[-1]].Low.min()
                        self.reward_risk_dict[sym] = round(reward / risk, 2)
                    elif DMA20 > low:
                        reward = float(sa.analyst_price_targets_dict[sym]['estimates'][sa.meta_data[sym]['data']['id']]['target_price']['0'][0]['dataitemvalue']) - self.symbols[sym].df.Close.iloc[-1]
                        risk = self.symbols[sym].df.Close.iloc[-1] - self.symbols[sym].df['20DMA'].iloc[-1]
                        self.reward_risk_dict[sym] = round(reward / risk, 2)
            except Exception as e:
                #TODO print(self.__class__, ': ', sym, '-', e) update error handling
                continue
        