"""
The next functions that I should work on are those that access the SEC API.
"""

from market_data import pd, json, os, datetime, np, yf, plt, px, FloatApi, find_peaks
from market_data.api_keys import sec_api_key, polygon_api_key
from market_data.price_data_import import api_import
from polygon.rest import RESTClient
# from audioop import reverse
pd.options.display.max_columns = 150
from matplotlib.colors import ListedColormap
from typing import Dict, List, Tuple, Any
    

sa_fundamental_data = pd.read_csv(r"E:\Market Research\Fundamental Analysis\Files\Seeking Alpha\hadv\hadv.txt")
# saa = seeking_alpha_api()
watchlista = None
sector_industry_dict_saved = None
basic_materials_fa = None
communication_services_fa = None
consumer_cyclical_fa = None
consumer_defensive_fa = None
energy_fa = None
financial_services_fa = None
healthcare_fa = None
industrials_fa = None
real_estate_fa = None
technology_fa = None
utilities_fa = None
sa_variables = { 'Summary' : ['Quant', 'SA Authors', 'Wall St.'],
                        'Ratings' : ['Valuation Grade', 'Growth Grade', 'Profitability Grade','Momentum Grade' ],
                        'Earnings' : ['EPS Estimate', 'Revenue Estimate', 'EPS Actual', 'Revenue Actual', 'Revenue Surprise'],
                        'Dividend' : ['Safety', 'Growth', 'Yield', 'Consistency', 'Yield TTM', 'Yield FWD', '4Y Avg Yield', 'Div Rate TTM', 'Div Rate FWD', 'Payout Ratio', '4Y Avg Payout', 'Div Growth 3Y', 'Div Growth 5Y', 'Years of Growth'],
                        'Valuation' : ['Market Cap', 'EV', 'P/E TTM', 'P/E FWD', 'PEG TTM', 'PEG FWD', 'Price / Sales', 'EV / Sales', 'EV / EBITDA', 'Price / Cash Flow'],
                        'Growth' : ['Revenue YoY', 'Revenue FWD', 'Revenue 3Y', 'Revenue 5Y', 'EBITDA YoY', 'EBITDA FWD', 'EBITDA 3Y', 'Net Income 3Y', 'EPS YoY', 'EPS FWD', 'EPS 3Y', 'Tangible Book 3Y', 'Total Assets 3Y', 'FCF 3Y'],
                        'Performance' : ['Price', '52W Low', '52W High', '5D Perf', '1M Perf', '6M Perf', 'YTD Perf', '1Y Perf', '3Y Perf', '3Y Total Return', '5Y Perf', '5Y Total Return', '10Y Perf', '10Y Total Return'],
                        'Momentum' : ['Price', '10D SMA', 'Last Price Vs. 10D SMA', '50D SMA', 'Last Price Vs. 50D SMA', '100D SMA', 'Last Price Vs. 100D SMA', '200D SMA', 'Last Price Vs. 200D SMA', 'Week Vol / Shares', '24M Beta', '60M Beta'],
                        'Profitability' : ['Revenue TTM', 'Net Income TTM', 'Cash from Operations', 'Profit Margin', 'EBIT Margin', 'EBITDA Margin', 'Net Income Margin', 'FCF Margin', 'Return on Equity', 'Return on Assets', 'Return on Total Captial', 'Asset Turnover', 'Net Income / Employee'], 
                        'Ownership' : ['Shares Outstanding', 'Float %', 'Insider Shares', 'Insider %', 'Institutional Shares', 'Institutional Percent'],
                        'Debt' : ['Total Debt', 'ST Debt', 'LT Debt', 'Total Cash', 'Debt to FCF', 'Current Ratio', 'Quick Ratio', 'Covered Ratio', 'Debt to Equity', 'LT Debt to Total Capital']
                    }
standard_columns = ['Symbol','quantRating','Wall St.','Valuation Grade',
                            'Growth Grade','Profitability Grade','EPS Surprise',
                            'Revenue Surprise','Market Cap','EV','P/E TTM',
                            'P/E FWD','Price / Sales','EV / Sales','EV / EBITDA',
                            'Price / Book','Price / Cash Flow','Revenue YoY',
                            'Revenue TTM','Revenue FWD','EBITDA YoY','EBITDA FWD',
                            'EBITDA 3Y','NET Income TTM','Net Income 3Y','EPS YoY',
                            'EPS FWD','FCF 3Y','1M Perf','6M Perf','YTD Perf',
                            '1Y Perf','Cash from Operations','Profit Margin',
                            'EBIT Margin','EBITDA Margin','Net Income Margin',
                            'FCF Margin','Return on Equity','Return on Assets',
                            'Return on Total Capital','Asset Turnover','Total Debt',
                            'ST Debt','LT Debt','Total Cash','Debt to FCF',
                            'Current Ratio','Quick Ratio','Debt to Equity',
                            'LT Debt to Total Capital','EBITDA','Sector','Industry'
                            ]
sectors_industries = json.load(open(r"E:\Market Research\Dataset\Fundamental Data\symbol_sector_industry.txt"))
company_headquarters = json.load(open(r"E:\Market Research\Dataset\Fundamental Data\company_headquarters.txt"))
bsum = json.load(open(r"E:\Market Research\Dataset\Fundamental Data\business_summary.txt"))
# f = Filings()

####################################################################################
#This section will contain scripts/functions for general study of seeking alpha
#files.
#The following is not being used. It is replaced by the function that follows 
#it.
# hadv_fa_combo = {}
# for item in os.listdir(r"E:\Market Research\Fundamental Analysis\Files\Seeking Alpha\hadv"):
#     temp = pd.read_excel(r"E:\Market Research\Fundamental Analysis\Files\Seeking Alpha\hadv\\" + item, sheet_name=None)
#     for key in temp.keys():
#         try:
#             hadv_fa_combo[key] = pd.concat([hadv_fa_combo[key],temp[key]])
#         except:
#             hadv_fa_combo[key] = temp[key]
# for a in foo.keys():
#     try:
#         fas = pd.merge(fas, foo[a], on='Symbol')
#     except:
#         fas = foo[a]
# # return fas.drop_duplicates().reset_index(drop=True)
# self.sa_fundamental_data = fas.drop_duplicates().reset_index(drop=True).loc[:,~fas.drop_duplicates().reset_index(drop=True).columns.duplicated()].copy()
##################################################################################################################
def load_data( file_path=None):
    global sa_fundamental_data, basic_materials_fa, communication_services_fa, consumer_cyclical_fa, consumer_defensive_fa, energy_fa, financial_services_fa, healthcare_fa, industrials_fa, real_estate_fa, technology_fa, utilities_fa
    sa_fundamental_data['sector'] = sa_fundamental_data['Symbol'].map({y:list(z.values())[-1] for y,z in sectors_industries.items()})
    sa_fundamental_data['industry'] = sa_fundamental_data['Symbol'].map({y:list(z.values())[-0] for y,z in sectors_industries.items()})
    sa_fundamental_data['hq'] = sa_fundamental_data['Symbol'].map({y:z for y,z in company_headquarters.items()})
    sa_fundamental_data.replace(to_replace='(^-|NM)',value=0,regex=True,inplace=True)
    sa_fundamental_data.replace(to_replace=['A+', 'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D+', 'D', 'D-', 'F'],
                                                value=[13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1], inplace=True)
    sa_fundamental_data = sa_fundamental_data.loc[~sa_fundamental_data.Symbol.apply(lambda x: isinstance(x, int))]
    basic_materials_fa = sa_fundamental_data.loc[sa_fundamental_data.sector == 'Basic Materials']
    communication_services_fa = sa_fundamental_data.loc[sa_fundamental_data.sector == 'Communication Services']
    consumer_cyclical_fa = sa_fundamental_data.loc[sa_fundamental_data.sector == 'Consumer Cyclical']
    consumer_defensive_fa = sa_fundamental_data.loc[sa_fundamental_data.sector == 'Consumer Defensive']
    energy_fa = sa_fundamental_data.loc[sa_fundamental_data.sector == 'Energy']
    financial_services_fa = sa_fundamental_data.loc[sa_fundamental_data.sector == 'Financial Services']
    healthcare_fa = sa_fundamental_data.loc[sa_fundamental_data.sector == 'Healthcare']
    industrials_fa = sa_fundamental_data.loc[sa_fundamental_data.sector == 'Industrials']
    real_estate_fa = sa_fundamental_data.loc[sa_fundamental_data.sector == 'Real Estate']
    technology_fa = sa_fundamental_data.loc[sa_fundamental_data.sector == 'Technology']
    utilities_fa = sa_fundamental_data.loc[sa_fundamental_data.sector == 'Utilities']
    

def symbol_data( symbols):
    
    return sa_fundamental_data.loc[sa_fundamental_data['Symbol'].isin(symbols)]

    #I want to be able to identify symnbol with fundamentals in a specific percentile.
    #I've tried using df.loc[]; however, it's returning errors. This is a workaround and
    #I may be able to make it into a reusable function.

def sa_fundamental_threshold( fundamental, percentile):
    """Produces a list of symbols from dataframe df that have the specified 
    fundamental with a value greater than percentile.
    The list produced has the variable name sa_fundamental_threshold.symbols."""
    foo = list(zip(list(sa_fundamental_data['Symbol']), list(sa_fundamental_data[fundamental].apply(pd.to_numeric, errors='coerce'))))
    symbols = [item[0] for item in foo if item[1] > sa_fundamental_data[fundamental].apply(pd.to_numeric, errors='coerce').quantile(percentile)]
    return symbols

####################################################################################
#The following lines will make a dictionary with the appropriate sectors the resulting
#symbols belong to and make a pie chart with the results.
@staticmethod
def watchlist_composition(lst, level='sector', chart_type='barh', name=str(None)):
    #TODO Figure out why the start in plt.barh started returning errors with the start as 20. This
    #TODO started after I changed the sector_industries to the results from sec-api package. I
    #TODO changed the start to 10 and it works now. I should try to change it back to 20 later.
    with open(r"E:\Market Research\Dataset\Fundamental Data\symbol_sector_industry.txt", "r") as f:
        symbol_sector_industry = eval(f.read())
    sector = {}
    for sym in lst:
        try:
            sector[sym] = symbol_sector_industry[sym]
        except Exception as e:
            #TODO print(sym, e) update error handling
            continue
    pie_list = []
    for sym in sector:
        pie_list.append(sector.get(sym, {}).get(level))
    unique_values = set(pie_list)
    pie_dict = dict(sorted({k:pie_list.count(k) for k in unique_values}.items(), key=lambda item: item[1], reverse=True))
    if None in pie_dict.keys():#Resolves the error raised from plt.barh when a value of None was appended to pie_list.
        logging.warning("A value of None was found for a symbol's sector/industry. This could have resulted in an error so the None value was removed. Identification of the symbol with a None valued sector/industry needs to be performed.")
        del pie_dict[None]
    pie_dict_values = [item for item in pie_dict.values()]
    pie_dict_keys = [item for item in pie_dict.keys()]
    y = np.array(pie_dict_values)
    if chart_type == 'barh':
        plt.barh(pie_dict_keys[10::-1], y[10::-1])#!start was 20. The error may have been that a None value was being appended in pie_list
        try:
            plt.xticks(np.arange(0, max(pie_dict.values()), step=int(max(pie_dict.values())/5)))
        except ZeroDivisionError:
            pass
        # if level == 'Industry':
        #     #I'm trying to improve the chart appearance the commented out lines aren't working as expected.
        #     #The primary problem is that there isnt enough spacing between the y-tick labels/bars
        #     #For now I'm using slice indexing to limit the number of variables on the y-axis.
        #     #plt.barh(pie_dict_keys, y)
        #     #plt.figure(figsize=(len(pie_dict_keys)*10, max(pie_dict.values())))
        #     #plt.ylim(200,600)
        #     #plt.tight_layout()
        #     #plt.yticks(len(pie_dict_keys),5)
        #     #ax = plt.gca()
        #     #ax.yaxis.set_tick_params(pad=10)
        #     #plt.yticks(np.arange(0, len(pie_dict_keys), step= 1))
        #     #plt.yticks(rotation=45)
        # else:
        #     pass
    elif chart_type == 'pie':
        plt.pie(y, labels=pie_dict_keys)
    if name == None:
        pass
    else:
        plt.title(name)
    plt.show()
    print(sorted(pie_dict.items(), key=lambda item: item[1], reverse=True))
####################################################################################

####################################################################################
#The following is a draft of a function that will add symbols and their respective
#sector and industry from yfinance.
#@staticmethod
def sector_industry_database_add(lst, by='ticker'):
    with open(r"E:\Market Research\Dataset\Fundamental Data\symbol_sector_industry.txt", "r") as f:
        symbol_sector_industry = eval(f.read())
    with open(r"E:\Market Research\Dataset\Fundamental Data\symbol_sector_industry_backup.txt", "w") as f:
        f.write(json.dumps(symbol_sector_industry, indent=4, sort_keys=True))
    for sym in lst:
        try:
            if by == 'ticker':
                sec = [item['sector'] for item in MappingApi(f.api_key).resolve(by, sym) if item['ticker'] == sym][0]
                ind = [item['industry'] for item in MappingApi(f.api_key).resolve(by, sym) if item['ticker'] == sym][0]
                symbol_sector_industry[sym] = {'sector' : sec}
                symbol_sector_industry[sym].update({'industry' : ind})
            else:
                queryApi = QueryApi(f.api_key)
                query = {
                "query": { "query_string": { 
                    "query": "ticker:" + sym,
                } },
                "from": "0",
                "size": "10",
                "sort": [{ "filedAt": { "order": "desc" } }]
                }

                response = queryApi.get_filings(query)
                cik = response['filings'][0]['cik']
                sec = [item['sector'] for item in MappingApi(f.api_key).resolve(by, cik) if item['ticker'] == sym][0]
                ind = [item['ind'] for item in MappingApi(f.api_key).resolve(by, cik) if item['ticker'] == sym][0]
                symbol_sector_industry[sym] = {'sector' : sec}
                symbol_sector_industry[sym].update({'industry' : ind})
        except Exception as e:
            #TODO print(sym, e) update error handling
            continue
    with open(r"E:\Market Research\Dataset\Fundamental Data\symbol_sector_industry.txt", "w") as f:
        f.write(json.dumps(symbol_sector_industry, indent=4, sort_keys=True))


#I want to make a function that will assign the appropriate sector/industry with an
#input of symbols.
#@staticmethod
def get_sym_sector_industry(lst, save=True):
    with open(r"E:\Market Research\Dataset\Fundamental Data\symbol_sector_industry.txt", "r") as f:
        symbol_sector_industry = eval(f.read())
    sector_industry_dict = {}
    for sym in lst:
        try:
            sector_industry_dict[sym] = symbol_sector_industry[sym]
        except:
            #TODO print(sym) update error handling
            continue
    if save == True:
        sector_industry_dict_saved = sector_industry_dict
    return sector_industry_dict

def get_sym_sector_industry_saved():
    with open(r"E:\Market Research\Dataset\Fundamental Data\symbol_sector_industry.txt", "r") as f:
        symbol_sector_industry = eval(f.read())
    sector_industry_dict = {}
    for sym in sa_fundamental_data['Symbol'].to_list():
        try:
            sector_industry_dict[sym] = symbol_sector_industry[sym]
        except:
            #TODO print(sym) update error handling
            continue
    sectors_industries = sector_industry_dict

#I'm not sure what the purpose of this function was. I'm not 
#sure that I'll keep it.
@staticmethod
def get_sym_by_sector_industry(dic, sector_industry, level='Sector'):
    """This function may be broken."""
    for k in dic:
        if dic[k][level] == sector_industry:
            print(k)

#@staticmethod
def sym_by_level(level_name, watchlist, level='industry'):
    """sym_by_level returns a list of symbols obtained from the input
    watchlist belonging to the specified Sector or Industry."""
    level_members = []
    with open(r"E:\Market Research\Dataset\Fundamental Data\symbol_sector_industry.txt", "r") as f:
        symbol_sector_industry = eval(f.read())
    for sym in watchlist:
        try:
            if symbol_sector_industry[sym][level] == level_name:
                level_members.append(sym)
        except:
            continue
    return level_members

#@staticmethod
def sector_industry_member_search(sector_industry_name, level='industry'):
    level_members = []
    with open(r"E:\Market Research\Dataset\Fundamental Data\symbol_sector_industry.txt", "r") as f:
        symbol_sector_industry = eval(f.read())
    for k,v in symbol_sector_industry.items():
        try:
            if symbol_sector_industry[k][level] == sector_industry_name:
                level_members.append(k)
        except:
            continue
    return level_members
    #The following line won't work because some symbols don't have
    #industry data.
    #return [k for k,v in symbol_sector_industry.items() if symbol_sector_industry[k][level] == sector_industry_name]

#These scripts can be used to find symbols with combinations of 
#high/low technical sentiment score and quant score.
#*I don't know what SA_watchlist is. The idea behind this function is interesting but there is a better way to implement it.
@staticmethod
def extremes_tsc_quant(Watchlista_watchlist, SA_watchlist, elevated=True, std=2, sort_ascending=False):
    """The function requires a Watchlista object and the 
    hadv_fa_combo dictionary; they should be changed to variables
    and can be called Watchlista_watchlist and SA_watchlist."""
    if elevated:
        if std > 0:
            foo = {k for k, v in Watchlista_watchlist.elevated_score_dict.items() if v > pd.DataFrame.from_dict(Watchlista_watchlist.elevated_score_dict, orient='index').describe().loc['mean'].item() + (std * pd.DataFrame.from_dict(Watchlista_watchlist.elevated_score_dict, orient='index').describe().loc['std'].item())}
            fas = [item if item in SA_watchlist['Symbol'].to_list() else print(item) for item in foo]
            baz = {item:SA_watchlist['quantRating'][SA_watchlist['Symbol']==item].to_list() for item in fas}
            bar = pd.DataFrame.from_dict(baz, orient='index')
        else:
            foo = {k for k, v in Watchlista_watchlist.elevated_score_dict.items() if v > pd.DataFrame.from_dict(Watchlista_watchlist.elevated_score_dict, orient='index').describe().loc['mean'].item()}
            fas = [item if item in SA_watchlist['Symbol'].to_list() else print(item) for item in foo]
            baz = {item:SA_watchlist['quantRating'][SA_watchlist['Symbol']==item].to_list() for item in fas}
            bar = pd.DataFrame.from_dict(baz, orient='index')
    else:
        if std > 0:
            foo = {k for k, v in Watchlista_watchlist.depressed_score_dict.items() if v < pd.DataFrame.from_dict(Watchlista_watchlist.depressed_score_dict, orient='index').describe().loc['mean'].item() + (std * pd.DataFrame.from_dict(Watchlista_watchlist.depressed_score_dict, orient='index').describe().loc['std'].item())}
            fas = [item if item in SA_watchlist['Symbol'].to_list() else print(item) for item in foo]
            baz = {item:SA_watchlist['quantRating'][SA_watchlist['Symbol']==item].to_list() for item in fas}
            bar = pd.DataFrame.from_dict(baz, orient='index')
        else:
            foo = {k for k, v in Watchlista_watchlist.depressed_score_dict.items() if v < pd.DataFrame.from_dict(Watchlista_watchlist.depressed_score_dict, orient='index').describe().loc['mean'].item()}
            fas = [item if item in SA_watchlist['Symbol'].to_list() else print(item) for item in foo]
            baz = {item:SA_watchlist['quantRating'][SA_watchlist['Symbol']==item].to_list() for item in fas}
            bar = pd.DataFrame.from_dict(baz, orient='index')
    if sort_ascending == False:
        return bar.apply(pd.to_numeric, errors='coerce').sort_values(0, ascending=False)
    else:
        return bar.apply(pd.to_numeric, errors='coerce').sort_values(0)

####################################################################
#The following function produces a dataframe of specified length
#with the symbol and quant score of symbols with the highest 
#seeking alpha quant rating.
def top_quant(watchlist, length=20):
    return sa_fundamental_data[['Symbol', 'quantRating']].loc[
    sa_fundamental_data['Symbol'].isin(watchlist.symbols)
    ].replace('-', 0).sort_values('quantRating', ascending=False)[:length]

#This function takes in a string and returns a dataframe with the
#symbol from the string and the quant score from the hadv or
#specified watchlist.
def quant_from_string(string, sort_ascending=False):
    boo = string.split('\n')
    return sa_fundamental_data[['Symbol', 'quantRating']].loc[
        sa_fundamental_data['Symbol'].isin(boo)].replace(
        '-', 0).sort_values('quantRating', ascending=sort_ascending)
####################################################################
####################################################################

#This function find stocks with earnings in a specified range.
#Currently its producing unexpected results with a high date range.
#The results appear correct with a low date range.
def upcoming_earnings( drange=5):
    end_date = pd.to_datetime('today') + pd.Timedelta(days=drange)
    return (sa_fundamental_data[['Symbol', 'Upcoming Announce Date', 'sector', 'industry']]
            .loc[sa_fundamental_data['Upcoming Announce Date'].astype(str)
            .between(pd.to_datetime('today').strftime(r'%#m/%d/%Y'), end_date.strftime(r'%#m/%d/%Y'))]
            .sort_values('Upcoming Announce Date'))

def correlation_fa( watchlist, column, matrix=False, sort_by=False):
    fas = {}
    if type(watchlist) is list:
        for item in column:
            try:
                fas[item] = sa_fundamental_data[item].loc[sa_fundamental_data['Symbol'].isin(watchlist)].apply(pd.to_numeric, errors='coerce')
            except:
                continue
    else:
        for item in column:
            try:
                fas[item] = sa_fundamental_data[item].loc[sa_fundamental_data['Symbol'].isin(watchlist.symbols)].apply(pd.to_numeric, errors='coerce')
            except:
                continue
        # foo = pd.Series()
        # for sym in watchlist.symbols:
        #     foo = pd.concat([foo, s.loc[df['Symbol'] == sym].apply(pd.to_numeric, errors='coerce').fillna(0)])
        # fas[s.name] = foo
    faz = pd.DataFrame.from_dict(fas).corr()
    print(faz)
    print(faz.describe().loc['mean'].sort_values(ascending=False))
    if matrix == True:
        cmap = plt.get_cmap('coolwarm')
        plt.matshow(faz, cmap=cmap)
        plt.xticks([item for item in range(len(fas.keys()))], fas.keys(), rotation=90)
        plt.yticks([item for item in range(len(fas.keys()))], fas.keys())
        plt.colorbar()
        #return faz
        if sort_by == False:
            return plt.show(), faz
        else:
            return plt.show(), faz.sort_values(sort_by, ascending=False)
    if sort_by == False:
        return faz
    else:
        return faz.sort_values(sort_by, ascending=False)

def fa_from_list( watchlist):
    # for sym in watchlist:
    #     fa_dict = {k:v[k].loc[v[k]['Symbol']==sym] for k,v in sa_source.items()}
    foo = pd.DataFrame()
    for sym in watchlist:
        foo = pd.concat([foo, sa_fundamental_data.loc[sa_fundamental_data['Symbol'] == sym]])
    return foo

def earnings_expectations( symbols, outlook='positive'):
    results = []
    if saa.earnings_dict == None:
        saa.earnings_load()
    if outlook == 'positive':
        for sym in symbols:
            try:
                if (sa_fundamental_data['EPS Estimate'].loc[sa_fundamental_data.Symbol == sym].values[0] > sa_fundamental_data['EPS Actual'].loc[sa_fundamental_data.Symbol == sym].values[0]) & (sa_fundamental_data['Revenue Estimate'].loc[sa_fundamental_data.Symbol == sym].values[0] > sa_fundamental_data['Revenue Actual'].loc[sa_fundamental_data.Symbol == sym].values[0]):
                # if (float(self.saa.earnings_dict[sym]['estimates']['eps_normalized_actual']['0'][0]['dataitemvalue']) < float(self.saa.earnings_dict[sym]['estimates']['eps_normalized_consensus_mean']['1'][0]['dataitemvalue'])) and (float(self.saa.earnings_dict[sym]['estimates']['revenue_actual']['0'][0]['dataitemvalue']) < float(self.saa.earnings_dict[sym]['estimates']['revenue_consensus_mean']['1'][0]['dataitemvalue'])):
                    results.append(sym)
            except KeyError as ke:
                #TODO print(sym, ke) update error handling
                continue
    elif outlook == 'negative':
        for sym in symbols:
            try:
                if (sa_fundamental_data['EPS Estimate'].loc[sa_fundamental_data.Symbol == sym].values[0] < sa_fundamental_data['EPS Actual'].loc[sa_fundamental_data.Symbol == sym].values[0]) & (sa_fundamental_data['Revenue Estimate'].loc[sa_fundamental_data.Symbol == sym].values[0] < sa_fundamental_data['Revenue Actual'].loc[sa_fundamental_data.Symbol == sym].values[0]):
                # if (float(self.saa.earnings_dict[sym]['estimates']['eps_normalized_actual']['0'][0]['dataitemvalue']) > float(self.saa.earnings_dict[sym]['estimates']['eps_normalized_consensus_mean']['1'][0]['dataitemvalue'])) and (float(self.saa.earnings_dict[sym]['estimates']['revenue_actual']['0'][0]['dataitemvalue']) > float(self.saa.earnings_dict[sym]['estimates']['revenue_consensus_mean']['1'][0]['dataitemvalue'])):
                    results.append(sym)
            except KeyError as ke:
                #TODO print(sym, ke) update error handling
                continue
    else:
        raise ValueError('variable outlook must be string and == positive or negative')
    return results

def ebitda( symbol, form_type='10-K'):
    try:
        income = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Income').iloc[0].value)
        taxes = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Taxes').iloc[0].value)
        interest = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Interest').iloc[0].value)
        d_a = float(f.single_fundamental(symbol, form_type=form_type, fundamental='DepreciationAndAmortization').iloc[0].value)
        ebitda = income + taxes + interest + d_a
        return ebitda
    except:
        return np.nan

def ebitda_growth( symbol, form_type='10-K'):
    try:
        current_income = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Income').iloc[0].value)
        current_taxes = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Taxes').iloc[0].value)
        current_interest = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Interest').iloc[0].value)
        current_d_a = float(f.single_fundamental(symbol, form_type=form_type, fundamental='DepreciationAndAmortization').iloc[0].value)
        current_ebitda = current_income + current_taxes + current_interest + current_d_a
        prev_income = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Income').iloc[1].value)
        prev_taxes = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Taxes').iloc[1].value)
        prev_interest = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Interest').iloc[1].value)
        prev_d_a = float(f.single_fundamental(symbol, form_type=form_type, fundamental='DepreciationAndAmortization').iloc[1].value)
        prev_ebitda = prev_income + prev_taxes + prev_interest + prev_d_a
        ebitda_yoy = ((current_ebitda - prev_ebitda) / prev_ebitda) * 100
        return ebitda_yoy
    except:
        return np.nan

def ebitda_margin( symbol, form_type='10-K'):
    try:
        ebitda = ebitda(symbol, form_type='10-K')
        revenue = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Revenue').iloc[0].value)
        ebitda_margin = ebitda / revenue
        return ebitda
    except:
        return np.nan

def ebit( symbol, form_type='10-K'):
    try:
        income = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Income').iloc[0].value)
        taxes = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Taxes').iloc[0].value)
        interest = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Interest').iloc[0].value)
        ebit = income + taxes + interest
        return ebit
    except:
        return np.nan

def ebit_growth( symbol, form_type='10-K'):
    try:
        current_income = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Income').iloc[0].value)
        current_taxes = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Taxes').iloc[0].value)
        current_interest = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Interest').iloc[0].value)
        current_ebit= current_income + current_taxes + current_interest
        prev_income = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Income').iloc[1].value)
        prev_taxes = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Taxes').iloc[1].value)
        prev_interest = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Interest').iloc[1].value)
        prev_ebit = prev_income + prev_taxes + prev_interest
        ebit_yoy = ((current_ebit- prev_ebit) / prev_ebit) * 100
        return ebit_yoy
    except:
        return np.nan

def ebit_margin( symbol, form_type='10-K'):
    try:
        ebit = ebit(symbol, form_type='10-K')
        revenue = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Revenue').iloc[0].value)
        ebit_margin = ebit / revenue
        return ebit
    except:
        return np.nan

def enterprise_value( symbol, form_type='10-K'):
    try:
        market_cap = sa_fundamental_data.marketCap.loc[sa_fundamental_data.Symbol == symbol].values[0]
        short_term_debt = float(f.single_fundamental(symbol, form_type=form_type, fundamental='ShortTermDebt').iloc[0].value)
        long_term_debt = float(f.single_fundamental(symbol, form_type=form_type, fundamental='LongTermDebt').iloc[0].value)
        total_debt = short_term_debt + long_term_debt
        cashandcashequivalents = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CashAndCashEquivalents').iloc[0].value)
        ev = market_cap + total_debt - cashandcashequivalents
        return ev
    except:
        return np.nan

def ev_sales( symbol, form_type='10-K'):
    try:
        ev = enterprise_value(symbol, form_type=form_type)
        revenue = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Revenue').iloc[0].value)
        ev_revenue = ev / revenue
        return ev_revenue
    except:
        return np.nan

def ev_ebitda( symbol, form_type='10-K'):
    try:
        ev = enterprise_value(symbol, form_type=form_type)
        ebitda = ebitda(symbol, form_type='10-K')
        ev_over_ebitda = ev / ebitda
        return ev_over_ebitda
    except:
        return np.nan

def ev_ebit( symbol, form_type='10-K'):
    try:
        ev = enterprise_value(symbol, form_type=form_type)
        ebit = ebit(symbol, form_type=form_type)
        ev_over_ebit = ev / ebit
        return ev_over_ebit
    except:
        return np.nan

def free_cash_flow( symbol, form_type='10-K'):
    try:
        cash_from_operations = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CashFromOperations').iloc[0].value)
        capex = capex(symbol, form_type=form_type)
        fcf = cash_from_operations - capex
        return fcf
    except:
        return np.nan

def price_fcf( symbol, form_type='10-K'):
    try:
        market_cap = sa_fundamental_data.marketCap.loc[sa_fundamental_data.Symbol == symbol].values[0]
        fcf = free_cash_flow(symbol, form_type=form_type)
        price_fcf_ratio = market_cap / fcf
        return price_fcf_ratio
    except:
        return np.nan

def levered_fcf( symbol, form_type='10-K'):
    try:
        ebitda = ebitda(symbol)
        current_assets = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CurrentAssets').iloc[0].value)
        current_liabilities = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CurrentLiabilities').iloc[0].value)
        delta_nwc = current_assets - current_liabilities
        ppe = float(f.single_fundamental(symbol, form_type=form_type, fundamental='PPE').iloc[0].value)
        d_a = float(f.single_fundamental(symbol, form_type=form_type, fundamental='DepreciationAndAmortization').iloc[0].value)
        capex = ppe + d_a
        short_term_debt = float(f.single_fundamental(symbol, form_type=form_type, fundamental='ShortTermDebt').iloc[0].value)
        long_term_debt = float(f.single_fundamental(symbol, form_type=form_type, fundamental='LongTermDebt').iloc[0].value)
        lfcf = ebitda - delta_nwc - capex - short_term_debt
        return lfcf
    except:
        return np.nan

def levered_fcf_margin( symbol, form_type='10-K'):
    try:
        levered_fcf = levered_fcf(symbol, form_type=form_type)
        revenue = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Revenue').iloc[0].value)
        lfcf_margin = (levered_fcf / revenue) * 100
        return lfcf_margin
    except:
        return np.nan

def fcf_per_share( symbol, form_type='10-K'):
    try:
        floatApi = FloatApi(f.api_key)
        float_data = floatApi.get_float(ticker=symbol)
        shares_float = float_data['data'][0]['float']['outstandingShares'][0]['value']
        fcf = free_cash_flow(symbol, form_type='10-K')
        fcf_per_share = fcf / shares_float
        return fcf_per_share
    except:
        return np.nan

def free_cash_flow_growth( symbol, form_type='10-K'):
    try:
        cur_cash_from_operations = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CashFromOperations').iloc[0].value)
        cur_capex = capex(symbol, form_type=form_type)
        cur_fcf = cur_cash_from_operations - cur_capex
        prev_cash_from_operations = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CashFromOperations').iloc[1].value)
        prev_ppe = float(f.single_fundamental(symbol, form_type=form_type, fundamental='PPE').iloc[1].value)
        prev_d_a = float(f.single_fundamental(symbol, form_type=form_type, fundamental='DepreciationAndAmortization').iloc[1].value)
        prev_capex = prev_ppe + prev_d_a
        prev_fcf = prev_cash_from_operations - prev_capex
        fcf_growth = ((cur_fcf - prev_fcf) / prev_fcf) * 100
        return fcf_growth
    except:
        return np.nan

def return_on_total_capital( symbol, form_type='10-K'):
    try:
        income = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Income').iloc[0].value)
        short_term_debt = float(f.single_fundamental(symbol, form_type=form_type, fundamental='ShortTermDebt').iloc[0].value)
        long_term_debt = float(f.single_fundamental(symbol, form_type=form_type, fundamental='LongTermDebt').iloc[0].value)
        total_debt = short_term_debt + long_term_debt
        equity = float(f.single_fundamental(symbol, form_type=form_type, fundamental='ShareholdersEquity').iloc[0].value)
        invested_capital = total_debt + equity
        rotc = income / invested_capital
        return rotc
    except:
        return np.nan

def return_on_total_assets( symbol, form_type='10-K'):
    try:
        assets_cur = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Assets').iloc[0].value)
        assets_prev = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Assets').iloc[1].value)
        ebit = ebit(symbol, form_type=form_type)
        avg_total_assets = (assets_cur + assets_prev) / 2
        rota = ebit / avg_total_assets
        return rota
    except:
        return np.nan

def income_per_employee( symbol, form_type='10-K'):
    try:
        num_employees = sa_fundamental_data.numberOfEmployees.loc[sa_fundamental_data.Symbol == symbol].values[0]
        income = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Income').iloc[0].value)
        income_per_employee = income / num_employees
        return income_per_employee
    except:
        return np.nan

def net_profit_margin( symbol, form_type='10-K'):
    try:
        income = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Income').iloc[0].value)
        revenue = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Revenue').iloc[0].value)
        npm = (income / revenue) * 100
        return npm
    except:
        return np.nan

def price_sales( symbol, form_type='10-K'):
    try:
        market_cap = sa_fundamental_data.marketCap.loc[sa_fundamental_data.Symbol == symbol].values[0]
        revenue = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Revenue').iloc[0].value)
        p_s = market_cap / revenue
        return p_s
    except:
        return np.nan

def sales(symbol, limit=10, timeframe='quarterly', plot=False) -> pd.Series:
    """
    #! Returns errors for some symbols. I believe most of them could be handled simply.
    Returns a Series of sales data for a given symbol.

    Args:
        symbol (_type_): _description_
        limit (int, optional): _description_. Defaults to 10.
        timeframe (str, optional): _description_. Defaults to 'quarterly'.
        plot (bool, optional): _description_. Defaults to False.

    Returns:
        pd.Series: _description_
    """
    client = RESTClient(polygon_api_key)
    financials = []
    for f in client.vx.list_stock_financials(
        ticker=f"{symbol}",
        order="asc",
        limit=limit,
        sort="filing_date",
        timeframe=timeframe
        ):
        financials.append(f)
    vals = []
    for item in financials:
        if item.financials.income_statement is not None:
            vals.append((item.filing_date, item.financials.income_statement.revenues.value))
    df = pd.DataFrame([val for val in vals if val[0] is not None])
    df.columns = ['filing_date', 'sales']
    df = df.set_index('filing_date')
    if plot:
        return px.line(df)
    return df

def revenue_growth(symbol, limit=10, timeframe='quarterly', plot=False, most_recent=True):
    df = sales(symbol, limit=limit, timeframe=timeframe, plot=plot)
    if plot:
        return df
    df['pct_change'] = df.sales.pct_change()
    if most_recent:
        return df['pct_change'].iloc[-1].item()
    else:
        return df['pct_change']

def capex( symbol, form_type='10-K'):
    try:
        ppe = float(f.single_fundamental(symbol, form_type=form_type, fundamental='PPE').iloc[0].value)
        d_a = float(f.single_fundamental(symbol, form_type=form_type, fundamental='DepreciationAndAmortization').iloc[0].value)
        capex = ppe + d_a
        return capex
    except:
        return np.nan

def capex_sales( symbol, form_type='10-K'):
    try:
        capex = capex(symbol, form_type=form_type)
        revenue = sales(symbol, form_type=form_type)
        capex_sales = capex / revenue
        return capex_sales
    except:
        return np.nan

def capex_growth( symbol, form_type='10-K'):
    try:
        cur_ppe = float(f.single_fundamental(symbol, form_type=form_type, fundamental='PPE').iloc[0].value)
        cur_d_a = float(f.single_fundamental(symbol, form_type=form_type, fundamental='DepreciationAndAmortization').iloc[0].value)
        cur_capex = cur_ppe + cur_d_a
        prev_ppe = float(f.single_fundamental(symbol, form_type=form_type, fundamental='PPE').iloc[1].value)
        prev_d_a = float(f.single_fundamental(symbol, form_type=form_type, fundamental='DepreciationAndAmortization').iloc[1].value)
        prev_capex = prev_ppe + prev_d_a
        capex_growth = ((cur_capex - prev_capex) / prev_capex) * 100
        return capex_growth
    except:
        return np.nan

def asset_turnover( symbol, form_type='10-K'):
    try:
        revenue = sales(symbol, form_type=form_type)
        ending_assets = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Assets').iloc[0].value)
        beginning_assets = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Assets').iloc[1].value)
        assets_avg = (beginning_assets + ending_assets) / 2
        asset_turnover_ratio = revenue / assets_avg
        return asset_turnover_ratio
    except:
        return np.nan

def p_e_basic( symbol, form_type='10-K'):
    try:
        price = symbols[symbol].iloc[-1].Close.values[0]
        eps_basic = float(f.single_fundamental(symbol, form_type=form_type, fundamental='EPSBasic').iloc[0].value)
        p_e = price / eps_basic
        return p_e
    except:
        return np.nan

def eps_basic_growth( symbol, form_type='10-K'):
    try:
        prev_eps_basic = float(f.single_fundamental(symbol, form_type=form_type, fundamental='EPSBasic').iloc[1].value)
        cur_eps_basic = float(f.single_fundamental(symbol, form_type=form_type, fundamental='EPSBasic').iloc[0].value)
        eps_growth = ((cur_eps_basic - prev_eps_basic) / prev_eps_basic) * 100
        return eps_growth
    except:
        return np.nan

def p_e_diluted( symbol, form_type='10-K'):
    try:
        price = symbols[symbol].iloc[-1].Close.values[0]
        eps_diluted = float(f.single_fundamental(symbol, form_type=form_type, fundamental='EPSDiluted').iloc[0].value)
        p_e = price / eps_diluted
        return p_e
    except:
        return np.nan

def eps_diluted_growth( symbol, form_type='10-K'):
    try:
        prev_eps_diluted = float(f.single_fundamental(symbol, form_type=form_type, fundamental='EPSDiluted').iloc[1].value)
        cur_eps_diluted = float(f.single_fundamental(symbol, form_type=form_type, fundamental='EPSDiluted').iloc[0].value)
        eps_growth = ((cur_eps_diluted - prev_eps_diluted) / prev_eps_diluted) * 100
        return eps_growth
    except:
        return np.nan

def book_value( symbol, form_type='10-K'):
    try:
        total_assets = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Assets').iloc[0].value)
        total_liabilities = float(f.single_fundamental(symbol, form_type=form_type, fundamental='Liabilities').iloc[0].value)
        bv = total_assets - total_liabilities
        return bv
    except:
        return np.nan

def price_book( symbol, form_type='10-K'):
    try:
        price = symbols[symbol].iloc[-1].Close.values[0]
        book = book_value(symbol, form_type)
        p_b = price / book
        return p_b
    except:
        return np.nan

def working_capital( symbol, form_type='10-K'):
    try:
        current_assets = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CurrentAssets').iloc[0].value)
        current_liabilities = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CurrentLiabilities').iloc[0].value)
        working_capital = current_assets - current_liabilities
        return working_capital
    except:
        return np.nan

def working_capital_growth( symbol, form_type='10-K'):
    try:
        cur_current_assets = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CurrentAssets').iloc[0].value)
        cur_current_liabilities = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CurrentLiabilities').iloc[0].value)
        cur_working_capital = cur_current_assets - cur_current_liabilities
        prev_current_assets = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CurrentAssets').iloc[1].value)
        prev_current_liabilities = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CurrentLiabilities').iloc[1].value)
        prev_working_capital = prev_current_assets - prev_current_liabilities
        working_capital_growth = ((cur_working_capital - prev_working_capital) / prev_working_capital) * 100
        return working_capital_growth
    except:
        return np.nan

def cash_from_operations( symbol, form_type='10-K'):
    try:
        cash_from_operations = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CashFromOperations').iloc[0].value)
        return cash_from_operations
    except:
        return np.nan

def cash_per_share( symbol, form_type='10-K'):
    try:
        cash_at_end_of_period = float(f.single_fundamental(symbol, form_type=form_type, fundamental='CashAtEndOfPeriod').iloc[0].value)
        floatApi = FloatApi(f.api_key)
        float_data = floatApi.get_float(ticker=symbol)
        shares_float = float_data['data'][0]['float']['outstandingShares'][0]['value']
        cash_per_share = cash_at_end_of_period / shares_float
        return cash_per_shares
    except:
        return np.nan
    
def current_float( symbol: str) -> Tuple[str,int]:
    floatApi = FloatApi(sec_api_key)
    response = floatApi.get_float(ticker=symbol)
    sharesOutstanding = response['data'][0]['float']['outstandingShares'][0]['value']
    return (symbol, sharesOutstanding)

def float_time_series(symbol: str) -> pd.DataFrame:
    floatApi = FloatApi(sec_api_key)
    response = floatApi.get_float(ticker=symbol)
    df = pd.json_normalize(response['data'])
    df = pd.DataFrame([(_[0]['period'], _[0]['value']) for _ in df['float.outstandingShares'] if len(_) > 0])
    df.columns = ['period', 'float_shares']
    df['period'] = pd.to_datetime(df['period'])
    df.set_index('period', inplace=True)
    df.sort_index(inplace=True)
    return df

def market_cap_time_series(symbol: str, symbol_df: pd.DataFrame=None) -> pd.DataFrame:
    float_df = float_time_series(symbol)
    if not isinstance(symbol_df, pd.DataFrame):
        symbol_df = api_import([symbol])[symbol]
    # Merge the two dataframes with symbol_df index as the final index
    merged_df = symbol_df.merge(float_df, left_index=True, right_index=True, how='left')
    # Forward fill the nan values in the value column
    merged_df['float_shares'] = merged_df['float_shares'].ffill()
    merged_df['market_cap'] = merged_df['Close'] * merged_df['float_shares']
    return merged_df['market_cap']

def price_to_fundamental(symbol: str, 
                         fundamental: callable,
                         symbol_df: pd.DataFrame=None, 
                         plot=False,
                         limit=10,
                         timeframe='annual',
                         prominence=None,
                         distance=90,
                         return_df=False) -> pd.Series:
    fundamental_col: str = fundamental.__name__
    if symbol_df is None:
        symbol_df = api_import([symbol])[symbol]
    if isinstance(fundamental, pd.DataFrame):
        fundamental_df = fundamental.copy()
    else:
        fundamental_df = fundamental(symbol, 
                                    limit=limit, 
                                    timeframe=timeframe, 
                                    plot=False)
    price_df = market_cap_time_series(symbol, symbol_df).to_frame().copy()
    price_df.index = pd.to_datetime(price_df.index)
    fundamental_df.index = pd.to_datetime(fundamental_df.index)
    fundamental_df_reindexed = fundamental_df.reindex(price_df.index, method='ffill')
    
    price_df[fundamental_col] = fundamental_df_reindexed[fundamental_col]

    col = f'price_to_{fundamental_col}'
    price_df[col] = price_df['market_cap'] / price_df[fundamental_col].replace(0, np.nan)
    price_df[col] = price_df[col].fillna(0)
    price_df[col] = price_df[col].ffill()
    
    # Find peaks (highs) and troughs (lows) in the price-to-fundamental ratio
    peaks, _ = find_peaks(price_df[col].dropna(), prominence=prominence, distance=distance)
    troughs, _ = find_peaks(-price_df[col].dropna(), prominence=prominence, distance=distance)
    
    # Get the actual index values for peaks and troughs
    peak_dates = price_df.index[peaks]
    trough_dates = price_df.index[troughs]
    peak_values = price_df[col].iloc[peaks]
    trough_values = price_df[col].iloc[troughs]
    if plot:
        fig = px.line(price_df[col], 
                      title=f'{symbol} Price to {fundamental_col.title()}',
                      labels={'index': 'Date', 'value': f'Price to {fundamental_col.title()}'})
        fig.add_scatter(x=price_df.index, y=price_df['market_cap'], 
                       mode='lines', name='market_cap', yaxis='y2')
        fig.update_layout(yaxis2=dict(overlaying='y', side='right', title='Market Cap'))
        
        #Scatter plot the peaks and troughs
        fig.add_scatter(x=peak_dates, y=peak_values, 
               mode='markers', name='Peaks', marker=dict(color='red', size=8))
        fig.add_scatter(x=trough_dates, y=trough_values, 
                mode='markers', name='Troughs', marker=dict(color='green', size=8))
        # # Add horizontal lines for peaks
        # for peak_value in peak_values:
        #     fig.add_hline(y=peak_value, line=dict(color='red', dash='dash'), 
        #                  annotation_text=f'Peak: {peak_value:.2f}')
        
        # # Add horizontal lines for troughs  
        # for trough_value in trough_values:
        #     fig.add_hline(y=trough_value, line=dict(color='green', dash='dash'),
        #                  annotation_text=f'Trough: {trough_value:.2f}')
        return fig
    if return_df:
        # Add true values at peak and trough locations
        price_df['peaks'] = False
        price_df['troughs'] = False
        price_df.loc[peak_dates, 'peaks'] = True
        price_df.loc[trough_dates, 'troughs'] = True
        return price_df
    else:
        return price_df[col]

