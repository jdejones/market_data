import requests
import json
import os
import logging
from datetime import datetime, timedelta
from market_data.api_keys import seeking_alpha_api_key
from market_data import fu, pd, bs
import sys



fa = fu.sa_fundamental_data
meta_data = None
key_data = None
summary_dict = {}
ratings_dict = None
earnings_dict = {}
earnings_revisions_dict = {}
earnings_previous_week = json.load(open(r"E:\Market Research\Dataset\seeking_alpha_api\seeking_alpha_earnings_previous_week.txt"))
analyst_price_targets_dict = None
filters = json.load(open(r"E:\Market Research\Dataset\seeking_alpha_api\screeners_filters.txt"))
analyst_ratings_dict = None
sec_ind_ids = json.load(open(r"E:\Market Research\Dataset\seeking_alpha_api\sector_industry_ids.txt"))

def meta_data_load():
    global meta_data
    with open(r"E:\Market Research\Dataset\seeking_alpha_api\seeking_alpha_meta_data.txt", "r") as f:
        meta_data = json.load(f)
    meta_data = meta_data  

def key_data_load():
    global key_data
    with open(r"E:\Market Research\Dataset\seeking_alpha_api\get_key_data.txt", "r") as f:
        key_data = json.load(f)
    key_data = key_data

def summary_data_load():
    global summary_dict
    with open(r"E:\Market Research\Dataset\seeking_alpha_api\get_summary.txt") as f:
        summary_data = json.load(f)
    summary_dict = summary_data

def get_ratings_load():
    global ratings_dict
    with open(r"E:\Market Research\Dataset\seeking_alpha_api\get_ratings.txt") as f:
        ratings_data = json.load(f)
    ratings_dict = ratings_data

def earnings_load():
    global earnings_dict
    global earnings_revisions_dict
    # with open(r"E:\Market Research\Dataset\seeking_alpha_api\seeking_alpha_earnings.txt", "r") as f:
    #     earnings_dict = json.load(f)
    available_earnings_folders = [item.name for item in os.scandir(r"E:\Market Research\Dataset\seeking_alpha_api\earnings_dataset\estimates")]
    available_revisions_folders = [item.name for item in os.scandir(r"E:\Market Research\Dataset\seeking_alpha_api\earnings_dataset\revisions")]
    for sym in available_earnings_folders:
        with open(f"E:\Market Research\Dataset\seeking_alpha_api\earnings_dataset\estimates\{sym}\earnings_estimates.txt") as f:
            earnings_dict[sym] = json.load(f)
    for sym in available_revisions_folders:
        with open(f"E:\Market Research\Dataset\seeking_alpha_api\earnings_dataset\\revisions\{sym}\earnings_revisions.txt") as f:
            earnings_revisions_dict[sym] = json.load(f)

def analyst_price_targets_load():
    global analyst_price_targets_dict
    with open(r"E:\Market Research\Dataset\seeking_alpha_api\analyst_price_targets.txt") as f:
        analyst_price_targets_dict = json.load(f)

def analyst_ratings_load():
    global analyst_ratings_dict
    with open(r"E:\Market Research\Dataset\seeking_alpha_api\analyst_ratings.txt") as f:
        analyst_ratings_dict = json.load(f)

def load_all():
    meta_data_load()
    key_data_load()
    summary_data_load()
    get_ratings_load()
    earnings_load()
    analyst_price_targets_load()
    analyst_ratings_load()
load_all()

def meta_data_save(symbols, replacement=False):
    requested_symbols = symbols
    if meta_data == None:
        meta_data_load()
    if replacement == False:
        for sym in symbols:
            if sym in meta_data.keys():
                requested_symbols.remove(sym)
    for sym in requested_symbols:
        try:
            url = "https://seeking-alpha.p.rapidapi.com/symbols/get-meta-data"
            querystring = {"symbol":sym}
            headers = {
                "X-RapidAPI-Key": f"{seeking_alpha_api_key}",
                "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
            }
            response = requests.request("GET", url, headers=headers, params=querystring)
            meta_data[sym] = response.json()
        except Exception as e:
            with open(r"E:\Market Research\Dataset\seeking_alpha_api\seeking_alpha_meta_data.txt", "w") as f:
                f.write(json.dumps(meta_data, indent=4, sort_keys=True))
            #TODO print(sym, e) update error handling
    with open(r"E:\Market Research\Dataset\seeking_alpha_api\seeking_alpha_meta_data.txt", "w") as f:
        f.write(json.dumps(meta_data, indent=4, sort_keys=True))

  

def earnings(ticker_ids, period_type='quarterly', relative_periods="-23,-22,-21,-20,-19,-18,-17,-16,-15,-14,-13,-12,-11,-10,-9,-8,-7,-6,-5,-4,-3,-2,-1,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23", data_items='all'): #"-23,-22,-21,-20,-19,-18,-17,-16,-15,-14,-13,-12,-11,-10,-9,-8,-7,-6,-5,-4,-3,-2,-1,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23"
    """Accepts a maximum of 4 symbols. If more are submitted an error will be raised.
    ticker_ids STRING e.g.:1742,146
        REQUIRED
        The value of id fields returned in …/symbols/get-meta-data endpoint. Separating by comma to query multiple tickers at once, ex : 1742,146
                
    period_type STRING  e.g.: quarterly
    OPTIONAL
    One of the followings : quarterly|annual

    relative_periods STRING e.g.:-3,-2,-1,0,1,2,3
    OPTIONAL
    Valid range -23,…,-2,-1,0,1,2,..,23

    estimates_data_items STRING e.g.: eps_gaap_actual,eps_gaap_consensus_mean,eps_normalized_actual,eps_normalized_consensus_mean,revenue_actual,revenue_consensus_mean
    OPTIONAL
    One of the followings : eps_gaap_actual,eps_gaap_consensus_mean,eps_normalized_actual,eps_normalized_consensus_mean,revenue_actual,revenue_consensus_mean . Separated by comma for multiple options

    revisions_data_items STRING e.g.:eps_normalized_actual,eps_normalized_consensus_mean,revenue_consensus_mean
    OPTIONAL
    One of the followings : eps_normalized_actual,eps_normalized_consensus_mean,revenue_consensus_mean . Separated by comma for multiple options
    
    returns a json object"""
    if meta_data == None:
        with open(r"E:\Market Research\Dataset\seeking_alpha_api\seeking_alpha_meta_data.txt", "r") as f:
            meta_data = json.load(f)
    if earnings_dict == None:
        earnings_load()
    symbol_id = {}
    errors_meta_data = {}
    errors_results = {}
    logger = logging.getLogger('earnings_logger')
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        f_handler = logging.FileHandler(r"E:\Programming\Python\Programs\Error_Logs\seeking_alpha_api\earnings\error_log.txt")        
        c_handler = logging.StreamHandler()
        f_handler.setLevel(logging.INFO)
        c_handler.setLevel(logging.WARNING)
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)
        c_handler.setFormatter(c_format)
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)
    for sym in ticker_ids:
        try:
            symbol_id[sym] = meta_data[sym]['data']['id']
        except Exception as e:
            errors_meta_data[sym] = e
            logger.info('%s meta data error: %s', sym, str(e))
            continue
    url = "https://seeking-alpha.p.rapidapi.com/symbols/get-earnings"
    querystring = {"ticker_ids":','.join(list(symbol_id.values())),
                    "period_type":period_type,
                    "relative_periods":relative_periods,
                    "estimates_data_items":"eps_gaap_actual,eps_gaap_consensus_mean,eps_normalized_actual,eps_normalized_consensus_mean,revenue_actual,revenue_consensus_mean",
                    "revisions_data_items":"eps_normalized_actual,eps_normalized_consensus_mean,revenue_consensus_mean"
                    }
    headers = {
        "X-RapidAPI-Key": f"{seeking_alpha_api_key}",
        "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
    }
    if len(relative_periods.split(',')) > 7:
        response = requests.request("GET", url, headers=headers, params=querystring).json()#.to_json() was added after I edited the export part of the function.
    else:
        response = requests.request("GET", url, headers=headers, params=querystring)
    # with open(r"E:\Market Research\Dataset\seeking_alpha_api\seeking_alpha_earnings.txt", "r") as f:
    #     earnings_dict = json.loads(f)
    #earnings_dict = {}
    symbol_id_keys = list(symbol_id.keys())
    symbol_id_values = list(symbol_id.values())
    if len(relative_periods.split(',')) > 7:
        for k in response.keys():
            for sym in response[k].keys():
                if symbol_id_keys[symbol_id_values.index(sym)] in [item.name for item in os.scandir(f"E:\Market Research\Dataset\seeking_alpha_api\earnings_dataset\{k}")]:
                    pass
                else:
                    #If preexisting folder not present create file.
                    os.makedirs(f"E:\Market Research\Dataset\seeking_alpha_api\earnings_dataset\{k}\{symbol_id_keys[symbol_id_values.index(sym)]}")
                # baz = pd.DataFrame()
                # for type in response[k][sym].keys():
                #     for report in response[k][sym][type].keys():
                #         baz = pd.concat([baz, pd.DataFrame.from_dict(response[k][sym][type][report][0])])
                #     baz.to_csv(f"E:\Market Research\Dataset\seeking_alpha_api\earnings_dataset\{k}\{symbol_id_keys[symbol_id_values.index(sym)]}\{type}")
                with open(f"E:\Market Research\Dataset\seeking_alpha_api\earnings_dataset\{k}\{symbol_id_keys[symbol_id_values.index(sym)]}\earnings_{k}.txt", "w") as f:
                    f.write(json.dumps(response[k][sym]))
    else:
        for sym in ticker_ids:
            try:
                earnings_dict[sym] = {'revisions': response.json()['revisions'][symbol_id[sym]]}
                earnings_dict[sym].update({'estimates': response.json()['estimates'][symbol_id[sym]]})
            except Exception as e:
                errors_results[sym] = e
                logger.info('%s results error: %s', sym, str(e))
                continue
        if (len(errors_meta_data) > 0) or (len(errors_results) > 0):
            logger.warning(str(len(errors_meta_data.keys()) + len(errors_results.keys())) + ' exceptions were raised. Check log at "E:\Programming\Python\Programs\Error_Logs\seeking_alpha_api\earnings\error_log.txt"')
        with open(r"E:\Market Research\Dataset\seeking_alpha_api\seeking_alpha_earnings.txt", "w") as f:
            f.write(json.dumps(earnings_dict, indent=4, sort_keys=True))



def key_data_save(symbols, replacement=False):
    #! This is returning a json decode error. This endpoint has been deprecated and may not be
    #! functional.
    """Calls the get-key-data endpoint. It will primarily be used to download and saved company
    descriptions and shares outstanding.

    Args:
        symbols (list): list containing symbols to call.

    Returns:
        None: returns nothing. This function is used to saved data to a text file.
    """
    requested_symbols = symbols
    if key_data == None:
        key_data_load()
    if replacement == False:
        for sym in symbols:
            if sym in key_data.keys():
                requested_symbols.remove(sym)
    errors = {}
    logger = logging.getLogger('key_data_save_logger')
    if not logger.handlers:
        f_handler = logging.FileHandler(r"E:\Programming\Python\Programs\Error_Logs\seeking_alpha_api\key_data_save\error_log.txt")        
        c_handler = logging.StreamHandler()
        f_handler.setLevel(logging.INFO)
        c_handler.setLevel(logging.WARNING)
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)
        c_handler.setFormatter(c_format)
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)
    for sym in requested_symbols:
        try:
            url = "https://seeking-alpha.p.rapidapi.com/symbols/get-key-data"
            querystring = {"symbol": sym}
            headers = {
                "X-RapidAPI-Key": f"{seeking_alpha_api_key}",
                "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
            }
            response = requests.request("GET", url, headers=headers, params=querystring)
            key_data[sym] = response.json()
        except Exception as e:
            errors[sym] = e
            logger.info(f'{sym} : {str(e)}')
            continue
    if len(errors) > 0:
        logger.warning(str(len(errors.keys())) + ' exceptions were raised. Check log at "E:\Programming\Python\Programs\Error_Logs\seeking_alpha_api\key_data_save\error_log.txt"')
    with open(r"E:\Market Research\Dataset\seeking_alpha_api\get_key_data.txt", "r") as f:
        saved_key_data = json.load(f)
    new_dict = {**saved_key_data, **key_data}
    with open(r"E:\Market Research\Dataset\seeking_alpha_api\get_key_data.txt", "w") as f:
        f.write(json.dumps(new_dict, indent=4, sort_keys=True))



#This isn't working and I'm wasting api calls. I'll have to come back to it.
def summary_saved(symbols, replacement=True):
    """Calls the get-summary endpoint. The primary use will be convenience of accessing the
    data it contains. 
    Accepts multiple symbols

    Args:
        symbols (list): List object containing the symbols to acquire the data from
        replacement (bool, optional): _description_. Defaults to False.

    Returns:
        _type_: _description_
    """
    import requests
    requested_symbols = symbols
    if summary_dict == None:
        summary_data_load()
    if replacement == False:
        for sym in symbols:
            if sym in summary_dict.keys():
                requested_symbols.remove(sym)
    url = "https://seeking-alpha.p.rapidapi.com/symbols/get-summary"
    errors = {}
    logger = logging.getLogger('summary_saved_logger')
    if not logger.handlers:
        f_handler = logging.FileHandler(r"E:\Programming\Python\Programs\Error_Logs\seeking_alpha_api\summary_saved\error_log.txt")        
        c_handler = logging.StreamHandler()
        f_handler.setLevel(logging.INFO)
        c_handler.setLevel(logging.WARNING)
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)
        c_handler.setFormatter(c_format)
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)
    for group in (requested_symbols[pos:pos+4] for pos in range(0, len(requested_symbols), 4)):
        try:
            querystring = {"symbols": ','.join(group)}
            headers = {
                "X-RapidAPI-Key": f"{seeking_alpha_api_key}",
                "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
            }
            response = requests.request("GET", url, headers=headers, params=querystring)
            group_dict = response.json()
            for item in range(0, len(group_dict['data'])):
                summary_dict[group_dict['data'][item]['id']] = group_dict['data'][item]['attributes']
        except Exception as e:
            errors[sym] = e
            logger.info(f'{sym} : {str(e)}')
            continue
    if len(errors) > 0:
        logger.warning(str(len(errors.keys())) + ' exceptions were raised. Check log at "E:\Programming\Python\Programs\Error_Logs\seeking_alpha_api\summary_saved\error_log.txt"')
    with open(r"E:\Market Research\Dataset\seeking_alpha_api\get_summary.txt", "r") as f:
        saved_summary_data = json.load(f)
    new_dict = {**saved_summary_data, **summary_dict}
    with open(r"E:\Market Research\Dataset\seeking_alpha_api\get_summary.txt", "w") as f:
        f.write(json.dumps(new_dict, indent=4, sort_keys=True))



#This isn't removing all items from requested_symbols. It's removing every other item.
def get_ratings(symbols, replacement=False, replace_old=None):
    # requested_symbols = symbols
    if ratings_dict == None:
        get_ratings_load()
    if replacement == False:
        requested_symbols = [sym for sym in symbols if sym not in ratings_dict.keys()]
        # for sym in symbols:
        #     print('loop start', sym)
        #     if sym in ratings_dict.keys():
        #         print('loop if', sym)
        #         requested_symbols.remove(sym)
        #         print(requested_symbols)
    elif replacement == True:
        if replace_old == True:
            # requested_symbols = [sym for sym in symbols if abs((datetime.strptime(ratings_dict[sym]['data'][0]['attributes']['asDate'], '%Y-%m-%d') - datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')).days) > 7]
            requested_symbols = [item for item in symbols]
            for sym in symbols:
                try:
                    if abs((datetime.strptime(ratings_dict[sym]['data'][0]['attributes']['asDate'], '%Y-%m-%d') - datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')).days) < timedelta(days=7).days:
                        requested_symbols.remove(sym)
                except KeyError as ke:
                    continue
        elif replace_old == False:
            requested_symbols = [item for item in symbols]
            for sym in symbols:
                try:
                    if abs((datetime.strptime(ratings_dict[sym]['data'][0]['attributes']['asDate'], '%Y-%m-%d') - datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')).days) >= timedelta(days=7).days:
                        requested_symbols.remove(sym)
                except KeyError as ke:
                    continue
        else:
            if replace_old == None:
                raise TypeError('replace old must be boolean')
            else:
                raise ValueError('replace old must be boolean')
    elif replacement == 'All':
        pass
    for sym in requested_symbols:
        try:
            url = "https://seeking-alpha.p.rapidapi.com/symbols/get-ratings"
            querystring = {"symbol":sym}
            headers = {
                "X-RapidAPI-Key": f"{seeking_alpha_api_key}",
                "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
            }
            response = requests.request("GET", url, headers=headers, params=querystring)
            ratings_dict[sym] = response.json()
        except:
            with open(r"E:\Market Research\Dataset\seeking_alpha_api\get_ratings.txt", "w") as f:
                f.write(json.dumps(ratings_dict, indent=4, sort_keys=True))
            return print(sym)            
    with open(r"E:\Market Research\Dataset\seeking_alpha_api\get_ratings.txt", "w") as f:
        f.write(json.dumps(ratings_dict, indent=4, sort_keys=True))
    


def quant_rating(symbols):
    sym_ratings = {}
    rating_needed = []
    if ratings_dict == None:
        get_ratings_load()
    for sym in symbols:
        try:
            sym_ratings[sym] = round(ratings_dict[sym]['data'][0]['attributes']['ratings']['quantRating'], 2)
        except KeyError:
            rating_needed.append(sym)
            continue
    if len(rating_needed) > 0:
        get_ratings(rating_needed)
        get_ratings_load()
        for sym in rating_needed:
            try:
                sym_ratings[sym] = round(ratings_dict[sym]['data'][0]['attributes']['ratings']['quantRating'], 2)
            except KeyError as ke:
                #TODO print(ke, ':', sym, 'rating not found') update error handling
                continue
    return sym_ratings

def earnings_call_transcripts(symbol, return_string=False):
    url = "https://seeking-alpha.p.rapidapi.com/transcripts/v2/list"

    querystring = {"id":symbol,"size":"20","number":"1"}

    headers = {
        "X-RapidAPI-Key": f"{seeking_alpha_api_key}",
        "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
    }

    response_id = requests.request("GET", url, headers=headers, params=querystring).json()
    symbol_id = response_id['data'][0]['id']
    url = "https://seeking-alpha.p.rapidapi.com/transcripts/v2/get-details"
    querystring = {"id":symbol_id}
    headers = {
        "X-RapidAPI-Key": f"{seeking_alpha_api_key}",
        "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    if return_string == True:
        transcript = bs(response.json()['data']['attributes']['content']).text
        return {'released': response.json()['data']['attributes']['publishOn'], 'transcript': ' '.join([item for item in transcript if item != None])}
    elif return_string == False:
        transcript = [item.string for item in bs(response.json()['data']['attributes']['content']).findAll()]
        return {'released': response.json()['data']['attributes']['publishOn'], 'transcript': ' '.join([item for item in transcript if item != None])}

def analyst_price_targets(symbols, save=True):
    """Saves data from seeking alpha api containing analyst price targets.
    Accepts a maximum of 4 symbols.

    Args:
        symbols (List): Symbols to obtain analyst price targets for.
        save (bool, optional): Save the data to file. Defaults to True.
    """
    if meta_data == None:
        meta_data_load()
    if analyst_price_targets_dict == None:
        analyst_price_targets_load()
    requested_symbols = []
    errors = {}
    logger = logging.getLogger('analyst_price_targets_logger')
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        f_handler = logging.FileHandler(r"E:\Programming\Python\Programs\Error_Logs\seeking_alpha_api\analyst_price_targets\error_log.txt")        
        c_handler = logging.StreamHandler()
        f_handler.setLevel(logging.INFO)
        c_handler.setLevel(logging.WARNING)
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)
        c_handler.setFormatter(c_format)
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)
    for sym in symbols:
        try:
            url = "https://seeking-alpha.p.rapidapi.com/symbols/get-analyst-price-target"
            querystring = {"ticker_ids": meta_data[sym]['data']['id'], "return_window":"1","group_by_month":"false"}
            headers = {
                "X-RapidAPI-Key": f"{seeking_alpha_api_key}",
                "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
            }
            response = requests.request("GET", url, headers=headers, params=querystring)
            analyst_price_targets_dict[sym] = response.json()
        except Exception as e:
            errors[sym] = e
            logger.info('%s : %s', sym, str(e))
            continue
    if len(errors) > 0:
        logger.warning(str(len(errors.keys())) + ' exceptions were raised. Check log at "E:\Programming\Python\Programs\Error_Logs\seeking_alpha_api\analyst_price_targets\error_log.txt"')
    if save == True:
        with open(r"E:\Market Research\Dataset\seeking_alpha_api\analyst_price_targets.txt", "w") as f:
            f.write(json.dumps(analyst_price_targets_dict, indent=4, sort_keys=True))



#I don't think this is working as expected.
def get_analyst_ratings(symbols, replacement=False, replace_old=None):
    if analyst_ratings_dict == None:
        analyst_ratings_load()
    if replacement == False:
        requested_symbols = [sym for sym in symbols if sym not in analyst_ratings_dict.keys()]
        # for sym in symbols:
        #     print('loop start', sym)
        #     if sym in ratings_dict.keys():
        #         print('loop if', sym)
        #         requested_symbols.remove(sym)
        #         print(requested_symbols)
    elif replacement == True:
        if replace_old == True:
            # requested_symbols = [sym for sym in symbols if abs((datetime.strptime(ratings_dict[sym]['data'][0]['attributes']['asDate'], '%Y-%m-%d') - datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')).days) > 7]
            requested_symbols = [item for item in symbols]
            for sym in symbols:
                try:
                    if abs((datetime.strptime(analyst_ratings_dict[sym]['data'][0]['attributes']['asDate'], '%Y-%m-%d') - datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')).days) < timedelta(days=7).days:
                        requested_symbols.remove(sym)
                except KeyError:
                    continue
        elif replace_old == False:
            requested_symbols = [item for item in symbols]
            for sym in symbols:
                try:
                    if abs((datetime.strptime(analyst_ratings_dict[sym]['data'][0]['attributes']['asDate'], '%Y-%m-%d') - datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')).days) >= timedelta(days=7).days:
                        requested_symbols.remove(sym)
                except KeyError as ke:
                    continue
        else:
            if replace_old == None:
                raise TypeError('replace old must be boolean')
            else:
                raise ValueError('replace old must be boolean')
    elif replacement == 'All':
        requested_symbols = [item for item in symbols]
    for sym in requested_symbols:
        try:
            url = "https://seeking-alpha.p.rapidapi.com/symbols/get-analyst-ratings"
            querystring = {"symbol":sym}
            headers = {
                "X-RapidAPI-Key": f"{seeking_alpha_api_key}",
                "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
            }
            response = requests.request("GET", url, headers=headers, params=querystring)
            analyst_ratings_dict[sym] = response.json()
        except:
            with open(r"E:\Market Research\Dataset\seeking_alpha_api\analyst_ratings.txt", "w") as f:
                f.write(json.dumps(analyst_ratings_dict, indent=4, sort_keys=True))
            return print(sym)
    with open(r"E:\Market Research\Dataset\seeking_alpha_api\analyst_ratings.txt", "w") as f:
        f.write(json.dumps(analyst_ratings_dict, indent=4, sort_keys=True))




