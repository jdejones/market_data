from sec_api import QueryApi, XbrlApi
import pandas as pd

try:
    from .api_keys import sec_api_key
except ImportError:
    from market_data.api_keys import sec_api_key
# from api_keys import sec_api_key

def filing_urls(symbol: str, form_type: str=None, filing_date: str=None, no_filings: int=5, query_size: int=10) -> list:
    """
    Return SEC filing HTML URLs from sec-api.io's Query API.

    This helper is the filing-discovery layer used by
    `Xbrl_Tags_Manager.find_statements_tags_current()`. It queries sec-api.io
    for filings matching a ticker and optional form type, then returns the
    `linkToHtml` values that can be passed to `XbrlApi.xbrl_to_json()`.

    Parameters
    ----------
    symbol:
        Ticker to query. The XBRL manager passes uppercase tickers here even
        though symbol-specific database tables are stored lowercase.
    form_type:
        Optional SEC form type such as `"10-K"` or `"10-Q"`. When provided, the
        query is restricted to that form type.
    filing_date:
        Optional date filter appended to the sec-api query as `filedAt:<date>`.
        Use the date syntax expected by sec-api.io.
    no_filings:
        Maximum number of filing URLs to return. Pass `None` to return every
        filing URL included in the query response.
    query_size:
        Number of records requested from sec-api.io. Increase this if the query
        does not return enough filings for the requested form type.

    Examples
    --------
    ```python
    filing_urls("AAPL", "10-K", no_filings=1)
    filing_urls("MSFT", "10-Q", no_filings=3)
    ```

    Returns
    -------
    list
        Filing HTML URLs suitable for `XbrlApi.xbrl_to_json(htm_url=url)`.
    """
    query_start = 0
    query_size=query_size
    queryApi = QueryApi(sec_api_key)

    def internal_query(query_start=query_start, query_size=query_size):     
        if form_type == None:
            query = {
                "query": { "query_string": { 
                    "query": "ticker:"+ symbol,
                } },
                "from": str(query_start),
                "size": str(query_size),
                "sort": [{ "filedAt": { "order": "desc" } }]
                }
        else:
            query = {
                "query": { "query_string": { 
                    "query": "ticker:"+ symbol + " AND formType:\"" +  form_type + "\"",
                } },
                "from": str(query_start),
                "size": str(query_size),
                "sort": [{ "filedAt": { "order": "desc" } }]
                }
        if filing_date == None:
            pass
        else:
            query = {
                "query": {"query_string": {
                    "query": query["query"]["query_string"]["query"] + f" AND filedAt:{filing_date}",
                }},
                "from": str(query_start),
                "size": str(query_size),
                "sort": [{ "filedAt": { "order": "desc" } }]
                }
        response = queryApi.get_filings(query)
        return response
    que =internal_query()
    form_filings = [
        item['linkToHtml']
        for item in que.get('filings', [])
        if form_type == None or item.get('formType') == form_type
    ]

    if no_filings is None:
        return form_filings

    return form_filings[:no_filings]

def single_fundamental(
    symbol: str,
    form_type: str,
    fundamental: str,
    plot: bool=False,
    xbrl_tags_manager=None,
) -> pd.DataFrame:
    #TODO add function that will identify the path to the specified fundamental in the xbrl_tags_dict. This may need to be an
    #TODO algorithm that could be in a separate class so that it's easier to repurpose.
    """
    Return one normalized fundamental time series for a symbol.

    This function preserves the old lookup behavior while using the new storage
    pattern established in `xbrl_tags_manager.py`. Symbol-specific XBRL tags are
    read from an `Xbrl_Tags_Manager` instance, whose `xbrl_tags_saved` payload is
    loaded from the `xbrl_symbol_tags` database.

    Parameters
    ----------
    symbol:
        Stock symbol. Uppercase and lowercase are both accepted for tag lookup;
        SEC filing queries are sent uppercase.
    form_type:
        SEC form type to query, such as `"10-K"`, `"10-Q"`, or `"20-F"`.
    fundamental:
        Normalized fundamental name to retrieve, such as `"Revenue"`,
        `"EPSDiluted"`, or `"CashFromOperations"`.
    plot:
        If True, plot the returned values in chronological order.
    xbrl_tags_manager:
        Optional existing `Xbrl_Tags_Manager`. Pass this when making repeated
        calls so the database-backed tag maps are not reloaded each time.

    Examples
    --------
    ```python
    df = single_fundamental("AAPL", "10-K", "Revenue")

    manager = Xbrl_Tags_Manager(["AAPL", "MSFT"])
    df = single_fundamental("MSFT", "10-K", "EPSDiluted", xbrl_tags_manager=manager)
    ```

    Returns
    -------
    pd.DataFrame
        Normalized XBRL fact rows for the requested fundamental.
    """
    if xbrl_tags_manager is None:
        from .xbrl_tags_manager import Xbrl_Tags_Manager

        xbrl_tags_manager = Xbrl_Tags_Manager([symbol])

    xbrl_tags_saved = xbrl_tags_manager.xbrl_tags_saved
    filings_10k = filing_urls(symbol.upper(), form_type)
    xbrl_api = XbrlApi(sec_api_key)
    df = pd.DataFrame()

    def tag_finder(sym, target):
        for k,v in xbrl_tags_saved[sym].items():
            if type(v) == dict:
                for k2, v2 in v.items():
                    if type(v2) == list:
                        if k2 == target:
                            return xbrl_tags_saved[sym][k][k2]
                    elif type(v2) != list:
                        raise ValueError('Expected list in second level of nested dictionary!')
            elif type(v) != dict:
                raise TypeError('Type should be dictionary. Review xbrl_symbol_tags database payload.')
        raise KeyError(f'Symbol not in dictionary/file.-tag_finder-fundamental:{target} ')

    tag_target = tag_finder(symbol, fundamental)

    def statement_finder(sym, target):
        for k,v in xbrl_tags_saved[sym].items():
            if type(v) == dict:
                for k2, v2 in v.items():
                    if type(v2) == list:
                        if k2 == target:
                            return xbrl_tags_saved[sym][k]['statement tags'][0]#! The return value was k. I changed it resolve an error in the subsequent line containing the df variable.
                    elif type(v2) != list:
                        raise ValueError('Expected list in second level of nested dictionary!')
            elif type(v) != dict:
                raise TypeError('Type should be dictionary. Review xbrl_symbol_tags database payload.')
        raise KeyError(f'Symbol not in dictionary/file.-statement_finder-fundamental{target} ')

    target_statement = statement_finder(symbol, fundamental)

    #if/elif for handling symbols that have been found to have multiple xbrl tags for a fundamental.
    if len(tag_target) == 1:
        for sub in filings_10k:
            try:
                df = pd.concat([df, pd.json_normalize(xbrl_api.xbrl_to_json(htm_url=sub)[target_statement][''.join(tag_target)])[:3]])#!The error is in the tag_target key in this line.
            except KeyError:
                continue
    elif len(tag_target) > 1:
        for sub in filings_10k:
            for tag in tag_target:
                try:
                    df = pd.concat([df, pd.json_normalize(xbrl_api.xbrl_to_json(htm_url=sub)[target_statement][''.join(tag)])[:3]])
                except:
                    continue
    try:
        df = df.drop_duplicates()
    except TypeError:
        df = df[['decimals', 'unitRef', 'value', 'period.startDate', 'period.endDate']].drop_duplicates()
    if 'period.endDate' in df.columns:
            df.sort_values('period.endDate', ascending=False)#!Not all dataframes will contain period.endDate.
            df.index = df['period.endDate']
    elif 'period.instant' in df.columns:
            df.sort_values('period.instant', ascending=False)
            df.index = df['period.instant']
    if plot == False:
            return df
    elif plot == True:
            df['value'][::-1].astype(float).plot(rot=20, ylabel=fundamental)

