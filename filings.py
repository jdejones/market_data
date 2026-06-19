from sec_api import QueryApi

from .api_keys import sec_api_key


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