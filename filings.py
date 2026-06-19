from sec_api import QueryApi

from .api_keys import sec_api_key


def filing_urls(symbol: str, form_type: str=None, filing_date: str=None, no_filings: int=5, query_size: int=10) -> list:
    """Returns a list of links to the sec filing specified by form type. The internal_while_loop()
    function is returning IndexError: 'list index out of range' when it's called multiple times. So,
    for now I'll need to adjust the query_size when the function is called as opposed to automating
    it through recursion, which is what this function was intended to do."""
    form_10k_filings = []
    query_start = 0
    query_size=query_size
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
        queryApi = QueryApi(sec_api_key)
        response = queryApi.get_filings(query)
        return response
    que =internal_query()
    n=0
    filings = [item['linkToHtml'] for item in que['filings']]
    if (len(filings) < no_filings) and (type(no_filings)!=None):
        no_filings = len(filings)
    while len(form_10k_filings) < no_filings:
        try:
            if form_type == None:
                form_10k_filings.append(que['filings'][n]['linkToHtml'])
            else:
                if que['filings'][n]['formType'] == form_type:
                    form_10k_filings.append(que['filings'][n]['linkToHtml'])
            n+=1
        except IndexError as ie:
            if len(form_10k_filings) > 0:
                break
            else:
                raise IndexError(ie)
    if len(form_10k_filings) >= no_filings:
        return form_10k_filings
    else:
        raise Exception('Something went wrong. Check number of filings returned from query')