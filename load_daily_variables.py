BASE = r"E:\Market Research\Dataset\daily_after_close_study"

def _load_one(name: str, _input: dict):
    """Worker: load one object and store it in the shared dict."""
    path = fr"{BASE}\{name}.pkl.gz"
    with gzip.open(path, "rb") as f:          # rb + pickle.load for reading
        _input[name] = pickle.load(f)

def load_all():
    """Load all saved objects concurrently; return a dict of name -> object."""
    names = [
        "symbols",
        "sec",
        "ind",
        "sp500",
        "mdy",
        "iwm",
        "etfs",
        "stock_stats",
        "ev",
        "all_returns",
        "sector_close_vwap_ratio",
        "industry_close_vwap_ratio",
        "ep_curdur",
        "ep_rr",
        "rel_stren",
        "prev_perf_since_earnings",
        "perf_since_earnings",
        "days_elevated_rvol",
        "days_range_expansion",
        "results_finvizsearch",
        "tsc",
        "tsc_sec",
        "tsc_ind",
        "qplus1",
        "qplus4",
        "interest_list_long",
    ]

    loaded = {}
    threads = []

    for name in names:
        t = threading.Thread(target=_load_one, args=(name, loaded), daemon=False)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return loaded

if __name__ == "__main__":
    import sys
    sys.path.insert(0, r"C:\Users\jdejo\Market_Data_Processing")    
    from market_data import threading, gzip, pickle
    
    loaded = load_all()


    # Optionally unpack into individual variables
    symbols = loaded["symbols"]
    sec = loaded["sec"]
    ind = loaded["ind"]
    sp500 = loaded["sp500"]
    mdy = loaded["mdy"]
    iwm = loaded["iwm"]
    etfs = loaded["etfs"]
    stock_stats = loaded["stock_stats"]
    ev = loaded["ev"]
    all_returns = loaded["all_returns"]
    sector_close_vwap_ratio = loaded["sector_close_vwap_ratio"]
    industry_close_vwap_ratio = loaded["industry_close_vwap_ratio"]
    ep_curdur = loaded["ep_curdur"]
    ep_rr = loaded["ep_rr"]
    rel_stren = loaded["rel_stren"]
    prev_perf_since_earnings = loaded["prev_perf_since_earnings"]
    perf_since_earnings = loaded["perf_since_earnings"]
    days_elevated_rvol = loaded["days_elevated_rvol"]
    days_range_expansion = loaded["days_range_expansion"]
    results_finvizsearch = loaded["results_finvizsearch"]
    tsc = loaded["tsc"]
    tsc_sec = loaded["tsc_sec"]
    tsc_ind = loaded["tsc_ind"]
    qplus1 = loaded["qplus1"]
    qplus4 = loaded["qplus4"]
    interest_list_long = loaded["interest_list_long"]