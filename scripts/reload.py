
if __name__ == "__main__":
    """
    Use for quick reload if kernel crashes.
    """
    import pickle

    with open(r"E:\Market Research\Dataset\daily_after_close_study\symbols.pkl", "rb") as f:
        symbols = pickle.load(f)

    with open(r"E:\Market Research\Dataset\daily_after_close_study\sec.pkl", "rb") as f:
        sec = pickle.load(f)

    with open(r"E:\Market Research\Dataset\daily_after_close_study\ind.pkl", "rb") as f:
        ind = pickle.load(f)

    with open(r"E:\Market Research\Dataset\daily_after_close_study\sp500.pkl", "rb") as f:
        sp500 = pickle.load(f)

    with open(r"E:\Market Research\Dataset\daily_after_close_study\mdy.pkl", "rb") as f:
        mdy = pickle.load(f)

    with open(r"E:\Market Research\Dataset\daily_after_close_study\iwm.pkl", "rb") as f:
        iwm = pickle.load(f)

    with open(r"E:\Market Research\Dataset\daily_after_close_study\etfs.pkl", "rb") as f:
        etfs = pickle.load(f)

    with open(r"E:\Market Research\Dataset\daily_after_close_study\stock_stats.pkl", "rb") as f:
        stock_stats = pickle.load(f)

