#!/usr/bin/env python3
import datetime
import pickle
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import multiprocessing

def worker(args):
    """
    Build a single 0/1 time‐series for one (pair, intervals).
    """
    pair, intervals, start_date, end_date = args
    idx = pd.date_range(start_date, end_date)
    series = pd.Series(0, index=idx, name=f'{pair[0]}_{pair[1]}')
    for start, end in intervals:
        series.loc[start:end] = 1
    return series

def main():
    # 1) Load or define your corr dict here
    #    corr: Dict[Tuple[str,str], List[Tuple[Timestamp, Timestamp]]]
    with open(r"E:\Market Research\Dataset\from_interactive_interpreter_temp\intermittent_correlations.pkl", "rb") as f:
        corr = pickle.load(f)
    
    # 2) Sanity check
    if not corr:
        print("⚠️  No correlations found in corr.pkl – nothing to process. Exiting.")
        return
    
    print(f"🚀  Submitting {len(corr)} jobs to the process pool…")
    
    # common date‐range for all workers
    start_date = '2024-01-01'
    end_date   = datetime.datetime.today().strftime('%Y-%m-%d')
    
    # prepare arguments for each worker
    tasks = [
      (pair, intervals, start_date, end_date)
      for pair, intervals in corr.items()
    ]
    
    series_list = []
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(worker, args) for args in tasks]
        for future in tqdm(as_completed(futures),
                           total=len(futures),
                           desc="Processing pairs"):
            series_list.append(future.result())
    
    # 3) Final safety check
    if not series_list:
        raise RuntimeError("No output series were generated – please check your corr data.")
    
    # concatenate into one DataFrame
    df = pd.concat(series_list, axis=1)
    df.to_csv('scripts\intermittent_correlation_matrix.csv')
    print("✅  Saved correlation_matrix.csv")

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()