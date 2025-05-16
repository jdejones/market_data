from market_data import requests
from functools import wraps
from market_data import time

def retry_on_read_timeout(max_retries=3):
    """
    A decorator that retries a function call if it raises a ReadTimeout error.
    
    :param max_retries: The number of times to retry (default is 2).
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts <= max_retries:
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.ReadTimeout as e:
                    attempts += 1
                    print(f"ReadTimeout encountered. Retrying {attempts}/{max_retries}...")
                    if attempts > max_retries:
                        print("Max retries reached. Raising the exception.")
                        raise
        return wrapper
    return decorator


def retry_on_missing_results(max_retries: int = 3, backoff: float = 1.0):
    """
    Decorator to retry a function if it raises KeyError('results').
    max_retries: total attempts (including the first).
    backoff: seconds to sleep between retries.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except KeyError as e:
                    # only retry on missing 'results' key
                    if e.args and e.args[0] == 'results':
                        last_exc = e
                        if attempt < max_retries:
                            time.sleep(backoff)
                            continue
                    # not a 'results' KeyError or out of retries: re-raise
                    raise
            # if somehow we exit loop without returning, re-raise last exception
            raise last_exc
        return wrapper
    return decorator


def retry_on_empty_or_missing_results(max_retries: int = 3, backoff: float = 1.0):
    """
    Retry the decorated function if:
      1) it raises KeyError('results')
      2) it returns an empty list
    max_retries: total attempts (including the first).
    backoff: seconds to sleep between attempts.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_retries + 1):
                try:
                    result = func(*args, **kwargs)
                except KeyError as e:
                    # only retry on missing 'results'
                    if e.args and e.args[0] == 'results':
                        last_exc = e
                        if attempt < max_retries:
                            time.sleep(backoff)
                            continue
                    # other KeyErrors or out of retries: re-raise
                    raise
                else:
                    # if the function returned a list, retry on empty
                    if isinstance(result, list) and not result:
                        last_exc = RuntimeError("Empty results list")
                        if attempt < max_retries:
                            time.sleep(backoff)
                            continue
                    return result
            # exhausted all retries
            raise last_exc
        return wrapper
    return decorator