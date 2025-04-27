from market_data import requests
from functools import wraps

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
