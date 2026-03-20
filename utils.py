import logging
import time
import requests
from functools import wraps

def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def retry(max_tries=3, delay=1, backoff=2, exceptions=(Exception,)):
    """
    Декоратор для повторных попыток при ошибках.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _delay = delay
            for attempt in range(max_tries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_tries - 1:
                        raise
                    logging.warning(f"Попытка {attempt+1} не удалась: {e}. Повтор через {_delay} сек.")
                    time.sleep(_delay)
                    _delay *= backoff
        return wrapper
    return decorator