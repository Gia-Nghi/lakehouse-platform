import time
from functools import wraps
from typing import Callable, Tuple, Type


def retry(
    max_attempts: int = 3,
    delay_seconds: int = 2,
    backoff: int = 2,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay_seconds

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    if attempt == max_attempts:
                        raise

                    time.sleep(current_delay)
                    current_delay *= backoff

        return wrapper

    return decorator