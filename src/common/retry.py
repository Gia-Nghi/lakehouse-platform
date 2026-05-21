import logging
import random
import time
from functools import wraps


logger = logging.getLogger(__name__)


def retry(
    max_attempts=5,
    delay_seconds=30,
    backoff_factor=2,
    max_delay_seconds=300,
):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    last_error = e
                    error_text = str(e).lower()

                    should_retry = (
                        "429" in error_text
                        or "too many requests" in error_text
                        or "timeout" in error_text
                        or "connection" in error_text
                        or "temporarily" in error_text
                    )

                    if not should_retry:
                        raise

                    if attempt >= max_attempts:
                        logger.error(
                            "Retry failed after %s attempts: %s",
                            max_attempts,
                            e,
                        )
                        raise

                    delay = min(
                        delay_seconds * (backoff_factor ** (attempt - 1)),
                        max_delay_seconds,
                    )

                    jitter = random.randint(10, 30)
                    sleep_time = delay + jitter

                    logger.warning(
                        "Attempt %s/%s failed: %s. Sleeping %ss before retry.",
                        attempt,
                        max_attempts,
                        e,
                        sleep_time,
                    )

                    time.sleep(sleep_time)

            raise last_error

        return wrapper

    return decorator