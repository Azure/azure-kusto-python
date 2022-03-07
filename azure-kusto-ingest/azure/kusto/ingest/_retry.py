import random
from time import sleep
from .exceptions import KustoFailedAfterRetry


class ExponentialRetry:
    """
    Helper class for retry policy implementation.
    """

    def __init__(self, max_attempts, sleep_base_secs: float = 1.0, max_jitter_secs: float = 1.0):
        self.max_attempts = max_attempts
        self.sleep_base_secs = sleep_base_secs
        self.max_jitter_secs = max_jitter_secs

        self.current_attempt = 0

    def do_backoff(self):
        """
        Update retry counter and sleep before the next retry.
        """
        self.current_attempt += 1
        if self:
            sleep((self.sleep_base_secs * (2**self.current_attempt)) + (random.uniform(0, self.max_jitter_secs)))

    def __bool__(self):
        return self.current_attempt < self.max_attempts


def func_retry_wrapper(func, max_retry, sleep_base_secs: float, *exceptions):
    """
    Wrap function with a retry mechanism.
    """
    retry = ExponentialRetry(max_retry, sleep_base_secs)
    if not exceptions:
        exceptions = (Exception,)
    while retry:
        try:
            return func()
        except exceptions as ex:
            retry.do_backoff()
            if not retry:
                raise KustoFailedAfterRetry(ex, max_retry) from ex
