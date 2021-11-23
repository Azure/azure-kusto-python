import random
from time import sleep


class ExponentialRetry:
    def __init__(self, max_attempts, sleep_base: float = 1.0, max_jitter: float = 1.0):
        self.max_attempts = max_attempts
        self.sleep_base = sleep_base
        self.max_jitter = max_jitter

        self.retries = 0

    def backoff(self):
        sleep((self.sleep_base * (2 ** self.retries)) + (random.uniform(0, self.max_jitter)))
        self.retries += 1

    def __bool__(self):
        return self.retries < self.max_attempts
