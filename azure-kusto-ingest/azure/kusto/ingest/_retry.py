import random
from time import sleep


class ExponentialRetry:
    def __init__(self, max_attempts, sleep_base_secs: float = 1.0, max_jitter_secs: float = 1.0):
        self.max_attempts = max_attempts
        self.sleep_base_secs = sleep_base_secs
        self.max_jitter_secs = max_jitter_secs

        self.current_attempt = 0

    def do_backoff(self):
        sleep((self.sleep_base_secs * (2 ** self.current_attempt)) + (random.uniform(0, self.max_jitter_secs)))
        self.current_attempt += 1

    def __bool__(self):
        return self.current_attempt < self.max_attempts
