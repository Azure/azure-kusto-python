import random
import time

from io import SEEK_END, SEEK_SET, BytesIO

# TODO - is there a better place for these functions?


def sleep_with_backoff(i: int):
    sleep = (2 ** i) + random.uniform(0, 1)
    time.sleep(sleep)


def get_stream_size(stream: BytesIO) -> int:
    previous = stream.tell()
    stream.seek(0, SEEK_END)
    result = stream.tell()
    stream.seek(previous, SEEK_SET)

    return result
