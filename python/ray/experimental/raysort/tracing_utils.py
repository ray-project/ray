import contextlib
import logging
import time


@contextlib.contextmanager
def timeit(event="operation", args={}):
    start = time.time()
    yield
    end = time.time()
    duration = end - start
    args = {"duration": duration}
    logging.info(f"{event} {args}")
