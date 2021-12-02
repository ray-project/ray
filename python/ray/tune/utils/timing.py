import functools
import time

_enabled = True
_timing = dict()


def enable_record_timing():
    global _enabled
    _enabled = True


def record_timing(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if _enabled:
            start = time.time()
            result = func(*args, **kwargs)
            total = _timing.get(func.__name__, 0)
            total += (time.time() - start)
            _timing[func.__name__] = total
            return result
        else:
            return func(*args, **kwargs)

    return wrapper


def print_timing():
    if _enabled:
        [print(f"key: {k}, time: {v}") for k, v in _timing.items()]
