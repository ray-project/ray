from torch.profiler import record_function
from functools import wraps


def profile(func):
    @wraps(func)
    def profile_func(*args, **kwargs):
        with record_function(func.__qualname__):
            return func(*args, **kwargs)

    return profile_func