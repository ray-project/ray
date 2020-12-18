from contextlib import contextmanager

_current_remote_func = None


@contextmanager
def current_func(f):
    global _current_remote_func
    remote_func = _current_remote_func
    _current_remote_func = f
    try:
        yield
    finally:
        _current_remote_func = remote_func


class ServerFunctionSentinel:
    def __init__(self):
        pass

    def __reduce__(self):
        global _current_remote_func
        if _current_remote_func is None:
            return (ServerFunctionSentinel, tuple())
        return (identity, (_current_remote_func, ))


def identity(x):
    return x
