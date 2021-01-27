from contextlib import contextmanager

_current_remote_obj = None


@contextmanager
def current_remote(r):
    global _current_remote_obj
    remote = _current_remote_obj
    _current_remote_obj = r
    try:
        yield
    finally:
        _current_remote_obj = remote


class ServerSelfReferenceSentinel:
    def __init__(self):
        pass

    def __reduce__(self):
        global _current_remote_obj
        if _current_remote_obj is None:
            return (ServerSelfReferenceSentinel, tuple())
        return (identity, (_current_remote_obj, ))


def identity(x):
    return x
