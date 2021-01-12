import os
from contextlib import contextmanager
from functools import wraps

client_mode_enabled = os.environ.get("RAY_CLIENT_MODE", "0") == "1"

_client_hook_enabled = True


def _enable_client_hook(val: bool):
    global _client_hook_enabled
    _client_hook_enabled = val


def _disable_client_hook():
    global _client_hook_enabled
    out = _client_hook_enabled
    _client_hook_enabled = False
    return out


def _explicitly_enable_client_mode():
    global client_mode_enabled
    client_mode_enabled = True


@contextmanager
def disable_client_hook():
    val = _disable_client_hook()
    try:
        yield None
    finally:
        _enable_client_hook(val)


def client_mode_hook(func):
    """Decorator for ray module methods to delegate to ray client"""
    from ray.util.client import ray

    @wraps(func)
    def wrapper(*args, **kwargs):
        global _client_hook_enabled
        if client_mode_enabled and _client_hook_enabled:
            return getattr(ray, func.__name__)(*args, **kwargs)
        return func(*args, **kwargs)

    return wrapper


def client_mode_convert_function(method):
    """Decorator to convert a RemoteFunction call into a client call

    The common case for this decorator is for methods on RemoteFunction
    that need to transparently convert that RemoteFunction to a
    ClientRemoteFunction. This happens in circumstances where the
    RemoteFunction is declared early, in a library and only then is Ray used in
    client mode -- nescessitating a conversion.
    """
    @wraps(method)
    def wrapper(*args, **kwargs):
        pass

    return wrapper

def client_mode_convert_actor(method):
    """Decorator to convert an ActorClass call into a client call

    The common case for this decorator is for methods on ActorClass
    that need to transparently convert that ActorClass to a
    ClientActorClass. This happens in circumstances where the
    ActorClass is declared early, in a library and only then is Ray used in
    client mode -- nescessitating a conversion.
    """
    @wraps(method)
    def wrapper(*args, **kwargs):
        pass

    return wrapper
