import os
from contextlib import contextmanager
from functools import wraps

# Attr set on func defs to mark they have been converted to client mode.
RAY_CLIENT_MODE_ATTR = "__ray_client_mode_key__"

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
        if client_mode_should_convert():
            return getattr(ray, func.__name__)(*args, **kwargs)
        return func(*args, **kwargs)

    return wrapper


def client_mode_should_convert():
    global _client_hook_enabled
    return client_mode_enabled and _client_hook_enabled


def client_mode_convert_function(func_cls, in_args, in_kwargs, **kwargs):
    """Runs a preregistered ray RemoteFunction through the ray client.

    The common case for this is to transparently convert that RemoteFunction
    to a ClientRemoteFunction. This happens in circumstances where the
    RemoteFunction is declared early, in a library and only then is Ray used in
    client mode -- nescessitating a conversion.
    """
    from ray.util.client import ray

    key = getattr(func_cls, RAY_CLIENT_MODE_ATTR, None)
    if key is None:
        key = ray._convert_function(func_cls)
        setattr(func_cls, RAY_CLIENT_MODE_ATTR, key)
    client_func = ray._get_converted(key)
    return client_func._remote(in_args, in_kwargs, **kwargs)


def client_mode_convert_actor(actor_cls, in_args, in_kwargs, **kwargs):
    """Runs a preregistered actor class on the ray client

    The common case for this decorator is for instantiating an ActorClass
    transparently as a ClientActorClass. This happens in circumstances where
    the ActorClass is declared early, in a library and only then is Ray used in
    client mode -- nescessitating a conversion.
    """
    from ray.util.client import ray

    key = getattr(actor_cls, RAY_CLIENT_MODE_ATTR, None)
    if key is None:
        key = ray._convert_actor(actor_cls)
        setattr(actor_cls, RAY_CLIENT_MODE_ATTR, key)
    client_actor = ray._get_converted(key)
    return client_actor._remote(in_args, in_kwargs, **kwargs)
