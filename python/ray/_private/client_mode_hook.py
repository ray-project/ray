import os
import threading
from contextlib import contextmanager
from functools import wraps

from ray._private.auto_init_hook import auto_init_ray

# Attr set on func defs to mark they have been converted to client mode.
RAY_CLIENT_MODE_ATTR = "__ray_client_mode_key__"

# Global setting of whether client mode is enabled. This default to OFF,
# but is enabled upon ray.client(...).connect() or in tests.
is_client_mode_enabled = os.environ.get("RAY_CLIENT_MODE", "0") == "1"

# When RAY_CLIENT_MODE == 1, we treat it as default enabled client mode
# This is useful for testing
is_client_mode_enabled_by_default = is_client_mode_enabled
os.environ.update({"RAY_CLIENT_MODE": "0"})

is_init_called = False

# Local setting of whether to ignore client hook conversion. This defaults
# to TRUE and is disabled when the underlying 'real' Ray function is needed.
_client_hook_status_on_thread = threading.local()
_client_hook_status_on_thread.status = True


def _get_client_hook_status_on_thread():
    """Get's the value of `_client_hook_status_on_thread`.
    Since `_client_hook_status_on_thread` is a thread-local variable, we may
    need to add and set the 'status' attribute.
    """
    global _client_hook_status_on_thread
    if not hasattr(_client_hook_status_on_thread, "status"):
        _client_hook_status_on_thread.status = True
    return _client_hook_status_on_thread.status


def _set_client_hook_status(val: bool):
    global _client_hook_status_on_thread
    _client_hook_status_on_thread.status = val


def _disable_client_hook():
    global _client_hook_status_on_thread
    out = _get_client_hook_status_on_thread()
    _client_hook_status_on_thread.status = False
    return out


def _explicitly_enable_client_mode():
    """Force client mode to be enabled.
    NOTE: This should not be used in tests, use `enable_client_mode`.
    """
    global is_client_mode_enabled
    is_client_mode_enabled = True


def _explicitly_disable_client_mode():
    global is_client_mode_enabled
    is_client_mode_enabled = False


@contextmanager
def disable_client_hook():
    val = _disable_client_hook()
    try:
        yield None
    finally:
        _set_client_hook_status(val)


@contextmanager
def enable_client_mode():
    _explicitly_enable_client_mode()
    try:
        yield None
    finally:
        _explicitly_disable_client_mode()


def client_mode_hook(func: callable):
    """Decorator for whether to use the 'regular' ray version of a function,
    or the Ray Client version of that function.

    Args:
        func: This function. This is set when this function is used
            as a decorator.
    """

    from ray.util.client import ray

    @wraps(func)
    def wrapper(*args, **kwargs):
        # NOTE(hchen): DO NOT use "import" inside this function.
        # Because when it's called within a `__del__` method, this error
        # will be raised (see #35114):
        # ImportError: sys.meta_path is None, Python is likely shutting down.
        if client_mode_should_convert():
            # Legacy code
            # we only convert init function if RAY_CLIENT_MODE=1
            if func.__name__ != "init" or is_client_mode_enabled_by_default:
                return getattr(ray, func.__name__)(*args, **kwargs)
        return func(*args, **kwargs)

    return wrapper


def client_mode_should_convert():
    """Determines if functions should be converted to client mode."""

    # `is_client_mode_enabled_by_default` is used for testing with
    # `RAY_CLIENT_MODE=1`. This flag means all tests run with client mode.
    return (
        is_client_mode_enabled or is_client_mode_enabled_by_default
    ) and _get_client_hook_status_on_thread()


def client_mode_wrap(func):
    """Wraps a function called during client mode for execution as a remote
    task.

    Can be used to implement public features of ray client which do not
    belong in the main ray API (`ray.*`), yet require server-side execution.
    An example is the creation of placement groups:
    `ray.util.placement_group.placement_group()`. When called on the client
    side, this function is wrapped in a task to facilitate interaction with
    the GCS.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        from ray.util.client import ray

        auto_init_ray()
        # Directly pass this through since `client_mode_wrap` is for
        # Placement Group APIs
        if client_mode_should_convert():
            f = ray.remote(num_cpus=0)(func)
            ref = f.remote(*args, **kwargs)
            return ray.get(ref)
        return func(*args, **kwargs)

    return wrapper


def client_mode_convert_function(func_cls, in_args, in_kwargs, **kwargs):
    """Runs a preregistered ray RemoteFunction through the ray client.

    The common case for this is to transparently convert that RemoteFunction
    to a ClientRemoteFunction. This happens in circumstances where the
    RemoteFunction is declared early, in a library and only then is Ray used in
    client mode -- necessitating a conversion.
    """
    from ray.util.client import ray

    key = getattr(func_cls, RAY_CLIENT_MODE_ATTR, None)

    # Second part of "or" is needed in case func_cls is reused between Ray
    # client sessions in one Python interpreter session.
    if (key is None) or (not ray._converted_key_exists(key)):
        key = ray._convert_function(func_cls)
        setattr(func_cls, RAY_CLIENT_MODE_ATTR, key)
    client_func = ray._get_converted(key)
    return client_func._remote(in_args, in_kwargs, **kwargs)


def client_mode_convert_actor(actor_cls, in_args, in_kwargs, **kwargs):
    """Runs a preregistered actor class on the ray client

    The common case for this decorator is for instantiating an ActorClass
    transparently as a ClientActorClass. This happens in circumstances where
    the ActorClass is declared early, in a library and only then is Ray used in
    client mode -- necessitating a conversion.
    """
    from ray.util.client import ray

    key = getattr(actor_cls, RAY_CLIENT_MODE_ATTR, None)
    # Second part of "or" is needed in case actor_cls is reused between Ray
    # client sessions in one Python interpreter session.
    if (key is None) or (not ray._converted_key_exists(key)):
        key = ray._convert_actor(actor_cls)
        setattr(actor_cls, RAY_CLIENT_MODE_ATTR, key)
    client_actor = ray._get_converted(key)
    return client_actor._remote(in_args, in_kwargs, **kwargs)
