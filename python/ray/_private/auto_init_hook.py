import os
import threading
from functools import wraps

import ray

auto_init_lock = threading.Lock()
enable_auto_connect = os.environ.get("RAY_ENABLE_AUTO_CONNECT", "") != "0"


def auto_init_ray():
    if enable_auto_connect and not ray.is_initialized():
        with auto_init_lock:
            if not ray.is_initialized():
                ray.init()


def wrap_auto_init(fn):
    @wraps(fn)
    def auto_init_wrapper(*args, **kwargs):
        auto_init_ray()
        return fn(*args, **kwargs)

    return auto_init_wrapper


def wrap_auto_init_for_all_apis(api_names):
    """Wrap public APIs with automatic ray.init."""
    for api_name in api_names:
        api = getattr(ray, api_name, None)
        assert api is not None, api_name
        setattr(ray, api_name, wrap_auto_init(api))
