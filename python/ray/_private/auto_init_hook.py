import ray
import os
from functools import wraps


def wrap_auto_init(fn):
    @wraps(fn)
    def auto_init_wrapper(*args, **kwargs):
        if (
            os.environ.get("RAY_ENABLE_AUTO_CONNECT", "") != "0"
            and not ray.is_initialized()
        ):
            ray.init()
            return fn(*args, **kwargs)

    return auto_init_wrapper


def wrap_auto_init_for_all_apis(api_names):
    """Wrap public APIs with automatic ray.init."""
    for api_name in AUTO_INIT_APIS:
        api = getattr(ray, api_name, None)
        assert api is not None, api_name
        setattr(ray, api_name, wrap_auto_init(api))
