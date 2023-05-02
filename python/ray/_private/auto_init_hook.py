import ray
import os
from functools import wraps


should_auto_init = True


def disable_auto_init():
    global should_auto_init
    should_auto_init = False


def auto_init_ray():
    global should_auto_init
    if (
        should_auto_init
        and os.environ.get("RAY_ENABLE_AUTO_CONNECT", "") != "0"
        and not ray.is_initialized()
    ):
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
