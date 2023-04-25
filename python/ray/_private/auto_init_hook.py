import ray
import os
from functools import wraps

# List of APIs that should not be wrapped.
SKIP_LIST = [
    "init",
    "is_initialized",
    "shutdown",
]

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
    function_type = type(lambda: None)
    for api_name in api_names:
        if api_name in SKIP_LIST:
            continue
        api = getattr(ray, api_name, None)
        if api is None or type(api) != function_type:
            # Only wrap functions.
            continue

        setattr(ray, api_name, wrap_auto_init(api))
