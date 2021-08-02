from typing import Any

import ray

CACHED_FUNCTIONS = {}


def cached_remote_fn(fn: Any, **ray_remote_args) -> Any:
    if fn not in CACHED_FUNCTIONS:
        if ray_remote_args:
            CACHED_FUNCTIONS[fn] = ray.remote(**ray_remote_args)(fn)
        else:
            CACHED_FUNCTIONS[fn] = ray.remote(fn)
    return CACHED_FUNCTIONS[fn]
