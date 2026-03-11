"""Ray constants for the Rust backend.

Provides the subset of ray._private.ray_constants needed by the
ray.experimental.rdt modules.
"""

import os


def env_integer(var_name, default):
    val = os.environ.get(var_name)
    if val is not None:
        return int(val)
    return default


RDT_FETCH_FAIL_TIMEOUT_SECONDS = (
    env_integer("RAY_rdt_fetch_fail_timeout_milliseconds", 60000) / 1000
)

NIXL_REMOTE_AGENT_CACHE_MAXSIZE = env_integer(
    "RAY_NIXL_REMOTE_AGENT_CACHE_MAXSIZE", 1000
)
