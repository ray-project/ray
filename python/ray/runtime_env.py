import logging
import ray

from ray.util.annotations import PublicAPI
from ray._private.client_mode_hook import client_mode_hook

_runtime_env = None

@PublicAPI(stability="beta")
@client_mode_hook(auto_init=False)
def get_current_runtime_env():
    """Get the runtime env of the current driver/worker.
    """
    global _runtime_env
    if _runtime_env is None:
        _runtime_env = dict(ray.get_runtime_context().runtime_env)

    return _runtime_env
