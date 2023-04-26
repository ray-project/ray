import logging

from typing import Dict, Any, Callable, Union

import ray
import ray._private.ray_constants as ray_constants
from ray.runtime_env import RuntimeEnv

logger = logging.getLogger(__name__)


def upload_worker_setup_hook_if_needed(
    runtime_env: Union[Dict[str, Any], RuntimeEnv],
    worker: "ray.Worker",
    decoder: Callable[[bytes], str],
) -> Dict[str, Any]:
    """Uploads the worker_setup_hook to GCS with a key.

    runtime_env["worker_setup_hook"] is converted to a decoded key
    that can load the worker setup hook function from GCS.
    I.e., you can use internalKV.Get(runtime_env["worker_setup_hook])
    to access the worker setup hook from GCS.

    Args:
        runtime_env: The runtime_env. The value will be modified
            when returned.
        worker: ray.worker instance.
        decoder: GCS requires the function key to be bytes. However,
            we cannot json serialize (which is required to serialize
            runtime env) the bytes. So the key should be decoded to
            a string. The given decoder is used to decode the function
            key.
    """
    setup_func = runtime_env.get("worker_setup_hook")
    if setup_func is None:
        return runtime_env

    if not isinstance(setup_func, Callable):
        raise TypeError(
            "worker_setup_hook must be a function, "
            f"got {type(setup_func)}."
        )
    
    key = worker.function_actor_manager.export_setup_func(setup_func)
    runtime_env["worker_setup_hook"] = decoder(key)
    return runtime_env
