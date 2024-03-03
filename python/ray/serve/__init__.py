import ray._private.worker

try:
    from ray.serve.api import (
        Application,
        Deployment,
        _run,
        delete,
        deployment,
        get_app_handle,
        get_deployment_handle,
        get_multiplexed_model_id,
        get_replica_context,
        ingress,
        multiplexed,
        run,
        shutdown,
        start,
        status,
    )
    from ray.serve.batching import batch
    from ray.serve.config import HTTPOptions

except ModuleNotFoundError as e:
    e.msg += (
        '. You can run `pip install "ray[serve]"` to install all Ray Serve'
        " dependencies."
    )
    raise e


# Mute the warning because Serve sometimes intentionally calls
# ray.get inside async actors.
ray._private.worker.blocking_get_inside_async_warned = True

__all__ = [
    "_run",
    "batch",
    "start",
    "HTTPOptions",
    "get_replica_context",
    "shutdown",
    "ingress",
    "deployment",
    "run",
    "delete",
    "Application",
    "Deployment",
    "multiplexed",
    "get_multiplexed_model_id",
    "status",
    "get_app_handle",
    "get_deployment_handle",
]
