try:
    from ray.serve.api import (
        start,
        get_replica_context,
        shutdown,
        ingress,
        deployment,
        get_deployment,
        list_deployments,
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
import ray.worker

ray.worker.blocking_get_inside_async_warned = True

__all__ = [
    "batch",
    "start",
    "HTTPOptions",
    "get_replica_context",
    "shutdown",
    "ingress",
    "deployment",
    "get_deployment",
    "list_deployments",
]
