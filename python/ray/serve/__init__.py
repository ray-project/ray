import ray._private.worker

try:
    from ray.serve.api import (
        deployment,
        get_deployment,
        get_replica_context,
        ingress,
        list_deployments,
        run,
        shutdown,
        start,
        delete,
    )
    from ray.serve.air_integrations import PredictorDeployment
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
    "batch",
    "start",
    "HTTPOptions",
    "get_replica_context",
    "shutdown",
    "ingress",
    "deployment",
    "get_deployment",
    "list_deployments",
    "run",
    "PredictorDeployment",
    "delete",
]
