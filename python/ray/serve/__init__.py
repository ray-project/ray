import ray._private.worker

try:
    from ray.serve._private.logging_utils import configure_default_serve_logger
    from ray.serve.api import (
        Application,
        Deployment,
        RunTarget,
        _run,
        _run_many,
        delete,
        deployment,
        get_app_handle,
        get_deployment_handle,
        get_multiplexed_model_id,
        get_replica_context,
        ingress,
        multiplexed,
        run,
        run_many,
        shutdown,
        shutdown_async,
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

# Setup default ray.serve logger to ensure all serve module logs are captured.
configure_default_serve_logger()

# Mute the warning because Serve sometimes intentionally calls
# ray.get inside async actors.
ray._private.worker.blocking_get_inside_async_warned = True

__all__ = [
    "_run",
    "_run_many",
    "batch",
    "start",
    "HTTPOptions",
    "get_replica_context",
    "shutdown",
    "shutdown_async",
    "ingress",
    "deployment",
    "run",
    "run_many",
    "RunTarget",
    "delete",
    "Application",
    "Deployment",
    "multiplexed",
    "get_multiplexed_model_id",
    "status",
    "get_app_handle",
    "get_deployment_handle",
]
