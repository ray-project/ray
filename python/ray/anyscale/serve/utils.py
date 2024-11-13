import asyncio
import errno
from typing import Any, Dict, Optional

import ray
from ray.anyscale.serve._private.tracing_utils import (
    get_trace_context as private_get_trace_context,
)
from ray.serve._private.utils import GENERATOR_COMPOSITION_NOT_SUPPORTED_ERROR


def get_trace_context() -> Optional[Dict[str, str]]:
    return private_get_trace_context()


def asyncio_grpc_exception_handler(loop, context):
    """Exception handler to filter out false positive BlockingIOErrors from gRPC

    Context: https://github.com/anyscale/rayturbo/issues/1027
    """
    exc = context.get("exception")
    msg = context.get("message")
    if (
        exc
        and isinstance(exc, BlockingIOError)
        and exc.errno == errno.EAGAIN
        and "PollerCompletionQueue._handle_events" in msg
    ):
        return

    loop.default_exception_handler(context)


async def resolve_deployment_resp_and_ray_objects(obj: Any, by_reference: bool):
    """Resolve `DeploymentResponse` objects to underlying object references.

    This enables composition without explicitly calling `_to_object_ref`.
    """
    from ray.serve.handle import DeploymentResponse, DeploymentResponseGenerator

    if isinstance(obj, DeploymentResponseGenerator):
        raise GENERATOR_COMPOSITION_NOT_SUPPORTED_ERROR
    elif isinstance(obj, DeploymentResponse):
        if by_reference:
            # If sending requests by reference, launch async task to
            # convert DeploymentResponse to an object ref
            return asyncio.create_task(obj._to_object_ref())
        else:
            # Otherwise, resolve DeploymentResponse directly to result
            return asyncio.create_task(obj.__await__())
    elif not by_reference and isinstance(obj, ray.ObjectRef):
        # If the router is sending requests by value (i.e. using gRPC),
        # resolve all Ray objects to mirror Ray behavior
        return asyncio.wrap_future(obj.future())
