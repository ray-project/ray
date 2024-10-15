import errno
from typing import Dict, Optional

from ray.anyscale.serve._private.tracing_utils import (
    get_trace_context as private_get_trace_context,
)


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
