from typing import Dict, Optional

from ray.serve._private.tracing_utils import get_trace_context as _get_trace_context
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
def get_trace_context() -> Optional[Dict[str, str]]:
    """Get the current OpenTelemetry trace context.

    This can be used inside a deployment to create child spans that are linked
    to the Serve request trace. Requires tracing to be enabled via the
    ``RAY_SERVE_TRACING_EXPORTER_IMPORT_PATH`` environment variable.

    Returns:
        The current OpenTelemetry context, or None if tracing is not enabled.

    Example:

    .. code-block:: python

        from opentelemetry import trace
        from ray import serve

        tracer = trace.get_tracer(__name__)

        @serve.deployment
        class MyDeployment:
            def __call__(self, request):
                with tracer.start_as_current_span(
                    "my_span", context=serve.get_trace_context()
                ):
                    ...
    """
    return _get_trace_context()
