import inspect
import os
from contextvars import ContextVar
from functools import wraps
from typing import Any, Callable, Dict, List, Optional

from ray._private.utils import import_attr
from ray.anyscale.serve._private.constants import (
    ANYSCALE_TRACING_EXPORTER_IMPORT_PATH,
    ANYSCALE_TRACING_SAMPLING_RATIO,
    DEFAULT_TRACING_EXPORTER_IMPORT_PATH,
)

try:
    from opentelemetry import trace
    from opentelemetry.context import attach, get_current
    from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
    from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
    from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
    from opentelemetry.trace import SpanKind
    from opentelemetry.trace.propagation import set_span_in_context
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )
    from opentelemetry.trace.status import Status, StatusCode

except ImportError:
    SpanProcessor = None
    ConsoleSpanExporter = None
    SimpleSpanProcessor = None
    trace = None
    SpanKind = None
    TracerProvider = None
    TraceIdRatioBased = None
    Status = None
    StatusCode = None
    set_span_in_context = None
    TraceContextTextMapPropagator = None
    get_current = None
    attach = None

TRACE_STACK: ContextVar[List[Any]] = ContextVar("trace_stack", default=[])


# Default tracing exporter needs to map to DEFAULT_TRACING_EXPORTER_IMPORT_PATH
# defined in "python/ray/anyscale/serve/_private/constants.py"
def default_tracing_exporter(tracing_file_name):
    from ray.serve._private.logging_utils import get_serve_logs_dir

    serve_logs_dir = get_serve_logs_dir()
    spans_dir = os.path.join(serve_logs_dir, "spans")
    os.makedirs(spans_dir, exist_ok=True)
    spans_file = os.path.join(spans_dir, tracing_file_name)

    return [SimpleSpanProcessor(ConsoleSpanExporter(out=open(spans_file, "a")))]


class TraceContextManager:
    def __init__(
        self, trace_name, span_kind=None, trace_context: Optional[Dict[str, str]] = None
    ):
        self.span = None
        self.trace_name = trace_name
        self.span_kind = span_kind
        self.trace_context = trace_context

        self.is_tracing_enabled = is_tracing_enabled()

    def __enter__(self):
        if self.is_tracing_enabled:
            self.span_kind = self.span_kind or SpanKind.SERVER

            tracer = trace.get_tracer(__name__)
            ctx = self.trace_context if self.trace_context else get_trace_context()

            self.span = tracer.start_span(
                self.trace_name,
                kind=self.span_kind,
                context=ctx,
            )
            new_ctx = set_span_in_context(self.span)
            set_trace_context(new_ctx)
            _append_trace_stack(self.span)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.is_tracing_enabled and self.span is not None:
            if exc_type:
                self.span.set_status(
                    Status(
                        status_code=StatusCode.ERROR,
                        description=(
                            f"An exception of type {exc_type} "
                            f"occurred with message: {exc_value}"
                        ),
                    )
                )
            else:
                self.span.set_status(Status(status_code=StatusCode.OK))

            self.span.end()
            _pop_trace_stack()

        return False


def tracing_decorator_factory(trace_name, span_kind=None):
    """
    Factory function to create a tracing decorator for instrumenting functions/methods
    with distributed tracing.

    Parameters:
    - trace_name (str): The name of the trace.
    - span_kind (trace.SpanKind, optional): The kind of span to create
    (e.g., SERVER, CLIENT). Defaults to trace.SpanKind.SERVER.

    Returns:
    - decorator (function): A decorator function that can be used to wrap
    functions/methods with distributed tracing.

    Example Usage:
    ```python
    @tracing_decorator_factory(
        "my_trace",
        span_kind=trace.SpanKind.CLIENT,
    )
    def my_function(obj):
        # Function implementation
    ```
    """

    def tracing_decorator(func):
        @wraps(func)
        def synchronous_wrapper(*args, **kwargs):
            with TraceContextManager(trace_name, span_kind):
                result = func(*args, **kwargs)

            return result

        @wraps(func)
        def generator_wrapper(*args, **kwargs):
            with TraceContextManager(trace_name, span_kind):
                for item in func(*args, **kwargs):
                    yield item

        @wraps(func)
        async def asynchronous_wrapper(*args, **kwargs):
            with TraceContextManager(trace_name, span_kind):
                result = await func(*args, **kwargs)
            return result

        @wraps(func)
        async def asyc_generator_wrapper(*args, **kwargs):
            with TraceContextManager(trace_name, span_kind):
                async for item in func(*args, **kwargs):
                    yield item

        is_generator = _is_generator_function(func)
        is_async = _is_async_function(func)
        if is_generator and is_async:
            return asyc_generator_wrapper
        elif is_async:
            return asynchronous_wrapper
        elif is_generator:
            return generator_wrapper
        else:
            return synchronous_wrapper

    return tracing_decorator


def setup_tracing(
    component_name: str,
    component_id: str,
    component_type: Optional["ServeComponentType"] = None,  # noqa: F821
    tracing_exporter_import_path: Optional[str] = ANYSCALE_TRACING_EXPORTER_IMPORT_PATH,
    tracing_sampling_ratio: Optional[float] = ANYSCALE_TRACING_SAMPLING_RATIO,
) -> bool:
    """
    Set up tracing for a specific Serve component.

    Args:
        component_name: The name of the component.
        component_id: The unique identifier of the component.
        component_type: The type of the component.
        tracing_exporter_import_path: Path to tracing exporter function.

    Returns:
        bool: True if tracing setup is successful, False otherwise.
    """
    if tracing_exporter_import_path == "":
        return False

    # Check dependencies
    if not trace:
        raise ImportError(
            "You must `pip install opentelemetry-api` and "
            "`pip install opentelemetry-sdk` "
            "to enable tracing on Ray Serve."
        )

    from ray.serve._private.utils import get_component_file_name

    tracing_file_name = get_component_file_name(
        component_name=component_name,
        component_id=component_id,
        component_type=component_type,
        suffix="_tracing.json",
    )

    span_processors = _load_span_processors(
        tracing_exporter_import_path, tracing_file_name
    )

    # Intialize tracing
    # Sets the tracer_provider. This is only allowed once~ per execution
    # context and will log a warning if attempted multiple times.
    sampler = TraceIdRatioBased(tracing_sampling_ratio)

    trace.set_tracer_provider(TracerProvider(sampler=sampler))

    for span_processor in span_processors:
        trace.get_tracer_provider().add_span_processor(span_processor)

    return True


def create_propagated_context() -> Dict[str, str]:
    """Create context that can be used across services and processes.

    This function retrieves the current context and converts it
    into a dictionary that can be used across actors and tasks since
    it is serializable.

    Returns:
    - Trace Context Propagator (dict or None): A dictionary containing the propagated
    trace context if available, otherwise None.
    """
    trace_context = get_trace_context()
    if trace_context and TraceContextTextMapPropagator:
        ctx = {}
        TraceContextTextMapPropagator().inject(ctx, trace_context)
        return ctx

    return None


def extract_propagated_context(
    propagated_context: Optional[Dict[str, str]] = None
) -> Optional[Dict[str, str]]:
    """Extract the trace context from a Trace Context Propagator."""
    if is_tracing_enabled() and propagated_context and TraceContextTextMapPropagator:
        return TraceContextTextMapPropagator().extract(carrier=propagated_context)

    return None


def set_trace_context(trace_context: Dict[str, str]):
    """Set the current trace context."""
    if attach is None:
        return

    attach(trace_context)


def get_trace_context() -> Optional[Dict[str, str]]:
    """Retrieve the current trace context."""
    if get_current is None:
        return None

    context = get_current()
    return context if context else None


def set_span_attributes(attributes):
    """Set attributes for the current span in context."""
    if TRACE_STACK:
        trace_stack = TRACE_STACK.get()
        if trace_stack:
            trace_stack[-1].set_attributes(attributes)


def set_trace_status(is_ok: bool, status_message: str = ""):
    """Set the status for the current span in context."""
    if TRACE_STACK:
        trace_stack = TRACE_STACK.get()
        if trace_stack:
            status_code = StatusCode.OK if is_ok else StatusCode.ERROR

            trace_stack[-1].set_status(
                Status(status_code=status_code, description=status_message)
            )


def is_tracing_enabled() -> bool:
    return ANYSCALE_TRACING_EXPORTER_IMPORT_PATH != "" and trace is not None


def _append_trace_stack(span):
    """Append span to global trace stack."""
    trace_stack = TRACE_STACK.get()
    trace_stack.append(span)
    TRACE_STACK.set(trace_stack)


def _pop_trace_stack():
    """Pop span to global trace stack."""
    trace_stack = TRACE_STACK.get()
    trace_stack.pop()
    TRACE_STACK.set(trace_stack)


def _validate_tracing_exporter(func: Callable) -> None:
    """Validate that the custom tracing exporter
    is a function that takes no arguments.
    """
    if inspect.isfunction(func) is False:
        raise TypeError("Tracing exporter must be a function.")

    signature = inspect.signature(func)

    if len(signature.parameters) != 0:
        raise TypeError("Tracing exporter cannot take any arguments.")


def _validate_tracing_exporter_processors(span_processors: List[Any]):
    """Validate that the output of a custom tracing exporter
    returns type List[SpanProcessor].
    """
    if not isinstance(span_processors, list):
        raise TypeError(
            "Output of tracing exporter needs to be of type "
            f"List[SpanProcessor], but received type {type(span_processors)}."
        )

    for span_processor in span_processors:
        if not isinstance(span_processor, SpanProcessor):
            raise TypeError(
                "Output of tracing exporter needs to be of "
                "type List[SpanProcessor], "
                f"but received type {type(span_processor)}."
            )


def _load_span_processors(
    tracing_exporter_import_path: str,
    tracing_file_name: str,
):
    """Load span processors from a custome tracing
    exporter function.
    """
    tracing_exporter_def = import_attr(tracing_exporter_import_path)

    if tracing_exporter_import_path == DEFAULT_TRACING_EXPORTER_IMPORT_PATH:
        return tracing_exporter_def(tracing_file_name)
    else:
        # Validate tracing exporter function
        _validate_tracing_exporter(tracing_exporter_def)
        # Validate tracing exporter processors
        span_processors = tracing_exporter_def()
        _validate_tracing_exporter_processors(span_processors)

    return span_processors


def _is_generator_function(func):
    return inspect.isgeneratorfunction(func) or inspect.isasyncgenfunction(func)


def _is_async_function(func):
    return inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)
