import inspect
import os
from typing import Any, Callable, List

from ray._private.utils import import_attr
from ray.anyscale.serve._private.constants import (
    ANYSCALE_TRACING_EXPORTER_IMPORT_PATH,
    DEFAULT_TRACING_EXPORTER_IMPORT_PATH,
)
from ray.serve._private.common import ServeComponentType
from ray.serve._private.logging_utils import get_serve_logs_dir
from ray.serve._private.utils import get_component_file_name

try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
except ImportError:
    ConsoleSpanExporter = None
    SimpleSpanProcessor = None
    trace = None
    TracerProvider = None


# Default tracing exporter needs to map to DEFAULT_TRACING_EXPORTER_IMPORT_PATH
# defined in "python/ray/anyscale/serve/_private/constants.py"
def default_tracing_exporter(tracing_file_name):
    serve_logs_dir = get_serve_logs_dir()
    spans_dir = os.path.join(serve_logs_dir, "spans")
    os.makedirs(spans_dir, exist_ok=True)
    spans_file = os.path.join(spans_dir, tracing_file_name)

    return [SimpleSpanProcessor(ConsoleSpanExporter(out=open(spans_file, "a")))]


def _validate_tracing_exporter(func: Callable) -> None:
    if inspect.isfunction(func) is False:
        raise TypeError("Tracing exporter must be a function.")

    signature = inspect.signature(func)

    if len(signature.parameters) != 0:
        raise TypeError("Tracing exporter cannot take any arguments.")


def _validate_tracing_exporter_processors(span_processors: List[Any]):
    if not isinstance(span_processors, list):
        raise TypeError(
            "Output of tracing exporter needs to be of type "
            f"List[SimpleSpanProcessor], but received type {type(span_processors)}."
        )

    for span_processor in span_processors:
        if not isinstance(span_processor, SimpleSpanProcessor):
            raise TypeError(
                "Output of tracing exporter needs to be of "
                "type List[SimpleSpanProcessor], "
                f"but received type {type(span_processor)}."
            )


def _load_span_processors(
    tracing_exporter_import_path: str,
    tracing_file_name: str,
):
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


def setup_tracing(
    component_type: ServeComponentType,
    component_name: str,
    component_id: str,
) -> None:
    # If exporter import path is empty, then shortcircuit
    tracing_exporter_import_path = ANYSCALE_TRACING_EXPORTER_IMPORT_PATH

    if tracing_exporter_import_path == "":
        return

    # Check dependencies
    if (
        not ConsoleSpanExporter
        or not SimpleSpanProcessor
        or not trace
        or not TracerProvider
    ):
        raise ImportError(
            "You must `pip install opentelemetry` and `pip install opentelemetry-sdk`"
            "to enable tracing on Ray Serve."
        )

    tracing_file_name = get_component_file_name(
        component_type=component_type,
        component_name=component_name,
        component_id=component_id,
        suffix="_tracing.json",
    )

    span_processors = _load_span_processors(
        tracing_exporter_import_path, tracing_file_name
    )

    # Intialize tracing
    # Sets the tracer_provider. This is only allowed once per execution
    # context and will log a warning if attempted multiple times.
    trace.set_tracer_provider(TracerProvider())

    for span_processor in span_processors:
        trace.get_tracer_provider().add_span_processor(span_processor)
