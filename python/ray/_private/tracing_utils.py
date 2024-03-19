import inspect
import os
from typing import Callable, List
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from ray._private.utils import import_attr
from ray.serve.schema import TracingConfig
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider


DEFAULT_TRACING_EXPORTER_IMPORT_PATH = (
    "ray._private.tracing_utils:default_tracing_exporter"
)


def default_tracing_exporter() -> List[SimpleSpanProcessor]:
    os.makedirs("/tmp/spans", exist_ok=True)
    return [
        SimpleSpanProcessor(
            ConsoleSpanExporter(out=open(f"/tmp/spans/{os.getpid()}.json", "a"))
        )
    ]


def validate_tracing_exporter(func: Callable) -> None:
    if inspect.isfunction(func) is False:
        raise TypeError("Tracing exporter must be called on a function.")

    if not isinstance(func, (Callable, str)):
        raise TypeError(
            f'Got invalid type "{type(func)}" for '
            "exporter_def. Expected exporter_def to be a "
            "function."
        )

    output = func()
    if not isinstance(output, list) or not all(
        isinstance(x, SimpleSpanProcessor) for x in output
    ):
        raise TypeError(
            f"Output of tracing exporter function needs to be of type List[SimpleSpanProcessor] {type(output[0])}"
        )


def get_exporter_import_path(tracing_config: TracingConfig) -> str:
    """If tracing is enabled, validate and return the exporter_import_path"""
    if not tracing_config or tracing_config.enable is False:
        return None

    # Import and validate exporter function
    if tracing_config.exporter_import_path:
        tracing_exporter = import_attr(tracing_config.exporter_import_path)
        validate_tracing_exporter(tracing_exporter)
        return tracing_config.exporter_import_path
    else:
        # If tracing is enabled, but an export path is not set
        # Use the default tracing exporter
        return DEFAULT_TRACING_EXPORTER_IMPORT_PATH


def setup_tracing(exporter_import_path: str) -> None:
    # Sets the tracer_provider. This is only allowed once per execution
    # context and will log a warning if attempted multiple times.

    tracing_exporter = import_attr(exporter_import_path)

    span_processors = tracing_exporter()

    trace.set_tracer_provider(TracerProvider())

    for span_processor in span_processors:
        trace.get_tracer_provider().add_span_processor(span_processor)
