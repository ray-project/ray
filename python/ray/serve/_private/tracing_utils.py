import inspect
import os
from typing import Callable, List
from ray._private.utils import import_attr
from ray.serve._private.logging_utils import get_serve_logs_dir
from ray.serve.schema import TracingConfig
try:
    from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
except ImportError:
    ConsoleSpanExporter = None
    SimpleSpanProcessor = None
    trace = None
    TracerProvider = None

DEFAULT_TRACING_EXPORTER_IMPORT_PATH = (
    "ray.serve._private.tracing_utils:default_tracing_exporter"
)

def default_tracing_exporter():
    
    serve_logs_dir = get_serve_logs_dir()
    
    spans_dir = os.path.join(serve_logs_dir, "spans")

    os.makedirs(spans_dir, exist_ok=True)

    spans_file = os.path.join(spans_dir, f"{os.getpid()}.json")

    return [
        SimpleSpanProcessor(
            ConsoleSpanExporter(out=open(spans_file, "a"))
        )
    ]


def validate_tracing_exporter(func: Callable) -> None:
    if inspect.isfunction(func) is False:
        raise TypeError("Tracing exporter must be a function.")

    if not isinstance(func, (Callable, str)):
        raise TypeError(
            f'Got invalid type "{type(func)}" for '
            "the tracing exporter. Expected the tracing exporter to be a "
            "function."
        )
    
    signature = inspect.signature(func)

    if len(signature.parameters) != 0:
        raise TypeError(
            f'Tracing exporter cannot take any arguments.'
        )

    span_processors = func()

    if not isinstance(span_processors, list):
        raise TypeError(
            f"Output of tracing exporter needs to be of type List[SimpleSpanProcessor], "
            f"but recieved type {type(span_processors)}."
        )
    for span_processor in span_processors:
        if not isinstance(span_processor, SimpleSpanProcessor):
            raise TypeError(
                f"Output of tracing exporter needs to be of type List[SimpleSpanProcessor], "
                f"but recieved type {type(span_processor)}."
            )            

def validate_tracing_config(tracing_config: TracingConfig):
    if not tracing_config or tracing_config.enabled is False:
        return
    
    if not ConsoleSpanExporter or not SimpleSpanProcessor or not trace or not TracerProvider:
        raise ImportError(
            "You must `pip install opentelemetry` and `pip install opentelemetry-sdk`"
            f"to enable tracing on Ray Serve."
        )

    if tracing_config.exporter_import_path:
        tracing_exporter = import_attr(tracing_config.exporter_import_path)
        validate_tracing_exporter(tracing_exporter)



def setup_tracing(tracing_config: dict) -> None:
    # Sets the tracer_provider. This is only allowed once per execution
    # context and will log a warning if attempted multiple times.
    tracing_config = TracingConfig(**tracing_config)

    if not tracing_config or tracing_config.enabled is False:
        return
    

    exporter_import_path = (
        tracing_config.exporter_import_path
        if tracing_config.exporter_import_path
        else DEFAULT_TRACING_EXPORTER_IMPORT_PATH
    )

    tracing_exporter = import_attr(exporter_import_path)

    # Intialize Tracing
    span_processors = tracing_exporter()

    trace.set_tracer_provider(TracerProvider())

    for span_processor in span_processors:
        trace.get_tracer_provider().add_span_processor(span_processor)
