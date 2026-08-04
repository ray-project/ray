import os

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)

spans_dir = "/tmp/spans/"


def setup_tracing() -> None:
    """Stand-in for a user-provided `setup_tracing` hook."""
    os.makedirs("/tmp/spans", exist_ok=True)
    # Sets the tracer_provider. This is only allowed once per execution
    # context and will log a warning if attempted multiple times.
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(
        SimpleSpanProcessor(
            ConsoleSpanExporter(
                out=open(f"{spans_dir}{os.getpid()}.txt", "w"),
                formatter=lambda span: span.to_json(indent=None) + os.linesep,
            )
        )
    )
