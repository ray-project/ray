# This file can be used as a default tracing startup hook for users who want
# to export spans to the folder /tmp/spans/
import os
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleExportSpanProcessor,
)


def setup_tracing() -> None:
    # Creates /tmp/spans folder
    os.makedirs("/tmp/spans", exist_ok=True)

    # Sets the tracer_provider. This is only allowed once per execution
    # context and will log a warning if attempted multiple times.
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(
        SimpleExportSpanProcessor(
            ConsoleSpanExporter(
                out=open(f"/tmp/spans/{os.getpid()}.json", "a"))))
