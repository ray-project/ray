# flake8: noqa

# __console_exporter_start__
from typing import List

from opentelemetry.sdk.trace import SpanProcessor
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor


def console_exporter() -> List[SpanProcessor]:
    """Print each span to the process's stdout."""
    return [SimpleSpanProcessor(ConsoleSpanExporter())]


# __console_exporter_end__


# __otlp_exporter_start__
from typing import List

from opentelemetry.sdk.trace import SpanProcessor
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def otlp_exporter() -> List[SpanProcessor]:
    """Send spans to an OTLP endpoint over gRPC.

    The exporter reads its endpoint and other settings from the standard
    OTEL_EXPORTER_OTLP_* environment variables. It doesn't connect until it
    exports spans.
    """
    # Import here, so the module loads without the OTLP package installed.
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )

    return [BatchSpanProcessor(OTLPSpanExporter())]


# __otlp_exporter_end__


if __name__ == "__main__":
    import requests
    from starlette.requests import Request

    from ray import serve
    from ray.serve.schema import TracingConfig

    # Both factories return what Serve expects; shut the processors down after.
    # The OTLP one needs opentelemetry-exporter-otlp-proto-grpc, so this fails
    # if the package is missing.
    for factory in (console_exporter, otlp_exporter):
        processors = factory()
        assert isinstance(processors, list)
        assert all(isinstance(p, SpanProcessor) for p in processors)
        for p in processors:
            p.shutdown()

    @serve.deployment
    class Echo:
        def __call__(self, request: Request) -> str:
            return "ok"

    # Serve imports the exporter by path in the controller, proxy, and replica
    # processes. A local driver shares its script directory with them.
    serve.start(
        tracing_config=TracingConfig(
            enabled=True,
            sampling_ratio=1.0,
            exporter_import_path="exporters:console_exporter",
        )
    )
    serve.run(Echo.bind())
    assert requests.get("http://localhost:8000/").text == "ok"
    serve.shutdown()
