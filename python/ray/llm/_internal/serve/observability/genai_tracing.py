"""Opt-in GenAI workflow spans for the typed Ray Serve LLM request pipeline.

The request carrier is a Pydantic private attribute. It is copied by Ray, but is
not part of the public OpenAI request or response. No workload names or prompt
content are added here. Kubernetes resource identity is supplied by deployment.
"""

import asyncio
import atexit
import os
import threading
from contextlib import aclosing
from functools import wraps
from typing import Any, AsyncGenerator

_lock = threading.Lock()
_provider = None


def enabled():
    return "gen_ai_latest_experimental" in {
        value.strip()
        for value in os.environ.get("OTEL_SEMCONV_STABILITY_OPT_IN", "").split(",")
    }


def get_tracer():
    global _provider
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    with _lock:
        if _provider is None:
            # Keep this exporter separate from Ray Core and Serve tracing. Adding
            # a processor to their global provider would export their spans twice.
            provider = TracerProvider(resource=Resource.create())
            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
            _provider = provider
            atexit.register(provider.shutdown)
    return _provider.get_tracer("ray.serve.llm.genai")


def inject_request_context(request):
    if enabled() and hasattr(request, "_otel_trace_headers"):
        from opentelemetry.trace.propagation.tracecontext import (
            TraceContextTextMapPropagator,
        )

        headers = {}
        TraceContextTextMapPropagator().inject(headers)
        request._otel_trace_headers = headers


async def trace_stream(
    stream: AsyncGenerator[Any, None], request: Any, name: str
) -> AsyncGenerator[Any, None]:
    """Trace a stream without leaving context attached across yields.

    Args:
        stream: The engine or orchestration async generator.
        request: A request carrying private W3C trace headers.
        name: The span name.

    Yields:
        Any: The items from the upstream generator.
    """
    if not enabled() or not hasattr(request, "_otel_trace_headers"):
        async with aclosing(stream):
            async for item in stream:
                yield item
        return

    from opentelemetry import trace
    from opentelemetry.trace import SpanKind, Status, StatusCode
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )

    propagator = TraceContextTextMapPropagator()
    parent = propagator.extract(request._otel_trace_headers)
    attributes = {"gen_ai.operation.name": "invoke_workflow"}
    model = getattr(request, "model", None)
    if model:
        attributes["gen_ai.request.model"] = model
    span = get_tracer().start_span(
        name, context=parent, kind=SpanKind.SERVER, attributes=attributes
    )
    headers = {}
    propagator.inject(headers, context=trace.set_span_in_context(span, parent))
    request._otel_trace_headers = headers
    try:
        while True:
            try:
                with trace.use_span(
                    span,
                    end_on_exit=False,
                    record_exception=False,
                    set_status_on_exception=False,
                ):
                    item = await stream.__anext__()
            except StopAsyncIteration:
                break
            yield item
    except (asyncio.CancelledError, GeneratorExit):
        span.set_attribute("request.cancelled", True)
        raise
    except Exception as error:
        span.set_status(Status(StatusCode.ERROR))
        span.set_attribute("error.type", type(error).__name__)
        raise
    finally:
        try:
            with trace.use_span(
                span,
                end_on_exit=False,
                record_exception=False,
                set_status_on_exception=False,
            ):
                await stream.aclose()
        finally:
            span.end()


def traced_generator(name):
    def decorate(function):
        @wraps(function)
        async def wrapped(self, request, *args, **kwargs):
            stream = trace_stream(
                function(self, request, *args, **kwargs), request, name
            )
            async with aclosing(stream):
                async for item in stream:
                    yield item

        return wrapped

    return decorate


class GenAIRequestMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if (
            not enabled()
            or scope["type"] != "http"
            or scope.get("path") not in {"/v1/chat/completions", "/v1/completions"}
        ):
            return await self.app(scope, receive, send)

        from opentelemetry.trace import SpanKind, Status, StatusCode
        from opentelemetry.trace.propagation.tracecontext import (
            TraceContextTextMapPropagator,
        )

        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        parent = TraceContextTextMapPropagator().extract(headers)
        with get_tracer().start_as_current_span(
            "ray.llm.frontend",
            context=parent,
            kind=SpanKind.SERVER,
            attributes={"gen_ai.operation.name": "invoke_workflow"},
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            response_complete = False

            async def traced_send(message):
                nonlocal response_complete
                if message["type"] == "http.response.start":
                    code = message["status"]
                    span.set_attribute("http.response.status_code", code)
                    if code >= 500:
                        span.set_status(Status(StatusCode.ERROR))
                await send(message)
                if message["type"] == "http.response.body" and not message.get(
                    "more_body", False
                ):
                    response_complete = True

            try:
                await self.app(scope, receive, traced_send)
            except asyncio.CancelledError:
                # Serve can cancel the ASGI task after the response was sent.
                if not response_complete:
                    span.set_attribute("request.cancelled", True)
                raise
            except Exception as error:
                span.set_status(Status(StatusCode.ERROR))
                span.set_attribute("error.type", type(error).__name__)
                raise
