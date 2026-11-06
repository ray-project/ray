"""Contract tests for native Ray spans, including concurrent streaming."""
import asyncio
import os
import pickle
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from ray.llm._internal.serve.observability import genai_tracing as tracing


def carrier(number, sampled=True):
    return {
        "traceparent": f"00-{number:032x}-{number+1:016x}-{'01' if sampled else '00'}",
        "tracestate": "vendor=request-123,other=parent-456",
    }


def request(number=10):
    return SimpleNamespace(model="test-model", _otel_trace_headers=carrier(number))


class TracingTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.exporter = InMemorySpanExporter()
        cls.provider = TracerProvider()
        cls.provider.add_span_processor(SimpleSpanProcessor(cls.exporter))

    def setUp(self):
        self.environment = patch.dict(
            os.environ, {"OTEL_SEMCONV_STABILITY_OPT_IN": "gen_ai_latest_experimental"}
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.provider_patch = patch.object(tracing, "_provider", self.provider)
        self.provider_patch.start()
        self.addCleanup(self.provider_patch.stop)
        self.exporter.clear()

    async def test_concurrent_streams_keep_separate_parents(self):
        async def run(number):
            req = request(number)

            async def chunks():
                for _ in range(3):
                    await asyncio.sleep(0)
                    self.assertEqual(
                        trace.get_current_span().get_span_context().trace_id, number
                    )
                    yield number

            values = []
            async for item in tracing.trace_stream(chunks(), req, "worker"):
                self.assertFalse(trace.get_current_span().get_span_context().is_valid)
                values.append(item)
            return values

        self.assertEqual(
            await asyncio.gather(run(100), run(200)), [[100] * 3, [200] * 3]
        )
        spans = self.exporter.get_finished_spans()
        self.assertEqual({span.context.trace_id for span in spans}, {100, 200})
        for span in spans:
            self.assertEqual(span.parent.span_id, span.context.trace_id + 1)
            self.assertEqual(
                span.context.trace_state.to_header(), carrier(1)["tracestate"]
            )
            self.assertEqual(
                span.attributes["gen_ai.operation.name"], "invoke_workflow"
            )

    async def test_closing_stream_finishes_span(self):
        closed = []

        async def chunks():
            try:
                yield 1
                yield 2
            finally:
                closed.append(True)

        stream = tracing.trace_stream(chunks(), request(), "worker")
        self.assertEqual(await anext(stream), 1)
        self.assertEqual(len(self.exporter.get_finished_spans()), 0)
        await stream.aclose()
        self.assertEqual(closed, [True])
        self.assertTrue(
            self.exporter.get_finished_spans()[0].attributes["request.cancelled"]
        )

    async def test_closing_decorated_stream_closes_engine_stream(self):
        closed = []

        class Server:
            @tracing.traced_generator("orchestration")
            async def run(self, req):
                try:
                    yield 1
                    yield 2
                finally:
                    closed.append(True)

        stream = Server().run(request())
        await anext(stream)
        await stream.aclose()
        self.assertEqual(closed, [True])
        self.assertEqual(len(self.exporter.get_finished_spans()), 1)

    def test_exporter_preserves_global_provider_and_pod_resource(self):
        exporter = InMemorySpanExporter()
        global_provider = trace.get_tracer_provider()
        with (
            patch.object(tracing, "_provider", None),
            patch.dict(os.environ, {"OTEL_RESOURCE_ATTRIBUTES": "k8s.pod.uid=pod-123"}),
            patch(
                "opentelemetry.exporter.otlp.proto.grpc.trace_exporter.OTLPSpanExporter",
                return_value=exporter,
            ),
        ):
            try:
                with tracing.get_tracer().start_as_current_span("first"):
                    pass
                with tracing.get_tracer().start_as_current_span("second"):
                    pass
                tracing._provider.force_flush()
                self.assertIs(trace.get_tracer_provider(), global_provider)
                spans = exporter.get_finished_spans()
                self.assertEqual(len(spans), 2)
                self.assertTrue(
                    all(
                        s.resource.attributes["k8s.pod.uid"] == "pod-123" for s in spans
                    )
                )
            finally:
                tracing._provider.shutdown()

    async def test_upstream_sampling_rejection(self):
        req = request()
        req._otel_trace_headers = carrier(10, False)

        async def chunks():
            yield 1

        self.assertEqual(
            [v async for v in tracing.trace_stream(chunks(), req, "worker")], [1]
        )
        self.assertEqual(len(self.exporter.get_finished_spans()), 0)

    async def test_opt_in_disabled(self):
        os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] = "not_gen_ai_latest_experimental"

        async def chunks():
            yield 1

        self.assertEqual(
            [v async for v in tracing.trace_stream(chunks(), request(), "worker")], [1]
        )
        self.assertEqual(len(self.exporter.get_finished_spans()), 0)

    async def test_frontend_propagation_and_health_exclusion(self):
        requests = []

        async def app(scope, receive, send):
            req = request()
            tracing.inject_request_context(req)
            requests.append(req)
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b"ok"})

        async def noop(*args):
            pass

        middleware = tracing.GenAIRequestMiddleware(app)
        await middleware(
            {
                "type": "http",
                "path": "/v1/chat/completions",
                "headers": [(k.encode(), v.encode()) for k, v in carrier(77).items()],
            },
            noop,
            noop,
        )
        spans = self.exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].context.trace_id, 77)
        self.assertEqual(
            requests[0]._otel_trace_headers["traceparent"].split("-")[2],
            f"{spans[0].context.span_id:016x}",
        )
        self.assertEqual(
            requests[0]._otel_trace_headers["tracestate"], carrier(77)["tracestate"]
        )
        await middleware({"type": "http", "path": "/health", "headers": []}, noop, noop)
        self.assertEqual(len(self.exporter.get_finished_spans()), 1)

    def test_private_carrier_survives_ray_serialization(self):
        from ray.llm._internal.serve.core.configs.openai_api_models import (
            ChatCompletionRequest,
            CompletionRequest,
        )

        for req in [
            ChatCompletionRequest(
                model="test-model", messages=[{"role": "user", "content": "test"}]
            ),
            CompletionRequest(model="test-model", prompt="test"),
        ]:
            req._otel_trace_headers = carrier(10)
            for copy in [req.model_copy(deep=True), pickle.loads(pickle.dumps(req))]:
                self.assertEqual(copy._otel_trace_headers, req._otel_trace_headers)
                self.assertNotIn("_otel_trace_headers", copy.model_dump_json())

    async def test_error_finishes_span_without_recording_error_message(self):
        async def chunks():
            raise ValueError("private input")
            yield

        with self.assertRaises(ValueError):
            await anext(tracing.trace_stream(chunks(), request(), "worker"))
        span = self.exporter.get_finished_spans()[0]
        self.assertEqual(span.status.status_code, trace.StatusCode.ERROR)
        self.assertEqual(span.attributes["error.type"], "ValueError")
        self.assertEqual(span.events, ())

    async def test_asgi_cleanup_cancellation_after_success_is_not_a_cancelled_request(
        self,
    ):
        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b"ok"})
            raise asyncio.CancelledError()

        async def noop(*args):
            pass

        with self.assertRaises(asyncio.CancelledError):
            await tracing.GenAIRequestMiddleware(app)(
                {"type": "http", "path": "/v1/chat/completions", "headers": []},
                noop,
                noop,
            )
        span = self.exporter.get_finished_spans()[0]
        self.assertEqual(span.attributes["http.response.status_code"], 200)
        self.assertNotIn("request.cancelled", span.attributes)

    async def test_cancellation_during_send_marks_request_cancelled(self):
        async def app(scope, receive, send):
            await send({"type": "http.response.body", "body": b"ok"})

        async def cancelled_send(message):
            raise asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await tracing.GenAIRequestMiddleware(app)(
                {"type": "http", "path": "/v1/chat/completions", "headers": []},
                None,
                cancelled_send,
            )
        self.assertTrue(
            self.exporter.get_finished_spans()[0].attributes["request.cancelled"]
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
