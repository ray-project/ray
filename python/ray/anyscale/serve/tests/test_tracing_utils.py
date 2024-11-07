import json
import os
import re
import shutil
from pathlib import Path
from unittest.mock import patch

import grpc
import pytest
import requests
import starlette
from starlette.requests import Request
from starlette.responses import StreamingResponse

import ray
from ray import serve
from ray.anyscale.serve._private.tracing_utils import (
    DEFAULT_TRACING_EXPORTER_IMPORT_PATH,
    _load_span_processors,
    _validate_tracing_exporter,
    _validate_tracing_exporter_processors,
    setup_tracing,
)
from ray.anyscale.serve.utils import get_trace_context
from ray.serve._private.common import ServeComponentType
from ray.serve._private.logging_utils import get_serve_logs_dir
from ray.serve.config import gRPCOptions
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )
except ImportError:
    raise ModuleNotFoundError(
        "`opentelemetry` or `opentelemetry.sdk.trace.export` not found"
    )

CUSTOM_EXPORTER_OUTPUT_FILENAME = "spans.txt"


@pytest.fixture
def use_custom_tracing_exporter():
    yield "ray.anyscale.serve.tests.test_tracing_utils:custom_tracing_exporter"

    # Clean up output file produced by custom exporter
    if os.path.exists(CUSTOM_EXPORTER_OUTPUT_FILENAME):
        os.remove(CUSTOM_EXPORTER_OUTPUT_FILENAME)


@pytest.fixture
def serve_and_ray_shutdown():
    serve.shutdown()
    ray.shutdown()
    yield
    serve.shutdown()


def test_disable_tracing_exporter():
    """Test that setting `tracing_exporter_import_path`
    to an empty string disables tracing.
    """
    is_tracing_setup_successful = setup_tracing(
        component_type=ServeComponentType.REPLICA,
        component_name="component_name",
        component_id="component_id",
        tracing_exporter_import_path="",
    )

    assert is_tracing_setup_successful is False


def test_validate_tracing_exporter_with_string():
    """Test exception message for invalid exporter type."""
    invalid_exporters = [1, "string", []]
    expected_exception = "Tracing exporter must be a function."

    for invalid_exporter in invalid_exporters:
        with pytest.raises(TypeError, match=expected_exception):
            _validate_tracing_exporter(invalid_exporter)


def test_validate_tracing_exporter_with_args():
    """Test exception message for _validate_tracing_exporter.
    if exporter contains an argument.
    """

    def test_exporter(arg):
        return arg

    expected_exception = "Tracing exporter cannot take any arguments."

    with pytest.raises(TypeError, match=expected_exception):
        _validate_tracing_exporter(test_exporter)


def test_validate_tracing_exporter_processors_list():
    """Test exception message for _validate_tracing_exporter.
    if exporter returns invalid return type.
    """
    invalid_span_processors = [1, "string"]
    for invalid_span_processor in invalid_span_processors:
        expected_exception = re.escape(
            "Output of tracing exporter needs to be of type "
            "List[SpanProcessor], but received type "
            f"{type(invalid_span_processor)}."
        )
        with pytest.raises(TypeError, match=expected_exception):
            _validate_tracing_exporter_processors(invalid_span_processor)


def test_validate_tracing_exporter_processors_full_output():
    """Test exception message for _validate_tracing_exporter.
    if exporter returns invalid return type.
    """
    invalid_span_processors = [[1, 2], ["1", "2"]]
    for invalid_span_processor in invalid_span_processors:
        expected_exception = re.escape(
            "Output of tracing exporter needs to be of "
            "type List[SpanProcessor], "
            f"but received type {type(invalid_span_processor[0])}."
        )
        with pytest.raises(TypeError, match=expected_exception):
            _validate_tracing_exporter_processors(invalid_span_processor)


def test_missing_dependencies():
    """Test that setup_tracing raises an exception if
    tracing module is not installed.
    """
    expected_exception = (
        "You must `pip install opentelemetry-api` and "
        "`pip install opentelemetry-sdk` "
        "to enable tracing on Ray Serve."
    )
    with patch("ray.anyscale.serve._private.tracing_utils.trace", new=None):
        with pytest.raises(ImportError, match=expected_exception):
            setup_tracing(
                component_type=ServeComponentType.REPLICA,
                component_name="component_name",
                component_id="component_id",
            )


def test_default_tracing_exporter(ray_start_cluster):
    """Test that the default tracing exporter returns
    List[SimpleSpanProcessor].
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    ray.init(address=cluster.address)

    span_processors = _load_span_processors(
        DEFAULT_TRACING_EXPORTER_IMPORT_PATH, "mock_file.json"
    )

    assert isinstance(span_processors, list)

    for span_processor in span_processors:
        assert isinstance(span_processor, SimpleSpanProcessor)


def test_custom_tracing_exporter(use_custom_tracing_exporter):
    """Test setup_tracing with a custom tracing exporter."""
    custom_tracing_exporter_path = use_custom_tracing_exporter

    span_processors = _load_span_processors(
        custom_tracing_exporter_path, "mock_file.json"
    )

    assert isinstance(span_processors, list)

    for span_processor in span_processors:
        assert isinstance(span_processor, SimpleSpanProcessor)

    is_tracing_setup_successful = setup_tracing(
        "component_name",
        "component_id",
        ServeComponentType.REPLICA,
        custom_tracing_exporter_path,
    )

    # Validate that tracing is setup successfully
    # and the tracing exporter created the output file
    assert is_tracing_setup_successful
    assert os.path.exists(CUSTOM_EXPORTER_OUTPUT_FILENAME)


def test_tracing_sampler(use_custom_tracing_exporter):
    """Test that tracing sampler is properly configured
    through tracing_sampling_ratio argument.
    """
    custom_tracing_exporter_path = use_custom_tracing_exporter
    tracing_sampling_ratio = 1

    is_tracing_setup_successful = setup_tracing(
        "component_name",
        "component_id",
        ServeComponentType.REPLICA,
        custom_tracing_exporter_path,
        tracing_sampling_ratio,
    )

    # Validate that tracing is setup successfully
    # and the tracing exporter created the output file
    assert is_tracing_setup_successful
    assert os.path.exists(CUSTOM_EXPORTER_OUTPUT_FILENAME)

    tracer = trace.get_tracer(__name__)
    tracer_data = tracer.__dict__

    assert "sampler" in tracer_data

    sampler = tracer_data["sampler"]
    sampler_data = sampler.__dict__
    assert "_rate" in sampler_data
    assert sampler_data["_rate"] == tracing_sampling_ratio


@pytest.mark.parametrize(
    (
        "serve_application",
        "expected_proxy_spans_path",
        "expected_replica_spans_path",
        "expected_upstream_spans_path",
    ),
    [
        (
            "basic",
            "fixtures/basic_proxy_spans.json",
            "fixtures/basic_replica_spans.json",
            "fixtures/basic_upstream_spans.json",
        ),
        (
            "streaming",
            "fixtures/streaming_proxy_spans.json",
            "fixtures/streaming_replica_spans.json",
            "fixtures/streaming_upstream_spans.json",
        ),
        (
            "grpc",
            "fixtures/grpc_proxy_spans.json",
            "fixtures/grpc_replica_spans.json",
            "fixtures/grpc_upstream_spans.json",
        ),
    ],
)
def test_tracing_e2e(
    serve_and_ray_shutdown,
    serve_application,
    expected_proxy_spans_path,
    expected_replica_spans_path,
    expected_upstream_spans_path,
):
    """Test tracing e2e."""

    @serve.deployment
    class BasicModel:
        def __call__(self, req: starlette.requests.Request):
            replica_context = serve.get_replica_context()
            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span(
                "application_span", context=get_trace_context()
            ) as span:
                span.set_attribute("deployment", replica_context.deployment)
                span.set_attribute("replica_id", replica_context.replica_id.unique_id)
                return "hello"

    def hi_gen_sync():
        for i in range(10):
            yield f"hello_{i}"

    @serve.deployment
    class StreamingModel:
        def __call__(self, request: Request) -> StreamingResponse:
            gen = hi_gen_sync()
            replica_context = serve.get_replica_context()
            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span(
                "application_span", context=get_trace_context()
            ) as span:
                span.set_attribute("deployment", replica_context.deployment)
                span.set_attribute("replica_id", replica_context.replica_id.unique_id)
                return StreamingResponse(gen, media_type="text/plain")

    @serve.deployment
    class GrpcDeployment:
        def __call__(self, user_message):
            replica_context = serve.get_replica_context()
            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span(
                "application_span", context=get_trace_context()
            ) as span:
                span.set_attribute("deployment", replica_context.deployment)
                span.set_attribute("replica_id", replica_context.replica_id.unique_id)

                greeting = f"Hello {user_message.name} from {user_message.foo}"
                num_x2 = user_message.num * 2
                user_response = serve_pb2.UserDefinedResponse(
                    greeting=greeting,
                    num_x2=num_x2,
                )
                return user_response

    if serve_application == "basic":
        serve.run(BasicModel.bind())

        setup_tracing(
            component_name="upstream_app",
            component_id="345",
        )
        tracer = trace.get_tracer("test_tracing")
        with tracer.start_as_current_span("upstream_app"):
            ctx = get_trace_context()
            headers = {}
            TraceContextTextMapPropagator().inject(headers, ctx)
            r = requests.post("http://127.0.0.1:8000/", headers=headers)
            assert r.text == "hello"

    elif serve_application == "streaming":
        serve.run(StreamingModel.bind())
        setup_tracing(
            component_name="upstream_app",
            component_id="345",
        )
        tracer = trace.get_tracer("test_tracing")
        with tracer.start_as_current_span("upstream_app"):
            ctx = get_trace_context()
            headers = {}
            TraceContextTextMapPropagator().inject(headers, ctx)
            r = requests.get("http://localhost:8000", stream=True, headers=headers)
            r.raise_for_status()
            for i, chunk in enumerate(
                r.iter_content(chunk_size=None, decode_unicode=True)
            ):
                assert chunk == f"hello_{i}"

    elif serve_application == "grpc":
        grpc_port = 9000
        grpc_servicer_functions = [
            "ray.serve.generated.serve_pb2_grpc"
            ".add_UserDefinedServiceServicer_to_server",
            "ray.serve.generated.serve_pb2_grpc.add_FruitServiceServicer_to_server",
        ]

        serve.start(
            grpc_options=gRPCOptions(
                port=grpc_port,
                grpc_servicer_functions=grpc_servicer_functions,
            ),
        )
        g = GrpcDeployment.options(name="grpc-deployment").bind()
        serve.run(g)

        setup_tracing(
            component_name="upstream_app",
            component_id="345",
        )
        tracer = trace.get_tracer("test_tracing")
        with tracer.start_as_current_span("upstream_app"):
            ctx = get_trace_context()
            headers = {}
            TraceContextTextMapPropagator().inject(headers, ctx)
            metadata = tuple(headers.items())

            channel = grpc.insecure_channel("localhost:9000")

            stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
            request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
            response, call = stub.__call__.with_call(request=request, metadata=metadata)

            assert call.code() == grpc.StatusCode.OK, call.code()
            assert response.greeting == "Hello foo from bar", response.greeting

    serve.shutdown()

    serve_logs_dir = get_serve_logs_dir()
    spans_dir = os.path.join(serve_logs_dir, "spans")

    files = os.listdir(spans_dir)

    assert len(files) == 3

    replica_filename = None
    proxy_filename = None
    upstream_filename = None
    for file in files:
        if "replica" in file:
            replica_filename = file
        elif "proxy" in file:
            proxy_filename = file
        elif "upstream" in file:
            upstream_filename = file
        else:
            assert False, f"Did not expect tracing file with name {file}"

    assert replica_filename and proxy_filename and upstream_filename

    upstream_spans = load_spans(os.path.join(spans_dir, upstream_filename))
    proxy_spans = load_spans(os.path.join(spans_dir, proxy_filename))
    replica_spans = load_spans(os.path.join(spans_dir, replica_filename))

    entire_trace = replica_spans + proxy_spans + upstream_spans
    validate_span_associations_in_trace(entire_trace)

    expected_upstream_spans = load_json_fixture(expected_upstream_spans_path)
    expected_proxy_spans = load_json_fixture(expected_proxy_spans_path)
    expected_replica_spans = load_json_fixture(expected_replica_spans_path)

    sanitize_spans(upstream_spans)
    sanitize_spans(proxy_spans)
    sanitize_spans(replica_spans)

    assert upstream_spans == expected_upstream_spans
    assert proxy_spans == expected_proxy_spans
    assert replica_spans == expected_replica_spans

    shutil.rmtree(spans_dir)


def custom_tracing_exporter():
    """Custom tracing exporter used for testing."""
    return [
        SimpleSpanProcessor(
            ConsoleSpanExporter(out=open(CUSTOM_EXPORTER_OUTPUT_FILENAME, "a"))
        )
    ]


def load_json_fixture(file_path):
    """Load json from a fixture."""
    with Path(__file__).parent.joinpath(file_path).open() as f:
        return json.load(f)


def load_spans(file_path):
    """Load and parse spans from a `.span` file.
    This requires special handling because ConsoleSpanExporter
    does not write proper JSON since the data is streamed.
    """
    with open(file_path, "r") as file:
        file_contents = file.read()

    raw_spans = file_contents.split("}\n{")
    spans = []
    for i, raw_span in enumerate(raw_spans):
        if len(raw_spans) > 1:
            if i == 0:
                raw_span = raw_span + "}"
            elif i == (len(raw_spans) - 1):
                raw_span = "{" + raw_span
            elif i != 0:
                raw_span = "{" + raw_span + "}"

        span_dict = json.loads(raw_span)

        spans.append(span_dict)

    spans.sort(reverse=True, key=lambda x: x["start_time"])

    return spans


def sanitize_spans(spans):
    """Remove span attributes with ephemeral data."""
    for span in spans:
        del span["context"]
        del span["parent_id"]
        del span["start_time"]
        del span["end_time"]
        del span["resource"]
        for k, _ in span["attributes"].items():
            if "_id" in k:
                span["attributes"][k] = ""


def validate_span_associations_in_trace(spans):
    """Validate that all spans in a
    trace are correctly associated with each
    other through the parent_id relationship.
    """
    if len(spans) <= 1:
        return

    class Span:
        def __init__(self, _span_id: str, _parent_id: str, _name: str):
            self.span_id = _span_id
            self.parent_id = _parent_id
            self.name = _name

    span_nodes = {}
    starting_span = None
    for span in spans:
        span_id = span["context"]["span_id"].lstrip("0x")
        parent_id = span["parent_id"].lstrip("0x") if span["parent_id"] else None
        name = span["name"]
        new_span = Span(span_id, parent_id, name)
        span_nodes[span_id] = new_span
        # The starting span is always the application span.
        if name == "application_span":
            assert starting_span is None, "Multiple starting spans found in trace"
            starting_span = new_span

    current_span = starting_span
    span_nodes.pop(current_span.span_id)
    while current_span:
        current_span = span_nodes.pop(current_span.parent_id, None)

    # All spans should have been visited.
    assert not span_nodes


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
