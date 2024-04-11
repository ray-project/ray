import json
import os
import re
import shutil
from unittest.mock import patch

import pytest
import requests
import starlette

import ray
from ray import serve
from ray.anyscale.serve._private.tracing_utils import (
    DEFAULT_TRACING_EXPORTER_IMPORT_PATH,
    _load_span_processors,
    _validate_tracing_exporter,
    _validate_tracing_exporter_processors,
    setup_tracing,
)
from ray.serve._private.logging_utils import get_serve_logs_dir
from ray.serve.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
except ImportError:
    raise ModuleNotFoundError(
        "`opentelemetry` or `opentelemetry.sdk.trace.export` not found"
    )


def test_default_tracing_exporter(ray_start_cluster):

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


def custom_tracing_exporter():
    return [SimpleSpanProcessor(ConsoleSpanExporter(out=open("spans", "a")))]


def test_custom_tracing_exporter():
    custom_tracing_exporter = (
        "ray.anyscale.serve.tests.test_tracing_utils:custom_tracing_exporter"
    )

    span_processors = _load_span_processors(custom_tracing_exporter, "mock_file.json")

    assert isinstance(span_processors, list)

    for span_processor in span_processors:
        assert isinstance(span_processor, SimpleSpanProcessor)


def test_validate_tracing_exporter_with_string():
    invalid_exporters = [1, "string", []]
    expected_exception = "Tracing exporter must be a function."

    for invalid_exporter in invalid_exporters:
        with pytest.raises(TypeError, match=expected_exception):
            _validate_tracing_exporter(invalid_exporter)


def test_validate_tracing_exporter_with_args():
    def test_exporter(arg):
        return arg

    expected_exception = "Tracing exporter cannot take any arguments."

    with pytest.raises(TypeError, match=expected_exception):
        _validate_tracing_exporter(test_exporter)


def test_validate_tracing_exporter_processors_list():
    invalid_span_processors = [1, "string"]
    for invalid_span_processor in invalid_span_processors:
        expected_exception = re.escape(
            "Output of tracing exporter needs to be of type "
            "List[SimpleSpanProcessor], but received type "
            f"{type(invalid_span_processor)}."
        )
        with pytest.raises(TypeError, match=expected_exception):
            _validate_tracing_exporter_processors(invalid_span_processor)


def test_validate_tracing_exporter_processors_full_output():
    invalid_span_processors = [[1, 2], ["1", "2"]]
    for invalid_span_processor in invalid_span_processors:
        expected_exception = re.escape(
            "Output of tracing exporter needs to be of "
            "type List[SimpleSpanProcessor], "
            f"but received type {type(invalid_span_processor[0])}."
        )
        with pytest.raises(TypeError, match=expected_exception):
            _validate_tracing_exporter_processors(invalid_span_processor)


def test_missing_dependencies():
    expected_exception = (
        "You must `pip install opentelemetry` and "
        "`pip install opentelemetry-sdk`"
        "to enable tracing on Ray Serve."
    )
    with patch(
        "ray.anyscale.serve._private.tracing_utils.ConsoleSpanExporter", new=None
    ):
        with pytest.raises(ImportError, match=expected_exception):
            setup_tracing("mock_file.json")


@pytest.fixture
def serve_and_ray_shutdown():
    serve.shutdown()
    ray.shutdown()
    yield
    serve.shutdown()


def test_tracing_e2e(serve_and_ray_shutdown):
    @serve.deployment
    class Model:
        def __call__(self, req: starlette.requests.Request):
            replica_context = serve.get_replica_context()
            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span("example_span") as span:
                span.set_attribute("deployment_name", replica_context.deployment)
                span.set_attribute("replica_id", replica_context.replica_id.unique_id)

    serve.run(Model.bind())
    requests.post("http://127.0.0.1:8000/")

    serve_logs_dir = get_serve_logs_dir()
    spans_dir = os.path.join(serve_logs_dir, "spans")

    files = os.listdir(spans_dir)

    assert len(files) == 1

    tracing_file = files[0]

    with open(os.path.join(spans_dir, tracing_file), "r") as file:
        data = json.load(file)

    assert "attributes" in data

    attributes = data["attributes"]

    assert "deployment_name" in attributes and attributes["deployment_name"] == "Model"
    assert "replica_id" in attributes

    shutil.rmtree(spans_dir)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
