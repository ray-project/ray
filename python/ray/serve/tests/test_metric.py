import time

import pytest
import requests

from ray import serve
from ray.serve.metric.client import MetricClient
from ray.serve.metric.sink import InMemorySink, PrometheusSink
from ray.serve.metric.types import MetricType, MetricMetadata

pytestmark = pytest.mark.asyncio


class MockSinkActor:
    def __init__(self):
        self.metadata = dict()
        self.batches = []

    @property
    def push_batch(self):
        return self

    async def remote(self, metadata, batch):
        self.metadata.update(metadata)
        self.batches.extend(batch)


async def test_client():
    sink = MockSinkActor()
    collector = MetricClient(
        sink, push_interval=2, default_labels={"default": "label"})
    counter = collector.new_counter(name="counter", label_names=("a", "b"))

    with pytest.raises(
            ValueError, match="labels doesn't have associated values"):
        counter.add()

    counter = counter.labels(a=1)

    counter.labels(b=2).add()
    counter.labels(b=3).add(42)

    measure = collector.new_measure("measure")
    measure.record(2)

    await collector._push_once()

    assert sink.metadata == {
        "counter": MetricMetadata(
            name="counter",
            type=MetricType.COUNTER,
            description="",
            label_names=("a", "b"),
            default_labels={"default": "label"},
        ),
        "measure": MetricMetadata(
            name="measure",
            type=MetricType.MEASURE,
            description="",
            label_names=(),
            default_labels={"default": "label"},
        )
    }
    assert sink.batches == [("counter", {
        "a": "1",
        "b": "2"
    }, 1), ("counter", {
        "a": "1",
        "b": "3"
    }, 42), ("measure", {}, 2)]


async def test_in_memory_sink(ray_instance):
    sink = InMemorySink.remote()
    collector = MetricClient(
        sink, push_interval=2, default_labels={"default": "label"})

    counter = collector.new_counter(name="my_counter", label_names=("a", ))
    measure = collector.new_measure(
        name="my_measure", description="help", label_names=("ray", "lang"))
    measure = measure.labels(lang="C++")

    counter.labels(a="1").add()
    measure.labels(ray="").record(0)
    measure.labels(ray="").record(42)

    await collector._push_once()

    metric_stored = await sink.get_metric.remote()
    assert metric_stored == [{
        "info": {
            "name": "my_counter",
            "type": "MetricType.COUNTER",
            "default": "label",
            "a": "1"
        },
        "value": 1
    }, {
        "info": {
            "name": "my_measure",
            "type": "MetricType.MEASURE",
            "default": "label",
            "lang": "C++",
            "ray": ""
        },
        "value": 42
    }]


async def test_prometheus_sink(ray_instance):
    sink = PrometheusSink.remote()
    collector = MetricClient(
        sink, push_interval=2, default_labels={"default": "label"})

    counter = collector.new_counter(name="my_counter", label_names=("a", ))
    measure = collector.new_measure(
        name="my_measure", description="help", label_names=("ray", "lang"))
    measure = measure.labels(lang="C++")

    counter.labels(a="1").add()
    measure.labels(ray="").record(0)
    measure.labels(ray="").record(42)

    await collector._push_once()

    metric_stored = await sink.get_metric.remote()
    metric_stored = metric_stored.decode()

    fragments = [
        "# HELP my_counter_total", "# TYPE my_counter_total counter",
        'my_counter_total{a="1",default="label"} 1.0',
        "# TYPE my_counter_created gauge",
        'my_counter_created{a="1",default="label"}', "# HELP my_measure help",
        "# TYPE my_measure gauge",
        'my_measure{default="label",lang="C++",ray=""} 42.0'
    ]

    for fragment in fragments:
        assert fragment in metric_stored


async def test_system_metric_endpoints(serve_instance):
    def test_error_counter(flask_request):
        1 / 0

    serve.create_endpoint("test_metrics", "/measure")
    serve.create_backend("m:v1", test_error_counter)
    serve.set_traffic("test_metrics", {"m:v1": 1})

    # Check metrics are exposed under http endpoint
    def test_metric_endpoint():
        requests.get("http://127.0.0.1:8000/measure", timeout=5)
        in_memory_metric = requests.get(
            "http://127.0.0.1:8000/-/metrics", timeout=5).json()

        # We don't want to check the values since this check might be retried.
        in_memory_metric_without_values = []
        for m in in_memory_metric:
            m.pop("value")
            in_memory_metric_without_values.append(m)

        target_metrics = [{
            "info": {
                "name": "num_http_requests",
                "type": "MetricType.COUNTER",
                "route": "/measure"
            },
        }, {
            "info": {
                "name": "num_router_requests",
                "type": "MetricType.COUNTER",
                "endpoint": "test_metrics"
            },
        }, {
            "info": {
                "name": "backend_error_counter",
                "type": "MetricType.COUNTER",
                "backend": "m:v1"
            },
        }]

        for target in target_metrics:
            assert target in in_memory_metric_without_values

    success = False
    for _ in range(3):
        try:
            test_metric_endpoint()
            success = True
            break
        except (AssertionError, requests.ReadTimeout):
            # Metrics may not have been propagated yet
            time.sleep(2)
            print("Metric not correct, retrying...")
    if not success:
        test_metric_endpoint()
