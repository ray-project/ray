import pytest
import requests

from ray import serve
from ray.serve.metric import MetricClient, InMemorySink, PrometheusSink
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
    collector = MetricClient(sink, {"default": "label"})
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
        "b": "3"
    }, 1), ("counter", {
        "a": "1",
        "b": "3"
    }, 42), ("measure", {}, 2)]


async def test_in_memory_sink(ray_instance):
    sink = InMemorySink.remote()
    collector = MetricClient(sink, {"default": "label"})

    counter = collector.new_counter(name="my_counter", label_names=("a", ))
    measure = collector.new_measure(
        name="my_measure", description="help", label_names=("ray", "lang"))
    measure = measure.labels(lang="C++")

    counter.labels(a="1").add()
    measure.labels(ray="").record(0)
    measure.labels(ray="").record(42)

    await collector._push_once()

    metric_stored = await sink.get_metric.remote()
    counter_key = [m for m in metric_stored if m.name == "my_counter"][0]
    assert counter_key.name == "my_counter"
    assert counter_key.type == MetricType.COUNTER
    assert counter_key.default == "label"
    assert counter_key.a == "1"
    assert metric_stored[counter_key] == 1
    measure_key = [m for m in metric_stored if m.name == "my_measure"][0]
    assert measure_key.name == "my_measure"
    assert measure_key.type == MetricType.MEASURE
    assert measure_key.default == "label"
    assert measure_key.lang == "C++"
    assert measure_key.ray == ""
    assert metric_stored[measure_key] == 42


async def test_prometheus_sink(ray_instance):
    sink = PrometheusSink.remote()
    collector = MetricClient(sink, {"default": "label"})

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

    serve.create_backend(test_error_counter, "m:v1")
    serve.create_endpoint("test_metrics", "/measure", methods=["GET", "POST"])
    serve.set_traffic("test_metrics", {"m:v1": 1})

    # Send one query
    requests.get("http://127.0.0.1:8000/measure")

    # Check metrics are exposed under http endpoint
    metric_text = requests.get("http://127.0.0.1:8000/-/metrics").text
    print(metric_text)
