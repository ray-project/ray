import pytest
import requests

from ray import serve
from ray.serve.metric import PushCollector, PrometheusSink

pytestmark = pytest.mark.asyncio


async def test_push_event():
    sink = PrometheusSink()
    collector = PushCollector(
        push_batch_callback=lambda *args: sink.push_batch(*args),
        default_labels={"a": "b"})

    counter = collector.new_counter(
        "my_cnt", description="my_cnt help", labels={"b": "c"})
    measure = collector.new_measure(
        "latency", description="latency help", labels={"c": "d"})
    counter.add()
    counter.add(33)
    measure.record(42)

    await collector.push_once()
    assert len(sink.metrics_cache) == 2

    text = sink.get_metric_text().decode()
    assert "# HELP my_cnt_total my_cnt help" in text
    assert "# TYPE my_cnt_total counter" in text
    assert 'my_cnt_total{a="b",b="c"} 34.0' in text
    assert 'latency{a="b",c="d"} 42.0' in text

    dict_data = sink.get_metric_dict()
    assert dict_data["my_cnt_total"] == {
        "labels": {
            "a": "b",
            "b": "c"
        },
        "value": 34.0
    }

    assert dict_data["latency"] == {
        "labels": {
            "a": "b",
            "c": "d"
        },
        "value": 42.0
    }


async def test_system_metric_endpoints(serve_instance):
    def test_error_counter(flask_request):
        1 / 0

    serve.create_backend(test_error_counter, "m:v1")
    serve.create_endpoint("test_metrics", "/measure", methods=["GET", "POST"])
    serve.link("test_metrics", "m:v1")

    # Send one query
    requests.get("http://127.0.0.1:8000/measure")

    # Check metrics are exposed under http endpoint
    metric_text = requests.get("http://127.0.0.1:8000/-/metrics").text
    print(metric_text)
    assert "num_http_requests_total" in metric_text
    assert "num_router_requests_created" in metric_text
    assert 'backend_error_counter_total{backend="m:v1"}' in metric_text
