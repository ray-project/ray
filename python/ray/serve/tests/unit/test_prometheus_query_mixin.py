import gc
import io
import json
import sys
import time
import urllib.request

import pytest

import ray.serve.autoscaling_policy as ap
from ray.serve.autoscaling_policy import PrometheusQueryMixin


class _FakeResp:
    def __init__(self, payload):
        self._bytes = json.dumps(payload).encode()

    def __enter__(self):
        return io.BytesIO(self._bytes)

    def __exit__(self, *exc):
        return False


def _vector(value):
    return {
        "status": "success",
        "data": {"resultType": "vector", "result": [{"value": [0, str(value)]}]},
    }


class TestNormalizeQueryUrl:
    @pytest.mark.parametrize(
        "address,expected",
        [
            ("localhost:9090", "http://localhost:9090/api/v1/query"),
            ("http://localhost:9090", "http://localhost:9090/api/v1/query"),
            ("https://prom:9090", "https://prom:9090/api/v1/query"),
            ("localhost:9090/", "http://localhost:9090/api/v1/query"),
            ("http://prom:9090/api/v1/query", "http://prom:9090/api/v1/query"),
        ],
    )
    def test_normalize(self, address, expected):
        assert ap._normalize_query_url(address) == expected


class TestQueryScalar:
    def _patch(self, monkeypatch, payload):
        monkeypatch.setattr(
            urllib.request, "urlopen", lambda url, timeout=None: _FakeResp(payload)
        )

    def test_vector_one_sample(self, monkeypatch):
        self._patch(monkeypatch, _vector(2.5))
        assert ap._query_scalar("http://x/api/v1/query", "q", 5.0) == 2.5

    def test_scalar_result_type_rejected(self, monkeypatch):
        # Only single-sample instant vectors are accepted.
        self._patch(
            monkeypatch,
            {
                "status": "success",
                "data": {"resultType": "scalar", "result": [0, "3.5"]},
            },
        )
        assert ap._query_scalar("http://x/api/v1/query", "q", 5.0) is None

    def test_multi_sample_rejected(self, monkeypatch):
        self._patch(
            monkeypatch,
            {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{"value": [0, "1.0"]}, {"value": [0, "2.0"]}],
                },
            },
        )
        assert ap._query_scalar("http://x/api/v1/query", "q", 5.0) is None

    def test_nan_is_no_data(self, monkeypatch):
        self._patch(monkeypatch, _vector(float("nan")))
        assert ap._query_scalar("http://x/api/v1/query", "q", 5.0) is None


class TestPrometheusQueryMixin:
    def test_no_address_no_thread(self, monkeypatch):
        monkeypatch.delenv("RAY_PROMETHEUS_HOST", raising=False)
        calls = []
        monkeypatch.setattr(ap, "_fetch_metrics", lambda *a, **k: calls.append(1) or {})
        mixin = PrometheusQueryMixin(prometheus_queries=["q"])  # no address
        assert mixin.prometheus_metrics is None
        time.sleep(0.2)
        assert calls == []

    def test_address_defaults_to_env(self, monkeypatch):
        monkeypatch.setenv("RAY_PROMETHEUS_HOST", "http://envhost:9090")
        mixin = PrometheusQueryMixin(prometheus_queries=["q"])
        assert mixin._prometheus_address == "http://envhost:9090"

    def test_explicit_address_overrides_env(self, monkeypatch):
        monkeypatch.setenv("RAY_PROMETHEUS_HOST", "http://envhost:9090")
        mixin = PrometheusQueryMixin(
            prometheus_address="http://explicit:9090", prometheus_queries=["q"]
        )
        assert mixin._prometheus_address == "http://explicit:9090"

    def test_read_does_not_block_on_fetch(self, monkeypatch):
        def slow(address, queries):
            time.sleep(0.8)
            return {queries[0]: 5.0}

        monkeypatch.setattr(ap, "_fetch_metrics", slow)
        mixin = PrometheusQueryMixin(
            prometheus_address="localhost:9090",
            prometheus_queries=["q"],
            fetch_interval_s=0.1,
        )
        start = time.monotonic()
        assert mixin.prometheus_metrics is None  # fetch still running
        # Must return well before the 0.8s fetch would complete.
        assert time.monotonic() - start < 0.5, "read blocked on the fetch"
        for _ in range(50):
            if mixin.prometheus_metrics == {"q": 5.0}:
                break
            time.sleep(0.1)
        assert mixin.prometheus_metrics == {"q": 5.0}

    def test_stale_cache_reads_none(self, monkeypatch):
        monkeypatch.setattr(ap, "_fetch_metrics", lambda a, q: {q[0]: 5.0})
        mixin = PrometheusQueryMixin(
            prometheus_address="localhost:9090",
            prometheus_queries=["q"],
            fetch_interval_s=100.0,
            cache_ttl_s=0.3,
        )
        for _ in range(50):
            if mixin.prometheus_metrics is not None:
                break
            time.sleep(0.05)
        assert mixin.prometheus_metrics is not None
        time.sleep(0.4)  # exceed ttl; next refresh is 100s away
        assert mixin.prometheus_metrics is None

    def test_thread_stops_on_gc(self, monkeypatch):
        monkeypatch.setattr(ap, "_fetch_metrics", lambda a, q: {q[0]: 5.0})
        mixin = PrometheusQueryMixin(
            prometheus_address="localhost:9090",
            prometheus_queries=["q"],
            fetch_interval_s=0.1,
        )
        _ = mixin.prometheus_metrics  # start thread
        time.sleep(0.2)
        stop_event = mixin._cache.stop
        assert not stop_event.is_set()
        del mixin
        gc.collect()
        time.sleep(0.1)
        assert stop_event.is_set(), "finalize should stop the refresh thread on GC"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
