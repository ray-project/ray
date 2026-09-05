import gc
import io
import json
import logging
import sys
import time
import urllib.request

import pytest

import ray.serve.autoscaling_policy as ap
from ray.serve.autoscaling_policy import (
    PrometheusQueryMixin,
    PrometheusSample,
    PrometheusScalar,
    PrometheusVector,
)


class _FakeResp:
    def __init__(self, payload):
        self._bytes = json.dumps(payload).encode()

    def __enter__(self):
        return io.BytesIO(self._bytes)

    def __exit__(self, *exc):
        return False


def _vector(value, *, labels=None, timestamp=0):
    return {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": labels or {},
                    "value": [timestamp, str(value)],
                }
            ],
        },
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


class TestQueryPrometheus:
    def _patch(self, monkeypatch, payload):
        monkeypatch.setattr(
            urllib.request, "urlopen", lambda url, timeout=None: _FakeResp(payload)
        )

    @pytest.mark.parametrize(
        "payload,expected",
        [
            pytest.param(
                _vector(2.5, labels={"model": "a"}, timestamp=123.0),
                PrometheusVector(
                    samples=(
                        PrometheusSample(
                            labels={"model": "a"}, value=2.5, timestamp=123.0
                        ),
                    )
                ),
                id="vector_one_sample",
            ),
            pytest.param(
                {
                    "status": "success",
                    "data": {"resultType": "scalar", "result": [123.0, "3.5"]},
                },
                PrometheusScalar(value=3.5, timestamp=123.0),
                id="scalar",
            ),
            pytest.param(
                {
                    "status": "success",
                    "data": {
                        "resultType": "vector",
                        "result": [
                            {"metric": {"model": "a"}, "value": [1.0, "1.0"]},
                            {"metric": {"model": "b"}, "value": [2.0, "2.0"]},
                        ],
                    },
                },
                PrometheusVector(
                    samples=(
                        PrometheusSample(
                            labels={"model": "a"}, value=1.0, timestamp=1.0
                        ),
                        PrometheusSample(
                            labels={"model": "b"}, value=2.0, timestamp=2.0
                        ),
                    )
                ),
                id="multi_sample_vector",
            ),
            pytest.param(
                {
                    "status": "success",
                    "data": {"resultType": "vector", "result": []},
                },
                PrometheusVector(samples=()),
                id="empty_vector",
            ),
            pytest.param(
                _vector(float("nan")),
                PrometheusVector(samples=()),
                id="nan_is_no_data",
            ),
        ],
    )
    def test_result_parsing(self, monkeypatch, payload, expected):
        self._patch(monkeypatch, payload)
        assert ap._query_prometheus("http://x/api/v1/query", "q", 5.0) == expected

    def test_native_histogram_rejected(self, monkeypatch):
        self._patch(
            monkeypatch,
            {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{"metric": {}, "histogram": [0, {"count": "1"}]}],
                },
            },
        )
        with pytest.raises(ValueError, match="Native histogram"):
            ap._query_prometheus("http://x/api/v1/query", "q", 5.0)

    def test_sends_headers(self, monkeypatch):
        captured = {}

        def urlopen(request, timeout=None):
            captured["authorization"] = request.get_header("Authorization")
            return _FakeResp(_vector(2.5))

        monkeypatch.setattr(urllib.request, "urlopen", urlopen)
        assert ap._query_prometheus(
            "http://x/api/v1/query",
            "q",
            5.0,
            {"Authorization": "Bearer token"},
        ) == PrometheusVector(
            samples=(PrometheusSample(labels={}, value=2.5, timestamp=0.0),)
        )
        assert captured["authorization"] == "Bearer token"


class TestFetchPrometheusResults:
    def test_query_failures_are_isolated(self, monkeypatch, caplog):
        def query_prometheus(query_url, query, timeout_s, headers=None):
            if query == "bad":
                raise OSError("unreachable")
            return PrometheusScalar(value=2.5, timestamp=0.0)

        monkeypatch.setattr(ap, "_query_prometheus", query_prometheus)
        with caplog.at_level(logging.WARNING, logger=ap.logger.name):
            assert ap._fetch_prometheus_results("http://x", ["bad", "good"]) == {
                "good": PrometheusScalar(value=2.5, timestamp=0.0)
            }
        assert "Failed to evaluate Prometheus query 'bad'" in caplog.text

    def test_empty_vector_is_distinct_from_failed_query(self, monkeypatch):
        def query_prometheus(query_url, query, timeout_s, headers=None):
            if query == "failed":
                raise OSError("unreachable")
            return PrometheusVector(samples=())

        monkeypatch.setattr(ap, "_query_prometheus", query_prometheus)
        assert ap._fetch_prometheus_results("http://x", ["failed", "empty"]) == {
            "empty": PrometheusVector(samples=())
        }


class TestPrometheusQueryMixin:
    def test_no_address_no_thread(self, monkeypatch):
        monkeypatch.delenv("RAY_PROMETHEUS_HOST", raising=False)
        calls = []
        monkeypatch.setattr(
            ap,
            "_fetch_prometheus_results",
            lambda *a, **k: calls.append(1) or {},
        )
        mixin = PrometheusQueryMixin(prometheus_queries=["q"])  # no address
        assert mixin.prometheus_results is None
        assert mixin.prometheus_metrics is None
        time.sleep(0.2)
        assert calls == []

    @pytest.mark.parametrize(
        "explicit_address,expected",
        [
            pytest.param(None, "http://envhost:9090", id="env_default"),
            pytest.param(
                "http://explicit:9090", "http://explicit:9090", id="explicit_override"
            ),
        ],
    )
    def test_address_resolution(self, monkeypatch, explicit_address, expected):
        monkeypatch.setenv("RAY_PROMETHEUS_HOST", "http://envhost:9090")
        mixin = PrometheusQueryMixin(
            prometheus_address=explicit_address, prometheus_queries=["q"]
        )
        assert mixin._prometheus_address == expected

    @pytest.mark.parametrize(
        "env_value,expected",
        [
            pytest.param(
                '{"Authorization": "Bearer token"}',
                {"Authorization": "Bearer token"},
                id="json_object",
            ),
            pytest.param(
                '[["X-Scope-OrgID", "tenant"]]',
                {"X-Scope-OrgID": "tenant"},
                id="list_of_pairs",
            ),
        ],
    )
    def test_headers_from_env(self, monkeypatch, env_value, expected):
        monkeypatch.setenv("RAY_PROMETHEUS_HEADERS", env_value)
        mixin = PrometheusQueryMixin()
        assert mixin._prometheus_headers == expected

    def test_read_does_not_block_on_fetch(self, monkeypatch):
        def slow(address, queries, timeout_s=5.0, headers=None):
            time.sleep(0.8)
            return {queries[0]: PrometheusScalar(value=5.0, timestamp=0.0)}

        monkeypatch.setattr(ap, "_fetch_prometheus_results", slow)
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

    def test_results_preserve_types_and_metrics_are_single_value_view(self):
        mixin = PrometheusQueryMixin()
        mixin._cache.results = {
            "scalar": PrometheusScalar(value=1.0, timestamp=0.0),
            "single": PrometheusVector(
                samples=(
                    PrometheusSample(labels={"model": "a"}, value=2.0, timestamp=0.0),
                )
            ),
            "multi": PrometheusVector(
                samples=(
                    PrometheusSample(labels={"model": "a"}, value=3.0, timestamp=0.0),
                    PrometheusSample(labels={"model": "b"}, value=4.0, timestamp=0.0),
                )
            ),
            "empty": PrometheusVector(samples=()),
        }
        mixin._cache.timestamp = time.monotonic()

        assert mixin.prometheus_results == mixin._cache.results
        assert mixin.prometheus_metrics == {"scalar": 1.0, "single": 2.0}
        with pytest.raises(TypeError):
            mixin.prometheus_results["single"].samples[0].labels["model"] = "b"

    def test_stale_cache_reads_none(self, monkeypatch):
        monkeypatch.setattr(
            ap,
            "_fetch_prometheus_results",
            lambda a, q, timeout_s=5.0, headers=None: {
                q[0]: PrometheusScalar(value=5.0, timestamp=0.0)
            },
        )
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
        monkeypatch.setattr(
            ap,
            "_fetch_prometheus_results",
            lambda a, q, timeout_s=5.0, headers=None: {
                q[0]: PrometheusScalar(value=5.0, timestamp=0.0)
            },
        )
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
