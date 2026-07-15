"""Unit tests for ray.util.prometheus_collector.

Pure tests: `scrape()` is driven with inline Prometheus text expositions and
assertions are made against the underlying `ray.util.metrics` objects, which
are swapped for MagicMocks as soon as the registry creates them (constructing
a Ray metric does not require a cluster). No network, no ray.init().
"""

import sys
import threading
import time
from unittest.mock import MagicMock

import pytest

from ray.util.metric_registry import MetricRegistry
from ray.util.prometheus_collector import (
    HTTPSource,
    PrometheusCollector,
)


class _MockedRegistry(MetricRegistry):
    """A registry whose handles record into MagicMocks instead of Ray.

    The mock for each metric is available in `self.mocks`, keyed by the
    sanitized metric name (for counters, before the `_total` strip).
    """

    def __init__(self, namespace: str = ""):
        super().__init__(namespace)
        self.mocks = {}

    def declare(self, name, kind, tag_keys, description="", buckets=None):
        handle = super().declare(name, kind, tag_keys, description, buckets)
        sanitized = self._sanitize(name)
        if sanitized not in self.mocks:
            self.mocks[sanitized] = handle._metric = MagicMock()
        return handle


@pytest.fixture
def registry():
    return _MockedRegistry()


def _collector(registry, **kwargs):
    # url=None: these tests drive scrape() directly, never the source.
    return PrometheusCollector(None, registry, **kwargs)


def test_counter_mirrored_as_per_scrape_deltas(registry):
    collector = _collector(registry)
    exposition = """\
# HELP requests_total Total requests.
# TYPE requests_total counter
requests_total{path="/a"} 5.0
"""
    collector.scrape(exposition)
    mock = registry.mocks["requests_total"]
    mock.inc.assert_called_once_with(5.0, {"path": "/a"})

    mock.reset_mock()
    collector.scrape(exposition.replace("5.0", "7.5"))
    mock.inc.assert_called_once_with(2.5, {"path": "/a"})

    # An unchanged cumulative value is a zero delta: no inc call.
    mock.reset_mock()
    collector.scrape(exposition.replace("5.0", "7.5"))
    mock.inc.assert_not_called()


def test_counter_reset_records_new_value(registry):
    # A cumulative value that goes backwards means the source restarted;
    # the new reading is the increment since zero, not a value to drop.
    collector = _collector(registry)
    exposition = "# TYPE restarts_total counter\nrestarts_total 7.0\n"
    collector.scrape(exposition)
    mock = registry.mocks["restarts_total"]
    mock.reset_mock()

    collector.scrape(exposition.replace("7.0", "3.0"))
    mock.inc.assert_called_once_with(3.0, {})


def test_counter_exported_name_folds_total_suffix():
    # Use a real (unmocked) registry to check the constructed Ray metric:
    # Ray's Counter re-appends `_total`, so it must be built without it.
    registry = MetricRegistry()
    collector = PrometheusCollector(None, registry)
    collector.scrape("# TYPE fold_check_total counter\nfold_check_total 1.0\n")
    handle = registry.counter("fold_check_total")
    assert handle.info["name"] == "fold_check"


def test_gauge_mirrors_latest_value(registry):
    collector = _collector(registry)
    exposition = '# TYPE queue_depth gauge\nqueue_depth{shard="0"} 12.0\n'
    collector.scrape(exposition)
    mock = registry.mocks["queue_depth"]
    mock.set.assert_called_once_with(12.0, {"shard": "0"})

    mock.reset_mock()
    collector.scrape(exposition.replace("12.0", "3.0"))
    mock.set.assert_called_once_with(3.0, {"shard": "0"})


def test_histogram_subseries_mirrored_as_gauges_with_labels(registry):
    # Histogram sub-series are pre-aggregated cumulative values, so they are
    # gauge-mirrored -- and each sub-series must keep all its labels
    # (including `le`), which requires declaring per sample name rather than
    # per family name.
    collector = _collector(registry)
    collector.scrape(
        """\
# HELP lat_seconds Request latency.
# TYPE lat_seconds histogram
lat_seconds_bucket{le="0.1",op="read"} 1.0
lat_seconds_bucket{le="+Inf",op="read"} 2.0
lat_seconds_sum{op="read"} 3.4
lat_seconds_count{op="read"} 2.0
"""
    )
    bucket = registry.mocks["lat_seconds_bucket"]
    assert bucket.set.call_args_list[0][0] == (1.0, {"le": "0.1", "op": "read"})
    assert bucket.set.call_args_list[1][0] == (2.0, {"le": "+Inf", "op": "read"})
    registry.mocks["lat_seconds_sum"].set.assert_called_once_with(3.4, {"op": "read"})
    registry.mocks["lat_seconds_count"].set.assert_called_once_with(2.0, {"op": "read"})
    assert "lat_seconds" not in registry.mocks


def test_created_and_nan_samples_skipped(registry):
    collector = _collector(registry)
    collector.scrape(
        """\
# TYPE something_created gauge
something_created 1.6e9
# TYPE maybe_nan gauge
maybe_nan NaN
"""
    )
    assert "something_created" not in registry.mocks
    assert "maybe_nan" not in registry.mocks


def test_custom_filters_replace_defaults(registry):
    collector = _collector(registry, filters=(lambda s: s.name.startswith("noisy_"),))
    collector.scrape(
        """\
# TYPE noisy_gauge gauge
noisy_gauge 1.0
# TYPE kept_created gauge
kept_created 2.0
"""
    )
    # The custom filter drops `noisy_*`; the default `_created` skip is
    # replaced, so `kept_created` now comes through.
    assert "noisy_gauge" not in registry.mocks
    registry.mocks["kept_created"].set.assert_called_once_with(2.0, {})


def test_help_text_and_colon_sanitizing():
    registry = MetricRegistry()
    collector = PrometheusCollector(None, registry)
    collector.scrape(
        """\
# HELP sglang:num_running Number of running requests.
# TYPE sglang:num_running gauge
sglang:num_running 4.0
"""
    )
    handle = registry.gauge("sglang:num_running")
    assert handle.info["name"] == "sglang_num_running"
    assert handle.info["description"] == "Number of running requests."


# `propagate_logs` (conftest.py) is required for caplog to see `ray.*`
# loggers: Ray sets propagate=False on the "ray" logger at import, and
# pytest < 8 only captures via the root handler.
def test_label_first_seen_later_warns_and_drops(registry, caplog, propagate_logs):
    # Ray cannot widen a metric's tag keys after creation: a label that
    # first appears on a later scrape is warned about once and dropped
    # rather than raising.
    collector = _collector(registry)
    collector.scrape("# TYPE late_labels gauge\nlate_labels 1.0\n")
    mock = registry.mocks["late_labels"]
    mock.reset_mock()

    with caplog.at_level("WARNING", logger="ray.util.metric_registry"):
        collector.scrape('# TYPE late_labels gauge\nlate_labels{new="x"} 2.0\n')
    assert any("'new'" in r.getMessage() for r in caplog.records)
    mock.set.assert_called_once_with(2.0, {})


def test_url_string_builds_http_source_and_registry():
    collector = PrometheusCollector("http://localhost:1234/metrics", timeout_s=9.0)
    assert isinstance(collector._source, HTTPSource)
    assert collector._source.url == "http://localhost:1234/metrics"
    assert collector._source.timeout_s == 9.0
    assert isinstance(collector.registry, MetricRegistry)


def test_namespace_prefixes_mirrored_names():
    collector = PrometheusCollector("http://x/metrics", namespace="node")
    collector.scrape("# TYPE load1 gauge\nload1 0.5\n")
    assert collector.registry.gauge("load1").info["name"] == "node_load1"


def test_registry_and_namespace_are_mutually_exclusive():
    with pytest.raises(ValueError, match="not both"):
        PrometheusCollector("http://x/metrics", MetricRegistry(), namespace="node")


class _FakeSource:
    """Source stub: counts fetches; optionally fails the first N of them."""

    def __init__(self, fail_first: int = 0):
        self.calls = 0
        self._fail_first = fail_first

    def fetch(self) -> str:
        self.calls += 1
        if self.calls <= self._fail_first:
            raise RuntimeError("scrape failed")
        return "# TYPE up gauge\nup 1.0\n"


def _wait_for(predicate, timeout_s: float = 5.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise TimeoutError("condition not met in time")


def test_start_scrapes_and_stop_is_prompt(registry):
    source = _FakeSource()
    collector = PrometheusCollector(source, registry).start(interval_s=0.01)
    _wait_for(lambda: source.calls >= 3)
    collector.stop()
    assert collector._thread is None
    calls_after_stop = source.calls
    time.sleep(0.05)
    assert source.calls == calls_after_stop


def test_start_after_stop_restarts(registry):
    # start() after stop() must begin a new run: the stop event from the
    # prior run must not leak into the new thread (which would make it
    # exit before its first scrape).
    source = _FakeSource()
    collector = PrometheusCollector(source, registry)
    collector.start(interval_s=0.01)
    _wait_for(lambda: source.calls >= 1)
    collector.stop()

    calls_after_stop = source.calls
    collector.start(interval_s=0.01)
    _wait_for(lambda: source.calls >= calls_after_stop + 2)
    collector.stop()


def test_scrape_loop_survives_fetch_errors(registry):
    source = _FakeSource(fail_first=1)
    collector = PrometheusCollector(source, registry).start(interval_s=0.01)
    try:
        # First fetch raises; the loop must stay alive, back off, and scrape
        # again successfully (backoff for the first error is 1s).
        _wait_for(lambda: source.calls >= 2, timeout_s=10.0)
        assert threading.active_count() >= 1
    finally:
        collector.stop()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
