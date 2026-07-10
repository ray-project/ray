"""Unit tests for ray.util.metric_registry.

These are pure wrapper tests in the style of the custom-metrics tests in
test_metrics_agent.py: real handles are created (constructing a Ray metric
does not require a cluster) and the underlying `ray.util.metrics` object is
swapped for a MagicMock to assert on the recorded calls. No ray.init().
"""

import sys
import threading
from unittest.mock import MagicMock

import pytest

from ray.util.metric_registry import (
    CounterHandle,
    GaugeHandle,
    HistogramHandle,
    MetricKind,
    MetricRegistry,
)


@pytest.fixture
def registry():
    return MetricRegistry()


def _mock_out(handle) -> MagicMock:
    """Replace the handle's underlying Ray metric with a MagicMock."""
    mock = MagicMock()
    handle._metric = mock
    return mock


def test_get_or_create_dedups_by_name(registry):
    c1 = registry.counter("dedup_requests", tag_keys=("a",))
    c2 = registry.counter("dedup_requests")
    c3 = registry.declare("dedup_requests", MetricKind.COUNTER, ())
    assert c1 is c2 is c3
    assert isinstance(c1, CounterHandle)


def test_namespace_prefix_and_colon_sanitizing():
    reg = MetricRegistry("myns")
    g = reg.gauge("cache:hits")
    assert g.info["name"] == "myns_cache_hits"
    # Same logical name resolves to the same handle after sanitizing.
    assert reg.gauge("cache:hits") is g


def test_counter_total_suffix_folded_in(registry):
    # Ray's Counter re-appends `_total` on export, so a trailing `_total`
    # in the requested name is stripped before construction.
    c = registry.counter("num_requests_total")
    assert c.info["name"] == "num_requests"
    # The fold applies to the registry key too: both spellings must dedup
    # to the same handle rather than creating two Ray metrics that would
    # export the same `num_requests_total` series.
    assert registry.counter("num_requests") is c
    assert registry.declare("num_requests_total", MetricKind.COUNTER, ()) is c
    without = registry.counter("num_launches")
    assert without.info["name"] == "num_launches"
    # Non-counters keep the suffix untouched.
    g = registry.gauge("weird_gauge_total")
    assert g.info["name"] == "weird_gauge_total"


def test_kind_mismatch_raises(registry):
    registry.counter("kind_clash")
    with pytest.raises(ValueError, match="already declared as counter"):
        registry.gauge("kind_clash")


def test_declare_accepts_string_kind(registry):
    h = registry.declare("string_kind", "gauge", ("a",))
    assert isinstance(h, GaugeHandle)
    assert h.kind is MetricKind.GAUGE


def test_histogram_requires_buckets(registry):
    with pytest.raises(ValueError, match="buckets"):
        registry.declare("no_buckets_hist", MetricKind.HISTOGRAM, ())
    h = registry.histogram("with_buckets_hist", buckets=[1.0, 2.0])
    assert isinstance(h, HistogramHandle)
    assert h.info["boundaries"] == [1.0, 2.0]
    # Buckets bind on first creation only; a later get returns the
    # existing metric unchanged.
    again = registry.histogram("with_buckets_hist", buckets=[5.0])
    assert again is h
    assert again.info["boundaries"] == [1.0, 2.0]


def test_description_binds_on_first_creation(registry):
    c = registry.counter("described", description="first")
    registry.counter("described", description="second")
    assert c.info["description"] == "first"


def test_missing_declared_tags_are_padded(registry):
    g = registry.gauge("padded_gauge", tag_keys=("a", "b"))
    mock = _mock_out(g)
    g.set(1, {"a": "x"})
    mock.set.assert_called_once_with(1, {"a": "x", "b": ""})
    mock.reset_mock()
    g.record(2)
    mock.set.assert_called_once_with(2, {"a": "", "b": ""})


def test_default_tags_are_not_padded_over(registry):
    # A key covered by default tags must be left absent from the record-time
    # tags, otherwise the "" padding would override the default.
    g = registry.gauge("default_tagged", tag_keys=("node_id", "other"))
    mock = _mock_out(g)
    g.set_default_tags({"node_id": "n1"})
    mock.set_default_tags.assert_called_once_with({"node_id": "n1"})
    g.set(3)
    mock.set.assert_called_once_with(3, {"other": ""})


def test_gauge_set_none_is_a_true_noop(registry, caplog, propagate_logs):
    # Ray's Gauge.set no-ops on None; the handle must return before tag
    # normalization so the call also has no warning side effects.
    g = registry.gauge("noop_gauge", tag_keys=("a",))
    mock = _mock_out(g)
    with caplog.at_level("WARNING", logger="ray.util.metric_registry"):
        g.set(None, {"unknown_key": "x"})
    mock.set.assert_not_called()
    assert not caplog.records


# `propagate_logs` (conftest.py) is required for caplog to see `ray.*`
# loggers: Ray sets propagate=False on the "ray" logger at import, and
# pytest < 8 only captures via the root handler.
def test_unknown_tag_key_warns_once_and_is_dropped(registry, caplog, propagate_logs):
    g = registry.gauge("surprise_keys", tag_keys=("a",))
    mock = _mock_out(g)
    with caplog.at_level("WARNING", logger="ray.util.metric_registry"):
        g.set(1, {"a": "x", "new": "y"})
        g.set(2, {"a": "x", "new": "y"})
    warnings = [r for r in caplog.records if "new" in r.getMessage()]
    assert len(warnings) == 1
    mock.set.assert_called_with(2, {"a": "x"})


def test_redeclare_with_new_key_warns_and_keeps_keys(registry, caplog, propagate_logs):
    g = registry.gauge("widen_attempt", tag_keys=("a",))
    with caplog.at_level("WARNING", logger="ray.util.metric_registry"):
        again = registry.gauge("widen_attempt", tag_keys=("a", "b"))
    assert again is g
    assert g.tag_keys == ("a",)
    assert any("'b'" in r.getMessage() for r in caplog.records)


def test_counter_record_noops_on_nonpositive(registry):
    c = registry.counter("delta_counter")
    mock = _mock_out(c)
    c.record(0)
    c.record(-1.5)
    mock.inc.assert_not_called()
    c.record(2.5)
    mock.inc.assert_called_once_with(2.5, {})


def test_counter_inc_keeps_strict_ray_semantics(registry):
    # inc() delegates to Ray's Counter.inc, which rejects value <= 0.
    c = registry.counter("strict_counter")
    with pytest.raises(ValueError):
        c.inc(0)


def test_histogram_observe_and_timer(registry):
    h = registry.histogram("timed_hist", buckets=[0.1, 1.0], tag_keys=("op",))
    mock = _mock_out(h)
    h.observe(0.5, {"op": "read"})
    mock.observe.assert_called_once_with(0.5, {"op": "read"})
    mock.reset_mock()

    with h.timer({"op": "write"}):
        pass
    (value, tags), _ = mock.observe.call_args
    assert value >= 0
    assert tags == {"op": "write"}


def test_get_or_create_is_thread_safe(registry):
    handles = []
    barrier = threading.Barrier(8)

    def create():
        barrier.wait()
        handles.append(registry.counter("racy_counter", tag_keys=("a",)))

    threads = [threading.Thread(target=create) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len({id(h) for h in handles}) == 1
    assert len(registry._metrics) == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
