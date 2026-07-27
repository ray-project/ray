import sys

import pytest

import ray._private.telemetry.metric_cardinality as mc
from ray._private.telemetry.metric_cardinality import (
    REPLICA_ID_TAG_KEY,
    TASK_OR_ACTOR_NAME_TAG_KEY,
    WORKER_ID_TAG_KEY,
    MetricCardinality,
)
from ray._private.telemetry.metric_types import MetricType

# A Serve metric carries ReplicaId; a plain user metric does not.
SERVE_TAGS = ["model_name", REPLICA_ID_TAG_KEY, WORKER_ID_TAG_KEY]
USER_TAGS = ["model_name", WORKER_ID_TAG_KEY]


@pytest.fixture
def set_level():
    """Set the cardinality level and clear the per-name label cache."""

    def _set(level: str):
        mc._CARDINALITY_LEVEL = MetricCardinality(level)
        mc._HIGH_CARDINALITY_LABELS.clear()

    original = mc._CARDINALITY_LEVEL
    yield _set
    mc._CARDINALITY_LEVEL = original
    mc._HIGH_CARDINALITY_LABELS.clear()


def drop(name, tag_keys):
    return MetricCardinality.get_high_cardinality_labels_to_drop(name, tag_keys)


def test_legacy_drops_nothing(set_level):
    set_level("legacy")
    assert drop("tasks", USER_TAGS) == []
    assert drop("vllm_foo", SERVE_TAGS) == []


@pytest.mark.parametrize("level", ["recommended", "low"])
def test_serve_metric_drops_worker_and_replica_id(set_level, level):
    set_level(level)
    assert drop("vllm_foo", SERVE_TAGS) == [WORKER_ID_TAG_KEY, REPLICA_ID_TAG_KEY]


def test_plain_user_metric_is_untouched(set_level):
    # No ReplicaId and not a Ray high-cardinality metric -> nothing dropped.
    for level in ("recommended", "low"):
        set_level(level)
        assert drop("my_app_metric", USER_TAGS) == []


def test_tasks_actors_follow_existing_rules(set_level):
    set_level("recommended")
    assert drop("tasks", USER_TAGS) == [WORKER_ID_TAG_KEY]
    set_level("low")
    assert drop("actors", USER_TAGS) == [
        WORKER_ID_TAG_KEY,
        TASK_OR_ACTOR_NAME_TAG_KEY,
    ]


def test_name_only_call_is_not_cached(set_level):
    # A call without tag_keys must not poison the cache for a Serve metric.
    set_level("recommended")
    assert drop("vllm_foo", None) == []
    assert drop("vllm_foo", SERVE_TAGS) == [WORKER_ID_TAG_KEY, REPLICA_ID_TAG_KEY]


def test_additive_gauge_defaults_to_sum(set_level):
    agg = MetricCardinality.get_aggregation_function(
        "vllm_num_requests_running", MetricType.GAUGE
    )
    assert agg([3.0, 3.0]) == 6.0


def test_ratio_gauge_uses_mean(set_level):
    agg = MetricCardinality.get_aggregation_function(
        "vllm_kv_cache_usage_perc", MetricType.GAUGE
    )
    assert agg([0.4, 0.6]) == 0.5


def test_counter_and_sum_always_sum(set_level):
    for metric_type in (MetricType.COUNTER, MetricType.SUM):
        agg = MetricCardinality.get_aggregation_function("vllm_foo", metric_type)
        assert agg([1.0, 2.0, 3.0]) == 6.0


def test_histogram_has_no_aggregation_function(set_level):
    with pytest.raises(ValueError):
        MetricCardinality.get_aggregation_function("vllm_foo", MetricType.HISTOGRAM)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
