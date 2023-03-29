from dataclasses import dataclass
from typing import Callable

import pytest

from ray.data._internal.metrics import (
    Metrics,
    DataMungingMetrics,
    ObjectStoreMetrics,
    MetricsCollector,
)
from ray.tests.conftest import *  # noqa


@dataclass
class TestMetrics(Metrics):
    a: int = 1
    b: int = 2


def test_metrics_to_dict():
    # Test that dictionary view of metrics is as expected.
    metrics = TestMetrics(a=2, b=3)

    assert metrics.to_metrics_dict() == {"a": 2, "b": 3}


def test_metrics_merge():
    # Test metrics merging.
    m1 = TestMetrics()
    m2 = TestMetrics(a=3, b=5)

    m = m1.merge_with(m2)
    assert m.a == 4
    assert m.b == 7
    assert m.to_metrics_dict() == {"a": 4, "b": 7}


@dataclass
class TestMetricsWithCustomMerger(Metrics):
    a: int = 0
    b: int = 2

    def _get_metric_merger(self, metric: str) -> Callable[[int, int], int]:
        if metric == "a":
            return max
        else:
            return super()._get_metric_merger(metric)


def test_metrics_custom_merger():
    # Test metrics merging with custom merger function.
    m1 = TestMetricsWithCustomMerger(a=3, b=3)
    m2 = TestMetricsWithCustomMerger(a=4, b=7)

    m = m1.merge_with(m2)
    assert m.a == 4
    assert m.b == 10
    assert m.to_metrics_dict() == {"a": 4, "b": 10}


def test_metrics_merging_different_types_fails():
    # Test that merging Metrics subclasses with different types fails.
    m1 = TestMetrics()
    m2 = TestMetricsWithCustomMerger()
    with pytest.raises(ValueError):
        m1.merge_with(m2)


@dataclass
class TestNestedMetrics(Metrics):
    a: int
    sub_metrics: TestMetrics


def test_metrics_nested():
    # Test that nested metrics are merged correctly and have the correct dict view.
    m1 = TestNestedMetrics(a=1, sub_metrics=TestMetrics())
    m2 = TestNestedMetrics(a=2, sub_metrics=TestMetrics(a=3, b=4))

    m = m1.merge_with(m2)
    assert m.a == 3
    assert m.sub_metrics.a == 4
    assert m.sub_metrics.b == 6
    assert m.to_metrics_dict() == {"a": 3, "sub_metrics": {"a": 4, "b": 6}}


def test_data_munging_metrics():
    # Test DataMungingMetrics merging and dict view.
    m1 = DataMungingMetrics(
        num_rows_copied=6,
        num_format_conversions=1,
        num_rows_sliced=2,
        num_rows_concatenated=4,
    )
    m2 = DataMungingMetrics(
        num_rows_copied=8,
        num_format_conversions=1,
        num_rows_sliced=3,
        num_rows_concatenated=5,
    )

    m = m1.merge_with(m2)
    assert m.num_rows_copied == 14
    assert m.num_format_conversions == 2
    assert m.num_rows_sliced == 5
    assert m.num_rows_concatenated == 9
    assert m.to_metrics_dict() == {
        "num_rows_copied": 14,
        "num_format_conversions": 2,
        "num_rows_sliced": 5,
        "num_rows_concatenated": 9,
    }


def test_object_store_metrics():
    # Test ObjectStoreMetrics merging and dict view.
    m1 = ObjectStoreMetrics(alloc=8, freed=2, cur=3, peak=4)
    m2 = ObjectStoreMetrics(alloc=16, freed=6, cur=4, peak=8)

    m = m1.merge_with(m2)
    assert m.alloc == 24
    assert m.freed == 8
    assert m.cur == 7
    assert m.peak == 8
    assert m.to_metrics_dict() == {
        "obj_store_mem_alloc": 24,
        "obj_store_mem_freed": 8,
        "obj_store_mem_peak": 8,
    }


def test_metrics_collector():
    # Test MetricsCollector metrics recording/merging.
    metrics_collector = MetricsCollector()
    m1 = DataMungingMetrics(
        num_rows_copied=6,
        num_format_conversions=1,
        num_rows_sliced=2,
        num_rows_concatenated=4,
    )
    metrics_collector.record_metrics(m1)
    m2 = DataMungingMetrics(
        num_rows_copied=8,
        num_format_conversions=1,
        num_rows_sliced=3,
        num_rows_concatenated=5,
    )
    metrics_collector.record_metrics(m2)

    m = metrics_collector.get_metrics()
    assert m.num_rows_copied == 14
    assert m.num_format_conversions == 2
    assert m.num_rows_sliced == 5
    assert m.num_rows_concatenated == 9
    assert m.to_metrics_dict() == {
        "num_rows_copied": 14,
        "num_format_conversions": 2,
        "num_rows_sliced": 5,
        "num_rows_concatenated": 9,
    }
