from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan
from ray.data.context import DataContext


def test_sources_singleton():
    ctx = DataContext.get_current()

    source = LogicalOperator("source", [])
    assert LogicalPlan(source, ctx).sources() == [source]


def test_sources_chain():
    ctx = DataContext.get_current()

    source = LogicalOperator("source", [])
    sink = LogicalOperator("sink", [source])
    assert LogicalPlan(sink, ctx).sources() == [source]


def test_sources_multiple_sources():
    ctx = DataContext.get_current()

    source1 = LogicalOperator("source1", [])
    source2 = LogicalOperator("source2", [])
    sink = LogicalOperator("sink", [source1, source2])
    assert LogicalPlan(sink, ctx).sources() == [source1, source2]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
