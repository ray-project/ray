from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan


def test_sources_singleton():
    source = LogicalOperator("source", [])
    assert LogicalPlan(source).sources() == [source]


def test_sources_chain():
    source = LogicalOperator("source", [])
    sink = LogicalOperator("sink", [source])
    assert LogicalPlan(sink).sources() == [source]


def test_sources_multiple_sources():
    source1 = LogicalOperator("source1", [])
    source2 = LogicalOperator("source2", [])
    sink = LogicalOperator("sink", [source1, source2])
    assert LogicalPlan(sink).sources() == [source1, source2]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
