from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan
from ray.data.context import DataContext


def test_sources_singleton():
    ctx = DataContext.get_current()

    source = LogicalOperator([], name="source")
    assert LogicalPlan(source, ctx).sources() == [source]


def test_sources_chain():
    ctx = DataContext.get_current()

    source = LogicalOperator([], name="source")
    sink = LogicalOperator([source], name="sink")
    assert LogicalPlan(sink, ctx).sources() == [source]


def test_sources_multiple_sources():
    ctx = DataContext.get_current()

    source1 = LogicalOperator([], name="source1")
    source2 = LogicalOperator([], name="source2")
    sink = LogicalOperator([source1, source2], name="sink")
    assert LogicalPlan(sink, ctx).sources() == [source1, source2]


def test_logical_operator_defaults_name_to_class_name():
    op = LogicalOperator([])
    assert op.name == "LogicalOperator"
    assert op.dag_str == "LogicalOperator[LogicalOperator]"


def test_logical_operator_does_not_track_output_dependencies():
    source = LogicalOperator([], name="source")
    sink = LogicalOperator([source], name="sink")

    # Logical operators should not maintain reverse output-dependency state.
    assert not hasattr(source, "_output_dependencies")
    assert not hasattr(sink, "_output_dependencies")

    transformed = sink._apply_transform(lambda op: op)
    assert transformed is sink
    assert not hasattr(source, "_output_dependencies")
    assert not hasattr(sink, "_output_dependencies")


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
