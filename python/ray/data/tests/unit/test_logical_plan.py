from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan
from ray.data.context import DataContext


class DummyLogicalOperator(LogicalOperator):
    @property
    def num_outputs(self):
        return self._num_outputs


def test_sources_singleton():
    ctx = DataContext.get_current()

    source = DummyLogicalOperator([], name="source")
    assert LogicalPlan(source, ctx).sources() == [source]


def test_sources_chain():
    ctx = DataContext.get_current()

    source = DummyLogicalOperator([], name="source")
    sink = DummyLogicalOperator([source], name="sink")
    assert LogicalPlan(sink, ctx).sources() == [source]


def test_sources_multiple_sources():
    ctx = DataContext.get_current()

    source1 = DummyLogicalOperator([], name="source1")
    source2 = DummyLogicalOperator([], name="source2")
    sink = DummyLogicalOperator([source1, source2], name="sink")
    assert LogicalPlan(sink, ctx).sources() == [source1, source2]


def test_logical_operator_defaults_name_to_class_name():
    op = DummyLogicalOperator([])
    assert op.name == "DummyLogicalOperator"
    assert op.dag_str == "DummyLogicalOperator[DummyLogicalOperator]"


def test_logical_operator_does_not_track_output_dependencies():
    source = DummyLogicalOperator([], name="source")
    sink = DummyLogicalOperator([source], name="sink")

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
