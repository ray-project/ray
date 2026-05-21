from typing import Optional

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan
from ray.data._internal.logical.rules.limit_pushdown import LimitPushdownRule
from ray.data._internal.logical.rules.predicate_pushdown import PredicatePushdown
from ray.data.context import DataContext


class DummyLogicalOperator(LogicalOperator):
    _name: Optional[str]
    _num_outputs: Optional[int]

    def __init__(self, input_dependencies, name=None, num_outputs=None):
        super().__init__(
            input_dependencies=input_dependencies,
        )
        object.__setattr__(self, "_name", name)
        object.__setattr__(self, "_num_outputs", num_outputs)

    @property
    def name(self):
        return self._name or self.__class__.__name__

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


def test_logical_operator_transform_supports_custom_subclasses():
    source = DummyLogicalOperator([], name="source")
    replacement = DummyLogicalOperator([], name="replacement")
    sink = DummyLogicalOperator([source], name="sink")

    transformed = sink._apply_transform(lambda op: replacement if op is source else op)

    assert transformed is not sink
    assert transformed.name == "sink"
    assert transformed.input_dependencies == [replacement]
    assert sink.input_dependencies == [source]


def test_rule_copy_fallback_supports_custom_subclasses():
    source = DummyLogicalOperator([], name="source")
    replacement = DummyLogicalOperator([], name="replacement")
    sink = DummyLogicalOperator([source], name="sink")

    predicate_result = PredicatePushdown._clone_op_with_new_inputs(sink, [replacement])
    assert predicate_result is not sink
    assert predicate_result.name == "sink"
    assert predicate_result.input_dependencies == [replacement]
    assert sink.input_dependencies == [source]

    limit_result = LimitPushdownRule()._recreate_operator_with_new_input(
        sink, replacement
    )
    assert limit_result is not sink
    assert limit_result.name == "sink"
    assert limit_result.input_dependencies == [replacement]
    assert sink.input_dependencies == [source]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
