from typing import List, Type

import pytest

from ray.data._internal.logical.interfaces.optimizer import Rule
from ray.data._internal.logical.ruleset import Ruleset


def test_add_rule():
    class A(Rule):
        pass

    ruleset = Ruleset([A])
    assert list(ruleset) == [A]


def test_add_rule_with_dependencies():
    class A(Rule):
        pass

    class B(Rule):
        @classmethod
        def dependencies(cls) -> List[Type[Rule]]:
            return [A]

    ruleset = Ruleset([A])
    ruleset.add(B)
    assert list(ruleset) == [A, B]


def test_add_rule_with_dependents():
    class A(Rule):
        pass

    class B(Rule):
        @classmethod
        def dependents(cls) -> List[Type[Rule]]:
            return [A]

    ruleset = Ruleset([A])
    ruleset.add(B)
    assert list(ruleset) == [B, A]


def test_add_rule_with_multiple_dependencies():
    class A(Rule):
        pass

    class B(Rule):
        pass

    class C(Rule):
        @classmethod
        def dependencies(cls) -> List[Type[Rule]]:
            return [A, B]

    ruleset = Ruleset([A, B])
    ruleset.add(C)

    rules = list(ruleset)
    assert set(rules) == {A, B, C}
    assert rules.index(A) < rules.index(B)
    assert rules.index(B) < rules.index(C)


def test_add_rule_with_multiple_dependents():
    class A(Rule):
        pass

    class B(Rule):
        pass

    class C(Rule):
        @classmethod
        def dependents(cls) -> List[Type[Rule]]:
            return [A, B]

    ruleset = Ruleset([A, B])
    ruleset.add(C)

    rules = list(ruleset)
    assert set(rules) == {A, B, C}
    assert rules[0] == C


def test_add_rule_with_missing_dependencies():
    class A(Rule):
        pass

    class B(Rule):
        @classmethod
        def dependencies(cls) -> List[Type[Rule]]:
            return [A]

    ruleset = Ruleset()
    ruleset.add(B)
    assert list(ruleset) == [B]


def test_add_rule_with_missing_dependents():
    class A(Rule):
        pass

    class B(Rule):
        @classmethod
        def dependents(cls) -> List[Type[Rule]]:
            return [A]

    ruleset = Ruleset()
    ruleset.add(B)
    assert list(ruleset) == [B]


def test_add_rule_with_cycle_raises_error():
    class A(Rule):
        @classmethod
        def dependencies(cls) -> List[Type[Rule]]:
            return [B]

    class B(Rule):
        @classmethod
        def dependencies(cls) -> List[Type[Rule]]:
            return [A]

    ruleset = Ruleset([A])
    with pytest.raises(ValueError):
        ruleset.add(B)


def test_edge_declared_from_both_ends_is_deduped():
    # A must precede B, declared redundantly from both ends. The edge should be
    # recorded once, not double-counted.
    class A(Rule):
        @classmethod
        def dependents(cls) -> List[Type[Rule]]:
            return [B]

    class B(Rule):
        @classmethod
        def dependencies(cls) -> List[Type[Rule]]:
            return [A]

    ruleset = Ruleset([A, B])

    nodes, indegree = ruleset._build_graph()
    node_a = next(n for n in nodes if n.rule is A)
    node_b = next(n for n in nodes if n.rule is B)
    assert indegree[id(node_b)] == 1  # not 2
    assert [n.rule for n in node_a.dependents] == [B]  # not [B, B]
    assert list(ruleset) == [A, B]


def test_disjoint_cycle_with_independent_root_raises_error():
    # An acyclic root (A) alongside a disjoint cycle (B <-> C) must still be
    # detected as a cycle -- the presence of a root must not mask it.
    class A(Rule):
        pass

    class B(Rule):
        @classmethod
        def dependencies(cls) -> List[Type[Rule]]:
            return [C]

    class C(Rule):
        @classmethod
        def dependencies(cls) -> List[Type[Rule]]:
            return [B]

    ruleset = Ruleset([A, B])
    with pytest.raises(ValueError):
        ruleset.add(C)


def test_remove_rule():
    class A(Rule):
        pass

    ruleset = Ruleset([A])
    ruleset.remove(A)
    assert list(ruleset) == []


def test_remove_rule_not_in_ruleset():
    class A(Rule):
        pass

    ruleset = Ruleset([])
    with pytest.raises(ValueError):
        ruleset.remove(A)


def test_remove_rule_with_dependants():
    class A(Rule):
        pass

    class B(Rule):
        @classmethod
        def dependencies(cls) -> List[Type[Rule]]:
            return [A]

    ruleset = Ruleset([A, B])
    ruleset.remove(A)
    assert list(ruleset) == [B]


def test_configure_map_task_memory_rule_dedups_diamond_dag():
    from collections import Counter
    from unittest.mock import MagicMock

    from ray.data._internal.execution.operators.input_data_buffer import (
        InputDataBuffer,
    )
    from ray.data._internal.execution.operators.map_operator import MapOperator
    from ray.data._internal.execution.operators.union_operator import UnionOperator
    from ray.data._internal.logical.interfaces.physical_plan import PhysicalPlan
    from ray.data._internal.logical.rules.configure_map_task_memory import (
        ConfigureMapTaskMemoryRule,
    )
    from ray.data.context import DataContext

    class DummyRule(ConfigureMapTaskMemoryRule):
        def __init__(self):
            self.calls = Counter()
            self.estimate = None

        def estimate_per_task_memory_requirement(self, op):
            self.calls[op] += 1
            return self.estimate

    data_context = DataContext.get_current()
    input_op = InputDataBuffer(data_context, [])
    shared_map = MapOperator.create(
        MagicMock(), input_op=input_op, data_context=data_context
    )
    branch_a = MapOperator.create(
        MagicMock(), input_op=shared_map, data_context=data_context
    )
    branch_b = MapOperator.create(
        MagicMock(), input_op=shared_map, data_context=data_context
    )
    sink = UnionOperator(data_context, branch_a, branch_b)

    plan = PhysicalPlan(sink, {}, data_context)
    rule = DummyRule()
    rule.apply(plan)

    # When no memory estimate is available yet (returns None), "memory" is not
    # populated in dynamic_ray_remote_args. If shared_map._ray_remote_args_fn
    # were wrapped twice across the two diamond branches, a single invocation
    # would call estimate_per_task_memory_requirement twice.
    assert shared_map._ray_remote_args_fn is not None
    assert branch_a._ray_remote_args_fn is not None
    assert branch_b._ray_remote_args_fn is not None
    assert shared_map._ray_remote_args_fn() == {}
    assert rule.calls[shared_map] == 1

    rule.estimate = 1024
    assert shared_map._ray_remote_args_fn() == {"memory": 1024}
    assert rule.calls[shared_map] == 2
    assert branch_a._ray_remote_args_fn() == {"memory": 1024}
    assert rule.calls[branch_a] == 1
    assert branch_b._ray_remote_args_fn() == {"memory": 1024}
    assert rule.calls[branch_b] == 1


def test_set_read_parallelism_rule_dedups_diamond_dag():
    from unittest.mock import MagicMock

    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.logical.interfaces.physical_plan import PhysicalPlan
    from ray.data._internal.logical.operators import Read
    from ray.data._internal.logical.rules.set_read_parallelism import (
        SetReadParallelismRule,
    )
    from ray.data.context import DataContext

    shared_read_phys = MagicMock(spec=PhysicalOperator)
    shared_read_phys.input_dependencies = []
    shared_read_logical = MagicMock(spec=Read)

    branch_a = MagicMock(spec=PhysicalOperator)
    branch_a.input_dependencies = [shared_read_phys]
    branch_b = MagicMock(spec=PhysicalOperator)
    branch_b.input_dependencies = [shared_read_phys]

    sink = MagicMock(spec=PhysicalOperator)
    sink.input_dependencies = [branch_a, branch_b]
    sink_logical = MagicMock()

    op_map = {
        shared_read_phys: shared_read_logical,
        branch_a: sink_logical,
        branch_b: sink_logical,
        sink: sink_logical,
    }
    plan = PhysicalPlan(sink, op_map, DataContext.get_current())

    visited_ops = []

    class TrackingRule(SetReadParallelismRule):
        def _apply(self, plan, op, logical_op):
            visited_ops.append(op)

    rule = TrackingRule()
    rule.apply(plan)

    assert visited_ops == [shared_read_phys]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
