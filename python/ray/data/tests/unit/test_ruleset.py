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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
