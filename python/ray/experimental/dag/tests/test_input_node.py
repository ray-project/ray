"""
Tests to ensure ray DAG can correctly mark its input(s) to take user
request, for all DAGNode types.
"""

import pytest
from ray.experimental.dag.input_node import InputNode
from typing import TypeVar

import ray

RayHandleLike = TypeVar("RayHandleLike")


def test_no_args_to_input_node(shared_ray_instance):
    @ray.remote
    def f(input):
        return input

    with pytest.raises(
        ValueError, match="InputNode should not take any args or kwargs"
    ):
        with InputNode(0) as dag_input:
            f._bind(dag_input)
    with pytest.raises(
        ValueError,
        match="InputNode should not take any args or kwargs",
    ):
        with InputNode(key=1) as dag_input:
            f._bind(dag_input)


def test_simple_func(shared_ray_instance):
    @ray.remote
    def a(input: str):
        return f"{input} -> a"

    @ray.remote
    def b(a: "RayHandleLike"):
        # At runtime, a is replaced with execution result of a.
        return f"{a} -> b"

    # input -> a - > b -> ouput
    with InputNode() as dag_input:
        a_node = a._bind(dag_input)
        dag = b._bind(a_node)

    assert ray.get(dag.execute("input")) == "input -> a -> b"
    assert ray.get(dag.execute("test")) == "test -> a -> b"


def test_func_dag(shared_ray_instance):
    @ray.remote
    def a(user_input):
        return user_input

    @ray.remote
    def b(x):
        return x * 2

    @ray.remote
    def c(x):
        return x + 1

    @ray.remote
    def d(x, y):
        return x + y

    with InputNode() as dag_input:
        a_ref = a._bind(dag_input)
        b_ref = b._bind(a_ref)
        c_ref = c._bind(a_ref)
        d_ref = d._bind(b_ref, c_ref)
        d1_ref = d._bind(d_ref, d_ref)
        d2_ref = d._bind(d1_ref, d_ref)
        dag = d._bind(d2_ref, d_ref)

    # [(2*2 + 2+1) + (2*2 + 2+1)] + [(2*2 + 2+1) + (2*2 + 2+1)]
    assert ray.get(dag.execute(2)) == 28
    # [(3*2 + 3+1) + (3*2 + 3+1)] + [(3*2 + 3+1) + (3*2 + 3+1)]
    assert ray.get(dag.execute(3)) == 40


def test_multi_input_func_dag(shared_ray_instance):
    @ray.remote
    def a(user_input):
        return user_input * 2

    @ray.remote
    def b(user_input):
        return user_input + 1

    @ray.remote
    def c(x, y):
        return x + y

    with InputNode() as dag_input:
        a_ref = a._bind(dag_input)
        b_ref = b._bind(dag_input)
        dag = c._bind(a_ref, b_ref)

    # (2*2) + (2*1)
    assert ray.get(dag.execute(2)) == 7
    # (3*2) + (3*1)
    assert ray.get(dag.execute(3)) == 10


def test_invalid_input_node_as_class_constructor(shared_ray_instance):
    @ray.remote
    class Actor:
        def __init__(self, val):
            self.val = val

        def get(self):
            return self.val

    with pytest.raises(
        ValueError,
        match=(
            "InputNode handles user dynamic input the the DAG, and "
            "cannot be used as args, kwargs, or other_args_to_resolve "
            "in ClassNode constructor because it is not available at "
            "class construction or binding time."
        ),
    ):
        with InputNode() as dag_input:
            Actor._bind(dag_input)


def test_class_method_input(shared_ray_instance):
    @ray.remote
    class Model:
        def __init__(self, weight: int):
            self.weight = weight

        def forward(self, input: "RayHandleLike"):
            return self.weight * input

    @ray.remote
    class FeatureProcessor:
        def __init__(self, scale):
            self.scale = scale

        def process(self, input: int):
            return input * self.scale

    with InputNode() as dag_input:
        preprocess = FeatureProcessor._bind(0.5)
        feature = preprocess.process._bind(dag_input)
        model = Model._bind(4)
        dag = model.forward._bind(feature)

    # 2 * 0.5 * 4
    assert ray.get(dag.execute(2)) == 4
    # 6 * 0.5 * 4
    assert ray.get(dag.execute(6)) == 12


def test_multi_class_method_input(shared_ray_instance):
    """
    Test a multiple class methods can all be used as inputs in a dag.
    """

    @ray.remote
    class Model:
        def __init__(self, weight: int):
            self.weight = weight

        def forward(self, input: int):
            return self.weight * input

    @ray.remote
    def combine(m1: "RayHandleLike", m2: "RayHandleLike"):
        return m1 + m2

    with InputNode() as dag_input:
        m1 = Model._bind(2)
        m2 = Model._bind(3)

        m1_output = m1.forward._bind(dag_input)
        m2_output = m2.forward._bind(dag_input)

        dag = combine._bind(m1_output, m2_output)

    # 1*2 + 1*3
    assert ray.get(dag.execute(1)) == 5
    # 2*2 + 2*3
    assert ray.get(dag.execute(2)) == 10


def test_func_class_mixed_input(shared_ray_instance):
    """
    Test both class method and function are used as input in the
    same dag.
    """

    @ray.remote
    class Model:
        def __init__(self, weight: int):
            self.weight = weight

        def forward(self, input: int):
            return self.weight * input

    @ray.remote
    def model_func(input: int):
        return input * 2

    @ray.remote
    def combine(m1: "RayHandleLike", m2: "RayHandleLike"):
        return m1 + m2

    with InputNode() as dag_input:
        m1 = Model._bind(3)
        m1_output = m1.forward._bind(dag_input)
        m2_output = model_func._bind(dag_input)

    dag = combine._bind(m1_output, m2_output)
    # 2*3 + 2*2
    assert ray.get(dag.execute(2)) == 10
    # 3*3 + 3*2
    assert ray.get(dag.execute(3)) == 15


def test_input_attr_partial_access(shared_ray_instance):
    @ray.remote
    class Model:
        def __init__(self, weight: int):
            self.weight = weight

        def forward(self, input: int):
            return self.weight * input

    @ray.remote
    def combine(a, b, c, d=None):
        if not d:
            return a + b + c
        else:
            return a + b + c + d["deep"]["nested"]

    # Same DAG, but with attribute accessor and keyword input
    with InputNode() as dag_input:
        m1 = Model._bind(1)
        m2 = Model._bind(2)
        m1_output = m1.forward._bind(dag_input[0])
        m2_output = m2.forward._bind(dag_input[1])
        dag = combine._bind(m1_output, m2_output, dag_input.m3, dag_input.m4)
    # 1*1 + 2*2 + 3 + 4 = 12
    assert ray.get(dag.execute(1, 2, m3=3, m4={"deep": {"nested": 4}})) == 12

    with pytest.raises(
        AssertionError,
        match="Please only use int index or str as first-level key",
    ):
        with InputNode() as dag_input:
            m1 = Model._bind(1)
            dag = m1.forward._bind(dag_input[(1, 2)])


def test_ensure_in_context_manager(shared_ray_instance):
    # No enforcement on creation given __enter__ executes after __init__
    input = InputNode()
    with pytest.raises(
        AssertionError,
        match=(
            "InputNode is a singleton instance that should be only used "
            "in context manager"
        ),
    ):
        input.execute()

    @ray.remote
    def f(input):
        return input

    # No enforcement on creation given __enter__ executes after __init__
    dag = f._bind(InputNode())
    with pytest.raises(
        AssertionError,
        match=(
            "InputNode is a singleton instance that should be only used "
            "in context manager"
        ),
    ):
        dag.execute()


def test_ensure_input_node_singleton(shared_ray_instance):
    @ray.remote
    def f(input):
        return input

    @ray.remote
    def combine(a, b):
        return a + b

    with InputNode() as input_1:
        a = f._bind(input_1)
    with InputNode() as input_2:
        b = f._bind(input_2)
        dag = combine._bind(a, b)

    with pytest.raises(
        AssertionError, match="Each DAG should only have one unique InputNode"
    ):
        _ = ray.get(dag.execute(2))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
