"""
Tests to ensure ray DAG can correctly mark its input(s) to take user
request, for all DAGNode types.
"""

import pytest
from ray.dag.dag_node import DAGNode
from ray.dag.input_node import InputNode
from typing import Any, TypeVar

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
            f.bind(dag_input)
    with pytest.raises(
        ValueError,
        match="InputNode should not take any args or kwargs",
    ):
        with InputNode(key=1) as dag_input:
            f.bind(dag_input)


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
        a_node = a.bind(dag_input)
        dag = b.bind(a_node)

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
        a_ref = a.bind(dag_input)
        b_ref = b.bind(a_ref)
        c_ref = c.bind(a_ref)
        d_ref = d.bind(b_ref, c_ref)
        d1_ref = d.bind(d_ref, d_ref)
        d2_ref = d.bind(d1_ref, d_ref)
        dag = d.bind(d2_ref, d_ref)

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
        a_ref = a.bind(dag_input)
        b_ref = b.bind(dag_input)
        dag = c.bind(a_ref, b_ref)

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
            Actor.bind(dag_input)


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
        preprocess = FeatureProcessor.bind(0.5)
        feature = preprocess.process.bind(dag_input)
        model = Model.bind(4)
        dag = model.forward.bind(feature)

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
        m1 = Model.bind(2)
        m2 = Model.bind(3)

        m1_output = m1.forward.bind(dag_input)
        m2_output = m2.forward.bind(dag_input)

        dag = combine.bind(m1_output, m2_output)

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
        m1 = Model.bind(3)
        m1_output = m1.forward.bind(dag_input)
        m2_output = model_func.bind(dag_input)

    dag = combine.bind(m1_output, m2_output)
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

    # 1) Test default wrapping of args and kwargs into internal python object
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[1])
        dag = combine.bind(m1_output, m2_output, dag_input.m3, dag_input.m4)
    # 1*1 + 2*2 + 3 + 4 = 12
    assert ray.get(dag.execute(1, 2, m3=3, m4={"deep": {"nested": 4}})) == 12

    # 2) Test user passed data object as only input to the dag.execute()
    class UserDataObj:
        user_object_field_0: Any
        user_object_field_1: Any
        field_3: Any

        def __init__(
            self, user_object_field_0: Any, user_object_field_1: Any, field_3: Any
        ) -> None:
            self.user_object_field_0 = user_object_field_0
            self.user_object_field_1 = user_object_field_1
            self.field_3 = field_3

    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input.user_object_field_0)
        m2_output = m2.forward.bind(dag_input.user_object_field_1)
        dag = combine.bind(m1_output, m2_output, dag_input.field_3)

    # 1*1 + 2*2 + 3
    assert ray.get(dag.execute(UserDataObj(1, 2, 3))) == 8

    # 3) Test user passed only one list object with regular list index accessor
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[1])
        dag = combine.bind(m1_output, m2_output, dag_input[2])
    # 1*1 + 2*2 + 3 + 4 = 12
    assert ray.get(dag.execute([1, 2, 3])) == 8

    # 4) Test user passed only one dict object with key str accessor
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input["m1"])
        m2_output = m2.forward.bind(dag_input["m2"])
        dag = combine.bind(m1_output, m2_output, dag_input["m3"])
    # 1*1 + 2*2 + 3 + 4 = 12
    assert ray.get(dag.execute({"m1": 1, "m2": 2, "m3": 3})) == 8

    with pytest.raises(
        AssertionError,
        match="Please only use int index or str as first-level key",
    ):
        with InputNode() as dag_input:
            m1 = Model.bind(1)
            dag = m1.forward.bind(dag_input[(1, 2)])


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
    dag = f.bind(InputNode())
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
        a = f.bind(input_1)
    with InputNode() as input_2:
        b = f.bind(input_2)
        dag = combine.bind(a, b)

    with pytest.raises(
        AssertionError, match="Each DAG should only have one unique InputNode"
    ):
        _ = ray.get(dag.execute(2))


def test_apply_recursive_caching(shared_ray_instance):
    @ray.remote
    def f(input):
        return input

    input = InputNode()
    f_node = f.bind(input)

    a, b = input, f_node
    for _ in range(10):
        a, b = f.bind(a, b), f.bind(a, b)

    counter = 0
    original_apply_recursive = DAGNode.apply_recursive

    def _apply_recursive_with_counter(self, fn):
        nonlocal counter
        counter += 1
        return original_apply_recursive(self, fn)

    DAGNode.apply_recursive = _apply_recursive_with_counter

    a.apply_recursive(lambda node: node)

    # Prior to #40337; count was 2559
    assert counter == 40
    DAGNode.apply_recursive = original_apply_recursive


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
