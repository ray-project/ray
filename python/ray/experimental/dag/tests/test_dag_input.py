"""
Tests to ensure ray DAG can correctly mark its input(s) to take user
request, for all DAGNode types.
"""

import pytest
import ray.experimental.dag as ray_dag
from typing import TypeVar

import ray

RayHandleLike = TypeVar("RayHandleLike")


def test_simple_func(shared_ray_instance):
    @ray.remote
    def a(input: str):
        return f"{input} -> a"

    @ray.remote
    def b(a: "RayHandleLike"):
        # At runtime, a is replaced with execution result of a.
        return f"{a} -> b"

    # input -> a - > b -> ouput
    a_node = a._bind(ray_dag.INPUT)
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

    a_ref = a._bind(ray_dag.INPUT)
    b_ref = b._bind(a_ref)
    c_ref = c._bind(a_ref)
    d_ref = d._bind(b_ref, c_ref)
    d1_ref = d._bind(d_ref, d_ref)
    d2_ref = d._bind(d1_ref, d_ref)
    dag = d._bind(d2_ref, d_ref)
    print(dag)

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

    a_ref = a._bind(ray_dag.INPUT)
    b_ref = b._bind(ray_dag.INPUT)
    dag = c._bind(a_ref, b_ref)
    print(dag)

    # (2*2) + (2*1)
    assert ray.get(dag.execute(2)) == 7
    # (3*2) + (3*1)
    assert ray.get(dag.execute(3)) == 10


def test_invalid_input_as_class_constructor(shared_ray_instance):
    @ray.remote
    class Actor:
        def __init__(self, val):
            self.val = val

        def get(self):
            return self.val

    with pytest.raises(
        ValueError, match="INPUT cannot be used as ClassNode args"
    ):
        Actor._bind(ray_dag.INPUT)


def test_invalid_input_conjunction_with_others(shared_ray_instance):
    @ray.remote
    def f(x, y):
        return x + y

    with pytest.raises(
        ValueError,
        match="dag.INPUT cannot be used in conjunction with other args",
    ):
        f._bind(ray_dag.INPUT, 1)

    with pytest.raises(
        ValueError, match="dag.INPUT cannot be used as a kwarg value"
    ):
        f._bind(1, kwarg=ray_dag.INPUT)

    @ray.remote
    class Actor:
        def __init__(self, val):
            self.val = val

        def get(self, input1, input2):
            return self.val + input1 + input2

    actor = Actor._bind(1)
    with pytest.raises(
        ValueError,
        match="dag.INPUT cannot be used in conjunction with other args.",
    ):
        actor.get._bind(ray_dag.INPUT, 2)
    with pytest.raises(
        ValueError, match="dag.INPUT cannot be used as a kwarg value"
    ):
        actor.get._bind(2, input2=ray_dag.INPUT)


def test_invalid_input_as_class_constructor(shared_ray_instance):
    @ray.remote
    class Actor:
        def __init__(self, val):
            self.val = val

        def get(self):
            return self.val

    with pytest.raises(
        ValueError, match="INPUT cannot be used as ClassNode args"
    ):
        Actor._bind(ray_dag.INPUT)


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

    preprocess = FeatureProcessor._bind(0.5)
    feature = preprocess.process._bind(ray_dag.INPUT)
    model = Model._bind(4)
    dag = model.forward._bind(feature)

    print(dag)
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

    m1 = Model._bind(2)
    m2 = Model._bind(3)

    m1_output = m1.forward._bind(ray_dag.INPUT)
    m2_output = m2.forward._bind(ray_dag.INPUT)

    dag = combine._bind(m1_output, m2_output)
    print(dag)
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

    m1 = Model._bind(3)
    m1_output = m1.forward._bind(ray_dag.INPUT)
    m2_output = model_func._bind(ray_dag.INPUT)

    dag = combine._bind(m1_output, m2_output)
    print(dag)
    # 2*3 + 2*2
    assert ray.get(dag.execute(2)) == 10
    # 3*3 + 3*2
    assert ray.get(dag.execute(3)) == 15


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
