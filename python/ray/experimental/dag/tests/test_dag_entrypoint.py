"""
Tests to ensure ray DAG can correctly mark its entrypoint(s) to take user
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
    a_node = a._bind(ray_dag.DAG_ENTRY_POINT)
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

    a_ref = a._bind(ray_dag.DAG_ENTRY_POINT)
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


def test_multi_entrypoint_func_dag(shared_ray_instance):
    @ray.remote
    def a(user_input):
        return user_input * 2

    @ray.remote
    def b(user_input):
        return user_input + 1

    @ray.remote
    def c(x, y):
        return x + y

    a_ref = a._bind(ray_dag.DAG_ENTRY_POINT)
    b_ref = b._bind(ray_dag.DAG_ENTRY_POINT)
    dag = c._bind(a_ref, b_ref)
    print(dag)

    # (2*2) + (2*1)
    assert ray.get(dag.execute(2)) == 7
    # (3*2) + (3*1)
    assert ray.get(dag.execute(3)) == 10


def test_invalid_entrypoint_as_class_constructor(shared_ray_instance):
    @ray.remote
    class Actor:
        def __init__(self, val):
            self.val = val

        def get(self):
            return self.val

    with pytest.raises(
        ValueError, match="DAG_ENTRY_POINT cannot be used as ClassNode args"
    ):
        dag = Actor._bind(ray_dag.DAG_ENTRY_POINT)


def test_class_method_entrypoint(shared_ray_instance):
    pass


def test_multi_class_method_entrypoint(shared_ray_instance):
    pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
