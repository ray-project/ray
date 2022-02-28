import pytest
import pickle

import ray
from ray.experimental.dag import (
    DAGNode,
    PARENT_CLASS_NODE_KEY,
    PREV_CLASS_METHOD_CALL_KEY,
)


@ray.remote
class Counter:
    def __init__(self, init_value=0):
        self.i = init_value

    def inc(self):
        self.i += 1

    def get(self):
        return self.i


@ray.remote
class Actor:
    def __init__(self, init_value):
        self.i = init_value

    def inc(self, x):
        self.i += x

    def get(self):
        return self.i


def test_serialize_warning():
    node = DAGNode([], {}, {}, {})
    with pytest.raises(ValueError):
        pickle.dumps(node)


def test_basic_actor_dag(shared_ray_instance):
    @ray.remote
    def combine(x, y):
        return x + y

    a1 = Actor._bind(10)
    res = a1.get._bind()
    print(res)
    assert ray.get(res.execute()) == 10

    a2 = Actor._bind(10)
    a1.inc._bind(2)
    a1.inc._bind(4)
    a2.inc._bind(6)
    dag = combine._bind(a1.get._bind(), a2.get._bind())

    print(dag)
    assert ray.get(dag.execute()) == 32


def test_class_as_class_constructor_arg(shared_ray_instance):
    @ray.remote
    class OuterActor:
        def __init__(self, inner_actor):
            self.inner_actor = inner_actor

        def inc(self, x):
            self.inner_actor.inc.remote(x)

        def get(self):
            return ray.get(self.inner_actor.get.remote())

    outer = OuterActor._bind(Actor._bind(10))
    outer.inc._bind(2)
    dag = outer.get._bind()
    print(dag)
    assert ray.get(dag.execute()) == 12


def test_class_as_function_constructor_arg(shared_ray_instance):
    @ray.remote
    def f(actor_handle):
        return ray.get(actor_handle.get.remote())

    dag = f._bind(Actor._bind(10))
    print(dag)
    assert ray.get(dag.execute()) == 10


def test_basic_actor_dag_constructor_options(shared_ray_instance):
    a1 = Actor._bind(10)
    dag = a1.get._bind()
    print(dag)
    assert ray.get(dag.execute()) == 10

    a1 = Actor.options(name="Actor", namespace="test", max_pending_calls=10)._bind(10)
    dag = a1.get._bind()
    print(dag)
    # Ensure execution result is identical with .options() in init()
    assert ray.get(dag.execute()) == 10
    # Ensure options are passed in
    assert a1.get_options().get("name") == "Actor"
    assert a1.get_options().get("namespace") == "test"
    assert a1.get_options().get("max_pending_calls") == 10


def test_actor_method_options(shared_ray_instance):
    a1 = Actor._bind(10)
    dag = a1.get.options(name="actor_method_options")._bind()
    print(dag)
    assert ray.get(dag.execute()) == 10
    assert dag.get_options().get("name") == "actor_method_options"


def test_basic_actor_dag_constructor_invalid_options(shared_ray_instance):
    a1 = Actor.options(num_cpus=-1)._bind(10)
    invalid_dag = a1.get._bind()
    with pytest.raises(ValueError, match=".*Resource quantities may not be negative.*"):
        ray.get(invalid_dag.execute())


def test_actor_options_complicated(shared_ray_instance):
    """Test a more complicated setup where we apply .options() in both
    constructor and method call with overlapping keys, and ensure end to end
    options correctness.
    """

    @ray.remote
    def combine(x, y):
        return x + y

    a1 = Actor.options(name="a1_v0")._bind(10)
    res = a1.get.options(name="v1")._bind()
    print(res)
    assert ray.get(res.execute()) == 10
    assert a1.get_options().get("name") == "a1_v0"
    assert res.get_options().get("name") == "v1"

    a1 = Actor.options(name="a1_v1")._bind(10)  # Cannot
    a2 = Actor.options(name="a2_v0")._bind(10)
    a1.inc.options(name="v1")._bind(2)
    a1.inc.options(name="v2")._bind(4)
    a2.inc.options(name="v3")._bind(6)
    dag = combine.options(name="v4")._bind(a1.get._bind(), a2.get._bind())

    print(dag)
    assert ray.get(dag.execute()) == 32
    test_a1 = dag.get_args()[0]  # call graph for a1.get._bind()
    test_a2 = dag.get_args()[1]  # call graph for a2.get._bind()
    assert test_a2.get_options() == {}  # No .options() at outer call
    # refer to a2 constructor .options() call
    assert (
        test_a2.get_other_args_to_resolve()[PARENT_CLASS_NODE_KEY]
        .get_options()
        .get("name")
        == "a2_v0"
    )
    # refer to actor method a2.inc.options() call
    assert (
        test_a2.get_other_args_to_resolve()[PREV_CLASS_METHOD_CALL_KEY]
        .get_options()
        .get("name")
        == "v3"
    )
    # refer to a1 constructor .options() call
    assert (
        test_a1.get_other_args_to_resolve()[PARENT_CLASS_NODE_KEY]
        .get_options()
        .get("name")
        == "a1_v1"
    )
    # refer to latest actor method a1.inc.options() call
    assert (
        test_a1.get_other_args_to_resolve()[PREV_CLASS_METHOD_CALL_KEY]
        .get_options()
        .get("name")
        == "v2"
    )
    # refer to first bound actor method a1.inc.options() call
    assert (
        test_a1.get_other_args_to_resolve()[PREV_CLASS_METHOD_CALL_KEY]
        .get_other_args_to_resolve()[PREV_CLASS_METHOD_CALL_KEY]
        .get_options()
        .get("name")
        == "v1"
    )


def test_pass_actor_handle(shared_ray_instance):
    @ray.remote
    class Actor:
        def ping(self):
            return "hello"

    @ray.remote
    def caller(handle):
        assert isinstance(handle, ray.actor.ActorHandle), handle
        return ray.get(handle.ping.remote())

    a1 = Actor._bind()
    dag = caller._bind(a1)
    print(dag)
    assert ray.get(dag.execute()) == "hello"


def test_dynamic_pipeline(shared_ray_instance):
    @ray.remote
    class Model:
        def __init__(self, arg):
            self.arg = arg

        def forward(self, x):
            return self.arg + str(x)

    @ray.remote
    class ModelSelection:
        def is_even(self, x):
            return x % 2 == 0

    @ray.remote
    def pipeline(x, m1, m2, selection):
        sel = selection.is_even.remote(x)
        if ray.get(sel):
            result = m1.forward.remote(x)
        else:
            result = m2.forward.remote(x)
        return ray.get(result)

    m1 = Model._bind("Even: ")
    m2 = Model._bind("Odd: ")
    selection = ModelSelection._bind()

    even_input = pipeline._bind(20, m1, m2, selection)
    print(even_input)
    assert ray.get(even_input.execute()) == "Even: 20"

    odd_input = pipeline._bind(21, m1, m2, selection)
    print(odd_input)
    assert ray.get(odd_input.execute()) == "Odd: 21"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
