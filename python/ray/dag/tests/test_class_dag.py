import pytest
import pickle

import ray
from ray.dag import (
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

    a1 = Actor.bind(10)
    res = a1.get.bind()
    print(res)
    assert ray.get(res.execute()) == 10

    a2 = Actor.bind(10)
    a1.inc.bind(2)
    a1.inc.bind(4)
    a2.inc.bind(6)
    dag = combine.bind(a1.get.bind(), a2.get.bind())

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

    outer = OuterActor.bind(Actor.bind(10))
    outer.inc.bind(2)
    dag = outer.get.bind()
    print(dag)
    assert ray.get(dag.execute()) == 12


def test_class_as_function_constructor_arg(shared_ray_instance):
    @ray.remote
    def f(actor_handle):
        return ray.get(actor_handle.get.remote())

    dag = f.bind(Actor.bind(10))
    print(dag)
    assert ray.get(dag.execute()) == 10


def test_basic_actor_dag_constructor_options(shared_ray_instance):
    a1 = Actor.bind(10)
    dag = a1.get.bind()
    print(dag)
    assert ray.get(dag.execute()) == 10

    a1 = Actor.options(name="Actor", namespace="test", max_pending_calls=10).bind(10)
    dag = a1.get.bind()
    print(dag)
    # Ensure execution result is identical with .options() in init()
    assert ray.get(dag.execute()) == 10
    # Ensure options are passed in
    assert a1.get_options().get("name") == "Actor"
    assert a1.get_options().get("namespace") == "test"
    assert a1.get_options().get("max_pending_calls") == 10


def test_actor_method_options(shared_ray_instance):
    a1 = Actor.bind(10)
    dag = a1.get.options(name="actor_method_options").bind()
    print(dag)
    assert ray.get(dag.execute()) == 10
    assert dag.get_options().get("name") == "actor_method_options"


def test_basic_actor_dag_constructor_invalid_options(shared_ray_instance):
    with pytest.raises(
        ValueError, match=r".*only accepts None, 0 or a positive number.*"
    ):
        a1 = Actor.options(num_cpus=-1).bind(10)
        invalid_dag = a1.get.bind()
        ray.get(invalid_dag.execute())


def test_actor_options_complicated(shared_ray_instance):
    """Test a more complicated setup where we apply .options() in both
    constructor and method call with overlapping keys, and ensure end to end
    options correctness.
    """

    @ray.remote
    def combine(x, y):
        return x + y

    a1 = Actor.options(name="a1_v0").bind(10)
    res = a1.get.options(name="v1").bind()
    print(res)
    assert ray.get(res.execute()) == 10
    assert a1.get_options().get("name") == "a1_v0"
    assert res.get_options().get("name") == "v1"

    a1 = Actor.options(name="a1_v1").bind(10)  # Cannot
    a2 = Actor.options(name="a2_v0").bind(10)
    a1.inc.options(name="v1").bind(2)
    a1.inc.options(name="v2").bind(4)
    a2.inc.options(name="v3").bind(6)
    dag = combine.options(name="v4").bind(a1.get.bind(), a2.get.bind())

    print(dag)
    assert ray.get(dag.execute()) == 32
    test_a1 = dag.get_args()[0]  # call graph for a1.get.bind()
    test_a2 = dag.get_args()[1]  # call graph for a2.get.bind()
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

    a1 = Actor.bind()
    dag = caller.bind(a1)
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

    m1 = Model.bind("Even: ")
    m2 = Model.bind("Odd: ")
    selection = ModelSelection.bind()

    even_input = pipeline.bind(20, m1, m2, selection)
    print(even_input)
    assert ray.get(even_input.execute()) == "Even: 20"

    odd_input = pipeline.bind(21, m1, m2, selection)
    print(odd_input)
    assert ray.get(odd_input.execute()) == "Odd: 21"


def test_unsupported_bind():
    @ray.remote
    class Actor:
        def ping(self):
            return "hello"

    with pytest.raises(
        AttributeError,
        match=r"\.bind\(\) cannot be used again on",
    ):
        actor = Actor.bind()
        _ = actor.bind()

    with pytest.raises(
        AttributeError,
        match=r"\.remote\(\) cannot be used on ClassMethodNodes",
    ):
        actor = Actor.bind()
        _ = actor.ping.remote()


def test_unsupported_remote():
    @ray.remote
    class Actor:
        def ping(self):
            return "hello"

    with pytest.raises(AttributeError, match="'Actor' has no attribute 'remote'"):
        _ = Actor.bind().remote()

    @ray.remote
    def func():
        return 1

    with pytest.raises(AttributeError, match=r"\.remote\(\) cannot be used on"):
        _ = func.bind().remote()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
