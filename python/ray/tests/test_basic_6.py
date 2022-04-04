# coding: utf-8
import logging
import pickle
import sys
import time

import numpy as np
import pytest

import ray.cluster_utils
from ray._private.test_utils import (
    client_test_enabled,
    get_error_message,
    SignalActor,
)

import ray

logger = logging.getLogger(__name__)


def test_wait_timing(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    def f():
        time.sleep(1)

    future = f.remote()

    start = time.time()
    ready, not_ready = ray.wait([future], timeout=0.2)
    assert 0.2 < time.time() - start < 0.3
    assert len(ready) == 0
    assert len(not_ready) == 1


@pytest.mark.skipif(client_test_enabled(), reason="internal _raylet")
def test_function_descriptor():
    python_descriptor = ray._raylet.PythonFunctionDescriptor(
        "module_name", "function_name", "class_name", "function_hash"
    )
    python_descriptor2 = pickle.loads(pickle.dumps(python_descriptor))
    assert python_descriptor == python_descriptor2
    assert hash(python_descriptor) == hash(python_descriptor2)
    assert python_descriptor.function_id == python_descriptor2.function_id
    java_descriptor = ray._raylet.JavaFunctionDescriptor(
        "class_name", "function_name", "signature"
    )
    java_descriptor2 = pickle.loads(pickle.dumps(java_descriptor))
    assert java_descriptor == java_descriptor2
    assert python_descriptor != java_descriptor
    assert python_descriptor != object()
    d = {python_descriptor: 123}
    assert d.get(python_descriptor2) == 123


def test_ray_options(shutdown_only):
    ray.init(num_cpus=10, num_gpus=10, resources={"custom1": 2})

    @ray.remote(num_cpus=2, num_gpus=3, memory=150 * 2 ** 20, resources={"custom1": 1})
    def foo(expected_resources):
        # Possibly wait until the available resources have been updated
        # (there might be a delay due to heartbeats)
        retries = 10
        keys = ["CPU", "GPU", "custom1"]
        while retries >= 0:
            resources = ray.available_resources()
            do_return = True
            for key in keys:
                if resources[key] != expected_resources[key]:
                    print(key, resources[key], expected_resources[key])
                    do_return = False
                    break
            if do_return:
                return resources["memory"]
            time.sleep(0.1)
            retries -= 1
        raise RuntimeError("Number of retries exceeded")

    expected_resources_without_options = {"CPU": 8.0, "GPU": 7.0, "custom1": 1.0}
    memory_available_without_options = ray.get(
        foo.remote(expected_resources_without_options)
    )

    expected_resources_with_options = {"CPU": 7.0, "GPU": 6.0, "custom1": 1.5}
    memory_available_with_options = ray.get(
        foo.options(
            num_cpus=3, num_gpus=4, memory=50 * 2 ** 20, resources={"custom1": 0.5}
        ).remote(expected_resources_with_options)
    )

    assert memory_available_without_options < memory_available_with_options


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "num_cpus": 0,
            "object_store_memory": 75 * 1024 * 1024,
            "_system_config": {"automatic_object_spilling_enabled": False},
        }
    ],
    indirect=True,
)
def test_fetch_local(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    cluster.add_node(num_cpus=2, object_store_memory=75 * 1024 * 1024)
    signal_actor = SignalActor.remote()

    @ray.remote
    def put():
        ray.wait([signal_actor.wait.remote()])
        return np.random.rand(5 * 1024 * 1024)  # 40 MB data

    local_ref = ray.put(np.random.rand(5 * 1024 * 1024))
    remote_ref = put.remote()
    # Data is not ready in any node
    (ready_ref, remaining_ref) = ray.wait([remote_ref], timeout=2, fetch_local=False)
    assert (0, 1) == (len(ready_ref), len(remaining_ref))
    ray.wait([signal_actor.send.remote()])

    # Data is ready in some node, but not local node.
    (ready_ref, remaining_ref) = ray.wait([remote_ref], fetch_local=False)
    assert (1, 0) == (len(ready_ref), len(remaining_ref))
    (ready_ref, remaining_ref) = ray.wait([remote_ref], timeout=2, fetch_local=True)
    assert (0, 1) == (len(ready_ref), len(remaining_ref))
    del local_ref
    (ready_ref, remaining_ref) = ray.wait([remote_ref], fetch_local=True)
    assert (1, 0) == (len(ready_ref), len(remaining_ref))


def test_nested_functions(ray_start_shared_local_modes):
    # Make sure that remote functions can use other values that are defined
    # after the remote function but before the first function invocation.
    @ray.remote
    def f():
        return g(), ray.get(h.remote())

    def g():
        return 1

    @ray.remote
    def h():
        return 2

    assert ray.get(f.remote()) == (1, 2)

    # Test a remote function that recursively calls itself.

    @ray.remote
    def factorial(n):
        if n == 0:
            return 1
        return n * ray.get(factorial.remote(n - 1))

    assert ray.get(factorial.remote(0)) == 1
    assert ray.get(factorial.remote(1)) == 1
    assert ray.get(factorial.remote(2)) == 2
    assert ray.get(factorial.remote(3)) == 6
    assert ray.get(factorial.remote(4)) == 24
    assert ray.get(factorial.remote(5)) == 120


def test_mutually_recursive_functions(ray_start_shared_local_modes):
    # Test remote functions that recursively call each other.
    @ray.remote
    def factorial_even(n):
        assert n % 2 == 0
        if n == 0:
            return 1
        return n * ray.get(factorial_odd.remote(n - 1))

    @ray.remote
    def factorial_odd(n):
        assert n % 2 == 1
        return n * ray.get(factorial_even.remote(n - 1))

    assert ray.get(factorial_even.remote(4)) == 24
    assert ray.get(factorial_odd.remote(5)) == 120


def test_ray_recursive_objects(ray_start_shared_local_modes):
    class ClassA:
        pass

    # Make a list that contains itself.
    lst = []
    lst.append(lst)
    # Make an object that contains itself as a field.
    a1 = ClassA()
    a1.field = a1
    # Make two objects that contain each other as fields.
    a2 = ClassA()
    a3 = ClassA()
    a2.field = a3
    a3.field = a2
    # Make a dictionary that contains itself.
    d1 = {}
    d1["key"] = d1
    # Create a list of recursive objects.
    recursive_objects = [lst, a1, a2, a3, d1]

    # Serialize the recursive objects.
    for obj in recursive_objects:
        ray.put(obj)


def test_passing_arguments_by_value_out_of_the_box(ray_start_shared_local_modes):
    @ray.remote
    def f(x):
        return x

    # Test passing lambdas.

    def temp():
        return 1

    assert ray.get(f.remote(temp))() == 1
    assert ray.get(f.remote(lambda x: x + 1))(3) == 4

    # Test sets.
    assert ray.get(f.remote(set())) == set()
    s = {1, (1, 2, "hi")}
    assert ray.get(f.remote(s)) == s

    # Test types.
    assert ray.get(f.remote(int)) == int
    assert ray.get(f.remote(float)) == float
    assert ray.get(f.remote(str)) == str

    class Foo:
        def __init__(self):
            pass

    # Make sure that we can put and get a custom type. Note that the result
    # won't be "equal" to Foo.
    ray.get(ray.put(Foo))


def test_putting_object_that_closes_over_object_ref(ray_start_shared_local_modes):
    # This test is here to prevent a regression of
    # https://github.com/ray-project/ray/issues/1317.

    class Foo:
        def __init__(self):
            self.val = ray.put(0)

        def method(self):
            f

    f = Foo()
    ray.put(f)


def test_keyword_args(ray_start_shared_local_modes):
    @ray.remote
    def keyword_fct1(a, b="hello"):
        return "{} {}".format(a, b)

    @ray.remote
    def keyword_fct2(a="hello", b="world"):
        return "{} {}".format(a, b)

    @ray.remote
    def keyword_fct3(a, b, c="hello", d="world"):
        return "{} {} {} {}".format(a, b, c, d)

    x = keyword_fct1.remote(1)
    assert ray.get(x) == "1 hello"
    x = keyword_fct1.remote(1, "hi")
    assert ray.get(x) == "1 hi"
    x = keyword_fct1.remote(1, b="world")
    assert ray.get(x) == "1 world"
    x = keyword_fct1.remote(a=1, b="world")
    assert ray.get(x) == "1 world"

    x = keyword_fct2.remote(a="w", b="hi")
    assert ray.get(x) == "w hi"
    x = keyword_fct2.remote(b="hi", a="w")
    assert ray.get(x) == "w hi"
    x = keyword_fct2.remote(a="w")
    assert ray.get(x) == "w world"
    x = keyword_fct2.remote(b="hi")
    assert ray.get(x) == "hello hi"
    x = keyword_fct2.remote("w")
    assert ray.get(x) == "w world"
    x = keyword_fct2.remote("w", "hi")
    assert ray.get(x) == "w hi"

    x = keyword_fct3.remote(0, 1, c="w", d="hi")
    assert ray.get(x) == "0 1 w hi"
    x = keyword_fct3.remote(0, b=1, c="w", d="hi")
    assert ray.get(x) == "0 1 w hi"
    x = keyword_fct3.remote(a=0, b=1, c="w", d="hi")
    assert ray.get(x) == "0 1 w hi"
    x = keyword_fct3.remote(0, 1, d="hi", c="w")
    assert ray.get(x) == "0 1 w hi"
    x = keyword_fct3.remote(0, 1, c="w")
    assert ray.get(x) == "0 1 w world"
    x = keyword_fct3.remote(0, 1, d="hi")
    assert ray.get(x) == "0 1 hello hi"
    x = keyword_fct3.remote(0, 1)
    assert ray.get(x) == "0 1 hello world"
    x = keyword_fct3.remote(a=0, b=1)
    assert ray.get(x) == "0 1 hello world"

    # Check that we cannot pass invalid keyword arguments to functions.
    @ray.remote
    def f1():
        return

    @ray.remote
    def f2(x, y=0, z=0):
        return

    # Make sure we get an exception if too many arguments are passed in.
    with pytest.raises(TypeError):
        f1.remote(3)

    with pytest.raises(TypeError):
        f1.remote(x=3)

    with pytest.raises(TypeError):
        f2.remote(0, w=0)

    with pytest.raises(TypeError):
        f2.remote(3, x=3)

    # Make sure we get an exception if too many arguments are passed in.
    with pytest.raises(TypeError):
        f2.remote(1, 2, 3, 4)

    @ray.remote
    def f3(x):
        return x

    assert ray.get(f3.remote(4)) == 4


def test_args_starkwargs(ray_start_shared_local_modes):
    def starkwargs(a, b, **kwargs):
        return a, b, kwargs

    class TestActor:
        def starkwargs(self, a, b, **kwargs):
            return a, b, kwargs

    def test_function(fn, remote_fn):
        assert fn(1, 2, x=3) == ray.get(remote_fn.remote(1, 2, x=3))
        with pytest.raises(TypeError):
            remote_fn.remote(3)

    remote_test_function = ray.remote(test_function)

    remote_starkwargs = ray.remote(starkwargs)
    test_function(starkwargs, remote_starkwargs)
    ray.get(remote_test_function.remote(starkwargs, remote_starkwargs))

    remote_actor_class = ray.remote(TestActor)
    remote_actor = remote_actor_class.remote()
    actor_method = remote_actor.starkwargs
    local_actor = TestActor()
    local_method = local_actor.starkwargs
    test_function(local_method, actor_method)
    ray.get(remote_test_function.remote(local_method, actor_method))


def test_args_named_and_star(ray_start_shared_local_modes):
    def hello(a, x="hello", **kwargs):
        return a, x, kwargs

    class TestActor:
        def hello(self, a, x="hello", **kwargs):
            return a, x, kwargs

    def test_function(fn, remote_fn):
        assert fn(1, x=2, y=3) == ray.get(remote_fn.remote(1, x=2, y=3))
        assert fn(1, 2, y=3) == ray.get(remote_fn.remote(1, 2, y=3))
        assert fn(1, y=3) == ray.get(remote_fn.remote(1, y=3))

        assert fn(1,) == ray.get(
            remote_fn.remote(
                1,
            )
        )
        assert fn(1) == ray.get(remote_fn.remote(1))

        with pytest.raises(TypeError):
            remote_fn.remote(1, 2, x=3)

    remote_test_function = ray.remote(test_function)

    remote_hello = ray.remote(hello)
    test_function(hello, remote_hello)
    ray.get(remote_test_function.remote(hello, remote_hello))

    remote_actor_class = ray.remote(TestActor)
    remote_actor = remote_actor_class.remote()
    actor_method = remote_actor.hello
    local_actor = TestActor()
    local_method = local_actor.hello
    test_function(local_method, actor_method)
    ray.get(remote_test_function.remote(local_method, actor_method))


def test_oversized_function(ray_start_shared_local_modes):
    bar = np.zeros(100 * 1024 * 125)

    @ray.remote
    class Actor:
        def foo(self):
            return len(bar)

    @ray.remote
    def f():
        return len(bar)

    with pytest.raises(ValueError, match="The remote function .*f is too large"):
        f.remote()

    with pytest.raises(ValueError, match="The actor Actor is too large"):
        Actor.remote()


def test_args_stars_after(ray_start_shared_local_modes):
    def star_args_after(a="hello", b="heo", *args, **kwargs):
        return a, b, args, kwargs

    class TestActor:
        def star_args_after(self, a="hello", b="heo", *args, **kwargs):
            return a, b, args, kwargs

    def test_function(fn, remote_fn):
        assert fn("hi", "hello", 2) == ray.get(remote_fn.remote("hi", "hello", 2))
        assert fn("hi", "hello", 2, hi="hi") == ray.get(
            remote_fn.remote("hi", "hello", 2, hi="hi")
        )
        assert fn(hi="hi") == ray.get(remote_fn.remote(hi="hi"))

    remote_test_function = ray.remote(test_function)

    remote_star_args_after = ray.remote(star_args_after)
    test_function(star_args_after, remote_star_args_after)
    ray.get(remote_test_function.remote(star_args_after, remote_star_args_after))

    remote_actor_class = ray.remote(TestActor)
    remote_actor = remote_actor_class.remote()
    actor_method = remote_actor.star_args_after
    local_actor = TestActor()
    local_method = local_actor.star_args_after
    test_function(local_method, actor_method)
    ray.get(remote_test_function.remote(local_method, actor_method))


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_object_id_backward_compatibility(ray_start_shared_local_modes):
    # We've renamed Python's `ObjectID` to `ObjectRef`, and added a type
    # alias for backward compatibility.
    # This test is to make sure legacy code can still use `ObjectID`.
    # TODO(hchen): once we completely remove Python's `ObjectID`,
    # this test can be removed as well.

    # Check that these 2 types are the same.
    assert ray.ObjectID == ray.ObjectRef
    object_ref = ray.put(1)
    # Check that users can use either type in `isinstance`
    assert isinstance(object_ref, ray.ObjectID)
    assert isinstance(object_ref, ray.ObjectRef)


def test_nonascii_in_function_body(ray_start_shared_local_modes):
    @ray.remote
    def return_a_greek_char():
        return "φ"

    assert ray.get(return_a_greek_char.remote()) == "φ"


def test_failed_task(ray_start_shared_local_modes, error_pubsub):
    @ray.remote
    def throw_exception_fct1():
        raise Exception("Test function 1 intentionally failed.")

    @ray.remote
    def throw_exception_fct2():
        raise Exception("Test function 2 intentionally failed.")

    @ray.remote(num_returns=3)
    def throw_exception_fct3(x):
        raise Exception("Test function 3 intentionally failed.")

    p = error_pubsub

    throw_exception_fct1.remote()
    throw_exception_fct1.remote()

    if ray.worker.global_worker.mode != ray.worker.LOCAL_MODE:
        msgs = get_error_message(p, 2, ray.ray_constants.TASK_PUSH_ERROR)
        assert len(msgs) == 2
        for msg in msgs:
            assert "Test function 1 intentionally failed." in msg.error_message

    x = throw_exception_fct2.remote()
    try:
        ray.get(x)
    except Exception as e:
        assert "Test function 2 intentionally failed." in str(e)
    else:
        # ray.get should throw an exception.
        assert False

    x, y, z = throw_exception_fct3.remote(1.0)
    for ref in [x, y, z]:
        try:
            ray.get(ref)
        except Exception as e:
            assert "Test function 3 intentionally failed." in str(e)
        else:
            # ray.get should throw an exception.
            assert False

    class CustomException(ValueError):
        def __init__(self, msg):
            super().__init__(msg)
            self.field = 1

        def f(self):
            return 2

    @ray.remote
    def f():
        raise CustomException("This function failed.")

    try:
        ray.get(f.remote())
    except Exception as e:
        assert "This function failed." in str(e)
        assert isinstance(e, ValueError)
        assert isinstance(e, CustomException)
        assert isinstance(e, ray.exceptions.RayTaskError)
        assert "RayTaskError(CustomException)" in repr(e)
        assert e.field == 1
        assert e.f() == 2
    else:
        # ray.get should throw an exception.
        assert False


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
