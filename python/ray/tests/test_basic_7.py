# coding: utf-8
import os
import logging
import sys
import threading
import time
import tempfile
import subprocess

import numpy as np
import pytest

from unittest.mock import MagicMock, patch

from ray._private.test_utils import client_test_enabled
from ray.ray_constants import KV_NAMESPACE_FUNCTION_TABLE

if client_test_enabled():
    from ray.util.client import ray
else:
    import ray

logger = logging.getLogger(__name__)


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_actor_large_objects(ray_start_regular_shared):
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self):
            time.sleep(1)
            return np.zeros(10000000)

    a = Actor.remote()
    obj_ref = a.f.remote()
    assert not ray.worker.global_worker.core_worker.object_exists(obj_ref)
    done, _ = ray.wait([obj_ref])
    assert len(done) == 1
    assert ray.worker.global_worker.core_worker.object_exists(obj_ref)
    assert isinstance(ray.get(obj_ref), np.ndarray)


def test_actor_pass_by_ref(ray_start_regular_shared):
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self, x):
            return x * 2

    @ray.remote
    def f(x):
        return x

    @ray.remote
    def error():
        sys.exit(0)

    a = Actor.remote()
    assert ray.get(a.f.remote(f.remote(1))) == 2

    fut = [a.f.remote(f.remote(i)) for i in range(100)]
    assert ray.get(fut) == [i * 2 for i in range(100)]

    # propagates errors for pass by ref
    with pytest.raises(Exception):
        ray.get(a.f.remote(error.remote()))


def test_actor_recursive(ray_start_regular_shared):
    @ray.remote
    class Actor:
        def __init__(self, delegate=None):
            self.delegate = delegate

        def f(self, x):
            if self.delegate:
                return ray.get(self.delegate.f.remote(x))
            return x * 2

    a = Actor.remote()
    b = Actor.remote(a)
    c = Actor.remote(b)

    result = ray.get([c.f.remote(i) for i in range(100)])
    assert result == [x * 2 for x in range(100)]

    result, _ = ray.wait([c.f.remote(i) for i in range(100)], num_returns=100)
    result = ray.get(result)
    assert result == [x * 2 for x in range(100)]


def test_actor_concurrent(ray_start_regular_shared):
    @ray.remote
    class Batcher:
        def __init__(self):
            self.batch = []
            self.event = threading.Event()

        def add(self, x):
            self.batch.append(x)
            if len(self.batch) >= 3:
                self.event.set()
            else:
                self.event.wait()
            return sorted(self.batch)

    a = Batcher.options(max_concurrency=3).remote()
    x1 = a.add.remote(1)
    x2 = a.add.remote(2)
    x3 = a.add.remote(3)
    r1 = ray.get(x1)
    r2 = ray.get(x2)
    r3 = ray.get(x3)
    assert r1 == [1, 2, 3]
    assert r1 == r2 == r3


def test_actor_max_concurrency(ray_start_regular_shared):
    """
    Test that an actor of max_concurrency=N should only run
    N tasks at most concurrently.
    """
    CONCURRENCY = 3

    @ray.remote
    class ConcurentActor:
        def __init__(self):
            self.threads = set()

        def call(self):
            # Record the current thread that runs this function.
            self.threads.add(threading.current_thread())

        def get_num_threads(self):
            return len(self.threads)

    @ray.remote
    def call(actor):
        for _ in range(CONCURRENCY * 100):
            ray.get(actor.call.remote())
        return

    actor = ConcurentActor.options(max_concurrency=CONCURRENCY).remote()
    # Start many concurrent tasks that will call the actor many times.
    ray.get([call.remote(actor) for _ in range(CONCURRENCY * 10)])

    # Check that the number of threads shouldn't be greater than CONCURRENCY.
    assert ray.get(actor.get_num_threads.remote()) <= CONCURRENCY


def test_wait(ray_start_regular_shared):
    @ray.remote
    def f(delay):
        time.sleep(delay)
        return

    object_refs = [f.remote(0), f.remote(0), f.remote(0), f.remote(0)]
    ready_ids, remaining_ids = ray.wait(object_refs)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 3
    ready_ids, remaining_ids = ray.wait(object_refs, num_returns=4)
    assert set(ready_ids) == set(object_refs)
    assert remaining_ids == []

    object_refs = [f.remote(0), f.remote(5)]
    ready_ids, remaining_ids = ray.wait(object_refs, timeout=0.5, num_returns=2)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 1

    # Verify that calling wait with duplicate object refs throws an
    # exception.
    x = ray.put(1)
    with pytest.raises(Exception):
        ray.wait([x, x])

    # Make sure it is possible to call wait with an empty list.
    ready_ids, remaining_ids = ray.wait([])
    assert ready_ids == []
    assert remaining_ids == []

    # Test semantics of num_returns with no timeout.
    obj_refs = [ray.put(i) for i in range(10)]
    (found, rest) = ray.wait(obj_refs, num_returns=2)
    assert len(found) == 2
    assert len(rest) == 8

    # Verify that incorrect usage raises a TypeError.
    x = ray.put(1)
    with pytest.raises(TypeError):
        ray.wait(x)
    with pytest.raises(TypeError):
        ray.wait(1)
    with pytest.raises(TypeError):
        ray.wait([1])


def test_duplicate_args(ray_start_regular_shared):
    @ray.remote
    def f(arg1, arg2, arg1_duplicate, kwarg1=None, kwarg2=None, kwarg1_duplicate=None):
        assert arg1 == kwarg1
        assert arg1 != arg2
        assert arg1 == arg1_duplicate
        assert kwarg1 != kwarg2
        assert kwarg1 == kwarg1_duplicate

    # Test by-value arguments.
    arg1 = [1]
    arg2 = [2]
    ray.get(f.remote(arg1, arg2, arg1, kwarg1=arg1, kwarg2=arg2, kwarg1_duplicate=arg1))

    # Test by-reference arguments.
    arg1 = ray.put([1])
    arg2 = ray.put([2])
    ray.get(f.remote(arg1, arg2, arg1, kwarg1=arg1, kwarg2=arg2, kwarg1_duplicate=arg1))


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_get_correct_node_ip():
    with patch("ray.worker") as worker_mock:
        node_mock = MagicMock()
        node_mock.node_ip_address = "10.0.0.111"
        worker_mock._global_node = node_mock
        found_ip = ray.util.get_node_ip_address()
        assert found_ip == "10.0.0.111"


def test_load_code_from_local(ray_start_regular_shared):
    # This case writes a driver python file to a temporary directory.
    #
    # The driver starts a cluster with
    # `ray.init(ray.job_config.JobConfig(code_search_path=<path list>))`,
    # then creates a nested actor. The actor will be loaded from code in
    # worker.
    #
    # This tests the following two cases when :
    # 1) Load a nested class.
    # 2) Load a class defined in the `__main__` module.
    code_test = """
import os
import ray

class A:
    @ray.remote
    class B:
        def get(self):
            return "OK"

if __name__ == "__main__":
    current_path = os.path.dirname(__file__)
    job_config = ray.job_config.JobConfig(code_search_path=[current_path])
    ray.init({}, job_config=job_config)
    b = A.B.remote()
    print(ray.get(b.get.remote()))
"""

    # Test code search path contains space.
    with tempfile.TemporaryDirectory(suffix="a b") as tmpdir:
        test_driver = os.path.join(tmpdir, "test_load_code_from_local.py")
        with open(test_driver, "w") as f:
            f.write(code_test.format(repr(ray_start_regular_shared["address"])))
        output = subprocess.check_output([sys.executable, test_driver])
        assert b"OK" in output


@pytest.mark.skipif(
    client_test_enabled(), reason="JobConfig doesn't work in client mode"
)
def test_use_dynamic_function_and_class():
    # Test use dynamically defined functions
    # and classes for remote tasks and actors.
    # See https://github.com/ray-project/ray/issues/12834.
    ray.shutdown()
    current_path = os.path.dirname(__file__)
    job_config = ray.job_config.JobConfig(code_search_path=[current_path])
    ray.init(job_config=job_config)

    def foo1():
        @ray.remote
        def foo2():
            return "OK"

        return foo2

    @ray.remote
    class Foo:
        @ray.method(num_returns=1)
        def foo(self):
            return "OK"

    f = foo1()
    assert ray.get(f.remote()) == "OK"
    # Check whether the dynamic function is exported to GCS.
    # Note, the key format should be kept
    # the same as in `FunctionActorManager.export`.
    key_func = (
        b"RemoteFunction:"
        + ray.worker.global_worker.current_job_id.hex().encode()
        + b":"
        + f._function_descriptor.function_id.binary()
    )
    assert ray.worker.global_worker.gcs_client.internal_kv_exists(
        key_func, KV_NAMESPACE_FUNCTION_TABLE
    )
    foo_actor = Foo.remote()

    assert ray.get(foo_actor.foo.remote()) == "OK"
    # Check whether the dynamic class is exported to GCS.
    # Note, the key format should be kept
    # the same as in `FunctionActorManager.export_actor_class`.
    key_cls = (
        b"ActorClass:"
        + ray.worker.global_worker.current_job_id.hex().encode()
        + b":"
        + foo_actor._ray_actor_creation_function_descriptor.function_id.binary()
    )
    assert ray.worker.global_worker.gcs_client.internal_kv_exists(
        key_cls, namespace=KV_NAMESPACE_FUNCTION_TABLE
    )


if __name__ == "__main__":
    import pytest

    # Skip test_basic_2_client_mode for now- the test suite is breaking.
    sys.exit(pytest.main(["-v", __file__]))
