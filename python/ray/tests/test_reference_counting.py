# coding: utf-8
import os
import json
import copy
import tempfile
import numpy as np
import time
import pytest
import logging
import uuid

import ray
import ray.cluster_utils
import ray.test_utils

logger = logging.getLogger(__name__)


def _check_refcounts(expected):
    actual = ray.worker.global_worker.core_worker.get_all_reference_counts()
    assert len(expected) == len(actual)
    for object_id, (local, submitted) in expected.items():
        assert object_id in actual
        assert local == actual[object_id]["local"]
        assert submitted == actual[object_id]["submitted"]


def check_refcounts(expected, timeout=10):
    start = time.time()
    while True:
        try:
            _check_refcounts(expected)
            break
        except AssertionError as e:
            if time.time() - start > timeout:
                raise e
            else:
                time.sleep(0.1)


def test_local_refcounts(ray_start_regular):
    oid1 = ray.put(None)
    check_refcounts({oid1: (1, 0)})
    oid1_copy = copy.copy(oid1)
    check_refcounts({oid1: (2, 0)})
    del oid1
    check_refcounts({oid1_copy: (1, 0)})
    del oid1_copy
    check_refcounts({})


def test_dependency_refcounts(ray_start_regular):
    # Return a large object that will be spilled to plasma.
    def large_object():
        return np.zeros(10 * 1024 * 1024, dtype=np.uint8)

    # TODO: Clean up tmpfiles?
    def random_path():
        return os.path.join(tempfile.gettempdir(), uuid.uuid4().hex)

    def touch(path):
        with open(path, "w"):
            pass

    def wait_for_file(path):
        while True:
            if os.path.exists(path):
                break
            time.sleep(0.1)

    @ray.remote
    def one_dep(dep, path=None, fail=False):
        if path is not None:
            wait_for_file(path)
        if fail:
            raise Exception("failed on purpose")

    @ray.remote
    def one_dep_large(dep, path=None):
        if path is not None:
            wait_for_file(path)
        # This should be spilled to plasma.
        return large_object()

    # Test that regular plasma dependency refcounts are decremented once the
    # task finishes.
    f = random_path()
    large_dep = ray.put(large_object())
    result = one_dep.remote(large_dep, path=f)
    check_refcounts({large_dep: (1, 1), result: (1, 0)})
    touch(f)
    # Reference count should be removed once the task finishes.
    check_refcounts({large_dep: (1, 0), result: (1, 0)})
    del large_dep, result
    check_refcounts({})

    # Test that inlined dependency refcounts are decremented once they are
    # inlined.
    f = random_path()
    dep = one_dep.remote(None, path=f)
    check_refcounts({dep: (1, 0)})
    result = one_dep.remote(dep)
    check_refcounts({dep: (1, 1), result: (1, 0)})
    touch(f)
    # Reference count should be removed as soon as the dependency is inlined.
    check_refcounts({dep: (1, 0), result: (1, 0)}, timeout=1)
    del dep, result
    check_refcounts({})

    # Test that spilled plasma dependency refcounts are decremented once
    # the task finishes.
    f1, f2 = random_path(), random_path()
    dep = one_dep_large.remote(None, path=f1)
    check_refcounts({dep: (1, 0)})
    result = one_dep.remote(dep, path=f2)
    check_refcounts({dep: (1, 1), result: (1, 0)})
    touch(f1)
    ray.get(dep, timeout=5.0)
    # Reference count should remain because the dependency is in plasma.
    check_refcounts({dep: (1, 1), result: (1, 0)})
    touch(f2)
    # Reference count should be removed because the task finished.
    check_refcounts({dep: (1, 0), result: (1, 0)})
    del dep, result
    check_refcounts({})

    # Test that regular plasma dependency refcounts are decremented if a task
    # fails.
    f = random_path()
    large_dep = ray.put(large_object())
    result = one_dep.remote(large_dep, path=f, fail=True)
    check_refcounts({large_dep: (1, 1), result: (1, 0)})
    touch(f)
    # Reference count should be removed once the task finishes.
    check_refcounts({large_dep: (1, 0), result: (1, 0)})
    del large_dep, result
    check_refcounts({})

    # Test that spilled plasma dependency refcounts are decremented if a task
    # fails.
    f1, f2 = random_path(), random_path()
    dep = one_dep_large.remote(None, path=f1)
    check_refcounts({dep: (1, 0)})
    result = one_dep.remote(dep, path=f2, fail=True)
    check_refcounts({dep: (1, 1), result: (1, 0)})
    touch(f1)
    ray.get(dep, timeout=5.0)
    # Reference count should remain because the dependency is in plasma.
    check_refcounts({dep: (1, 1), result: (1, 0)})
    touch(f2)
    # Reference count should be removed because the task finished.
    check_refcounts({dep: (1, 0), result: (1, 0)})
    del dep, result
    check_refcounts({})


def test_basic_pinning(shutdown_only):
    ray.init(object_store_memory=100 * 1024 * 1024)

    @ray.remote
    def f(array):
        return np.sum(array)

    @ray.remote
    class Actor(object):
        def __init__(self):
            # Hold a long-lived reference to a ray.put object's ID. The object
            # should not be garbage collected while the actor is alive because
            # the object is pinned by the raylet.
            self.large_object = ray.put(
                np.zeros(25 * 1024 * 1024, dtype=np.uint8))

        def get_large_object(self):
            return ray.get(self.large_object)

    actor = Actor.remote()

    # Fill up the object store with short-lived objects. These should be
    # evicted before the long-lived object whose reference is held by
    # the actor.
    for batch in range(10):
        intermediate_result = f.remote(
            np.zeros(10 * 1024 * 1024, dtype=np.uint8))
        ray.get(intermediate_result)

    # The ray.get below would fail with only LRU eviction, as the object
    # that was ray.put by the actor would have been evicted.
    ray.get(actor.get_large_object.remote())


def test_pending_task_dependency_pinning(shutdown_only):
    ray.init(object_store_memory=100 * 1024 * 1024, use_pickle=True)

    @ray.remote
    def pending(input1, input2):
        return

    @ray.remote
    def slow(dep):
        pass

    # The object that is ray.put here will go out of scope immediately, so if
    # pending task dependencies aren't considered, it will be evicted before
    # the ray.get below due to the subsequent ray.puts that fill up the object
    # store.
    np_array = np.zeros(40 * 1024 * 1024, dtype=np.uint8)
    random_id = ray.ObjectID.from_random()
    oid = pending.remote(np_array, slow.remote(random_id))

    for _ in range(2):
        ray.put(np_array)

    ray.worker.global_worker.put_object(None, object_id=random_id)
    ray.get(oid)


def test_feature_flag(shutdown_only):
    ray.init(
        object_store_memory=100 * 1024 * 1024,
        _internal_config=json.dumps({
            "object_pinning_enabled": 0
        }))

    @ray.remote
    def f(array):
        return np.sum(array)

    @ray.remote
    class Actor(object):
        def __init__(self):
            self.large_object = ray.put(
                np.zeros(25 * 1024 * 1024, dtype=np.uint8))

        def wait_for_actor_to_start(self):
            pass

        def get_large_object(self):
            return ray.get(self.large_object)

    actor = Actor.remote()
    ray.get(actor.wait_for_actor_to_start.remote())

    for batch in range(10):
        intermediate_result = f.remote(
            np.zeros(10 * 1024 * 1024, dtype=np.uint8))
        ray.get(intermediate_result)

    # The ray.get below fails with only LRU eviction, as the object
    # that was ray.put by the actor should have been evicted.
    with pytest.raises(ray.exceptions.RayTimeoutError):
        ray.get(actor.get_large_object.remote(), timeout=1)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
