# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import copy
import tempfile
import numpy as np
import time
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


def check_refcounts(expected, timeout=1):
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


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
