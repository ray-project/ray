# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import json
import numpy as np
import time
import logging
import pytest

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

def check_refcounts(expected, timeout=0.1):
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
    @ray.remote
    def one_dep(dep):
        pass

    @ray.remote
    def one_dep_large(dep):
        # Return a 10MiB object - this should be spilled to plasma.
        return np.zeros(10*1024*1024, dtype=np.uint8)

    @ray.remote
    def two_deps(dep1, dep2):
        pass

    dep = one_dep.remote(None)
    check_refcounts({dep: (1, 0)})
    ray.get(dep)
    result = one_dep.remote(dep)
    # Should be inlined immediately, so no submitted refcount should be seen.
    check_refcounts({dep: (1, 0), result: (1, 0)})
    del dep, result

    check_refcounts({})
    random_id = ray.ObjectID.from_random()
    dep = one_dep.remote(random_id)
    check_refcounts({random_id: (1, 1), dep: (1, 0)})
    ray.worker.global_worker.put_object(None, object_id=random_id)
    check_refcounts({random_id: (1, 0), dep: (1, 0)})
    del random_id, dep

    check_refcounts({})
    random_id_1 = ray.ObjectID.from_random()
    large_dep = one_dep_large.remote(random_id_1)
    check_refcounts({random_id_1: (1, 1), large_dep: (1, 0)})
    random_id_2 = ray.ObjectID.from_random()
    result = two_deps.remote(large_dep, random_id_2)
    check_refcounts({random_id_1: (1, 1), random_id_2: (1, 1), large_dep: (1, 1), result: (1, 0)})
    ray.worker.global_worker.put_object(None, object_id=random_id_1)
    check_refcounts({random_id_1: (1, 0), random_id_2: (1, 1), large_dep: (1, 1), result: (1, 0)})
    ray.worker.global_worker.put_object(None, object_id=random_id_2)
    check_refcounts({random_id_1: (1, 0), random_id_2: (1, 0), large_dep: (1, 0), result: (1, 0)})
    del random_id_1, random_id_2, large_dep, result
    check_refcounts({})

if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
