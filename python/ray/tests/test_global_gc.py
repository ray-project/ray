# coding: utf-8
import gc
import logging
import weakref

import numpy as np

import pytest

import ray
import ray.cluster_utils
from ray.test_utils import wait_for_condition
from ray.internal.internal_api import global_gc

logger = logging.getLogger(__name__)


def test_auto_local_gc(shutdown_only):
    ray.init(num_cpus=2, _system_config={"local_gc_interval_s": 1})

    class ObjectWithCyclicRef:
        def __init__(self):
            self.loop = self

    @ray.remote(num_cpus=1)
    class GarbageHolder:
        def __init__(self):
            gc.disable()
            x = ObjectWithCyclicRef()
            self.garbage = weakref.ref(x)

        def has_garbage(self):
            return self.garbage() is not None

    try:
        gc.disable()

        # Local driver.
        local_ref = weakref.ref(ObjectWithCyclicRef())

        # Remote workers.
        actors = [GarbageHolder.remote() for _ in range(2)]
        assert local_ref() is not None
        assert all(ray.get([a.has_garbage.remote() for a in actors]))

        def check_refs_gced():
            return (local_ref() is None and
                    not any(ray.get([a.has_garbage.remote() for a in actors])))

        wait_for_condition(check_refs_gced)
    finally:
        gc.enable()


def test_global_gc(shutdown_only):
    cluster = ray.cluster_utils.Cluster()
    for _ in range(2):
        cluster.add_node(num_cpus=1, num_gpus=0)
    ray.init(address=cluster.address)

    class ObjectWithCyclicRef:
        def __init__(self):
            self.loop = self

    @ray.remote(num_cpus=1)
    class GarbageHolder:
        def __init__(self):
            gc.disable()
            x = ObjectWithCyclicRef()
            self.garbage = weakref.ref(x)

        def has_garbage(self):
            return self.garbage() is not None

    try:
        gc.disable()

        # Local driver.
        local_ref = weakref.ref(ObjectWithCyclicRef())

        # Remote workers.
        actors = [GarbageHolder.remote() for _ in range(2)]
        assert local_ref() is not None
        assert all(ray.get([a.has_garbage.remote() for a in actors]))

        # GC should be triggered for all workers, including the local driver.
        global_gc()

        def check_refs_gced():
            return (local_ref() is None and
                    not any(ray.get([a.has_garbage.remote() for a in actors])))

        wait_for_condition(check_refs_gced)
    finally:
        gc.enable()


def test_global_gc_when_full(shutdown_only):
    cluster = ray.cluster_utils.Cluster()
    for _ in range(2):
        cluster.add_node(
            num_cpus=1, num_gpus=0, object_store_memory=100 * 1024 * 1024)
    ray.init(address=cluster.address)

    class LargeObjectWithCyclicRef:
        def __init__(self):
            self.loop = self
            self.large_object = ray.put(
                np.zeros(40 * 1024 * 1024, dtype=np.uint8))

    @ray.remote(num_cpus=1)
    class GarbageHolder:
        def __init__(self):
            gc.disable()
            x = LargeObjectWithCyclicRef()
            self.garbage = weakref.ref(x)

        def has_garbage(self):
            return self.garbage() is not None

        def return_large_array(self):
            return np.zeros(80 * 1024 * 1024, dtype=np.uint8)

    try:
        gc.disable()

        # Local driver.
        local_ref = weakref.ref(LargeObjectWithCyclicRef())

        # Remote workers.
        actors = [GarbageHolder.remote() for _ in range(2)]
        assert local_ref() is not None
        assert all(ray.get([a.has_garbage.remote() for a in actors]))

        # GC should be triggered for all workers, including the local driver,
        # when the driver tries to ray.put a value that doesn't fit in the
        # object store. This should cause the captured ObjectRefs' numpy arrays
        # to be evicted.
        ray.put(np.zeros(80 * 1024 * 1024, dtype=np.uint8))

        def check_refs_gced():
            return (local_ref() is None and
                    not any(ray.get([a.has_garbage.remote() for a in actors])))

        wait_for_condition(check_refs_gced)

        # Local driver.
        local_ref = weakref.ref(LargeObjectWithCyclicRef())

        # Remote workers.
        actors = [GarbageHolder.remote() for _ in range(2)]
        assert all(ray.get([a.has_garbage.remote() for a in actors]))

        # GC should be triggered for all workers, including the local driver,
        # when a remote task tries to put a return value that doesn't fit in
        # the object store. This should cause the captured ObjectRefs' numpy
        # arrays to be evicted.
        ray.get(actors[0].return_large_array.remote())

        def check_refs_gced():
            return (local_ref() is None and
                    not any(ray.get([a.has_garbage.remote() for a in actors])))

        wait_for_condition(check_refs_gced)
    finally:
        gc.enable()


def test_global_gc_actors(shutdown_only):
    ray.init(
        num_cpus=1, _system_config={"debug_dump_period_milliseconds": 500})

    try:
        gc.disable()

        @ray.remote(num_cpus=1)
        class A:
            def f(self):
                return "Ok"

        # Try creating 3 actors. Unless python GC is triggered to break
        # reference cycles, this won't be possible.
        for i in range(3):
            a = A.remote()
            cycle = [a]
            cycle.append(cycle)
            ray.get(a.f.remote())
            print("iteration", i)
            del a
            del cycle
    finally:
        gc.enable()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
