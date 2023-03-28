# coding: utf-8
import logging
import sys
import time

import numpy as np
import pytest

import ray
import ray.cluster_utils
from ray._private.test_utils import dicts_equal

logger = logging.getLogger(__name__)


def test_auto_global_gc(shutdown_only):
    # 100MB
    ray.init(num_cpus=1, object_store_memory=100 * 1024 * 1024)

    @ray.remote
    class Test:
        def __init__(self):
            self.collected = False
            import gc

            gc.disable()

            def gc_called(phase, info):
                self.collected = True

            gc.callbacks.append(gc_called)

        def circular_ref(self):
            # 20MB
            buf1 = b"0" * (10 * 1024 * 1024)
            buf2 = b"1" * (10 * 1024 * 1024)
            ref1 = ray.put(buf1)
            ref2 = ray.put(buf2)
            b = []
            a = []
            b.append(a)
            a.append(b)
            b.append(ref1)
            a.append(ref2)
            return a

        def collected(self):
            return self.collected

    test = Test.remote()
    # 60MB
    for i in range(3):
        ray.get(test.circular_ref.remote())
    time.sleep(2)
    assert not ray.get(test.collected.remote())
    # 80MB
    for _ in range(1):
        ray.get(test.circular_ref.remote())
    time.sleep(2)
    assert ray.get(test.collected.remote())


@pytest.mark.skipif(
    sys.version_info >= (3, 10, 0),
    reason=("Currently not passing for Python 3.10"),
)
def test_many_fractional_resources(shutdown_only):
    ray.init(num_cpus=2, num_gpus=2, resources={"Custom": 2})

    @ray.remote
    def g():
        return 1

    @ray.remote
    def f(block, accepted_resources):
        true_resources = {
            resource: value[0][1]
            for resource, value in ray._private.worker.get_resource_ids().items()
        }
        if block:
            ray.get(g.remote())
        return dicts_equal(true_resources, accepted_resources)

    # Check that the resource are assigned correctly.
    result_ids = []
    for rand1, rand2, rand3 in np.random.uniform(size=(100, 3)):
        resource_set = {"CPU": int(rand1 * 10000) / 10000}
        result_ids.append(f._remote([False, resource_set], num_cpus=rand1))

        resource_set = {"CPU": 1, "GPU": int(rand1 * 10000) / 10000}
        result_ids.append(f._remote([False, resource_set], num_gpus=rand1))

        resource_set = {"CPU": 1, "Custom": int(rand1 * 10000) / 10000}
        result_ids.append(f._remote([False, resource_set], resources={"Custom": rand1}))

        resource_set = {
            "CPU": int(rand1 * 10000) / 10000,
            "GPU": int(rand2 * 10000) / 10000,
            "Custom": int(rand3 * 10000) / 10000,
        }
        result_ids.append(
            f._remote(
                [False, resource_set],
                num_cpus=rand1,
                num_gpus=rand2,
                resources={"Custom": rand3},
            )
        )
        result_ids.append(
            f._remote(
                [True, resource_set],
                num_cpus=rand1,
                num_gpus=rand2,
                resources={"Custom": rand3},
            )
        )
    assert all(ray.get(result_ids))

    # Check that the available resources at the end are the same as the
    # beginning.
    stop_time = time.time() + 10
    correct_available_resources = False
    while time.time() < stop_time:
        available_resources = ray.available_resources()
        if (
            "CPU" in available_resources
            and ray.available_resources()["CPU"] == 2.0
            and "GPU" in available_resources
            and ray.available_resources()["GPU"] == 2.0
            and "Custom" in available_resources
            and ray.available_resources()["Custom"] == 2.0
        ):
            correct_available_resources = True
            break
    if not correct_available_resources:
        assert False, "Did not get correct available resources."


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
