import pytest
import platform
import numpy as np
import re

import ray
from ray._private.test_utils import wait_for_condition
from ray.cluster_utils import AutoscalingCluster


# Triggers the addition of a worker node.
@ray.remote(num_cpus=1)
class Actor:
    def __init__(self):
        self.data = []

    def f(self):
        pass

    def recv(self, obj):
        pass

    def create(self, size):
        return np.zeros(size)


# Tests that we scale down even if secondary copies of objects are present on
# idle nodes: https://github.com/ray-project/ray/issues/21870
@pytest.mark.skipif(platform.system() == "Windows", reason="Failing on Windows.")
def test_scaledown_shared_objects(shutdown_only):
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "cpu_node": {
                "resources": {
                    "CPU": 1,
                    "object_store_memory": 100 * 1024 * 1024,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 5,
            },
        },
        idle_timeout_minutes=0.05,
    )

    try:
        cluster.start(_system_config={"scheduler_report_pinned_bytes_only": True})
        ray.init("auto")

        actors = [Actor.remote() for _ in range(5)]
        ray.get([a.f.remote() for a in actors])
        print("All five nodes launched")

        # Verify scale-up.
        wait_for_condition(lambda: ray.cluster_resources().get("CPU", 0) == 5)

        data = actors[0].create.remote(1024 * 1024 * 5)
        ray.get([a.recv.remote(data) for a in actors])
        print("Data broadcast successfully, deleting actors.")
        del actors

        # Verify scale-down.
        wait_for_condition(
            lambda: ray.cluster_resources().get("CPU", 0) == 1, timeout=30
        )
    finally:
        cluster.shutdown()


def check_memory(local_objs, num_spilled_objects=None, num_plasma_objects=None):
    def ok():
        s = ray.internal.internal_api.memory_summary()
        print(f"\n\nMemory Summary:\n{s}\n")

        actual_objs = re.findall(r"LOCAL_REFERENCE[\s|\|]+([0-9a-f]+)", s)
        if sorted(actual_objs) != sorted(local_objs):
            raise RuntimeError(
                f"Expect local objects={local_objs}, actual={actual_objs}"
            )

        if num_spilled_objects is not None:
            m = re.search(r"Spilled (\d+) MiB, (\d+) objects", s)
            if m is not None:
                actual_spilled_objects = int(m.group(2))
                if actual_spilled_objects < num_spilled_objects:
                    raise RuntimeError(
                        f"Expected spilled objects={num_spilled_objects} "
                        f"greater than actual={actual_spilled_objects}"
                    )

        if num_plasma_objects is not None:
            m = re.search(r"Plasma memory usage (\d+) MiB, (\d+) objects", s)
            if m is None:
                raise RuntimeError(
                    "Memory summary does not contain Plasma memory objects count"
                )
            actual_plasma_objects = int(m.group(2))
            if actual_plasma_objects != num_plasma_objects:
                raise RuntimeError(
                    f"Expected plasma objects={num_plasma_objects} not equal "
                    f"to actual={actual_plasma_objects}"
                )

        return True

    wait_for_condition(ok, timeout=30, retry_interval_ms=5000)


# Tests that node with live spilled object does not get scaled down.
@pytest.mark.skipif(platform.system() == "Windows", reason="Failing on Windows.")
def test_no_scaledown_with_spilled_objects(shutdown_only):
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "cpu_node": {
                "resources": {
                    "CPU": 1,
                    "object_store_memory": 75 * 1024 * 1024,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
        idle_timeout_minutes=0.05,
    )

    try:
        cluster.start(
            _system_config={
                "scheduler_report_pinned_bytes_only": True,
                "min_spilling_size": 0,
            }
        )
        ray.init("auto")

        actors = [Actor.remote() for _ in range(2)]
        ray.get([a.f.remote() for a in actors])

        # Verify scale-up.
        wait_for_condition(lambda: ray.cluster_resources().get("CPU", 0) == 2)
        print("All nodes launched")

        # Put 10 x 80MiB objects into the object store with 75MiB memory limit.
        obj_size = 10 * 1024 * 1024
        objs = []
        for i in range(10):
            obj = actors[0].create.remote(obj_size)
            ray.get(actors[1].recv.remote(obj))
            objs.append(obj)
            print(f"obj {i}={obj.hex()}")
            del obj

        # At least 9 out of the 10 objects should have spilled.
        check_memory([obj.hex() for obj in objs], num_spilled_objects=9)
        print("Objects spilled, deleting actors and object references.")

        # Assume the 1st object always gets spilled.
        spilled_obj = objs[0]
        del objs
        del actors

        # Verify scale-down to 1 node.
        def scaledown_to_one():
            cpu = ray.cluster_resources().get("CPU", 0)
            assert cpu > 0, "Scale-down should keep at least 1 node"
            return cpu == 1

        wait_for_condition(scaledown_to_one, timeout=30)

        # Verify the spilled object still exists, and there is no object in the
        # plasma store.
        check_memory([spilled_obj.hex()], num_plasma_objects=0)

        # Delete the spilled object, the remaining worker node should be scaled
        # down.
        del spilled_obj
        wait_for_condition(lambda: ray.cluster_resources().get("CPU", 0) == 0)
        check_memory([], num_plasma_objects=0)
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
