# coding: utf-8
import os
import sys
import time
import pytest

import ray
from ray._private.test_utils import RayTestTimeoutException, wait_for_condition
from ray.util.placement_group import placement_group
from ray.util.accelerators import INTEL_GAUDI
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy



def test_hpu_ids(shutdown_only):
    num_hpus = 3
    ray.init(num_cpus=num_hpus, resources={"HPU": num_hpus})

    def get_hpu_ids(hpus_per_worker):
        hpu_ids = ray.get_runtime_context().get_resource_ids()["HPU"]
        assert len(hpu_ids) == hpus_per_worker
        modules = os.environ.get("HABANA_VISIBLE_MODULES")
        if modules is not None:
            assert modules == ",".join([str(i) for i in hpu_ids])  # noqa
        for hpu_id in hpu_ids:
            assert hpu_id in [str(i) for i in range(num_hpus)]
        return hpu_ids

    f0 = ray.remote(resources={"HPU": 0})(lambda: get_hpu_ids(0))
    f1 = ray.remote(resources={"HPU": 1})(lambda: get_hpu_ids(1))
    f2 = ray.remote(resources={"HPU": 2})(lambda: get_hpu_ids(2))

    # Wait for all workers to start up.
    @ray.remote
    def g():
        time.sleep(0.2)
        return os.getpid()

    start_time = time.time()
    while True:
        num_workers_started = len(set(ray.get([g.remote() for _ in range(num_hpus)])))
        if num_workers_started == num_hpus:
            break
        if time.time() > start_time + 10:
            raise RayTestTimeoutException(
                "Timed out while waiting for workers to start up."
            )

    list_of_ids = ray.get([f0.remote() for _ in range(10)])
    assert list_of_ids == 10 * [[]]
    ray.get([f1.remote() for _ in range(10)])
    ray.get([f2.remote() for _ in range(10)])

    # Test that actors have NEURON_RT_VISIBLE_CORES set properly.

    @ray.remote
    class Actor0:
        def __init__(self):
            hpu_ids = ray.get_runtime_context().get_resource_ids()[
                "HPU"
            ]
            assert len(hpu_ids) == 0
            assert os.environ["HABANA_VISIBLE_MODULES"] == ",".join(
                [str(i) for i in hpu_ids]  # noqa
            )
            # Set self.x to make sure that we got here.
            self.x = 0

        def test(self):
            hpu_ids = ray.get_runtime_context().get_resource_ids()[
                "HPU"
            ]
            assert len(hpu_ids) == 0
            assert os.environ["HABANA_VISIBLE_MODULES"] == ",".join(
                [str(i) for i in hpu_ids]  # noqa
            )
            return self.x

    @ray.remote(resources={"HPU": 1})
    class Actor1:
        def __init__(self):
            hpu_ids = ray.get_runtime_context().get_resource_ids()[
                "HPU"
            ]
            assert len(hpu_ids) == 1
            assert os.environ["HABANA_VISIBLE_MODULES"] == ",".join(
                [str(i) for i in hpu_ids]  # noqa
            )
            # Set self.x to make sure that we got here.
            self.x = 1

        def test(self):
            hpu_ids = ray.get_runtime_context().get_resource_ids()[
                "HPU"
            ]
            assert len(hpu_ids) == 1
            assert os.environ["HABANA_VISIBLE_MODULES"] == ",".join(
                [str(i) for i in hpu_ids]
            )
            return self.x

    @ray.remote(resources={"HPU": 2})
    class Actor2:
        def __init__(self):
            hpu_ids = ray.get_runtime_context().get_resource_ids()[
                "HPU"
            ]
            assert len(hpu_ids) == 2
            assert os.environ["HABANA_VISIBLE_MODULES"] == ",".join(
                [str(i) for i in hpu_ids]
            )
            # Set self.x to make sure that we got here.
            self.x = 2

        def test(self):
            hpu_ids = ray.get_runtime_context().get_resource_ids()[
                "HPU"
            ]
            assert len(hpu_ids) == 2
            assert os.environ["HABANA_VISIBLE_MODULES"] == ",".join(
                [str(i) for i in hpu_ids]
            )
            return self.x

    a0 = Actor0.remote()
    assert ray.get(a0.test.remote()) == 0

    a1 = Actor1.remote()
    assert ray.get(a1.test.remote()) == 1

    a2 = Actor2.remote()
    assert ray.get(a2.test.remote()) == 2

def test_hpu_with_placement_group(shutdown_only):
    num_hpus = 2
    ray.init(num_cpus=1, resources={"HPU": num_hpus})

    @ray.remote(resources={"HPU": num_hpus})
    class HPUActor:
        def __init__(self):
            pass

        def ready(self):
            hpu_ids = ray.get_runtime_context().get_resource_ids()[
                "HPU"
            ]
            assert len(hpu_ids) == num_hpus
            assert os.environ["HABANA_VISIBLE_MODULES"] == ",".join(
                [str(i) for i in hpu_ids]  # noqa
            )

    # Reserve a placement group of 1 bundle that reserves 1 CPU and 2 HPU.
    pg = placement_group([{"CPU": 1, "HPU": num_hpus}])

    # Wait until placement group is created.
    ray.get(pg.ready(), timeout=10)

    actor = HPUActor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
        )
    ).remote()

    ray.get(actor.ready.remote(), timeout=10)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
