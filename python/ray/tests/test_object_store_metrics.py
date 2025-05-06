from collections import defaultdict
import pytest
from typing import Dict
import numpy as np
import sys

import requests
import ray
from ray._private.test_utils import (
    raw_metrics,
    wait_for_condition,
)
from ray._private.worker import RayContext
from ray.dashboard.consts import RAY_DASHBOARD_STATS_UPDATING_INTERVAL

KiB = 1 << 10
MiB = 1 << 20

_SYSTEM_CONFIG = {
    "automatic_object_spilling_enabled": True,
    "max_io_workers": 100,
    "min_spilling_size": 1,
    "object_spilling_threshold": 0.99,  # to prevent premature spilling
    "metrics_report_interval_ms": 200,
}


def _objects_by_tag(info: RayContext, tag: str) -> Dict:
    res = raw_metrics(info)
    objects_info = defaultdict(int)
    if "ray_object_store_memory" in res:
        for sample in res["ray_object_store_memory"]:
            # NOTE: SPILLED sample doesn't report sealing states. So need to
            # filter those empty label value out.
            print(sample)
            if tag in sample.labels and sample.labels[tag] != "":
                objects_info[sample.labels[tag]] += sample.value

    print(f"Objects by {tag}: {objects_info}")
    return objects_info


def objects_by_seal_state(info: RayContext) -> Dict:
    return _objects_by_tag(info, "ObjectState")


def objects_by_loc(info: RayContext) -> Dict:
    return _objects_by_tag(info, "Location")


def approx_eq_dict_in(actual: Dict, expected: Dict, e: int) -> bool:
    """Check if two dict are approximately similar (with error allowed)"""
    assert set(actual.keys()) == set(expected.keys()), "Unequal key sets."

    for k, actual_v in actual.items():
        expect_v = expected[k]
        assert (
            abs(expect_v - actual_v) <= e
        ), f"expect={expect_v}, actual={actual_v}, diff allowed={e}"

    return True


@pytest.mark.skipif(
    sys.platform == "darwin", reason="Timing out on macos. Not enough time to run."
)
def test_shared_memory_and_inline_worker_heap(shutdown_only):
    """Test objects allocated in shared memory"""
    import numpy as np

    info = ray.init(
        object_store_memory=100 * MiB,
        _system_config={
            **_SYSTEM_CONFIG,
            **{
                "max_direct_call_object_size": 10 * MiB,
                "task_rpc_inlined_bytes_limit": 100 * MiB,
            },
        },
    )

    # Allocate 80MiB data
    objs_in_use = ray.get(
        [ray.put(np.zeros(20 * MiB, dtype=np.uint8)) for _ in range(4)]
    )

    expected = {
        "MMAP_SHM": 80 * MiB,
        "MMAP_DISK": 0,
        "SPILLED": 0,
        "WORKER_HEAP": 0,
    }

    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 2 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )

    # Allocate inlined task returns
    @ray.remote(num_cpus=0.1)
    def func():
        return np.zeros(4 * MiB, dtype=np.uint8)

    tasks_with_inlined_return = [func.remote() for _ in range(5)]

    expected = {
        "MMAP_SHM": 80 * MiB,
        "MMAP_DISK": 0,
        "SPILLED": 0,
        "WORKER_HEAP": 20 * MiB,
    }

    returns = ray.get(tasks_with_inlined_return)

    wait_for_condition(
        # 4 KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 4 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )

    # Free all of them
    del objs_in_use
    del returns
    del tasks_with_inlined_return

    expected = {
        "MMAP_SHM": 0,
        "MMAP_DISK": 0,
        "SPILLED": 0,
        "WORKER_HEAP": 0,
    }

    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 2 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )


@pytest.mark.skipif(
    sys.platform == "darwin", reason="Timing out on macos. Not enough time to run."
)
def test_spilling(object_spilling_config, shutdown_only):
    """Test metrics with object spilling occurred"""

    object_spilling_config, _ = object_spilling_config
    delta = 5
    info = ray.init(
        num_cpus=1,
        object_store_memory=100 * MiB + delta * MiB,
        _system_config={
            **_SYSTEM_CONFIG,
            **{"object_spilling_config": object_spilling_config},
        },
    )

    # Create and use 100MiB data, which should fit in memory
    objs1 = [ray.put(np.zeros(50 * MiB, dtype=np.uint8)) for _ in range(2)]

    expected = {
        "MMAP_SHM": 100 * MiB,
        "MMAP_DISK": 0,
        "SPILLED": 0,
        "WORKER_HEAP": 0,
    }

    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 1 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )

    # Create additional 100MiB, so that it needs to be triggered
    objs2 = [ray.put(np.zeros(50 * MiB, dtype=np.uint8)) for _ in range(2)]

    expected = {
        "WORKER_HEAP": 0,
        "MMAP_SHM": 100 * MiB,
        "MMAP_DISK": 0,
        "SPILLED": 100 * MiB,
    }
    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 1 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )

    # Delete spilled objects
    del objs1
    expected = {
        "MMAP_SHM": 100 * MiB,
        "MMAP_DISK": 0,
        "SPILLED": 0,
        "WORKER_HEAP": 0,
    }
    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 1 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )

    # Delete all
    del objs2
    expected = {
        "MMAP_SHM": 0,
        "MMAP_DISK": 0,
        "SPILLED": 0,
        "WORKER_HEAP": 0,
    }
    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 1 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )


@pytest.mark.skipif(
    sys.platform == "darwin", reason="Timing out on macos. Not enough time to run."
)
def test_fallback_memory(shutdown_only):
    """Test some fallback allocated objects"""

    expected_fallback = 6
    expected_in_memory = 5
    obj_size_mb = 20

    # So expected_in_memory objects could fit in object store
    delta_mb = 5
    info = ray.init(
        object_store_memory=expected_in_memory * obj_size_mb * MiB + delta_mb * MiB,
        _system_config=_SYSTEM_CONFIG,
    )
    obj_refs = [
        ray.put(np.zeros(obj_size_mb * MiB, dtype=np.uint8))
        for _ in range(expected_in_memory)
    ]

    # Getting and using the objects to prevent spilling
    in_use_objs = [ray.get(obj) for obj in obj_refs]

    # No fallback and spilling yet
    expected = {
        "MMAP_SHM": expected_in_memory * obj_size_mb * MiB,
        "MMAP_DISK": 0,
        "SPILLED": 0,
        "WORKER_HEAP": 0,
    }

    wait_for_condition(
        # 2KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 3 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )

    # Fallback allocated and make them not spillable
    obj_refs_fallback = []
    in_use_objs_fallback = []
    for _ in range(expected_fallback):
        obj = ray.put(np.zeros(obj_size_mb * MiB, dtype=np.uint8))
        in_use_objs_fallback.append(ray.get(obj))
        obj_refs_fallback.append(obj)

        # NOTE(rickyx): I actually wasn't aware this reference would
        # keep the reference count? Removing this line would cause
        # a single object not deleted.
        del obj

    # Fallback allocated and still no spilling
    expected = {
        "MMAP_SHM": expected_in_memory * obj_size_mb * MiB,
        "MMAP_DISK": expected_fallback * obj_size_mb * MiB,
        "SPILLED": 0,
        "WORKER_HEAP": 0,
    }

    wait_for_condition(
        # 3KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 3 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )

    # Free all of them
    del in_use_objs
    del obj_refs
    del in_use_objs_fallback
    del obj_refs_fallback

    expected = {
        "MMAP_SHM": 0,
        "MMAP_DISK": 0,
        "SPILLED": 0,
        "WORKER_HEAP": 0,
    }

    wait_for_condition(
        # 3KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 3 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )


@pytest.mark.skipif(
    sys.platform == "darwin", reason="Timing out on macos. Not enough time to run."
)
def test_seal_memory(shutdown_only):
    """Test objects sealed states reported correctly"""
    import numpy as np

    info = ray.init(
        object_store_memory=100 * MiB,
        _system_config=_SYSTEM_CONFIG,
    )

    # Allocate 80MiB data
    objs_in_use = ray.get(
        [ray.put(np.zeros(20 * MiB, dtype=np.uint8)) for _ in range(4)]
    )

    expected = {
        "SEALED": 80 * MiB,
        "UNSEALED": 0,
    }

    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_seal_state(info), expected, 2 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )

    del objs_in_use

    expected = {
        "SEALED": 0,
        "UNSEALED": 0,
    }

    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_seal_state(info), expected, 2 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )


def test_object_store_memory_matches_dashboard_obj_memory(shutdown_only):
    # https://github.com/ray-project/ray/issues/32092
    # Verify the dashboard's object store memory report is same as
    # the one from metrics
    ctx = ray.init(
        object_store_memory=500 * MiB,
    )

    def verify():
        resources = raw_metrics(ctx)["ray_resources"]
        object_store_memory_bytes_from_metrics = 0
        for sample in resources:
            # print(sample)
            if sample.labels["Name"] == "object_store_memory":
                object_store_memory_bytes_from_metrics += sample.value

        r = requests.get(f"http://{ctx.dashboard_url}/nodes?view=summary")
        object_store_memory_bytes_from_dashboard = int(
            r.json()["data"]["summary"][0]["raylet"]["objectStoreAvailableMemory"]
        )

        assert (
            object_store_memory_bytes_from_dashboard
            == object_store_memory_bytes_from_metrics
        )
        assert object_store_memory_bytes_from_dashboard == 500 * MiB
        return True

    wait_for_condition(verify, timeout=RAY_DASHBOARD_STATS_UPDATING_INTERVAL * 1.5)


if __name__ == "__main__":
    import sys
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
