from collections import defaultdict
import pytest
from typing import Dict
import numpy as np

import ray
from ray._private.test_utils import (
    raw_metrics,
    wait_for_condition,
)
from ray._private.worker import RayContext

KiB = 1 << 10
MiB = 1 << 20

_SYSTEM_CONFIG = {
    "automatic_object_spilling_enabled": True,
    "max_io_workers": 100,
    "min_spilling_size": 1,
    "object_spilling_threshold": 0.99,  # to prevent premature spilling
    "metrics_report_interval_ms": 200,
    "event_stats_print_interval_ms": 100,  # so metrics get exported
}


def objects_by_loc(info: RayContext) -> Dict:
    res = raw_metrics(info)
    objects_info = defaultdict(int)
    if "ray_object_store_memory" in res:
        for sample in res["ray_object_store_memory"]:
            objects_info[sample.labels["Location"]] += sample.value

    print(f"Objects by location: {objects_info}")
    return objects_info


def approx_eq_dict_in(actual: Dict, expected: Dict, e: int) -> bool:
    """Check if two dict are approximately similar (with error allowed)"""
    assert set(actual.keys()) == set(expected.keys()), "Unequal key sets."

    for k, actual_v in actual.items():
        expect_v = expected[k]
        assert (
            abs(expect_v - actual_v) <= e
        ), f"expect={expect_v}, actual={actual_v}, diff allowed={e}"

    return True


def test_all_shared_memory(shutdown_only):
    """Test objects allocated in shared memory"""
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
        "IN_MEMORY": 80 * MiB,
        "SPILLED": 0,
        "UNSEALED": 0,
        "FALLBACK": 0,
    }

    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 1 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )

    # Free all of them
    del objs_in_use

    expected = {
        "IN_MEMORY": 0,
        "SPILLED": 0,
        "UNSEALED": 0,
        "FALLBACK": 0,
    }

    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 1 * KiB),
        timeout=20,
        retry_interval_ms=500,
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
        "IN_MEMORY": 100 * MiB,
        "SPILLED": 0,
        "UNSEALED": 0,
        "FALLBACK": 0,
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
        "IN_MEMORY": 100 * MiB,
        "SPILLED": 100 * MiB,
        "UNSEALED": 0,
        "FALLBACK": 0,
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
        "IN_MEMORY": 100 * MiB,
        "SPILLED": 0,
        "UNSEALED": 0,
        "FALLBACK": 0,
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
        "IN_MEMORY": 0,
        "SPILLED": 0,
        "UNSEALED": 0,
        "FALLBACK": 0,
    }
    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 1 * KiB),
        timeout=20,
        retry_interval_ms=500,
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
        "IN_MEMORY": expected_in_memory * obj_size_mb * MiB,
        "FALLBACK": 0,
        "SPILLED": 0,
        "UNSEALED": 0,
    }

    wait_for_condition(
        # 2KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 2 * KiB),
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
        "IN_MEMORY": expected_in_memory * obj_size_mb * MiB,
        "FALLBACK": expected_fallback * obj_size_mb * MiB,
        "SPILLED": 0,
        "UNSEALED": 0,
    }

    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 2 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )

    # Free all of them
    del in_use_objs
    del obj_refs
    del in_use_objs_fallback
    del obj_refs_fallback

    expected = {
        "IN_MEMORY": 0,
        "FALLBACK": 0,
        "SPILLED": 0,
        "UNSEALED": 0,
    }

    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 2 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )


if __name__ == "__main__":
    import sys
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
