from collections import defaultdict
import pytest
import ray
from typing import Dict

from ray._private.test_utils import (
    raw_metrics,
    wait_for_condition,
)

KiB = 1 << 10
MiB = 1 << 20


def objects_by_loc(info) -> Dict:
    res = raw_metrics(info)
    objects_info = defaultdict(int)
    if "ray_object_store_memory" in res:
        for sample in res["ray_object_store_memory"]:
            objects_info[sample.labels["Location"]] += sample.value

    print(f"Objects by location: {objects_info}")
    return objects_info


def approx_eq_dict_in(actual: Dict, expected: Dict, e: int) -> bool:
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
        _system_config={
            "metrics_report_interval_ms": 100,
        },
    )

    # Allocate 80MiB data
    objs = [ray.put(np.zeros(20 * MiB, dtype=np.uint8)) for _ in range(4)]

    expected = {
        "InMemory": 80 * MiB,
        "Fallback": 0,
    }

    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 1 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )

    # Free all of them
    del objs

    expected = {
        "InMemory": 0,
        "Fallback": 0,
    }

    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 1 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )


def test_fallback_memory(shutdown_only):
    """Test some fallback allocated objects"""
    import numpy as np

    expected_fallback = 5
    expected_in_memory = 5
    obj_size_mb = 20

    # So expected_in_memory objects could fit in object store
    delta_mb = 5
    info = ray.init(
        object_store_memory=expected_in_memory * obj_size_mb * MiB + delta_mb * MiB,
        _system_config={
            "metrics_report_interval_ms": 100,
        },
    )
    obj_refs = [
        ray.put(np.zeros(obj_size_mb * MiB, dtype=np.uint8))
        for _ in range(expected_fallback + expected_in_memory)
    ]

    # Getting and using the objects to prevent spilling
    in_use_objs = [ray.get(obj) for obj in obj_refs]

    expected = {
        "InMemory": expected_in_memory * obj_size_mb * MiB,
        "Fallback": expected_fallback * obj_size_mb * MiB,
    }

    wait_for_condition(
        # 2KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 2 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )

    # Free all of them
    del in_use_objs
    del obj_refs

    expected = {
        "InMemory": 0,
        "Fallback": 0,
    }

    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 1 * KiB),
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
