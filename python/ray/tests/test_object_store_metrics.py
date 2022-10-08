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
    }
    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 1 * KiB),
        timeout=20,
        retry_interval_ms=500,
    )


@pytest.mark.parametrize("metric_report_interval_ms", [500, 1000, 3000])
def test_object_metric_report_interval(shutdown_only, metric_report_interval_ms):
    """Test objects allocated in shared memory"""
    import time

    info = ray.init(
        object_store_memory=100 * MiB,
        _system_config={"metrics_report_interval_ms": metric_report_interval_ms},
    )

    # Put object to make sure metric shows up
    obj = ray.get(ray.put(np.zeros(20 * MiB, dtype=np.uint8)))

    expected = {
        "IN_MEMORY": 20 * MiB,
        "SPILLED": 0,
        "UNSEALED": 0,
    }
    start = time.time()
    wait_for_condition(
        # 1KiB for metadata difference
        lambda: approx_eq_dict_in(objects_by_loc(info), expected, 1 * KiB),
        timeout=10,
        retry_interval_ms=100,
    )

    end = time.time()
    # Also shouldn't have metrics reported too quickly
    assert (end - start) * 1000 > metric_report_interval_ms, "Reporting too quickly"

    del obj


if __name__ == "__main__":
    import sys
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
