from collections import defaultdict
import pytest
from typing import Dict

import ray
from ray._private.test_utils import (
    raw_metrics,
    wait_for_condition,
)
from ray._private.worker import RayContext
from ray.util.placement_group import remove_placement_group

_SYSTEM_CONFIG = {
    "metrics_report_interval_ms": 200,
}


def groups_by_state(info: RayContext) -> Dict:
    res = raw_metrics(info)
    info = defaultdict(int)
    if "ray_placement_groups" in res:
        for sample in res["ray_placement_groups"]:
            info[sample.labels["State"]] += sample.value
    for k, v in info.copy().items():
        if v == 0:
            del info[k]
    print(f"Groups by state: {info}")
    return info


def test_basic_states(shutdown_only):
    info = ray.init(num_cpus=3, _system_config=_SYSTEM_CONFIG)

    pg1 = ray.util.placement_group(bundles=[{"CPU": 1}])
    pg2 = ray.util.placement_group(bundles=[{"CPU": 1}])
    pg3 = ray.util.placement_group(bundles=[{"CPU": 4}])
    ray.get([pg1.ready(), pg2.ready()])

    expected = {
        "CREATED": 2,
        "PENDING": 1,
    }
    wait_for_condition(
        lambda: groups_by_state(info) == expected,
        timeout=20,
        retry_interval_ms=500,
    )

    remove_placement_group(pg1)
    remove_placement_group(pg2)
    remove_placement_group(pg3)

    expected = {
        "REMOVED": 3,
    }
    wait_for_condition(
        lambda: groups_by_state(info) == expected,
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
