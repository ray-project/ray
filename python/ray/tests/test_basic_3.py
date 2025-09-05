# coding: utf-8
import gc
import logging
import math
import random
import sys
import time
from typing import Dict

import pytest

import ray
import ray.cluster_utils
from ray._common.test_utils import wait_for_condition

logger = logging.getLogger(__name__)


def test_auto_global_gc(shutdown_only):
    ray.init(num_cpus=1, object_store_memory=100 * 1024 * 1024)

    @ray.remote
    class Test:
        def __init__(self):
            self.collected = False
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


def _resource_dicts_close(d1: Dict, d2: Dict, *, abs_tol: float = 1e-4):
    """Return if all values in the dicts are within the abs_tol."""

    # A resource value of 0 is equivalent to the key not being present,
    # so filter keys whose values are 0.
    d1 = {k: v for k, v in d1.items() if v != 0}
    d2 = {k: v for k, v in d2.items() if v != 0}

    if d1.keys() != d2.keys():
        return False

    for k, v in d1.items():
        if (
            isinstance(v, float)
            and isinstance(d2[k], float)
            and math.isclose(v, d2[k], abs_tol=abs_tol)
        ):
            continue
        if v != d2[k]:
            return False
    return True


def test_many_fractional_resources(shutdown_only):
    ray.init(num_cpus=2, num_gpus=2, resources={"Custom": 2})

    def _get_available_resources() -> Dict[str, float]:
        """Get only the resources we care about in this test."""
        return {
            k: v
            for k, v in ray.available_resources().items()
            if k in {"CPU", "GPU", "Custom"}
        }

    original_available_resources = _get_available_resources()

    @ray.remote
    def g():
        return 1

    @ray.remote
    def check_assigned_resources(block: bool, expected_resources: Dict[str, float]):
        assigned_resources = ray.get_runtime_context().get_assigned_resources()

        # Have some tasks block to release their occupied resources to further
        # stress the scheduler.
        if block:
            ray.get(g.remote())

        if not _resource_dicts_close(assigned_resources, expected_resources):
            raise RuntimeError(
                "Mismatched resources.",
                "Expected:",
                expected_resources,
                "Assigned:",
                assigned_resources,
            )

    def _rand_resource_val() -> float:
        return int(random.random() * 10000) / 10000

    # Submit many tasks with random resource requirements and assert that they are
    # assigned the correct resources.
    result_ids = []
    for i in range(10):
        resources = {
            "CPU": _rand_resource_val(),
            "GPU": _rand_resource_val(),
            "Custom": _rand_resource_val(),
        }

        for block in [False, True]:
            result_ids.append(
                check_assigned_resources.options(
                    num_cpus=resources["CPU"],
                    num_gpus=resources["GPU"],
                    resources={"Custom": resources["Custom"]},
                ).remote(block, resources)
            )

    # This would raise if any assigned resources don't match the expectation.
    ray.get(result_ids)

    # Check that the available resources are reset to their original values.
    wait_for_condition(
        lambda: _get_available_resources() == original_available_resources,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
