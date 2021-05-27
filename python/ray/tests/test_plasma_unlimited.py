import numpy as np
import time

import ray
from ray.test_utils import wait_for_condition

MB = 1024 * 1024


def _init_ray():
    ray.init(num_cpus=2, object_store_memory=500e6, _system_config={"plasma_unlimited": "1"})


def test_fallback_when_spilling_impossible_on_put():
    try:
        _init_ray()
        x1 = ray.put(np.zeros(400 * 1024 * 1024, dtype=np.uint8))
        x1p = ray.get(x1)
        # x2 will be fallback allocated on the filesystem.
        x2 = ray.put(np.zeros(400 * 1024 * 1024, dtype=np.uint8))
        x2p = ray.get(x2)
        # TODO: check we spilled 0MB
    finally:
        ray.shutdown()


def test_fallback_when_spilling_impossible_on_get():
    try:
        _init_ray()
        x1 = ray.put(np.zeros(400 * 1024 * 1024, dtype=np.uint8))
        # x1 will be spilled.
        x2 = ray.put(np.zeros(400 * 1024 * 1024, dtype=np.uint8))
        # TODO: check we spilled 400MB
        # x1 will be restored, x2 will be spilled.
        x1p = ray.get(x1)
        # TODO: check we spilled 800MB
        # x2 will be restored, triggering a fallback allocation.
        x2p = ray.get(x2)
        # TODO: check we spilled 800MB
    finally:
        ray.shutdown()


def test_spilling_when_possible_on_get():
    try:
        _init_ray()
        x1 = ray.put(np.zeros(400 * 1024 * 1024, dtype=np.uint8))
        # x1 will be spilled.
        x2 = ray.put(np.zeros(400 * 1024 * 1024, dtype=np.uint8))
        # TODO: check we spilled 400MB
        # x1 will be restored, x2 will be spilled.
        ray.get(x1)
        # TODO: check we spilled 800MB
        # x2 will be restored, spilling x1.
        ray.get(x2)
        # TODO: check we spilled 1200MB
    finally:
        ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
