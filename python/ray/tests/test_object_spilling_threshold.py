import pytest

import numpy as np
import ray

OBJECT_STORE_MEMORY = 2_000_000_000
OBJECT_SIZE = 200_000_000
NUM_OBJECTS = 10


def _check_spilled(expected_spilled):
    def ok():
        s = ray.internal.internal_api.memory_summary(stats_only=True)
        print(s)
        if expected_spilled:
            return "Spilled " in s
        else:
            return "Spilled " not in s

    ray.test_utils.wait_for_condition(ok, timeout=5, retry_interval_ms=3000)


def _test_object_spilling_threshold(thres, expected_spilled):
    ray.init(
        object_store_memory=OBJECT_STORE_MEMORY,
        _system_config={"object_spilling_threshold": thres} if thres else {})

    objs = [
        ray.put(np.empty(OBJECT_SIZE, dtype=np.uint8))
        for _ in range(NUM_OBJECTS)
    ]
    print(objs)

    _check_spilled(expected_spilled)

    ray.shutdown()


def test_object_spilling_threshold_0_1():
    _test_object_spilling_threshold(0.1, True)


def test_object_spilling_threshold_1_0():
    _test_object_spilling_threshold(1.0, False)


def test_object_spilling_threshold_default():
    _test_object_spilling_threshold(None, False)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
