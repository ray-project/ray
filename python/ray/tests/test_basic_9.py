# coding: utf-8
import logging
import os
import sys

import pytest

import ray.cluster_utils
from ray._private.test_utils import (
    run_string_as_driver,
)

import ray

logger = logging.getLogger(__name__)


# https://github.com/ray-project/ray/issues/17842
def test_disable_cuda_devices():
    script = """
import ray
ray.init()

@ray.remote
def check():
    import os
    assert "CUDA_VISIBLE_DEVICES" not in os.environ

print("remote", ray.get(check.remote()))
"""

    run_string_as_driver(
        script, dict(os.environ, **{"RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES": "1"})
    )


def test_put_get(shutdown_only):
    ray.init(num_cpus=0)

    for i in range(100):
        value_before = i * 10 ** 6
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after

    for i in range(100):
        value_before = i * 10 ** 6 * 1.0
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after

    for i in range(100):
        value_before = "h" * i
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after

    for i in range(100):
        value_before = [1] * i
        object_ref = ray.put(value_before)
        value_after = ray.get(object_ref)
        assert value_before == value_after


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
