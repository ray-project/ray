# coding: utf-8
import logging
import os
import sys

import pytest

import ray
import ray.cluster_utils

logger = logging.getLogger(__name__)


def test_put_mutable_object(ray_start_cluster):
    # ref = ray.create_mutable_object(size_bytes=1000)

    max_readers = 1
    arr = b"binary"
    ref = ray.put(arr, max_readers=max_readers)
    ray.release(ref)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
