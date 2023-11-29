# coding: utf-8
import logging
import os
import sys

import pytest

import ray
import ray.cluster_utils

logger = logging.getLogger(__name__)


def test_put_mutable_object(ray_start_cluster):
    ray.init()
    ref = ray._create_mutable_object(1000)
    ray._put_mutable_object(b"hello", ref, num_readers=1)
    assert ray.get(ref) == b"hello"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
