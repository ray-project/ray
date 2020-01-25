# coding: utf-8
import collections
import io
import json
import logging
import os
import re
import string
import sys
import threading
import time

import numpy as np
import pytest

import ray
from ray.exceptions import RayTimeoutError
import ray.cluster_utils
import ray.test_utils

logger = logging.getLogger(__name__)

@ray.remote
def f(delay):
    time.sleep(delay)
    return

def test_wait_returns_one_item_with_no_timeout_by_default(ray_start_regular):
    object_ids = [f.remote(0), f.remote(0), f.remote(0), f.remote(0)]
    ready_ids, remaining_ids = ray.wait(object_ids)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 3

def test_wait_with_no_timeout_returns_num_returns_items(ray_start_regular):
    oids = [ray.put(i) for i in range(10)]
    (found, rest) = ray.wait(oids, num_returns=2)
    assert len(found) == 2
    assert len(rest) == 8

def test_wait_with_no_timeout_returns_object_ids_size_if_it_is_less_than_num_returns(ray_start_regular):
    object_ids = [f.remote(0), f.remote(0), f.remote(0)]
    ready_ids, remaining_ids = ray.wait(object_ids, num_returns=4)
    assert len(ready_ids) == 3
    assert len(remaining_ids) == 0

def test_wait_with_timeout_returns_up_to_num_returns_items(ray_start_regular):
    object_ids = [f.remote(0), f.remote(5)]
    ready_ids, remaining_ids = ray.wait(object_ids, timeout=0.5, num_returns=2)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 1

def test_wait_accepts_empty_object_ids_list(ray_start_regular):
    ready_ids, remaining_ids = ray.wait([])
    assert ready_ids == []
    assert remaining_ids == []

def test_wait_raises_if_object_ids_contains_duplicates(ray_start_regular):
    x = ray.put(1)
    with pytest.raises(Exception):
        ray.wait([x, x])

def test_wait_raises_if_object_ids_contains_wrong_types(ray_start_regular):
    with pytest.raises(TypeError):
        ray.wait([1])

def test_wait_raises_if_object_ids_is_not_a_list(ray_start_regular):
    x = ray.put(1)
    with pytest.raises(TypeError):
        ray.wait(x)
    with pytest.raises(TypeError):
        ray.wait(1)


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
