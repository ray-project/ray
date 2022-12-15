from unittest.mock import patch
import os
import sys

import pytest
from ray.util.spark.utils import (
    get_spark_task_assigned_physical_gpus,
    _calc_mem_per_ray_worker_node,
)


def test_get_spark_task_assigned_physical_gpus():
    with patch.dict(os.environ, {}, clear=True):
        assert get_spark_task_assigned_physical_gpus([2, 5]) == [2, 5]

    with patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "2,3,6"}, clear=True):
        assert get_spark_task_assigned_physical_gpus([0, 1]) == [2, 3]
        assert get_spark_task_assigned_physical_gpus([0, 2]) == [2, 6]


def test_calc_mem_per_ray_worker_node():
    assert _calc_mem_per_ray_worker_node(4, 1000000, 400000, 0.4) == (120000, 80000)
    assert _calc_mem_per_ray_worker_node(6, 1000000, 400000, 0.4) == (80000, 53333)
    assert _calc_mem_per_ray_worker_node(4, 800000, 600000, 0.2) == (128000, 32000)
    assert _calc_mem_per_ray_worker_node(4, 800000, 600000, 0.5) == (80000, 80000)
    assert _calc_mem_per_ray_worker_node(8, 2000000, 600000, 0.3) == (140000, 60000)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
