from unittest.mock import patch
import os

from ray.spark import (
    get_spark_task_assigned_physical_gpus,
)

from ray.spark.utils import _calc_mem_per_ray_worker


def test_get_spark_task_assigned_physical_gpus():
    with patch.dict(os.environ, {}, clear=True):
        assert get_spark_task_assigned_physical_gpus([2, 5]) == [2, 5]

    with patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "2,3,6"}, clear=True):
        assert get_spark_task_assigned_physical_gpus([0, 1]) == [2, 3]
        assert get_spark_task_assigned_physical_gpus([0, 2]) == [2, 6]


def test_calc_mem_per_ray_worker():
    assert _calc_mem_per_ray_worker(4, 1000000, 400000) == (120000, 80000)
    assert _calc_mem_per_ray_worker(6, 1000000, 400000) == (80000, 53333)
