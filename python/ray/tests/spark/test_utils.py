from unittest.mock import patch
import os

from ray.spark import (
    get_spark_task_assigned_physical_gpus,
    get_spark_driver_hostname,
)

from ray.spark.utils import _calc_mem_per_ray_worker


def test_get_spark_task_assigned_physical_gpus():
    with patch.dict(os.environ, {}, clear=True):
        assert get_spark_task_assigned_physical_gpus({"gpu": [2, 5]}) == [2, 5]

    with patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "2,3,6"}, clear=True):
        assert get_spark_task_assigned_physical_gpus({"gpu": [0, 1]}) == [2, 3]
        assert get_spark_task_assigned_physical_gpus({"gpu": [0, 2]}) == [2, 6]


def test_get_spark_driver_hostname():
    assert get_spark_driver_hostname("local") == "127.0.0.1"
    assert get_spark_driver_hostname("local[4]") == "127.0.0.1"
    assert get_spark_driver_hostname("local-cluster[1, 4, 1024]") == "127.0.0.1"
    assert get_spark_driver_hostname("spark://23.195.26.187:7077") == "23.195.26.187"
    assert get_spark_driver_hostname("spark://aa.xx.yy:7077") == "aa.xx.yy"


def test_calc_mem_per_ray_worker():
    assert _calc_mem_per_ray_worker(4, 1000000, 400000) == (120000, 80000)
    assert _calc_mem_per_ray_worker(6, 1000000, 400000) == (80000, 53333)
