from unittest.mock import patch
import os
import sys

import pytest
from ray.util.spark.utils import (
    get_spark_task_assigned_physical_gpus,
    _calc_mem_per_ray_worker_node,
    _get_avail_mem_per_ray_worker_node,
)

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="Ray on spark only supports running on Linux.",
)


def test_get_spark_task_assigned_physical_gpus():
    with patch.dict(os.environ, {}, clear=True):
        assert get_spark_task_assigned_physical_gpus([2, 5]) == [2, 5]

    with patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "2,3,6"}, clear=True):
        assert get_spark_task_assigned_physical_gpus([0, 1]) == [2, 3]
        assert get_spark_task_assigned_physical_gpus([0, 2]) == [2, 6]


def test_calc_mem_per_ray_worker_node():
    assert _calc_mem_per_ray_worker_node(4, 1000000, 400000, 100000) == (120000, 80000)
    assert _calc_mem_per_ray_worker_node(4, 1000000, 400000, 70000) == (130000, 70000)
    assert _calc_mem_per_ray_worker_node(4, 1000000, 400000, None) == (140000, 60000)
    assert _calc_mem_per_ray_worker_node(4, 1000000, 200000, None) == (160000, 40000)


def test_get_avail_mem_per_ray_worker_node(monkeypatch):
    monkeypatch.setenv("RAY_ON_SPARK_WORKER_CPU_CORES", "4")
    monkeypatch.setenv("RAY_ON_SPARK_WORKER_GPU_NUM", "8")
    monkeypatch.setenv("RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES", "1000000")
    monkeypatch.setenv("RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES", "500000")

    assert _get_avail_mem_per_ray_worker_node(
        num_cpus_per_node=1,
        num_gpus_per_node=2,
        object_store_memory_per_node=None,
    ) == (140000, 60000, None)

    assert _get_avail_mem_per_ray_worker_node(
        num_cpus_per_node=1,
        num_gpus_per_node=2,
        object_store_memory_per_node=80000,
    ) == (120000, 80000, None)

    assert _get_avail_mem_per_ray_worker_node(
        num_cpus_per_node=1,
        num_gpus_per_node=2,
        object_store_memory_per_node=120000,
    ) == (100000, 100000, None)

    assert _get_avail_mem_per_ray_worker_node(
        num_cpus_per_node=2,
        num_gpus_per_node=2,
        object_store_memory_per_node=None,
    ) == (280000, 120000, None)

    assert _get_avail_mem_per_ray_worker_node(
        num_cpus_per_node=1,
        num_gpus_per_node=4,
        object_store_memory_per_node=None,
    ) == (280000, 120000, None)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
