from unittest.mock import patch
import os
import sys

import pytest
from ray.util.spark.utils import (
    get_spark_task_assigned_physical_gpus,
    _calc_mem_per_ray_worker_node,
    get_target_spark_tasks,
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


def test_target_spark_tasks():
    def _mem_in_gbs(gb):
        return 1024 * 1024 * 1024 * gb

    # CPU availability sets the task count
    cpu_defined_task_count = get_target_spark_tasks(
        max_concurrent_tasks=400,
        num_spark_task_cpus=4,
        num_spark_task_gpus=None,
        ray_worker_heap_memory_bytes=_mem_in_gbs(10),
        ray_worker_object_store_memory_bytes=_mem_in_gbs(2),
        num_spark_tasks=None,
        total_cpus=400,
        total_gpus=None,
        total_heap_memory_bytes=_mem_in_gbs(800),
        total_object_store_memory_bytes=_mem_in_gbs(100),
    )
    assert cpu_defined_task_count == 100

    # Heap memory sets the task count
    heap_defined_task_count = get_target_spark_tasks(
        max_concurrent_tasks=1600,
        num_spark_task_cpus=8,
        num_spark_task_gpus=None,
        ray_worker_heap_memory_bytes=_mem_in_gbs(20),
        ray_worker_object_store_memory_bytes=_mem_in_gbs(4),
        num_spark_tasks=None,
        total_cpus=1600,
        total_gpus=None,
        total_heap_memory_bytes=_mem_in_gbs(8000),
        total_object_store_memory_bytes=_mem_in_gbs(400),
    )
    assert heap_defined_task_count == 400

    # GPU
    gpu_defined_task_count = get_target_spark_tasks(
        max_concurrent_tasks=400,
        num_spark_task_cpus=None,
        num_spark_task_gpus=4,
        ray_worker_heap_memory_bytes=_mem_in_gbs(40),
        ray_worker_object_store_memory_bytes=_mem_in_gbs(8),
        num_spark_tasks=None,
        total_cpus=None,
        total_gpus=80,
        total_heap_memory_bytes=_mem_in_gbs(400),
        total_object_store_memory_bytes=_mem_in_gbs(80),
    )
    assert gpu_defined_task_count == 20

    # Invalid configuration raises
    with pytest.raises(ValueError, match="The value of `num_worker_nodes` argument"):
        get_target_spark_tasks(
            max_concurrent_tasks=400,
            num_spark_task_cpus=None,
            num_spark_task_gpus=4,
            ray_worker_heap_memory_bytes=_mem_in_gbs(40),
            ray_worker_object_store_memory_bytes=_mem_in_gbs(8),
            num_spark_tasks=-2,
            total_cpus=None,
            total_gpus=80,
            total_heap_memory_bytes=_mem_in_gbs(400),
            total_object_store_memory_bytes=_mem_in_gbs(80),
        )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
