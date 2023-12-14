import sys
import pytest
import os
import time
import functools
from abc import ABC
from pyspark.sql import SparkSession
from ray.tests.spark.test_basic import (
    RayOnSparkCPUClusterTestBase,
    _setup_ray_cluster,
    _setup_ray_on_spark_envs,
    _RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES,
    _RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES,
)
from ray.util.spark.utils import _calc_mem_per_ray_worker_node
from ray._private.test_utils import wait_for_condition

import ray

pytestmark = [
    pytest.mark.skipif(
        os.name != "posix",
        reason="Ray on spark only supports running on POSIX system.",
    ),
    pytest.mark.timeout(1500),
]


def setup_module():
    _setup_ray_on_spark_envs()


class RayOnSparkGPUClusterTestBase(RayOnSparkCPUClusterTestBase, ABC):
    num_total_gpus = None
    num_gpus_per_spark_task = None

    def test_gpu_allocation(self):
        for max_worker_nodes, num_cpus_worker_node, num_gpus_worker_node in [
            (
                self.max_spark_tasks // 2,
                self.num_cpus_per_spark_task,
                self.num_gpus_per_spark_task,
            ),
            (
                self.max_spark_tasks,
                self.num_cpus_per_spark_task,
                self.num_gpus_per_spark_task,
            ),
            (
                self.max_spark_tasks // 2,
                self.num_cpus_per_spark_task * 2,
                self.num_gpus_per_spark_task * 2,
            ),
            (
                self.max_spark_tasks // 2,
                self.num_cpus_per_spark_task,
                self.num_gpus_per_spark_task * 2,
            ),
        ]:
            with _setup_ray_cluster(
                max_worker_nodes=max_worker_nodes,
                num_cpus_worker_node=num_cpus_worker_node,
                num_gpus_worker_node=num_gpus_worker_node,
                head_node_options={"include_dashboard": False},
            ):
                ray.init()
                worker_res_list = self.get_ray_worker_resources_list()
                assert len(worker_res_list) == max_worker_nodes

                num_ray_task_slots = self.max_spark_tasks // (
                    num_gpus_worker_node // self.num_gpus_per_spark_task
                )
                (
                    mem_worker_node,
                    object_store_mem_worker_node,
                    _,
                ) = _calc_mem_per_ray_worker_node(
                    num_task_slots=num_ray_task_slots,
                    physical_mem_bytes=_RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES,
                    shared_mem_bytes=_RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES,
                    configured_object_store_bytes=None,
                )

                for worker_res in worker_res_list:
                    assert worker_res["CPU"] == num_cpus_worker_node
                    assert worker_res["GPU"] == num_gpus_worker_node
                    assert worker_res["memory"] == mem_worker_node
                    assert (
                        worker_res["object_store_memory"]
                        == object_store_mem_worker_node
                    )

                @ray.remote(
                    num_cpus=num_cpus_worker_node, num_gpus=num_gpus_worker_node
                )
                def f(_):
                    # Add a sleep to avoid the task finishing too fast,
                    # so that it can make all ray tasks concurrently running in all idle
                    # task slots.
                    time.sleep(5)
                    return [
                        int(gpu_id)
                        for gpu_id in os.environ["CUDA_VISIBLE_DEVICES"].split(",")
                    ]

                futures = [f.remote(i) for i in range(max_worker_nodes)]
                results = ray.get(futures)
                merged_results = functools.reduce(lambda x, y: x + y, results)
                # Test all ray tasks are assigned with different GPUs.
                assert sorted(merged_results) == list(
                    range(num_gpus_worker_node * max_worker_nodes)
                )

    def test_gpu_autoscaling(self):
        for max_worker_nodes, num_cpus_worker_node, num_gpus_worker_node in [
            (
                self.max_spark_tasks,
                self.num_cpus_per_spark_task,
                self.num_gpus_per_spark_task,
            ),
            (
                self.max_spark_tasks // 2,
                self.num_cpus_per_spark_task * 2,
                self.num_gpus_per_spark_task * 2,
            ),
        ]:
            num_ray_task_slots = self.max_spark_tasks // (
                num_gpus_worker_node // self.num_gpus_per_spark_task
            )
            (
                mem_worker_node,
                object_store_mem_worker_node,
                _,
            ) = _calc_mem_per_ray_worker_node(
                num_task_slots=num_ray_task_slots,
                physical_mem_bytes=_RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES,
                shared_mem_bytes=_RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES,
                configured_object_store_bytes=None,
            )

            with _setup_ray_cluster(
                max_worker_nodes=max_worker_nodes,
                num_cpus_worker_node=num_cpus_worker_node,
                num_gpus_worker_node=num_gpus_worker_node,
                head_node_options={"include_dashboard": False},
                min_worker_nodes=0,
                autoscale_idle_timeout_minutes=0.1,
            ):
                ray.init()
                worker_res_list = self.get_ray_worker_resources_list()
                assert len(worker_res_list) == 0

                @ray.remote(
                    num_cpus=num_cpus_worker_node, num_gpus=num_gpus_worker_node
                )
                def f(x):
                    import time

                    time.sleep(5)
                    return x * x

                # Test scale up
                futures = [f.remote(i) for i in range(8)]
                results = ray.get(futures)
                assert results == [i * i for i in range(8)]

                worker_res_list = self.get_ray_worker_resources_list()
                assert len(worker_res_list) == max_worker_nodes and all(
                    worker_res_list[i]["CPU"] == num_cpus_worker_node
                    and worker_res_list[i]["GPU"] == num_gpus_worker_node
                    and worker_res_list[i]["memory"] == mem_worker_node
                    and worker_res_list[i]["object_store_memory"]
                    == object_store_mem_worker_node
                    for i in range(max_worker_nodes)
                )

                # Test scale down
                wait_for_condition(
                    lambda: len(self.get_ray_worker_resources_list()) == 0,
                    timeout=60,
                    retry_interval_ms=1000,
                )


class TestBasicSparkGPUCluster(RayOnSparkGPUClusterTestBase):
    @classmethod
    def setup_class(cls):
        cls.num_total_cpus = 2
        cls.num_total_gpus = 2
        cls.num_cpus_per_spark_task = 1
        cls.num_gpus_per_spark_task = 1
        cls.max_spark_tasks = 2
        gpu_discovery_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "discover_2_gpu.sh"
        )
        os.environ["SPARK_WORKER_CORES"] = "4"
        cls.spark = (
            SparkSession.builder.master("local-cluster[1, 2, 1024]")
            .config("spark.task.cpus", "1")
            .config("spark.task.resource.gpu.amount", "1")
            .config("spark.executor.cores", "2")
            .config("spark.worker.resource.gpu.amount", "2")
            .config("spark.executor.resource.gpu.amount", "2")
            .config("spark.task.maxFailures", "1")
            .config(
                "spark.worker.resource.gpu.discoveryScript", gpu_discovery_script_path
            )
            .config("spark.executorEnv.RAY_ON_SPARK_WORKER_CPU_CORES", "2")
            .config("spark.executorEnv.RAY_ON_SPARK_WORKER_GPU_NUM", "2")
            .getOrCreate()
        )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
