import sys
import pytest
import os
import time
import functools
from abc import ABC
from pyspark.sql import SparkSession
from ray.tests.spark.test_basic import RayOnSparkCPUClusterTestBase

import ray
from ray.util.spark.cluster_init import _init_ray_cluster


os.environ["RAY_ON_SPARK_BACKGROUND_JOB_STARTUP_WAIT"] = "1"
os.environ["RAY_ON_SPARK_RAY_WORKER_NODE_STARTUP_INTERVAL"] = "5"


class RayOnSparkGPUClusterTestBase(RayOnSparkCPUClusterTestBase, ABC):

    num_total_gpus = None
    num_gpus_per_spark_task = None

    def test_gpu_allocation(self):

        for num_spark_tasks in [self.max_spark_tasks // 2, self.max_spark_tasks]:
            with _init_ray_cluster(num_worker_nodes=num_spark_tasks, safe_mode=False):
                worker_res_list = self.get_ray_worker_resources_list()
                assert len(worker_res_list) == num_spark_tasks
                for worker_res in worker_res_list:
                    assert worker_res["GPU"] == self.num_gpus_per_spark_task

    def test_basic_ray_app_using_gpu(self):

        with _init_ray_cluster(num_worker_nodes=self.max_spark_tasks, safe_mode=False):

            @ray.remote(num_cpus=1, num_gpus=1)
            def f(_):
                # Add a sleep to avoid the task finishing too fast,
                # so that it can make all ray tasks concurrently running in all idle task slots.
                time.sleep(5)
                return [
                    int(gpu_id)
                    for gpu_id in os.environ["CUDA_VISIBLE_DEVICES"].split(",")
                ]

            futures = [f.remote(i) for i in range(self.num_total_gpus)]
            results = ray.get(futures)
            merged_results = functools.reduce(lambda x, y: x + y, results)
            # Test all ray tasks are assigned with different GPUs.
            assert sorted(merged_results) == list(range(self.num_total_gpus))


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
            .getOrCreate()
        )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
