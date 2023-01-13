import sys
import pytest
import os
import time
import functools
from abc import ABC
from pyspark.sql import SparkSession
from ray.tests.spark.test_basic import RayOnSparkCPUClusterTestBase, _setup_ray_cluster

import ray

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="Ray on spark only supports running on Linux.",
)


class RayOnSparkGPUClusterTestBase(RayOnSparkCPUClusterTestBase, ABC):

    num_total_gpus = None
    num_gpus_per_spark_task = None

    def test_gpu_allocation(self):
        for num_worker_nodes, num_cpus_per_node, num_gpus_per_node in [
            (self.max_spark_tasks // 2, self.num_cpus_per_spark_task, self.num_gpus_per_spark_task),
            (self.max_spark_tasks, self.num_cpus_per_spark_task, self.num_gpus_per_spark_task),
            (self.max_spark_tasks // 2, self.num_cpus_per_spark_task * 2, self.num_gpus_per_spark_task * 2),
            (self.max_spark_tasks // 2, self.num_cpus_per_spark_task, self.num_gpus_per_spark_task * 2),
        ]:
            with _setup_ray_cluster(
                num_worker_nodes=num_worker_nodes,
                num_cpus_per_node=num_cpus_per_node,
                num_gpus_per_node=num_gpus_per_node,
                safe_mode=False,
            ):
                ray.init()
                worker_res_list = self.get_ray_worker_resources_list()
                assert len(worker_res_list) == num_worker_nodes
                for worker_res in worker_res_list:
                    assert worker_res["CPU"] == num_cpus_per_node
                    assert worker_res["GPU"] == num_gpus_per_node

                @ray.remote(num_cpus=num_cpus_per_node, num_gpus=num_gpus_per_node)
                def f(_):
                    # Add a sleep to avoid the task finishing too fast,
                    # so that it can make all ray tasks concurrently running in all idle
                    # task slots.
                    time.sleep(5)
                    return [
                        int(gpu_id)
                        for gpu_id in os.environ["CUDA_VISIBLE_DEVICES"].split(",")
                    ]

                futures = [f.remote(i) for i in range(num_worker_nodes)]
                results = ray.get(futures)
                merged_results = functools.reduce(lambda x, y: x + y, results)
                # Test all ray tasks are assigned with different GPUs.
                assert sorted(merged_results) == list(range(num_gpus_per_node * num_worker_nodes))


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
