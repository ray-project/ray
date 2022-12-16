import os
import pytest
import sys

from abc import ABC

import ray

from ray.util.spark.cluster_init import _init_ray_cluster
from ray.util.spark.utils import check_port_open
from pyspark.sql import SparkSession
import time


class RayOnSparkCPUClusterTestBase(ABC):

    spark = None
    num_total_cpus = None
    num_cpus_per_spark_task = None
    max_spark_tasks = None

    @classmethod
    def setup_class(cls):
        raise NotImplementedError()

    @classmethod
    def teardown_class(cls):
        time.sleep(8)  # Wait all background spark job canceled.
        cls.spark.stop()

    @staticmethod
    def get_ray_worker_resources_list():
        wr_list = []
        for node in ray.nodes():
            # exclude dead node and head node (with 0 CPU resource)
            if node["Alive"] and node["Resources"].get("CPU", 0) > 0:
                wr_list.append(node["Resources"])
        return wr_list

    def test_cpu_allocation(self):
        for num_spark_tasks in [self.max_spark_tasks // 2, self.max_spark_tasks]:
            with _init_ray_cluster(num_worker_nodes=num_spark_tasks, safe_mode=False):
                worker_res_list = self.get_ray_worker_resources_list()
                assert len(worker_res_list) == num_spark_tasks
                for worker_res in worker_res_list:
                    assert worker_res["CPU"] == self.num_cpus_per_spark_task

    def test_basic_ray_app(self):
        with _init_ray_cluster(
            num_worker_nodes=self.max_spark_tasks, safe_mode=False
        ) as cluster:

            @ray.remote
            def f(x):
                return x * x

            futures = [f.remote(i) for i in range(32)]
            results = ray.get(futures)
            assert results == [i * i for i in range(32)]

        # assert temp dir is removed.
        time.sleep(5)
        assert not os.path.exists(cluster.temp_dir)

    def test_ray_cluster_shutdown(self):
        with _init_ray_cluster(
            num_worker_nodes=self.max_spark_tasks, safe_mode=False
        ) as cluster:
            assert len(self.get_ray_worker_resources_list()) == self.max_spark_tasks

            # Test: cancel background spark job will cause all ray worker nodes exit.
            cluster._cancel_background_spark_job()
            time.sleep(6)

            assert len(self.get_ray_worker_resources_list()) == 0

        time.sleep(2)  # wait ray head node exit.
        # assert ray head node exit by checking head port being closed.
        hostname, port = cluster.address.split(":")
        assert not check_port_open(hostname, int(port))

    def test_background_spark_job_exit_trigger_ray_head_exit(self):
        with _init_ray_cluster(
            num_worker_nodes=self.max_spark_tasks, safe_mode=False
        ) as cluster:
            # Mimic the case the job failed unexpectedly.
            cluster._cancel_background_spark_job()
            cluster.spark_job_is_canceled = False
            time.sleep(3)

            # assert ray head node exit by checking head port being closed.
            hostname, port = cluster.address.split(":")
            assert not check_port_open(hostname, int(port))


class TestBasicSparkCluster(RayOnSparkCPUClusterTestBase):
    @classmethod
    def setup_class(cls):
        cls.num_total_cpus = 2
        cls.num_total_gpus = 0
        cls.num_cpus_per_spark_task = 1
        cls.num_gpus_per_spark_task = 0
        cls.max_spark_tasks = 2
        os.environ["SPARK_WORKER_CORES"] = "2"
        cls.spark = (
            SparkSession.builder.master("local-cluster[1, 2, 1024]")
            .config("spark.task.cpus", "1")
            .config("spark.task.maxFailures", "1")
            .getOrCreate()
        )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
