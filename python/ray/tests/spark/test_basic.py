import os
import shutil
import tempfile
import socket
import pytest
import sys

from abc import ABC

import ray

import ray.util.spark.cluster_init
from ray.util.spark import setup_ray_cluster, shutdown_ray_cluster, MAX_NUM_WORKER_NODES
from ray.util.spark.utils import check_port_open
from pyspark.sql import SparkSession
import time
import logging
from contextlib import contextmanager


@contextmanager
def _setup_ray_cluster(*args, **kwds):
    # Code to acquire resource, e.g.:
    setup_ray_cluster(*args, **kwds)
    try:
        yield ray.util.spark.cluster_init._active_ray_cluster
    finally:
        shutdown_ray_cluster()


pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="Ray on spark only supports running on Linux.",
)

_logger = logging.getLogger(__name__)


class RayOnSparkCPUClusterTestBase(ABC):

    spark = None
    num_total_cpus = None
    num_cpus_per_spark_task = None
    max_spark_tasks = None

    @classmethod
    def teardown_class(cls):
        time.sleep(10)  # Wait all background spark job canceled.
        os.environ.pop("SPARK_WORKER_CORES", None)
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
        for num_worker_nodes, num_cpus_per_node, num_worker_nodes_arg in [
            (
                self.max_spark_tasks // 2,
                self.num_cpus_per_spark_task,
                self.max_spark_tasks // 2,
            ),
            (self.max_spark_tasks, self.num_cpus_per_spark_task, MAX_NUM_WORKER_NODES),
            (
                self.max_spark_tasks // 2,
                self.num_cpus_per_spark_task * 2,
                MAX_NUM_WORKER_NODES,
            ),
            (
                self.max_spark_tasks // 2,
                self.num_cpus_per_spark_task * 2,
                self.max_spark_tasks // 2 + 1,
            ),  # Test case: requesting resources exceeding all cluster resources
        ]:
            with _setup_ray_cluster(
                num_worker_nodes=num_worker_nodes_arg,
                num_cpus_per_node=num_cpus_per_node,
                head_node_options={"include_dashboard": False},
            ):
                ray.init()
                worker_res_list = self.get_ray_worker_resources_list()
                assert len(worker_res_list) == num_worker_nodes
                for worker_res in worker_res_list:
                    assert worker_res["CPU"] == num_cpus_per_node

    def test_public_api(self):
        try:
            ray_temp_root_dir = tempfile.mkdtemp()
            collect_log_to_path = tempfile.mkdtemp()
            setup_ray_cluster(
                num_worker_nodes=MAX_NUM_WORKER_NODES,
                collect_log_to_path=collect_log_to_path,
                ray_temp_root_dir=ray_temp_root_dir,
                head_node_options={"include_dashboard": True},
            )

            assert (
                os.environ["RAY_ADDRESS"]
                == ray.util.spark.cluster_init._active_ray_cluster.address
            )

            ray.init()

            @ray.remote
            def f(x):
                return x * x

            futures = [f.remote(i) for i in range(32)]
            results = ray.get(futures)
            assert results == [i * i for i in range(32)]

            shutdown_ray_cluster()

            assert "RAY_ADDRESS" not in os.environ

            time.sleep(7)
            # assert temp dir is removed.
            assert len(os.listdir(ray_temp_root_dir)) == 1 and os.listdir(
                ray_temp_root_dir
            )[0].endswith(".lock")

            # assert logs are copied to specified path
            listed_items = os.listdir(collect_log_to_path)
            assert len(listed_items) == 1 and listed_items[0].startswith("ray-")
            log_dest_dir = os.path.join(
                collect_log_to_path, listed_items[0], socket.gethostname()
            )
            assert os.path.exists(log_dest_dir) and len(os.listdir(log_dest_dir)) > 0
        finally:
            if ray.util.spark.cluster_init._active_ray_cluster is not None:
                # if the test raised error and does not destroy cluster,
                # destroy it here.
                ray.util.spark.cluster_init._active_ray_cluster.shutdown()
                time.sleep(5)
            shutil.rmtree(ray_temp_root_dir, ignore_errors=True)
            shutil.rmtree(collect_log_to_path, ignore_errors=True)

    def test_ray_cluster_shutdown(self):
        with _setup_ray_cluster(num_worker_nodes=self.max_spark_tasks) as cluster:
            ray.init()
            assert len(self.get_ray_worker_resources_list()) == self.max_spark_tasks

            # Test: cancel background spark job will cause all ray worker nodes exit.
            cluster._cancel_background_spark_job()
            time.sleep(8)

            assert len(self.get_ray_worker_resources_list()) == 0

        time.sleep(2)  # wait ray head node exit.
        # assert ray head node exit by checking head port being closed.
        hostname, port = cluster.address.split(":")
        assert not check_port_open(hostname, int(port))

    def test_background_spark_job_exit_trigger_ray_head_exit(self):
        with _setup_ray_cluster(num_worker_nodes=self.max_spark_tasks) as cluster:
            ray.init()
            # Mimic the case the job failed unexpectedly.
            cluster._cancel_background_spark_job()
            cluster.spark_job_is_canceled = False
            time.sleep(5)

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
            .config("spark.executorEnv.RAY_ON_SPARK_WORKER_CPU_CORES", "2")
            .getOrCreate()
        )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
