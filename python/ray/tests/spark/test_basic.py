import os
import shutil
import tempfile
import socket
import threading
import re
import pytest
import sys
from unittest import mock
from abc import ABC

import ray

import ray.util.spark.cluster_init
from ray.util.spark import (
    setup_ray_cluster,
    shutdown_ray_cluster,
    setup_global_ray_cluster,
    MAX_NUM_WORKER_NODES,
)
from ray.util.spark.utils import (
    _calc_mem_per_ray_worker_node,
)
from pyspark.sql import SparkSession
import time
import logging
from contextlib import contextmanager
from ray._private.test_utils import wait_for_condition


pytestmark = [
    pytest.mark.skipif(
        os.name != "posix",
        reason="Ray on spark only supports running on POSIX system.",
    ),
    pytest.mark.timeout(1500),
]


_RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES = 2000000000
_RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES = 10000000000


def _setup_ray_on_spark_envs():
    os.environ["RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES"] = str(
        _RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES
    )
    os.environ["RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES"] = str(
        _RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES
    )


def setup_module():
    _setup_ray_on_spark_envs()


@contextmanager
def _setup_ray_cluster(*args, **kwds):
    setup_ray_cluster(*args, **kwds)
    try:
        yield ray.util.spark.cluster_init._active_ray_cluster
    finally:
        shutdown_ray_cluster()


_logger = logging.getLogger(__name__)


class RayOnSparkCPUClusterTestBase(ABC):
    spark = None
    num_total_cpus = None
    num_total_gpus = None
    num_cpus_per_spark_task = None
    num_gpus_per_spark_task = None
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
        for max_worker_nodes, num_cpus_worker_node, max_worker_nodes_arg in [
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
            num_ray_task_slots = self.max_spark_tasks // (
                num_cpus_worker_node // self.num_cpus_per_spark_task
            )
            (
                mem_worker_node,
                object_store_mem_worker_node,
                _,
            ) = _calc_mem_per_ray_worker_node(
                num_task_slots=num_ray_task_slots,
                physical_mem_bytes=_RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES,
                shared_mem_bytes=_RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES,
                configured_heap_memory_bytes=None,
                configured_object_store_bytes=None,
            )
            with _setup_ray_cluster(
                max_worker_nodes=max_worker_nodes_arg,
                num_cpus_worker_node=num_cpus_worker_node,
                num_gpus_worker_node=0,
                head_node_options={"include_dashboard": False},
            ):
                ray.init()
                worker_res_list = self.get_ray_worker_resources_list()
                assert len(worker_res_list) == max_worker_nodes
                for worker_res in worker_res_list:
                    assert (
                        worker_res["CPU"] == num_cpus_worker_node
                        and worker_res["memory"] == mem_worker_node
                        and worker_res["object_store_memory"]
                        == object_store_mem_worker_node
                    )

    def test_public_api(self):
        try:
            ray_temp_root_dir = tempfile.mkdtemp(dir="/tmp")
            collect_log_to_path = tempfile.mkdtemp(dir="/tmp")
            # Test the case that `collect_log_to_path` directory does not exist.
            shutil.rmtree(collect_log_to_path, ignore_errors=True)
            setup_ray_cluster(
                max_worker_nodes=MAX_NUM_WORKER_NODES,
                num_cpus_worker_node=1,
                num_gpus_worker_node=0,
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

    def test_autoscaling(self):
        for max_worker_nodes, num_cpus_worker_node, min_worker_nodes in [
            (self.max_spark_tasks, self.num_cpus_per_spark_task, 0),
            (self.max_spark_tasks // 2, self.num_cpus_per_spark_task * 2, 0),
            (self.max_spark_tasks, self.num_cpus_per_spark_task, 1),
        ]:
            num_ray_task_slots = self.max_spark_tasks // (
                num_cpus_worker_node // self.num_cpus_per_spark_task
            )
            (
                mem_worker_node,
                object_store_mem_worker_node,
                _,
            ) = _calc_mem_per_ray_worker_node(
                num_task_slots=num_ray_task_slots,
                physical_mem_bytes=_RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES,
                shared_mem_bytes=_RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES,
                configured_heap_memory_bytes=None,
                configured_object_store_bytes=None,
            )

            with _setup_ray_cluster(
                max_worker_nodes=max_worker_nodes,
                min_worker_nodes=min_worker_nodes,
                num_cpus_worker_node=num_cpus_worker_node,
                num_gpus_worker_node=0,
                head_node_options={"include_dashboard": False},
                autoscale_idle_timeout_minutes=0.1,
            ):
                ray.init()
                worker_res_list = self.get_ray_worker_resources_list()
                assert len(worker_res_list) == min_worker_nodes

                @ray.remote(num_cpus=num_cpus_worker_node)
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
                    and worker_res_list[i]["memory"] == mem_worker_node
                    and worker_res_list[i]["object_store_memory"]
                    == object_store_mem_worker_node
                    for i in range(max_worker_nodes)
                )

                # Test scale down
                wait_for_condition(
                    lambda: len(self.get_ray_worker_resources_list())
                    == min_worker_nodes,
                    timeout=60,
                    retry_interval_ms=1000,
                )
                if min_worker_nodes > 0:
                    # Test scaling down keeps nodes number >= min_worker_nodes
                    time.sleep(30)
                    assert len(self.get_ray_worker_resources_list()) == min_worker_nodes


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


class TestSparkLocalCluster:
    @classmethod
    def setup_class(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .config("spark.task.cpus", "1")
            .config("spark.task.maxFailures", "1")
            .getOrCreate()
        )

    @classmethod
    def teardown_class(cls):
        time.sleep(10)  # Wait all background spark job canceled.
        cls.spark.stop()

    def test_basic(self):
        local_addr, remote_addr = setup_ray_cluster(
            max_worker_nodes=2,
            head_node_options={"include_dashboard": False},
            collect_log_to_path="/tmp/ray_log_collect",
        )

        for cluster_addr in [local_addr, remote_addr]:
            ray.init(address=cluster_addr)

            @ray.remote
            def f(x):
                return x * x

            futures = [f.remote(i) for i in range(32)]
            results = ray.get(futures)
            assert results == [i * i for i in range(32)]

            ray.shutdown()

        shutdown_ray_cluster()

    def test_use_driver_resources(self):
        setup_ray_cluster(
            max_worker_nodes=1,
            num_cpus_head_node=3,
            num_gpus_head_node=2,
            object_store_memory_head_node=256 * 1024 * 1024,
            head_node_options={"include_dashboard": False},
            min_worker_nodes=0,
        )

        ray.init()
        head_resources_list = []
        for node in ray.nodes():
            if node["Alive"] and node["Resources"].get("CPU", 0) == 3:
                head_resources_list.append(node["Resources"])
        assert len(head_resources_list) == 1
        head_resources = head_resources_list[0]
        assert head_resources.get("GPU", 0) == 2

        shutdown_ray_cluster()

    def test_setup_global_ray_cluster(self):
        shutil.rmtree("/tmp/ray", ignore_errors=True)

        assert ray.util.spark.cluster_init._global_ray_cluster_cancel_event is None

        def start_serve_thread():
            def serve():
                try:
                    with mock.patch(
                        "ray.util.spark.cluster_init.get_spark_session",
                        return_value=self.spark,
                    ):
                        setup_global_ray_cluster(
                            max_worker_nodes=1,
                            min_worker_nodes=0,
                        )
                except BaseException:
                    # For debugging testing failure.
                    import traceback

                    traceback.print_exc()
                    raise

            threading.Thread(target=serve, daemon=True).start()

        start_serve_thread()

        wait_for_condition(
            (
                lambda: ray.util.spark.cluster_init._global_ray_cluster_cancel_event
                is not None
            ),
            timeout=120,
            retry_interval_ms=10000,
        )

        # assert it uses default temp directory
        assert os.path.exists("/tmp/ray")

        # assert we can connect to it on client server port 10001
        assert (
            ray.util.spark.cluster_init._active_ray_cluster.ray_client_server_port
            == 10001
        )

        with mock.patch("ray.util.spark.cluster_init._active_ray_cluster", None):
            # assert we cannot create another global mode cluster at a time
            with pytest.raises(
                ValueError,
                match=re.compile(
                    "Acquiring global lock failed for setting up new global mode "
                    "Ray on spark cluster"
                ),
            ):
                setup_global_ray_cluster(
                    max_worker_nodes=1,
                    min_worker_nodes=0,
                )

        # shut down the cluster
        ray.util.spark.cluster_init._global_ray_cluster_cancel_event.set()

        # assert temp directory is deleted
        wait_for_condition(
            lambda: not os.path.exists("/tmp/ray"),
            timeout=60,
            retry_interval_ms=10000,
        )

    def test_autoscaling_config_generation(self):
        from ray.util.spark.cluster_init import AutoscalingCluster

        autoscaling_cluster = AutoscalingCluster(
            head_resources={
                "CPU": 3,
                "GPU": 4,
                "memory": 10000000,
                "object_store_memory": 20000000,
            },
            worker_node_types={
                "ray.worker": {
                    "resources": {
                        "CPU": 5,
                        "GPU": 6,
                        "memory": 30000000,
                        "object_store_memory": 40000000,
                    },
                    "node_config": {},
                    "min_workers": 0,
                    "max_workers": 100,
                },
            },
            extra_provider_config={
                "extra_aa": "abc",
                "extra_bb": 789,
            },
            upscaling_speed=2.0,
            idle_timeout_minutes=3.0,
        )

        config = autoscaling_cluster._config

        assert config["max_workers"] == 100

        assert config["available_node_types"]["ray.head.default"] == {
            "resources": {
                "CPU": 3,
                "GPU": 4,
                "memory": 10000000,
                "object_store_memory": 20000000,
            },
            "node_config": {},
            "max_workers": 0,
        }
        assert config["available_node_types"]["ray.worker"] == {
            "resources": {
                "CPU": 5,
                "GPU": 6,
                "memory": 30000000,
                "object_store_memory": 40000000,
            },
            "node_config": {},
            "min_workers": 0,
            "max_workers": 100,
        }
        assert config["upscaling_speed"] == 2.0
        assert config["idle_timeout_minutes"] == 3.0
        assert config["provider"]["extra_aa"] == "abc"
        assert config["provider"]["extra_bb"] == 789

    def test_start_ray_node_in_new_process_group(self):
        from ray.util.spark.cluster_init import _start_ray_head_node

        proc, _ = _start_ray_head_node(
            [
                sys.executable,
                "-m",
                "ray.util.spark.start_ray_node",
                "--head",
                "--block",
                "--port=44335",
            ],
            synchronous=False,
            extra_env={
                "RAY_ON_SPARK_COLLECT_LOG_TO_PATH": "",
                "RAY_ON_SPARK_START_RAY_PARENT_PID": str(os.getpid()),
            },
        )
        time.sleep(10)

        # Assert the created Ray head node process has a different
        # group id from parent process group id.
        assert os.getpgid(proc.pid) != os.getpgrp()
        proc.terminate()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
