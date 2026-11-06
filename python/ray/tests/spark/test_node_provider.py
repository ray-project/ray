import sys
import threading
from unittest import mock

import pytest
import requests

from ray.autoscaler._private.spark.node_provider import SparkNodeProvider
from ray.autoscaler._private.spark.spark_job_server import _start_spark_job_server
from ray.autoscaler.tags import (
    NODE_KIND_WORKER,
    TAG_RAY_LAUNCH_REQUEST,
    TAG_RAY_NODE_KIND,
    TAG_RAY_USER_NODE_TYPE,
)
from ray.util.spark.cluster_init import AutoscalingCluster


def _provider_config(server):
    return {
        "ray_head_ip": "127.0.0.1",
        "ray_head_port": 9339,
        "cluster_unique_id": "test-cluster",
        "using_stage_scheduling": False,
        "ray_temp_dir": "/tmp/ray-spark-test",
        "worker_node_options": {},
        "collect_log_to_path": None,
        "spark_job_server_port": server.server_address[1],
    }


def _worker_resources():
    return {
        "CPU": 1,
        "GPU": 0,
        "memory": 100_000_000,
        "object_store_memory": 100_000_000,
    }


def test_spark_node_provider_recovers_nodes_from_job_server():
    worker_exit = threading.Event()
    worker_started = threading.Event()
    worker_start_kwargs = []
    spark = mock.Mock()

    def run_worker(**kwargs):
        worker_start_kwargs.append(kwargs)
        worker_started.set()
        worker_exit.wait(timeout=30)

    with mock.patch(
        "ray.autoscaler._private.spark.spark_job_server._start_ray_worker_nodes",
        side_effect=run_worker,
    ):
        server = _start_spark_job_server("127.0.0.1", 0, spark, {})
        try:
            tags = {
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
                TAG_RAY_USER_NODE_TYPE: "ray.worker",
                TAG_RAY_LAUNCH_REQUEST: "launch-request-1",
            }
            provider = SparkNodeProvider(_provider_config(server), "spark")
            provider.create_node_with_resources_and_labels(
                {}, tags, 1, _worker_resources(), {}
            )
            assert worker_started.wait(timeout=10)
            assert worker_start_kwargs[0]["node_id"] == "1"
            assert worker_start_kwargs[0]["node_type"] == "ray.worker"

            spark_job_group_id = provider._gen_spark_job_group_id("1")
            assert not server.mark_node_running("1", "wrong-job-group")
            assert server.mark_node_running("1", spark_job_group_id)

            recovered_provider = SparkNodeProvider(_provider_config(server), "spark")
            assert recovered_provider.non_terminated_nodes({}) == ["0", "1"]
            assert recovered_provider.is_running("1")
            assert (
                recovered_provider.node_tags("1")[TAG_RAY_LAUNCH_REQUEST]
                == "launch-request-1"
            )

            recovered_provider.create_node_with_resources_and_labels(
                {}, tags, 1, _worker_resources(), {}
            )
            assert set(recovered_provider.non_terminated_nodes({})) == {"0", "1", "2"}

            recovered_provider.terminate_node("1")
            recovered_provider.terminate_node("1")
            assert set(recovered_provider.non_terminated_nodes({})) == {"0", "2"}
            spark.sparkContext.cancelJobGroup.assert_called_once_with(
                spark_job_group_id
            )
        finally:
            worker_exit.set()
            server.shutdown()
            server.server_close()


def test_spark_job_server_rejects_duplicate_spark_task_node_id():
    spark = mock.Mock()
    server = _start_spark_job_server("127.0.0.1", 0, spark, {})
    address = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        first = requests.post(
            address + "/check_node_id_availability", json={"node_id": "1"}
        )
        second = requests.post(
            address + "/check_node_id_availability", json={"node_id": "1"}
        )
        assert first.json() == {"available": True}
        assert second.json() == {"available": False}
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.parametrize(
    "autoscaler_v2_env,enabled",
    [
        ("0", False),
        ("1", True),
        ("true", True),
    ],
)
def test_autoscaler_head_identity(tmp_path, autoscaler_v2_env, enabled):
    cluster = AutoscalingCluster(
        head_resources={
            "CPU": 0,
            "GPU": 0,
            "memory": 100_000_000,
            "object_store_memory": 100_000_000,
        },
        worker_node_types={},
        extra_provider_config={},
        upscaling_speed=1.0,
        idle_timeout_minutes=1.0,
    )

    with mock.patch(
        "ray.util.spark.cluster_init._preallocate_ray_worker_port_range",
        return_value=(20000, 20100),
    ), mock.patch(
        "ray.util.spark.cluster_init._start_ray_head_node",
        return_value=(mock.Mock(), None),
    ) as start_head:
        cluster.start(
            ray_head_ip="127.0.0.1",
            ray_head_port=9339,
            ray_client_server_port=10001,
            ray_temp_dir=str(tmp_path),
            dashboard_options=[],
            head_node_options={},
            collect_log_to_path=None,
            ray_node_custom_env={"RAY_enable_autoscaler_v2": autoscaler_v2_env},
        )

    extra_env = start_head.call_args.kwargs["extra_env"]
    assert extra_env["RAY_enable_autoscaler_v2"] == autoscaler_v2_env
    if enabled:
        assert extra_env["RAY_CLOUD_INSTANCE_ID"] == "0"
        assert extra_env["RAY_NODE_TYPE_NAME"] == "ray.head.default"
    else:
        assert "RAY_CLOUD_INSTANCE_ID" not in extra_env
        assert "RAY_NODE_TYPE_NAME" not in extra_env


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
