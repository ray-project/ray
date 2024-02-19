import os
import sys
import time
from typing import Dict
from mock import MagicMock

from pprint import pprint as print

import ray
import pytest
from ray.autoscaler._private.fake_multi_node.node_provider import FAKE_HEAD_NODE_ID
from ray.autoscaler.v2.autoscaler import Autoscaler
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.autoscaler.v2.sdk import get_cluster_status, request_cluster_resources
from ray._private.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.cluster_utils import Cluster

DEFAULT_AUTOSCALING_CONFIG = {
    "cluster_name": "fake_multinode",
    "max_workers": 8,
    "provider": {
        "type": "fake_multinode",
    },
    "available_node_types": {
        "ray.head.default": {
            "resources": {
                "CPU": 0,
            },
            "max_workers": 0,
            "node_config": {},
        },
        "ray.worker.cpu": {
            "resources": {
                "CPU": 1,
            },
            "min_workers": 0,
            "max_workers": 10,
            "node_config": {},
        },
        "ray.worker.gpu": {
            "resources": {
                "GPU": 1,
            },
            "min_workers": 0,
            "max_workers": 10,
            "node_config": {},
        },
    },
    "head_node_type": "ray.head.default",
    "upscaling_speed": 0,
    "idle_timeout_minutes": 0.08,
}


def make_head_node(**node_args):
    default_kwargs = {
        "num_cpus": 0,
        "num_gpus": 0,
        "object_store_memory": 150 * 1024 * 1024,  # 150 MiB
        "min_worker_port": 0,
        "max_worker_port": 0,
        "dashboard_port": None,
    }
    ray_params = ray._private.parameter.RayParams(**node_args)
    ray_params.update_if_absent(**default_kwargs)

    node = ray._private.node.Node(
        ray_params,
        head=True,
    )
    # redis_address = self.head_node.redis_address
    # redis_password = node_args.get(
    #     "redis_password", ray_constants.REDIS_DEFAULT_PASSWORD
    # )
    # self.webui_url = self.head_node.webui_url
    # # Init global state accessor when creating head node.
    # gcs_options = GcsClientOptions.from_gcs_address(node.gcs_address)
    # self.global_state._initialize_global_state(gcs_options)
    # Write the Ray cluster address for convenience in unit
    # testing. ray.init() and ray.init(address="auto") will connect
    # to the local cluster.
    ray._private.utils.write_ray_address(node.gcs_address)

    return node


def make_cluster(head_node_kwargs: Dict = None):
    cluster = Cluster(
        initialize_head=True, head_node_args=head_node_kwargs, connect=True
    )
    return cluster


def test_autoscaler_v2():
    cluster = make_cluster(
        head_node_kwargs={
            "env_vars": {
                "RAY_CLOUD_INSTANCE_ID": FAKE_HEAD_NODE_ID,
                "RAY_OVERRIDE_NODE_ID_FOR_TESTING": FAKE_HEAD_NODE_ID,
            },
            "num_cpus": 0,
        }
    )

    # head_node = make_head_node(
    #     env_vars={
    #         "RAY_CLOUD_INSTANCE_ID": FAKE_HEAD_NODE_ID,
    #         "RAY_OVERRIDE_NODE_ID_FOR_TESTING": FAKE_HEAD_NODE_ID,
    #     }
    # )
    # print(head_node)
    mock_config_reader = MagicMock()
    config = DEFAULT_AUTOSCALING_CONFIG
    gcs_address = cluster.address

    # Configs for the node provider
    config["provider"]["gcs_address"] = gcs_address
    config["provider"]["head_node_id"] = FAKE_HEAD_NODE_ID
    config["provider"]["launch_multiple"] = True
    os.environ["RAY_FAKE_CLUSTER"] = "1"
    mock_config_reader.get_autoscaling_config.return_value = AutoscalingConfig(
        configs=config, skip_content_hash=True
    )
    gcs_address = gcs_address
    gcs_client = GcsClient(gcs_address)

    autoscaler = Autoscaler(
        session_name="test",
        config_reader=mock_config_reader,
        gcs_client=gcs_client,
    )
    autoscaler.initialize()
    ray_state = autoscaler.get_cluster_resource_state()
    autoscaler.update_autoscaling_state(ray_state)

    # Resource requests
    print("=================== Test scaling up constraint 1/2====================")
    request_cluster_resources(gcs_address, [{"CPU": 1}, {"GPU": 1}])

    def verify():
        cluster_state = get_cluster_status(gcs_address)
        ray_state = autoscaler.get_cluster_resource_state()
        autoscaling_state = autoscaler.update_autoscaling_state(ray_state)
        assert len(cluster_state.active_nodes + cluster_state.idle_nodes) == 3
        return True

    wait_for_condition(verify, retry_interval_ms=2000)

    # Test scaling down shouldn't happen
    print("=================== Test scaling down constraint 2/2 ====================")
    import time

    time.sleep(6)

    wait_for_condition(verify, retry_interval_ms=2000)

    # Test scaling down.
    print("=================== Test scaling down idle ====================")
    request_cluster_resources(gcs_address, [])

    def verify():
        cluster_state = get_cluster_status(gcs_address)
        ray_state = autoscaler.get_cluster_resource_state()
        autoscaling_state = autoscaler.update_autoscaling_state(ray_state)
        assert len(cluster_state.active_nodes + cluster_state.idle_nodes) == 1
        return True

    wait_for_condition(verify, retry_interval_ms=2000)
    print("=================== Test scaling up with tasks ====================")

    # Test scaling up again with tasks
    @ray.remote
    def task():
        while True:
            time.sleep(2)
            print("running...")

    a = task.options(num_cpus=1).remote()
    b = task.options(num_cpus=0, num_gpus=1).remote()

    def verify():
        cluster_state = get_cluster_status(gcs_address)
        ray_state = autoscaler.get_cluster_resource_state()
        print(ray_state)
        autoscaling_state = autoscaler.update_autoscaling_state(ray_state)
        assert len(cluster_state.active_nodes + cluster_state.idle_nodes) == 3
        return True

    wait_for_condition(verify, retry_interval_ms=2000)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
