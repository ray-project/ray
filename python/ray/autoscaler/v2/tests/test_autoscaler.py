import os
import sys
import time
from typing import Dict
from mock import MagicMock

import ray
import pytest
from ray.autoscaler.v2.autoscaler import Autoscaler
from ray.autoscaler.v2.tests.util import MockAutoscalingConfig
from ray._raylet import GcsClient

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
        },
    },
    "head_node_type": "ray.head.default",
    "upscaling_speed": 0,
    "idle_timeout_minutes": 10,
}


def test_autoscaler_v2():
    ctx = ray.init()
    mock_config_reader = MagicMock()
    config = DEFAULT_AUTOSCALING_CONFIG

    # Configs for the node provider
    config["provider"]["gcs_address"] = ctx.address_info["gcs_address"]
    config["provider"]["head_node_id"] = ctx.address_info["node_id"]
    config["provider"]["launch_multiple"] = True
    os.environ["RAY_FAKE_CLUSTER"] = "1"
    mock_config_reader.get_autoscaling_config.return_value = MockAutoscalingConfig(
        configs=config
    )
    gcs_client = GcsClient(ctx.address_info["gcs_address"])

    autoscaler = Autoscaler(
        session_name="test",
        config_reader=mock_config_reader,
        gcs_client=gcs_client,
    )

    ray_state = autoscaler.get_cluster_resource_state()

    autoscaler_state = autoscaler.compute_autoscaling_state(ray_state)
    print(autoscaler_state)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
