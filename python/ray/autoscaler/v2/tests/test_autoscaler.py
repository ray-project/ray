import logging
import os
import subprocess
import sys
import time
from unittest.mock import MagicMock

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.autoscaler._private.fake_multi_node.node_provider import FAKE_HEAD_NODE_ID
from ray.autoscaler.v2.autoscaler import Autoscaler
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.autoscaler.v2.sdk import get_cluster_status, request_cluster_resources
from ray.autoscaler.v2.tests.util import MockEventLogger
from ray.cluster_utils import Cluster

logger = logging.getLogger(__name__)

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
    "idle_timeout_minutes": 0.08,  # ~5 seconds
}


@pytest.fixture(scope="function")
def make_autoscaler():
    ctx = {}

    def _make_autoscaler(config):
        head_node_kwargs = {
            "env_vars": {
                "RAY_CLOUD_INSTANCE_ID": FAKE_HEAD_NODE_ID,
                "RAY_OVERRIDE_NODE_ID_FOR_TESTING": FAKE_HEAD_NODE_ID,
                "RAY_NODE_TYPE_NAME": "ray.head.default",
            },
            "num_cpus": config["available_node_types"]["ray.head.default"]["resources"][
                "CPU"
            ],
        }
        cluster = Cluster(
            initialize_head=True, head_node_args=head_node_kwargs, connect=True
        )
        ctx["cluster"] = cluster

        mock_config_reader = MagicMock()
        gcs_address = cluster.address

        # Configs for the node provider
        config["provider"]["gcs_address"] = gcs_address
        config["provider"]["head_node_id"] = FAKE_HEAD_NODE_ID
        config["provider"]["launch_multiple"] = True
        os.environ["RAY_FAKE_CLUSTER"] = "1"
        mock_config_reader.get_cached_autoscaling_config.return_value = (
            AutoscalingConfig(configs=config, skip_content_hash=True)
        )
        gcs_address = gcs_address
        gcs_client = GcsClient(gcs_address)

        event_logger = AutoscalerEventLogger(MockEventLogger(logger))

        autoscaler = Autoscaler(
            session_name="test",
            config_reader=mock_config_reader,
            gcs_client=gcs_client,
            event_logger=event_logger,
        )

        return autoscaler

    yield _make_autoscaler
    try:
        ray.shutdown()
        ctx["cluster"].shutdown()
    except Exception:
        logger.exception("Error during teardown")
        # Run ray stop to clean up everything
        subprocess.run(
            ["ray", "stop", "--force"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )


def test_basic_scaling(make_autoscaler):
    config = DEFAULT_AUTOSCALING_CONFIG
    autoscaler = make_autoscaler(DEFAULT_AUTOSCALING_CONFIG)
    gcs_address = autoscaler._gcs_client.address

    # Resource requests
    print("=================== Test scaling up constraint 1/2====================")
    request_cluster_resources(gcs_address, [{"CPU": 1}, {"GPU": 1}])

    def verify():
        autoscaler.update_autoscaling_state()
        cluster_state = get_cluster_status(gcs_address)
        assert len(cluster_state.active_nodes + cluster_state.idle_nodes) == 3
        return True

    wait_for_condition(verify, retry_interval_ms=2000)

    # Test scaling down shouldn't happen
    print("=================== Test scaling down constraint 2/2 ====================")

    idle_timeout_s = config["idle_timeout_minutes"] * 60
    time.sleep(idle_timeout_s + 2)

    wait_for_condition(verify, retry_interval_ms=2000)

    # Test scaling down.
    print("=================== Test scaling down idle ====================")
    request_cluster_resources(gcs_address, [])

    def verify():
        autoscaler.update_autoscaling_state()
        cluster_state = get_cluster_status(gcs_address)
        assert len(cluster_state.active_nodes + cluster_state.idle_nodes) == 1
        return True

    wait_for_condition(verify, retry_interval_ms=2000)

    # Test scaling up again with tasks
    print("=================== Test scaling up with tasks ====================")

    @ray.remote
    def task():
        time.sleep(999)

    task.options(num_cpus=1).remote()
    task.options(num_cpus=0, num_gpus=1).remote()

    def verify():
        autoscaler.update_autoscaling_state()
        cluster_state = get_cluster_status(gcs_address)
        assert len(cluster_state.active_nodes + cluster_state.idle_nodes) == 3
        return True

    wait_for_condition(verify, retry_interval_ms=2000)

    # Test with placement groups
    print(
        "=================== Test scaling up with placement groups ===================="
    )

    # Spread to create another 2 nodes.
    ray.util.placement_group(
        name="pg", strategy="STRICT_SPREAD", bundles=[{"CPU": 0.5}, {"CPU": 0.5}]
    )

    def verify():
        autoscaler.update_autoscaling_state()
        cluster_state = get_cluster_status(gcs_address)
        assert len(cluster_state.active_nodes + cluster_state.idle_nodes) == 5
        return True

    wait_for_condition(verify, retry_interval_ms=2000)
    # Pack with feasible ones
    ray.util.placement_group(
        name="pg_feasible", strategy="STRICT_PACK", bundles=[{"CPU": 0.5}, {"CPU": 0.5}]
    )

    def verify():
        autoscaler.update_autoscaling_state()
        cluster_state = get_cluster_status(gcs_address)
        assert len(cluster_state.active_nodes + cluster_state.idle_nodes) == 6
        return True

    wait_for_condition(verify, retry_interval_ms=2000)

    # Pack with infeasible request
    ray.util.placement_group(
        name="pg_infeasible", strategy="STRICT_PACK", bundles=[{"CPU": 1}, {"CPU": 1}]
    )

    def verify():
        autoscaling_state = autoscaler.update_autoscaling_state()
        cluster_state = get_cluster_status(gcs_address)
        assert len(cluster_state.active_nodes + cluster_state.idle_nodes) == 6
        assert len(autoscaling_state.infeasible_gang_resource_requests) == 1

        return True

    wait_for_condition(verify, retry_interval_ms=2000)

    # Test report autoscaling state
    def verify():
        autoscaling_state = autoscaler.update_autoscaling_state()
        assert len(autoscaling_state.infeasible_gang_resource_requests) == 1
        # For now, we just track that it's called ok.
        autoscaler._gcs_client.report_autoscaling_state(
            autoscaling_state.SerializeToString()
        )
        return True

    wait_for_condition(verify, retry_interval_ms=2000)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
