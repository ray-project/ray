import copy
import logging
import os
import subprocess
import sys
import tempfile
import time
from unittest.mock import MagicMock

import pytest

import ray
from ray._private.ray_constants import DEFAULT_DASHBOARD_AGENT_LISTEN_PORT
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray._raylet import GcsClient
from ray.autoscaler._private.fake_multi_node.node_provider import FAKE_HEAD_NODE_ID
from ray.autoscaler.v2.autoscaler import Autoscaler
from ray.autoscaler.v2.event_logger import AutoscalerEventLogger
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.autoscaler.v2.sdk import (
    get_cluster_resource_state,
    get_cluster_status,
    request_cluster_resources,
)
from ray.autoscaler.v2.tests.util import MockEventLogger
from ray.cluster_utils import Cluster
from ray.job_submission import JobStatus, JobSubmissionClient

import requests
from typing import Dict

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
        "1c2g": {
            "resources": {"CPU": 1},
            "min_workers": 0,
            "max_workers": 10,
            "node_config": {},
        },
        "2c4g": {
            "resources": {"CPU": 2},
            "min_workers": 0,
            "max_workers": 10,
            "node_config": {},
        },
    },
    "head_node_type": "ray.head.default",
    "upscaling_speed": 0,
    "idle_timeout_minutes": 0.2,  # ~12 sec
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
            "_system_config": {
                "enable_autoscaler_v2": True,
                "virtual_cluster_enabled": True,
            },
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

        return autoscaler, cluster

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


# Test autoscaler can get right cluster resource state (including virtual clusters).
def test_get_cluster_resource_state(make_autoscaler):
    config = copy.deepcopy(DEFAULT_AUTOSCALING_CONFIG)
    config["idle_timeout_minutes"] = 10
    autoscaler, cluster = make_autoscaler(config)

    # Add one `1c2g` and one `2c4g` node to the primary cluster.
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "1c2g"}, num_cpus=1)
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "2c4g"}, num_cpus=2)
    cluster.wait_for_nodes()

    print("=================== Create a virtual cluster ====================")
    ip, _ = cluster.webui_url.split(":")
    agent_address = f"{ip}:{DEFAULT_DASHBOARD_AGENT_LISTEN_PORT}"
    assert wait_until_server_available(agent_address)
    assert wait_until_server_available(cluster.webui_url)
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)

    resp = requests.post(
        webui_url + "/virtual_clusters",
        json={
            "virtualClusterId": "virtual_cluster_1",
            "divisible": False,
            # The virtual cluster requires one `1c2g` node.
            "replicaSets": {"1c2g": 1},
            "revision": 0,
        },
        timeout=10,
    )
    result = resp.json()
    print(result)
    assert result["result"]

    client = JobSubmissionClient(webui_url)
    temp_dir = None
    file_path = None

    try:
        # Define driver: create two actors and two pg bundles.
        driver_content = """
import ray
import time
@ray.remote
class SmallActor():
    def __init__(self):
        self.children = []
    def echo(self):
        return 1
    def create_child(self, num_cpus):
        self.children.append(SmallActor.options(num_cpus=num_cpus).remote())

print("Start creating actors.")
root_actor = SmallActor.options(num_cpus=1).remote()
ray.get(root_actor.echo.remote())
root_actor.create_child.remote(num_cpus=2)

placement_group = ray.util.placement_group(
    name="pg_test",
    strategy="STRICT_SPREAD",
    bundles=[{"CPU": 2}, {"CPU": 2}],
)
ray.get(placement_group.ready())
        """

        # Create a temporary Python file.
        temp_dir = tempfile.mkdtemp()
        file_path = os.path.join(temp_dir, "test_driver.py")

        with open(file_path, "w") as file:
            file.write(driver_content)

        absolute_path = os.path.abspath(file_path)

        # Submit the job to the virtual cluster.
        job = client.submit_job(
            entrypoint=f"python {absolute_path}",
            virtual_cluster_id="virtual_cluster_1",
        )

        def check_job_running():
            status = client.get_job_status(job)
            return status == JobStatus.RUNNING

        wait_for_condition(check_job_running)

        def check_virtual_cluster():
            try:
                cluster_resource_state = get_cluster_resource_state(
                    autoscaler._gcs_client
                )
                # One head and two worker nodes in total.
                assert len(cluster_resource_state.node_states) == 3
                assert (
                    len(cluster_resource_state.virtual_cluster_states) == 2
                    and "virtual_cluster_1"
                    in cluster_resource_state.virtual_cluster_states
                )
                state = cluster_resource_state.virtual_cluster_states[
                    "virtual_cluster_1"
                ]
                # The virtual cluster has one (`1c2g`) node.
                assert len(state.nodes) == 1
                assert len(state.pending_resource_requests) > 0
                for request_by_count in state.pending_resource_requests:
                    for (
                        resource_name,
                        amount,
                    ) in request_by_count.request.resources_bundle.items():
                        # The actor requiring two CPUs should be pending.
                        if resource_name == "CPU":
                            assert amount == 2

                # Two pg bundles should be pending as well.
                assert len(state.pending_gang_resource_requests[0].requests) == 2

                assert (
                    "kPrimaryClusterID" in cluster_resource_state.virtual_cluster_states
                )
                # There are two unassigned nodes in the primary cluster.
                assert (
                    len(
                        cluster_resource_state.virtual_cluster_states[
                            "kPrimaryClusterID"
                        ].nodes
                    )
                    == 2
                )
                return True

            except Exception as ex:
                logger.info(ex)
                return False

        wait_for_condition(
            check_virtual_cluster,
            timeout=30,
            retry_interval_ms=2000,
        )

    finally:
        if file_path:
            os.remove(file_path)
        if temp_dir:
            os.rmdir(temp_dir)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
