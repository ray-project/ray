# coding: utf-8
import os
import sys

import pytest  # noqa

from ray._private.test_utils import load_test_config
from ray.autoscaler._private.event_summarizer import EventSummarizer
from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeProviderAvailabilityTracker,
)
from ray.autoscaler.node_launch_exception import NodeLaunchException
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.autoscaler.v2.instance_manager.node_provider import (
    NodeProviderAdapter,
    UpdateCloudNodeProviderRequest,
)
from ray.autoscaler.v2.tests.util import FakeCounter
from ray.tests.autoscaler_test_utils import MockBatchingProvider, MockProvider


@pytest.fixture(scope="function")
def node_providers(request):
    if hasattr(request, "param") and request.param == "batch":
        base_provider = MockBatchingProvider()
    else:
        base_provider = MockProvider()
    node_provider = NodeProviderAdapter(
        base_provider,
        BaseNodeLauncher(
            base_provider,
            FakeCounter(),
            EventSummarizer(),
            NodeProviderAvailabilityTracker(),
        ),
        AutoscalingConfig(load_test_config("test_ray_complex.yaml")),
    )

    yield base_provider, node_provider


def request(to_launch=None, to_terminate=None):
    if to_launch is None:
        to_launch = {}
    if to_terminate is None:
        to_terminate = []
    return UpdateCloudNodeProviderRequest(to_launch, to_terminate)


@pytest.mark.parametrize(
    "node_providers",
    ["sync", "batch"],
    indirect=True,
)
def test_node_providers_pass_through(node_providers):
    base_provider, node_provider = node_providers
    # Launch 1
    num_launching = node_provider.update(
        request(to_launch={"worker_nodes1": 1})
    ).num_launching
    assert num_launching == {"worker_nodes1": 1}

    assert len(base_provider.mock_nodes) == 1
    assert len(node_provider.get_running_nodes()) == 0

    base_provider.finish_starting_nodes()
    assert len(node_provider.get_running_nodes()) == 1
    assert node_provider.get_running_nodes()[0].cloud_instance_id == "0"
    assert node_provider.get_running_nodes()[0].node_type == "worker_nodes1"

    # Launch multiple
    num_launching = node_provider.update(
        request(to_launch={"worker_nodes": 2})
    ).num_launching
    assert num_launching == {"worker_nodes": 2}

    base_provider.finish_starting_nodes()

    running_cloud_ids_types = [
        (n.cloud_instance_id, n.node_type) for n in node_provider.get_running_nodes()
    ]
    assert sorted(running_cloud_ids_types) == sorted(
        [("0", "worker_nodes1"), ("1", "worker_nodes"), ("2", "worker_nodes")]
    )

    # Terminate one
    terminating = node_provider.update(request(to_terminate=["0"])).terminating
    assert terminating == ["0"]
    running_cloud_ids_types = [
        (n.cloud_instance_id, n.node_type) for n in node_provider.get_running_nodes()
    ]
    assert sorted(running_cloud_ids_types) == sorted(
        [("1", "worker_nodes"), ("2", "worker_nodes")]
    )


def test_create_node_failure(node_providers):
    base_provider, node_provider = node_providers
    base_provider.error_creates = NodeLaunchException(
        "hello", "failed to create node", src_exc_info=None
    )
    reply = node_provider.update(request(to_launch={"worker_nodes": 1}))
    assert reply.num_launching == {"worker_nodes": 0}
    assert "worker_nodes" in reply.launch_failures
    assert type(reply.launch_failures["worker_nodes"].exception) == NodeLaunchException
    assert len(base_provider.mock_nodes) == 0
    assert (
        "worker_nodes"
        in node_provider._node_launcher.node_provider_availability_tracker.summary().node_availabilities  # noqa
    )
    assert node_provider.get_running_nodes() == []


def test_terminate_node_failure(node_providers):
    base_provider, node_provider = node_providers
    base_provider.error_terminates = Exception("failed to terminate node")
    node_provider.update(request(to_launch={"worker_nodes": 1}))
    base_provider.finish_starting_nodes()
    node_provider.update(request(to_terminate=["0"]))
    assert len(base_provider.mock_nodes) == 1
    assert (
        "worker_nodes"
        in node_provider._node_launcher.node_provider_availability_tracker.summary().node_availabilities  # noqa
    )
    running_cloud_ids_types = [
        (n.cloud_instance_id, n.node_type) for n in node_provider.get_running_nodes()
    ]
    assert running_cloud_ids_types == [("0", "worker_nodes")]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
