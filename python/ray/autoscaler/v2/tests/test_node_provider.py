# coding: utf-8
import os
import sys

import pytest  # noqa

from ray._private.test_utils import load_test_config, wait_for_condition
from ray.autoscaler._private.event_summarizer import EventSummarizer
from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeProviderAvailabilityTracker,
)
from ray.autoscaler.node_launch_exception import NodeLaunchException
from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig
from ray.autoscaler.v2.instance_manager.node_provider import NodeProviderAdapter
from ray.autoscaler.v2.tests.util import FakeCounter
from ray.tests.autoscaler_test_utils import MockBatchingProvider, MockProvider

import logging
from ray.autoscaler.v2.instance_manager.node_provider import logger
logger.setLevel(logging.DEBUG)


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
        NodeProviderConfig(load_test_config("test_ray_complex.yaml")),
    )

    yield base_provider, node_provider


@pytest.mark.parametrize(
    "node_providers",
    ["sync", "batch"],
    indirect=True,
)
def test_node_providers_basic(node_providers):
    base_provider, node_provider = node_providers
    # Launch one and don't wait
    node_provider.update(
        id="1",
        target_running_nodes={"worker_nodes": 1},
        to_terminate=[],
    )

    assert len(node_provider.get_state().running_nodes) == 0
    wait_for_condition(lambda: len(base_provider.mock_nodes) == 1)

    base_provider.finish_starting_nodes()
    assert len(node_provider.get_state().running_nodes) == 1
    assert (
        list(node_provider.get_state().running_nodes.values())[0].node_type
        == "worker_nodes"
    )


    # Launch multiple and wait
    node_provider.update(
        id="2",
        target_running_nodes={"worker_nodes": 2},
        to_terminate=[],
        wait=True,
    )
    base_provider.finish_starting_nodes()

    def get_running_cloud_ids_types(state):
        return [
            (n.cloud_instance_id, n.node_type) for n in state.running_nodes.values()
        ]

    assert sorted(get_running_cloud_ids_types(node_provider.get_state())) == sorted(
        [("0", "worker_nodes"), ("1", "worker_nodes")]
    )

    # Terminate with updates.
    node_provider.update(id="3", target_running_nodes={"worker_nodes": 2}, to_terminate=["0"], wait=True)

    def verify():
        base_provider.finish_starting_nodes()
        state = node_provider.get_state()
        running_cloud_ids_types = [
            (n.cloud_instance_id, n.node_type) for n in state.running_nodes.values()
        ]
        assert sorted(running_cloud_ids_types) == sorted(
            [("1", "worker_nodes"), ("2", "worker_nodes")]
        )
        return True
    wait_for_condition(verify)

def test_create_node_failure(node_providers):
    base_provider, node_provider = node_providers
    base_provider.error_creates = NodeLaunchException(
        "hello", "failed to create node", src_exc_info=None
    )
    node_provider.update(id="id1", target_running_nodes={"worker_nodes": 1})

    def verify():
        state = node_provider.get_state()
        assert len(state.running_nodes) == 0
        assert len(state.launch_errors) == 1
        assert type(state.launch_errors[0].exception) == NodeLaunchException
        assert state.launch_errors[0].node_type == "worker_nodes"
        assert len(base_provider.mock_nodes) == 0
        assert (
            "worker_nodes"
            in node_provider._node_launcher.node_provider_availability_tracker.summary().node_availabilities  # noqa
        )
        return True
    
    wait_for_condition(verify)

def test_terminate_node_failure(node_providers):
    base_provider, node_provider = node_providers
    base_provider.error_terminates = Exception("failed to terminate node")
    node_provider.update(id="launch1", target_running_nodes={"worker_nodes": 1})

    def nodes_launched():
        base_provider.finish_starting_nodes()
        state = node_provider.get_state()
        return len(state.running_nodes) == 1
    
    wait_for_condition(nodes_launched)

    node_provider.update(id="terminate1", target_running_nodes={}, to_terminate=["0"])

    def verify():
        state = node_provider.get_state()
        assert len(state.running_nodes) == 1
        assert len(state.termination_errors) == 1
        assert type(state.termination_errors[0].exception) == Exception
        assert state.termination_errors[0].cloud_instance_id == "0"
        return True
    
    wait_for_condition(verify)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
