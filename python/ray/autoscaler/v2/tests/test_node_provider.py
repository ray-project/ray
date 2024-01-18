import logging
import os
import sys

# coding: utf-8
from collections import defaultdict
from typing import Literal

import pytest  # noqa

from ray._private.test_utils import get_test_config_path, wait_for_condition
from ray.autoscaler.v2.instance_manager.config import FileConfigReader
from ray.autoscaler.v2.instance_manager.node_provider import (
    LaunchNodeError,
    NodeProviderAdapter,
    TerminateNodeError,
    logger,
)
from ray.tests.autoscaler_test_utils import MockBatchingProvider, MockProvider

logger.setLevel(logging.DEBUG)


@pytest.fixture(scope="function")
def node_providers(request: pytest.FixtureRequest):
    if hasattr(request, "param") and request.param == "batch":
        base_provider = MockBatchingProvider()
    else:
        base_provider = MockProvider()
    node_provider = NodeProviderAdapter(
        base_provider,
        FileConfigReader(
            get_test_config_path("test_ray_complex.yaml"), skip_content_hash=True
        ),
        sync_launch=True,
    )

    yield base_provider, node_provider


@pytest.mark.parametrize(
    "node_providers",
    ["sync", "batch"],
    indirect=True,
)
def test_node_providers_basic(node_providers: Literal["sync", "batch"]):
    base_provider, node_provider = node_providers
    # Test launching.
    node_provider.launch(
        shape={"worker_nodes": 2},
        request_id="1",
    )

    node_provider.launch(
        request_id="2",
        shape={"worker_nodes": 2, "worker_nodes1": 1},
    )

    base_provider.finish_starting_nodes()

    def verify():
        nodes_by_type = defaultdict(int)
        for node in node_provider.get_non_terminated().values():
            nodes_by_type[node.node_type] += 1
        errors = node_provider.poll_errors()
        print(errors)
        assert nodes_by_type == {"worker_nodes": 4, "worker_nodes1": 1}
        return True

    wait_for_condition(verify)

    nodes = node_provider.get_non_terminated().keys()

    # Terminate them all
    node_provider.terminate(
        ids=nodes,
        request_id="3",
    )

    # Launch some.
    node_provider.launch(
        shape={"worker_nodes": 1},
        request_id="4",
    )

    base_provider.finish_starting_nodes()

    def verify():
        nodes_by_type = defaultdict(int)
        for node in node_provider.get_non_terminated().values():
            nodes_by_type[node.node_type] += 1

        assert nodes_by_type == {"worker_nodes": 1}
        for node in node_provider.get_non_terminated().values():
            assert node.request_id == "4"
        return True

    wait_for_condition(verify)


@pytest.mark.parametrize(
    "node_providers",
    ["sync", "batch"],
    indirect=True,
)
def test_launch_failure(node_providers):
    base_provider, node_provider = node_providers

    node_provider.launch(
        shape={"not_existing_worker_nodes": 2},
        request_id="1",
    )

    def verify():
        errors = node_provider.poll_errors()
        assert len(errors) == 1
        assert isinstance(errors[0], LaunchNodeError)
        assert errors[0].node_type == "not_existing_worker_nodes"
        assert errors[0].request_id == "1"
        return True

    wait_for_condition(verify)

    base_provider.error_creates = Exception("failed to create node")

    node_provider.launch(
        shape={"worker_nodes": 2},
        request_id="2",
    )

    def verify():
        errors = node_provider.poll_errors()
        assert len(errors) == 1
        assert isinstance(errors[0], LaunchNodeError)
        assert errors[0].node_type == "worker_nodes"
        assert errors[0].request_id == "2"
        return True

    wait_for_condition(verify)


@pytest.mark.parametrize(
    "node_providers",
    ["sync", "batch"],
    indirect=True,
)
def test_terminate_node_failure(node_providers):
    base_provider, node_provider = node_providers
    base_provider.error_terminates = Exception("failed to terminate node")

    node_provider.launch(request_id="launch1", shape={"worker_nodes": 1})

    def nodes_launched():
        base_provider.finish_starting_nodes()
        nodes = node_provider.get_non_terminated()
        return len(nodes) == 1

    wait_for_condition(nodes_launched)

    node_provider.terminate(request_id="terminate1", ids=["0"])

    def verify():
        errors = node_provider.poll_errors()
        nodes = node_provider.get_non_terminated()
        assert len(nodes) == 1
        assert len(errors) == 1
        assert isinstance(errors[0], TerminateNodeError)
        assert errors[0].cloud_instance_id == "0"
        assert errors[0].request_id == "terminate1"
        return True

    wait_for_condition(verify)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
