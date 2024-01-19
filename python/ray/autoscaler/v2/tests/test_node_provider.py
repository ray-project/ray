import logging
import os
import sys
import time

# coding: utf-8
from collections import defaultdict
from unittest.mock import MagicMock

import pytest  # noqa

from ray._private.test_utils import get_test_config_path, wait_for_condition
from ray.autoscaler._private.constants import (
    AUTOSCALER_MAX_CONCURRENT_TERMINATING,
    AUTOSCALER_MAX_CONCURRENT_TYPES_TO_LAUNCH,
)
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
    if not hasattr(request, "param"):
        base_provider = MockProvider()
        node_provider = NodeProviderAdapter(
            base_provider,
            FileConfigReader(
                get_test_config_path("test_ray_complex.yaml"), skip_content_hash=True
            ),
        )
        return base_provider, node_provider

    param = request.param

    if param.get("kind") == "batch":
        base_provider = MockBatchingProvider()
    elif param.get("kind") == "mock":
        base_provider = MagicMock()
    elif param.get("kind") == "sync":
        base_provider = MockProvider()
    else:
        raise ValueError(f"Unknown provider kind {param['kind']}")

    launch_concurrency = param.get(
        "launch_concurrency", AUTOSCALER_MAX_CONCURRENT_TYPES_TO_LAUNCH
    )
    terminate_concurrency = param.get(
        "terminate_concurrency", AUTOSCALER_MAX_CONCURRENT_TERMINATING
    )

    print(
        f"Using provider {param['kind']} with launch_concurrency={launch_concurrency} "
        f"and terminate_concurrency={terminate_concurrency}"
    )

    node_provider = NodeProviderAdapter(
        base_provider,
        FileConfigReader(
            get_test_config_path("test_ray_complex.yaml"), skip_content_hash=True
        ),
        max_concurrent_types_to_launch=launch_concurrency,
        max_concurrent_to_terminate=terminate_concurrency,
    )

    yield base_provider, node_provider


@pytest.mark.parametrize(
    "node_providers",
    [{"kind": "batch"}, {"kind": "sync"}],
    indirect=True,
)
def test_node_providers_basic(node_providers):
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
    [{"kind": "batch"}, {"kind": "sync"}],
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
    [{"kind": "batch"}, {"kind": "sync"}],
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


@pytest.mark.parametrize(
    "node_providers",
    [{"kind": "mock", "launch_concurrency": 1, "terminate_concurrency": 1}],
    indirect=True,
)
def test_launch_terminate_executor_concurrency(node_providers):
    import threading

    base_provider, node_provider = node_providers

    launch_event = threading.Event()

    def loop(*args, **kwargs):
        launch_event.wait()

    base_provider.create_node_with_resources_and_labels.side_effect = loop

    node_provider.launch(
        shape={
            "worker_nodes": 1,
            "worker_nodes1": 1,
        },  # 2 types, but concurrent types to launch is 1.
        request_id="1",
    )
    # Assert called only once.
    for _ in range(10):
        assert base_provider.create_node_with_resources_and_labels.call_count <= 1
        time.sleep(0.1)

    # Finish the call.
    launch_event.set()

    def verify():
        assert base_provider.create_node_with_resources_and_labels.call_count == 2
        return True

    wait_for_condition(verify)

    # Test terminator concurrency when enforced.

    terminate_event = threading.Event()

    def loop(*args, **kwargs):
        terminate_event.wait()

    base_provider.terminate_node.side_effect = loop

    node_provider.terminate(
        ids=["0", "1"],
        request_id="2",
    )

    # Assert called only once.
    for _ in range(10):
        assert base_provider.terminate_node.call_count <= 1
        time.sleep(0.1)

    # Finish the call.
    terminate_event.set()

    def verify():
        assert base_provider.terminate_node.call_count == 2
        return True

    wait_for_condition(verify)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
