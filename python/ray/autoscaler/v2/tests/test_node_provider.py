import logging
import os
import sys
import time

# coding: utf-8
from collections import defaultdict
from unittest.mock import MagicMock

import pytest  # noqa

import ray
from ray._private.test_utils import get_test_config_path, wait_for_condition
from ray.autoscaler._private.constants import (
    AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
    AUTOSCALER_MAX_LAUNCH_BATCH,
)
from ray.autoscaler._private.fake_multi_node.node_provider import FakeMultiNodeProvider
from ray.autoscaler.v2.instance_manager.config import FileConfigReader
from ray.autoscaler.v2.instance_manager.node_provider import (
    LaunchNodeError,
    NodeProviderAdapter,
    TerminateNodeError,
    logger,
)
from ray.tests.autoscaler_test_utils import MockProvider

logger.setLevel(logging.DEBUG)


class NodeProviderTestWrapper:
    def __init__(
        self,
        kind: str,
        max_concurrent_launches: int,
        max_launch_batch_per_type: int,
    ):
        self.kind = kind

        self.config_reader = FileConfigReader(
            get_test_config_path("test_ray_complex.yaml"), skip_content_hash=True
        )
        config = self.config_reader.get_autoscaling_config()
        self.ray_session = None

        if self.kind == "mock":
            self.base_provider = MagicMock()
        elif self.kind == "sync":
            self.base_provider = MockProvider()
        elif self.kind == "fake_multi":
            os.environ["RAY_FAKE_CLUSTER"] = "1"
            provider_config = config.get_provider_config()
            # This is a bit hacky but we need a fake head node.
            self.ray_session = ray.init()
            self.base_provider = FakeMultiNodeProvider(
                provider_config,
                cluster_name="test",
                gcs_address=self.ray_session.address_info["gcs_address"],
                head_node_id=self.ray_session.address_info["node_id"],
                launch_multiple=True,
            )

        print(
            f"Using provider {kind} with "
            f"max_concurrent_launches={max_concurrent_launches} "
            f"and max_launch_batch={max_launch_batch_per_type}"
        )
        self.node_provider = NodeProviderAdapter(
            self.base_provider,
            self.config_reader,
            max_concurrent_launches=max_concurrent_launches,
            max_launch_batch_per_type=max_launch_batch_per_type,
        )

    def shutdown(self):
        if self.ray_session:
            ray.shutdown()

    def launch(self, request_id, shape):
        self.node_provider.launch(shape=shape, request_id=request_id)

    def terminate(self, request_id, ids):
        self.node_provider.terminate(ids=ids, request_id=request_id)

    def poll_errors(self):
        return self.node_provider.poll_errors()

    def get_non_terminated(self):
        nodes = self.node_provider.get_non_terminated()
        if self.kind == "fake_multi":
            # Remove the head node.
            nodes.pop(self.ray_session.address_info["node_id"], None)
        return nodes

    ############################
    # Test mock methods
    ############################
    def test_add_error_creates(self, e: Exception):
        if self.kind == "mock":
            self.base_provider.create_node_with_resources_and_labels.side_effect = e
        elif self.kind == "sync":
            self.base_provider.error_creates = e
        elif self.kind == "fake_multi":
            self.base_provider._test_add_error_creates(e)

    def test_add_error_terminates(self, e: Exception):
        if self.kind == "mock":
            self.base_provider.terminate_nodes.side_effect = e
        elif self.kind == "sync":
            self.base_provider.error_terminates = e
        elif self.kind == "fake_multi":
            self.base_provider._test_add_error_terminates(e)


@pytest.fixture(scope="function")
def node_provider(request: pytest.FixtureRequest):
    if not hasattr(request, "param"):
        return NodeProviderTestWrapper("sync")

    param = request.param
    max_concurrent_launches = param.get(
        "max_concurrent_launches", AUTOSCALER_MAX_CONCURRENT_LAUNCHES
    )
    max_launch_batch = param.get("max_launch_batch", AUTOSCALER_MAX_LAUNCH_BATCH)

    provider = NodeProviderTestWrapper(
        param.get("kind", "sync"),
        max_concurrent_launches=max_concurrent_launches,
        max_launch_batch_per_type=max_launch_batch,
    )
    yield provider
    provider.shutdown()


@pytest.mark.parametrize(
    "node_provider",
    [{"kind": "sync"}, {"kind": "fake_multi"}],
    indirect=True,
)
def test_node_providers_basic(node_provider):
    node_provider
    # Test launching.
    node_provider.launch(
        shape={"worker_nodes": 2},
        request_id="1",
    )

    node_provider.launch(
        request_id="2",
        shape={"worker_nodes": 2, "worker_nodes1": 1},
    )

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
    "node_provider",
    [{"kind": "sync"}, {"kind": "fake_multi"}],
    indirect=True,
)
def test_launch_failure(node_provider):
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

    node_provider.test_add_error_creates(Exception("failed to create node"))

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
    "node_provider",
    [{"kind": "sync"}, {"kind": "fake_multi"}],
    indirect=True,
)
def test_terminate_node_failure(node_provider):
    node_provider.test_add_error_terminates(Exception("failed to terminate node"))

    node_provider.launch(request_id="launch1", shape={"worker_nodes": 1})

    def nodes_launched():
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
    "node_provider",
    [{"kind": "mock", "max_concurrent_launches": 1}],
    indirect=True,
)
def test_launch_executor_concurrency(node_provider):
    import threading

    launch_event = threading.Event()

    def loop(*args, **kwargs):
        launch_event.wait()

    node_provider.base_provider.create_node_with_resources_and_labels.side_effect = loop

    node_provider.launch(
        shape={
            "worker_nodes": 1,
            "worker_nodes1": 1,
        },  # 2 types, but concurrent types to launch is 1.
        request_id="1",
    )
    # Assert called only once.
    for _ in range(10):
        assert (
            node_provider.base_provider.create_node_with_resources_and_labels.call_count
            <= 1
        )
        time.sleep(0.1)

    # Finish the call.
    launch_event.set()

    def verify():
        assert (
            node_provider.base_provider.create_node_with_resources_and_labels.call_count
            == 2
        )
        return True

    wait_for_condition(verify)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
