"""Unit test for BatchingNodeProvider.
Validates BatchingNodeProvider's book-keeping logic.
"""
from copy import copy
from uuid import uuid4
import os
import random
import sys
from typing import Any, Dict
from collections import defaultdict

import pytest

from ray.autoscaler.batching_node_provider import (
    BatchingNodeProvider,
    NodeData,
    ScaleRequest,
)
from ray.autoscaler._private.util import NodeID, NodeType
from ray.autoscaler.tags import (
    STATUS_UP_TO_DATE,
    TAG_RAY_USER_NODE_TYPE,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_STATUS,
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
)
from ray.autoscaler._private.constants import (
    DISABLE_LAUNCH_CONFIG_CHECK_KEY,
    DISABLE_NODE_UPDATERS_KEY,
    FOREGROUND_NODE_LAUNCH_KEY,
)


class MockBatchingNodeProvider(BatchingNodeProvider):
    """Mock implementation of a BatchingNodeProvider."""

    def __init__(
        self,
        provider_config: Dict[str, Any],
        cluster_name: str,
        _allow_multiple: bool = False,
    ) -> None:
        BatchingNodeProvider.__init__(
            self, provider_config, cluster_name, _allow_multiple
        )
        # Fake cluster manager state:
        self._node_data_dict: Dict[NodeID, NodeData] = {}

        self._add_node(node_type="head", node_kind=NODE_KIND_HEAD)
        # Allow unit test to the control output of safe_to_scale.
        self._safe_to_scale_flag = True
        self._scale_request_submitted_count = 0
        # Track non_terminated_nodes_calls for use in test_autoscaler.py
        self.num_non_terminated_nodes_calls = 0

    def get_node_data(self) -> Dict[NodeID, NodeData]:
        self.num_non_terminated_nodes_calls += 1
        return self._node_data_dict

    def submit_scale_request(self, scale_request: ScaleRequest) -> None:
        """Simulate modification of cluster state by an external cluster manager."""
        self._scale_request_submitted_count += 1
        # Delete workers.
        for node_id in self.scale_request.workers_to_delete:
            del self._node_data_dict[node_id]
        # Get counts of workers after the deletion.
        cur_num_workers = self._cur_num_workers(self._node_data_dict)
        for node_type in self.scale_request.desired_num_workers:
            # How many nodes to add.
            diff = (
                self.scale_request.desired_num_workers[node_type]
                - cur_num_workers[node_type]
            )
            # The next assertion validates the node provider and the test structure.
            # After removing nodes to terminate, there should be no more
            # nodes to terminate!
            assert diff >= 0, diff
            for _ in range(diff):
                self._add_node(node_type, NODE_KIND_WORKER)

    def _add_node(self, node_type, node_kind):
        new_node_id = str(uuid4())
        self._node_data_dict[new_node_id] = NodeData(
            kind=node_kind, ip=str(uuid4()), status=STATUS_UP_TO_DATE, type=node_type
        )

    def non_terminated_node_ips(self, tag_filters):
        """This method is used in test_autoscaler.py."""
        return [
            node_data.ip
            for node_id, node_data in self._node_data_dict.items()
            if tag_filters.items() <= self.node_tags(node_id).items()
        ]

    def safe_to_scale(self) -> bool:
        return self.safe_to_scale_flag

    def _assert_worker_counts(
        self, expected_worker_counts: Dict[NodeType, int]
    ) -> None:
        assert self._cur_num_workers(self._node_data_dict) == expected_worker_counts


class BatchingNodeProviderTester:
    """Utility to test BatchingNodeProvider."""

    def __init__(self):
        self.node_provider = MockBatchingNodeProvider(
            provider_config={
                DISABLE_LAUNCH_CONFIG_CHECK_KEY: True,
                DISABLE_NODE_UPDATERS_KEY: True,
                FOREGROUND_NODE_LAUNCH_KEY: True,
            },
            cluster_name="test-cluster",
            _allow_multiple=True,
        )
        # Maps node types to expected node counts.
        self.expected_node_counts = defaultdict(int)
        self.expected_node_counts["head"] = 1
        # Tracks how many times we expect a scale request to have been submitted.
        self.expected_scale_request_submitted_count = 0

    def update(
        self, create_node_requests, terminate_nodes_requests, safe_to_scale_flag
    ):
        """Simulates an autoscaler update with multiple terminate and create calls.

        Calls non_terminated_nodes, then create/terminate nodes, then post_process.

        Args:
            create_node_requests (List[Tuple(str, int)]): List of pairs
                (node type, count). Each pair is used in a create_node call that
                creates count nodes of the node type.
            terminate_nodes_requests (List[Tuple(str, int)]): List of pairs
                (node type, count). Each pair is used in a terminate_nodes call
                that terminates up to count nodes of the node type.
            safe_to_scale_flag (bool): Passed to the node provider to determine  # noqa
                where provider.safe_to_scale() evaluates to True or False.
        """
        self.node_provider.safe_to_scale_flag = safe_to_scale_flag

        # Call non_terminated_nodes to refresh internal caches.
        # Also validate node provider state.
        self.validate_non_terminated_nodes()

        # Abort if it's not safe to scale.
        # This behavior is tested in the context of an actual autoscaler update in
        # test_autoscaler:test_safe_to_scale.
        if not self.node_provider.safe_to_scale():
            return
        # Terminate some nodes.
        # Set to track nodes marked for termination during the update.
        to_terminate_this_update = set()
        for node_type, count in terminate_nodes_requests:
            # Each iteration makes a provider.terminate_nodes call.
            # Terminate "count" nodes of the given node_type.
            # If count is greater than the number of remaining nodes of the type,
            # terminate all of the nodes of the type.
            to_terminate_this_request = []
            for node in self.node_provider._node_data_dict:
                if len(to_terminate_this_request) >= count:
                    break
                if (
                    self.node_provider.node_tags(node)[TAG_RAY_USER_NODE_TYPE]
                    != node_type
                ):
                    continue
                if node in to_terminate_this_update:
                    continue
                to_terminate_this_update.add(node)
                to_terminate_this_request.append(node)
            self.node_provider.terminate_nodes(to_terminate_this_request)
            self.expected_node_counts[node_type] -= len(to_terminate_this_request)
            # else: the scale request will not be submitted.

        # Create some nodes.
        for node_type, count in create_node_requests:
            self.node_provider.create_node(
                node_config={}, tags={TAG_RAY_USER_NODE_TYPE: node_type}, count=count
            )
            self.expected_node_counts[node_type] += count
            # else: the scale request will not be submitted.

        # Scale change is needed exactly when there's something to create or terminate.
        assert self.node_provider.scale_change_needed is bool(
            create_node_requests or terminate_nodes_requests
        )

        # Submit the scale request.
        self.node_provider.post_process()
        # Expect a scale request to be submitted iff we called create or terminate
        # at least one.
        if create_node_requests or terminate_nodes_requests:
            self.expected_scale_request_submitted_count += 1

    def validate_non_terminated_nodes(self):
        """Calls non_terminated_nodes and validates output against this test classes's
        accumulated expected state.

        Tests methods internal_ip, node_tags, non_terminated_nodes of
        BatchingNodeProvider.
        """
        nodes = self.node_provider.non_terminated_nodes({})

        # Get counts of nodes using provider.node_tags to extract node type.
        actual_node_counts = defaultdict(int)
        for node in nodes:
            # Trivial check. Just confirming we can call internal_ip with no issue.
            assert isinstance(self.node_provider.internal_ip(node), str)

            # Check tag structure.
            tags = self.node_provider.node_tags(node)
            assert set(tags.keys()) == {
                TAG_RAY_USER_NODE_TYPE,
                TAG_RAY_NODE_STATUS,
                TAG_RAY_NODE_KIND,
            }
            node_type = tags[TAG_RAY_USER_NODE_TYPE]
            node_kind = tags[TAG_RAY_NODE_KIND]
            node_status = tags[TAG_RAY_NODE_STATUS]
            if node_type == "head":
                assert node_kind == NODE_KIND_HEAD
            else:
                assert node_kind == NODE_KIND_WORKER
            # Just by construction of this test:
            assert node_status == STATUS_UP_TO_DATE

            actual_node_counts[node_type] += 1

        # Remove 0 values from expected_node_counts before comparing.
        for k, v in copy(self.expected_node_counts).items():
            if v == 0:
                del self.expected_node_counts[k]
        assert actual_node_counts == self.expected_node_counts

        # Get node counts again using tag filters.
        actual_node_counts_again = {}
        for node_type in actual_node_counts:
            actual_node_counts_again[node_type] = len(
                self.node_provider.non_terminated_nodes(
                    tag_filters={TAG_RAY_USER_NODE_TYPE: node_type}
                )
            )
        assert actual_node_counts_again == self.expected_node_counts

        # Check filtering by node kind.
        workers = self.node_provider.non_terminated_nodes(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
        )
        heads = self.node_provider.non_terminated_nodes(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_HEAD}
        )
        assert len(heads) == 1
        assert set(nodes) == set(workers) | set(heads)

        # Check filtering by status.
        up_to_date_nodes = self.node_provider.non_terminated_nodes(
            tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE}
        )
        assert set(up_to_date_nodes) == set(nodes)

        # Make some assertions about internal structure of the node provider.
        expected_node_counts_without_head = copy(self.expected_node_counts)
        del expected_node_counts_without_head["head"]
        # Desired number of workers immediately after calling non_terminated_nodes is
        # current number of workers.
        assert (
            self.node_provider.scale_request.desired_num_workers
            == expected_node_counts_without_head
        )
        # scale_change_needed should be reset after calling non_terminated_nodes
        # (meaning: we've just obtained cluster state and have no indication
        # from create_node or terminate_node calls that scale needs to change.)
        assert self.node_provider.scale_change_needed is False
        # We've submitted the expected number of scale requests:
        assert (
            self.node_provider._scale_request_submitted_count
            == self.expected_scale_request_submitted_count
        )

    def update_with_random_requests(self):
        random_requests = self.generate_random_requests()
        self.update(*random_requests)

    def generate_random_requests(self):
        """Generates random sequences of create_node and terminate_nodes requests
        for the node provider. Generates random safe_to_scale_flag.
        """
        num_creates = random.choice(range(100))
        num_terminates = random.choice(range(100))

        create_node_requests = []
        for _ in range(num_creates):
            # Choose from 5 worker types.
            node_type = random.choice([f"type-{x}" for x in range(5)])
            # Create up to 9 workers.
            count = random.choice(range(10))
            create_node_requests.append((node_type, count))

        terminate_nodes_requests = []
        for _ in range(num_terminates):
            node_type = random.choice([f"type-{x}" for x in range(5)])
            # Terminate up to 9 workers.
            count = random.choice(range(10))
            terminate_nodes_requests.append((node_type, count))

        # 50% chance of the update being executed.
        safe_to_scale_flag = random.choice([True, False])

        return create_node_requests, terminate_nodes_requests, safe_to_scale_flag

    def assert_worker_counts(self, expected_worker_counts):
        """Validates worker counts against internal node provider state."""
        self.node_provider._assert_worker_counts(expected_worker_counts)


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Not relevant on Windows.")
def test_batching_node_provider_basic():
    tester = BatchingNodeProviderTester()

    tester.update(
        create_node_requests=[
            ("type-1", 5),
        ],
        terminate_nodes_requests=[],
        safe_to_scale_flag=True,
    )
    tester.assert_worker_counts({"type-1": 5})
    assert tester.node_provider._scale_request_submitted_count == 1

    tester.update(
        create_node_requests=[("type-2", 5), ("type-2", 5)],
        terminate_nodes_requests=[("type-1", 2)],
        safe_to_scale_flag=True,
    )
    tester.assert_worker_counts({"type-1": 3, "type-2": 10})
    assert tester.node_provider._scale_request_submitted_count == 2

    tester.update(
        create_node_requests=[],
        terminate_nodes_requests=[("type-1", 2), ("type-2", 1), ("type-2", 1)],
        safe_to_scale_flag=True,
    )
    tester.assert_worker_counts({"type-1": 1, "type-2": 8})
    assert tester.node_provider._scale_request_submitted_count == 3

    tester.update(
        create_node_requests=[],
        terminate_nodes_requests=[],
        safe_to_scale_flag=True,
    )
    tester.assert_worker_counts({"type-1": 1, "type-2": 8})
    # No scale request submitted, since there were no create/terminate calls.
    assert tester.node_provider._scale_request_submitted_count == 3


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Not relevant on Windows.")
def test_batching_node_provider_many_requests():
    """Simulate 10 autoscaler updates with randomly generated create/terminate
    requests.
    """
    tester = BatchingNodeProviderTester()
    for _ in range(2):
        tester.update_with_random_requests()
    # Final check.
    tester.validate_non_terminated_nodes()


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Not relevant on Windows.")
def test_terminate_safeguards():
    """Tests the following behaviors:
    - the node provider ignores requests to terminate a node twice.
    - the node provider ignores requests to terminate an unknown node.
    """
    node_provider = MockBatchingNodeProvider(
        provider_config={
            DISABLE_LAUNCH_CONFIG_CHECK_KEY: True,
            DISABLE_NODE_UPDATERS_KEY: True,
            FOREGROUND_NODE_LAUNCH_KEY: True,
        },
        cluster_name="test-cluster",
        _allow_multiple=True,
    )
    nodes = node_provider.non_terminated_nodes({})
    assert len(nodes) == 1
    head_node = nodes[0]
    node_provider.create_node(
        node_config={}, tags={TAG_RAY_USER_NODE_TYPE: "type"}, count=1
    )
    node_provider.post_process()
    nodes = node_provider.non_terminated_nodes({})
    assert len(nodes) == 2

    worker_node = ""
    for node in nodes:
        if node == head_node:
            continue
        else:
            worker_node = node

    # This node is not in our list.
    unknown_node = node + worker_node
    node_provider.terminate_node(unknown_node)
    node_provider.post_process()
    nodes = node_provider.non_terminated_nodes({})
    # Terminate request was ignored because the node is unknown.
    assert len(nodes) == 2

    node_provider.terminate_node(worker_node)
    node_provider.terminate_node(worker_node)
    node_provider.post_process()
    nodes = node_provider.non_terminated_nodes({})
    # Second terminate request was ignored.
    assert len(nodes) == 1


if __name__ == "__main__":

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
