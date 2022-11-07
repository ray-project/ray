"""Unit test for BatchingNodeProvider.
Validates BatchingNodeProvider's book-keeping logic.
"""
from uuid import uuid4
from typing import Any, Dict

from ray.autoscaler.batching_node_provider import (
    BatchingNodeProvider,
    NodeData,
    ScaleRequest,
)
from ray.autoscaler._private.util import NodeID, NodeKind
from ray.autoscaler.tags import STATUS_UP_TO_DATE, TAG_RAY_USER_NODE_TYPE
from ray.autoscaler._private.constants import (
    DISABLE_LAUNCH_CONFIG_CHECK_KEY,
    DISABLE_NODE_UPDATERS_KEY,
    FOREGROUND_NODE_LAUNCH_KEY,
)


class MockBatchingNodeProvider(BatchingNodeProvider):
    def __init__(
        self,
        provider_config: Dict[str, Any],
        cluster_name: str,
        _allow_multiple: bool = False,
    ) -> None:
        BatchingNodeProvider.__init__(
            self, provider_config, cluster_name, _allow_multiple
        )
        self._node_data_dict: Dict[NodeID, NodeData] = {}
        self._add_node(node_type="head-group", node_kind=NodeKind.HEAD)
        # Allow unit test to the control output of safe_to_scale.
        self._safe_to_scale_test_flag = True

    def get_node_data(self) -> Dict[NodeID, NodeData]:
        return self._node_data_dict

    def submit_scale_request(self, scale_request: ScaleRequest) -> None:
        """Simulate modification of cluster state by an external cluster manager."""
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
            # After removing nodes to terminate, there should be no more nodes to terminate!
            assert diff >= 0, diff
            for _ in range(diff):
                self._add_node(node_type, NodeKind.WORKER)

    def _add_node(self, node_type, node_kind):
        new_node_id = str(uuid4())
        self._node_data_dict[new_node_id] = NodeData(
            kind=node_kind, ip=str(uuid4()), status=STATUS_UP_TO_DATE, type=node_type
        )

    def safe_to_scale(self) -> bool:
        return self._safe_to_scale_test_flag


class BatchingNodeProviderTester:
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
        self.debug_print()

    def update(self, create_node_requests, terminate_nodes_requests):
        # Terminate some nodes.
        for node_type, count in terminate_nodes_requests:
            to_terminate_list = []
            for node in self.node_provider._node_data_dict:
                if len(to_terminate_list) >= count:
                    break
                if (
                    self.node_provider.node_tags(node)[TAG_RAY_USER_NODE_TYPE]
                    == node_type
                ):
                    to_terminate_list.append(node)
            self.node_provider.terminate_nodes(to_terminate_list)

        # Create some nodes.
        for node_type, count in create_node_requests:
            self.node_provider.create_node(
                node_config={}, tags={TAG_RAY_USER_NODE_TYPE: node_type}, count=count
            )

        # Submit the scale request.
        self.node_provider.post_process()
        self.debug_print()

    def debug_print(self):
        print("==========================================")
        for node in self.node_provider.non_terminated_nodes(tag_filters={}):
            print(
                self.node_provider.node_tags(node),
                self.node_provider.internal_ip(node),
            )


if __name__ == "__main__":
    tester = BatchingNodeProviderTester()
    tester.update(
        create_node_requests=[
            ("blah", 5),
        ],
        terminate_nodes_requests=[],
    )
    tester.update(
        create_node_requests=[],
        terminate_nodes_requests=[("blah", 1)],
    )
