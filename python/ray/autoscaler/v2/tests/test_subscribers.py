# coding: utf-8
import os
import sys

import pytest

import mock

from ray._private.test_utils import wait_for_condition
from ray._private.utils import binary_to_hex, hex_to_binary
from ray.autoscaler.v2.instance_manager.subscribers.ray_stopper import (  # noqa
    RayStopper,
)
from ray.core.generated.autoscaler_pb2 import DrainNodeReason, DrainNodeReply
from ray.core.generated.instance_manager_pb2 import (
    Instance,
    InstanceUpdateEvent,
    TerminationRequest,
)


class TestRayStopper:
    def test_no_op(self):
        mock_gcs_client = mock.MagicMock()
        ray_stopper = RayStopper(gcs_client=mock_gcs_client)

        ray_stopper.notify(
            [
                InstanceUpdateEvent(
                    instance_id="test_id",
                    new_instance_status=Instance.REQUESTED,
                )
            ]
        )
        assert mock_gcs_client.drain_node.call_count == 0

        # no termination request
        ray_stopper.notify(
            [
                InstanceUpdateEvent(
                    instance_id="test_id",
                    new_instance_status=Instance.RAY_STOPPING,
                )
            ]
        )
        assert mock_gcs_client.drain_node.call_count == 0

    def test_idle_termination(self):
        mock_gcs_client = mock.MagicMock()
        reply = DrainNodeReply(is_accepted=True)
        mock_gcs_client.drain_node.return_value = reply
        ray_stopper = RayStopper(gcs_client=mock_gcs_client)

        ray_stopper.notify(
            [
                InstanceUpdateEvent(
                    instance_id="test_id",
                    new_instance_status=Instance.RAY_STOPPING,
                    termination_request=TerminationRequest(
                        cause=TerminationRequest.Cause.IDLE,
                        idle_time_ms=1000,
                        ray_node_id=binary_to_hex(hex_to_binary(b"0000")),
                    ),
                )
            ]
        )

        def verify():
            mock_gcs_client.drain_node.assert_has_calls(
                [
                    mock.call(
                        node_id=hex_to_binary(b"0000"),
                        reason=DrainNodeReason.DRAIN_NODE_REASON_IDLE_TERMINATION,
                        reason_message="Idle termination of node for 1.0 seconds.",
                        deadline_timestamp_ms=0,
                    )
                ]
            )
            return True

        wait_for_condition(verify)

    def test_preemption(self):
        mock_gcs_client = mock.MagicMock()
        mock_gcs_client.drain_nodes.return_value = [0]
        ray_stopper = RayStopper(gcs_client=mock_gcs_client)

        ray_stopper.notify(
            [
                InstanceUpdateEvent(
                    instance_id="i-1",
                    new_instance_status=Instance.RAY_STOPPING,
                    termination_request=TerminationRequest(
                        cause=TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
                        max_num_nodes_per_type=10,
                        ray_node_id=binary_to_hex(hex_to_binary(b"1111")),
                    ),
                ),
                InstanceUpdateEvent(
                    instance_id="i-2",
                    new_instance_status=Instance.RAY_STOPPING,
                    termination_request=TerminationRequest(
                        cause=TerminationRequest.Cause.MAX_NUM_NODES,
                        max_num_nodes=100,
                        ray_node_id=binary_to_hex(hex_to_binary(b"2222")),
                    ),
                ),
            ]
        )

        def verify():
            mock_gcs_client.drain_nodes.assert_has_calls(
                [
                    mock.call(
                        node_ids=[hex_to_binary(b"1111")],
                    ),
                    mock.call(
                        node_ids=[hex_to_binary(b"2222")],
                    ),
                ]
            )
            return True

        wait_for_condition(verify)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
