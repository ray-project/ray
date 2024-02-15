# coding: utf-8
import os
from queue import Queue
import sys

import pytest

import mock

from ray._private.test_utils import wait_for_condition
from ray._private.utils import binary_to_hex, hex_to_binary
from ray.autoscaler.v2.instance_manager.subscribers.ray_stopper import (  # noqa
    RayStopper,
)
from ray.core.generated.autoscaler_pb2 import DrainNodeReason
from ray.core.generated.instance_manager_pb2 import (
    Instance,
    InstanceUpdateEvent,
    TerminationRequest,
)


class TestRayStopper:
    def test_no_op(self):
        mock_gcs_client = mock.MagicMock()
        ray_stopper = RayStopper(gcs_client=mock_gcs_client, error_queue=Queue())

        ray_stopper.notify(
            [
                InstanceUpdateEvent(
                    instance_id="test_id",
                    new_instance_status=Instance.REQUESTED,
                )
            ]
        )
        assert mock_gcs_client.drain_node.call_count == 0

    @pytest.mark.parametrize(
        "drain_accepted",
        [True, False],
        ids=["drain_accepted", "drain_rejected"],
    )
    def test_idle_termination(self, drain_accepted):
        mock_gcs_client = mock.MagicMock()
        mock_gcs_client.drain_node.return_value = drain_accepted
        error_queue = Queue()
        ray_stopper = RayStopper(gcs_client=mock_gcs_client, error_queue=error_queue)

        ray_stopper.notify(
            [
                InstanceUpdateEvent(
                    instance_id="test_id",
                    new_instance_status=Instance.RAY_STOP_REQUESTED,
                    termination_request=TerminationRequest(
                        cause=TerminationRequest.Cause.IDLE,
                        idle_time_ms=1000,
                        ray_node_id="0000",
                    ),
                )
            ]
        )

        def verify():
            mock_gcs_client.drain_node.assert_has_calls(
                [
                    mock.call(
                        node_id="0000",
                        reason=DrainNodeReason.DRAIN_NODE_REASON_IDLE_TERMINATION,
                        reason_message="Idle termination of node for 1.0 seconds.",
                        deadline_timestamp_ms=0,
                    )
                ]
            )

            if drain_accepted:
                assert error_queue.empty()
            else:
                error = error_queue.get_nowait()
                assert error.im_instance_id == "test_id"

            return True

        wait_for_condition(verify)

    @pytest.mark.parametrize(
        "stop_accepted",
        [True, False],
        ids=["stop_accepted", "stop_rejected"],
    )
    def test_preemption(self, stop_accepted):
        mock_gcs_client = mock.MagicMock()
        mock_gcs_client.drain_nodes.return_value = [0] if stop_accepted else []
        error_queue = Queue()
        ray_stopper = RayStopper(gcs_client=mock_gcs_client, error_queue=error_queue)

        ray_stopper.notify(
            [
                InstanceUpdateEvent(
                    instance_id="i-1",
                    new_instance_status=Instance.RAY_STOP_REQUESTED,
                    termination_request=TerminationRequest(
                        cause=TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
                        max_num_nodes_per_type=10,
                        ray_node_id=binary_to_hex(hex_to_binary(b"1111")),
                    ),
                ),
                InstanceUpdateEvent(
                    instance_id="i-2",
                    new_instance_status=Instance.RAY_STOP_REQUESTED,
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

            if stop_accepted:
                assert error_queue.empty()
            else:
                error_in_ids = set()
                while not error_queue.empty():
                    error = error_queue.get_nowait()
                    error_in_ids.add(error.im_instance_id)

                assert error_in_ids == {"i-1", "i-2"}
            return True

        wait_for_condition(verify)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
