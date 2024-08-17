# coding: utf-8
import os
import sys
from queue import Queue
from unittest import mock

import pytest

from ray._private.test_utils import wait_for_condition
from ray._private.utils import binary_to_hex, hex_to_binary
from ray.autoscaler.v2.instance_manager.subscribers.cloud_instance_updater import (
    CloudInstanceUpdater,
)
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
        [True, False, None],
        ids=["drain_accepted", "drain_rejected", "drain_error"],
    )
    def test_idle_termination(self, drain_accepted):
        mock_gcs_client = mock.MagicMock()
        if drain_accepted is None:
            mock_gcs_client.drain_node.side_effect = Exception("error")
        else:
            mock_gcs_client.drain_node.return_value = (
                drain_accepted,
                f"accepted={str(drain_accepted)}",
            )
        error_queue = Queue()
        ray_stopper = RayStopper(gcs_client=mock_gcs_client, error_queue=error_queue)

        ray_stopper.notify(
            [
                InstanceUpdateEvent(
                    instance_id="test_id",
                    new_instance_status=Instance.RAY_STOP_REQUESTED,
                    termination_request=TerminationRequest(
                        cause=TerminationRequest.Cause.IDLE,
                        idle_duration_ms=1000,
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
                        reason_message=(
                            "Termination of node that's idle for 1.0 seconds."
                        ),
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


class TestCloudInstanceUpdater:
    def test_launch_no_op(self):
        mock_provider = mock.MagicMock()
        launcher = CloudInstanceUpdater(mock_provider)
        launcher.notify(
            [
                InstanceUpdateEvent(
                    new_instance_status=Instance.RAY_RUNNING,
                    launch_request_id="1",
                    instance_type="type-1",
                ),
            ]
        )
        mock_provider.launch.assert_not_called()

    def test_launch_new_instances(self):
        mock_provider = mock.MagicMock()
        launcher = CloudInstanceUpdater(mock_provider)
        launcher.notify(
            [
                InstanceUpdateEvent(
                    new_instance_status=Instance.REQUESTED,
                    launch_request_id="1",
                    instance_type="type-1",
                ),
                InstanceUpdateEvent(
                    new_instance_status=Instance.REQUESTED,
                    launch_request_id="1",
                    instance_type="type-1",
                ),
                InstanceUpdateEvent(
                    new_instance_status=Instance.REQUESTED,
                    launch_request_id="2",
                    instance_type="type-1",
                ),
                InstanceUpdateEvent(
                    new_instance_status=Instance.REQUESTED,
                    launch_request_id="2",
                    instance_type="type-2",
                ),
            ]
        )

        def verify():
            mock_provider.launch.assert_has_calls(
                [
                    mock.call(shape={"type-1": 2}, request_id="1"),
                    mock.call(shape={"type-1": 1, "type-2": 1}, request_id="2"),
                ]
            )
            return True

        wait_for_condition(verify)

    def test_multi_notify(self):
        mock_provider = mock.MagicMock()
        launcher = CloudInstanceUpdater(mock_provider)

        launcher.notify(
            [
                InstanceUpdateEvent(
                    new_instance_status=Instance.REQUESTED,
                    launch_request_id="1",
                    instance_type="type-1",
                ),
            ]
        )

        launcher.notify(
            [
                InstanceUpdateEvent(
                    new_instance_status=Instance.REQUESTED,
                    launch_request_id="2",
                    instance_type="type-1",
                ),
            ]
        )

        def verify():
            assert mock_provider.launch.call_count == 2
            mock_provider.launch.assert_has_calls(
                [
                    mock.call(shape={"type-1": 1}, request_id="1"),
                    mock.call(shape={"type-1": 1}, request_id="2"),
                ]
            )
            return True

        wait_for_condition(verify)

    def test_terminate_no_op(self):
        mock_provider = mock.MagicMock()
        launcher = CloudInstanceUpdater(mock_provider)
        launcher.notify(
            [
                InstanceUpdateEvent(
                    new_instance_status=Instance.RAY_RUNNING,
                    instance_id="1",
                    cloud_instance_id="c1",
                ),
            ]
        )

        def verify():
            mock_provider.terminate.assert_not_called()
            return True

        wait_for_condition(verify)

    def test_terminate_instances(self):
        mock_provider = mock.MagicMock()
        launcher = CloudInstanceUpdater(mock_provider)
        launcher.notify(
            [
                InstanceUpdateEvent(
                    new_instance_status=Instance.TERMINATING,
                    instance_id="1",
                    cloud_instance_id="c1",
                ),
                InstanceUpdateEvent(
                    new_instance_status=Instance.TERMINATING,
                    instance_id="2",
                    cloud_instance_id="c2",
                ),
                InstanceUpdateEvent(
                    new_instance_status=Instance.TERMINATING,
                    instance_id="3",
                    cloud_instance_id="c3",
                ),
            ]
        )

        def verify():
            mock_provider.terminate.assert_called_once_with(
                ids=["c1", "c2", "c3"], request_id=mock.ANY
            )
            return True

        wait_for_condition(verify)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
