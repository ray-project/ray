# coding: utf-8
import os
import sys

import pytest

import mock

from ray._private.test_utils import wait_for_condition
from ray.autoscaler.v2.instance_manager.subscribers.cloud_instance_updater import (
    CloudInstanceUpdater,
)
from ray.core.generated.instance_manager_pb2 import Instance, InstanceUpdateEvent


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
