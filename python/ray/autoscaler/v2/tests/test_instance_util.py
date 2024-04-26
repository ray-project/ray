import os
import sys
import unittest

# coding: utf-8
from unittest.mock import patch

import pytest

from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.core.generated.instance_manager_pb2 import Instance


class InstanceUtilTest(unittest.TestCase):
    def test_basic(self):
        # New instance.
        instance = InstanceUtil.new_instance("i-123", "type_1", Instance.QUEUED)
        assert instance.instance_id == "i-123"
        assert instance.instance_type == "type_1"
        assert instance.status == Instance.QUEUED

        # Set status.
        assert InstanceUtil.set_status(instance, Instance.REQUESTED)
        assert instance.status == Instance.REQUESTED

        # Set status with invalid status.
        assert not InstanceUtil.set_status(instance, Instance.RAY_RUNNING)

        assert not InstanceUtil.set_status(instance, Instance.UNKNOWN)

    def test_transition_graph(self):
        # Assert on each edge in the graph.
        all_status = set(Instance.InstanceStatus.values())

        g = InstanceUtil.get_valid_transitions()

        assert g[Instance.QUEUED] == {Instance.REQUESTED}
        all_status.remove(Instance.QUEUED)

        assert g[Instance.REQUESTED] == {
            Instance.ALLOCATED,
            Instance.QUEUED,
            Instance.ALLOCATION_FAILED,
        }
        all_status.remove(Instance.REQUESTED)

        assert g[Instance.ALLOCATED] == {
            Instance.RAY_INSTALLING,
            Instance.RAY_RUNNING,
            Instance.RAY_STOPPING,
            Instance.RAY_STOPPED,
            Instance.TERMINATING,
            Instance.TERMINATED,
        }
        all_status.remove(Instance.ALLOCATED)

        assert g[Instance.RAY_INSTALLING] == {
            Instance.RAY_RUNNING,
            Instance.RAY_INSTALL_FAILED,
            Instance.RAY_STOPPED,
            Instance.TERMINATING,
            Instance.TERMINATED,
        }
        all_status.remove(Instance.RAY_INSTALLING)

        assert g[Instance.RAY_RUNNING] == {
            Instance.RAY_STOP_REQUESTED,
            Instance.RAY_STOPPING,
            Instance.RAY_STOPPED,
            Instance.TERMINATING,
            Instance.TERMINATED,
        }
        all_status.remove(Instance.RAY_RUNNING)

        assert g[Instance.RAY_STOP_REQUESTED] == {
            Instance.RAY_STOPPING,
            Instance.RAY_STOPPED,
            Instance.TERMINATED,
            Instance.RAY_RUNNING,
        }
        all_status.remove(Instance.RAY_STOP_REQUESTED)

        assert g[Instance.RAY_STOPPING] == {
            Instance.RAY_STOPPED,
            Instance.TERMINATING,
            Instance.TERMINATED,
        }

        all_status.remove(Instance.RAY_STOPPING)

        assert g[Instance.RAY_STOPPED] == {Instance.TERMINATED, Instance.TERMINATING}
        all_status.remove(Instance.RAY_STOPPED)

        assert g[Instance.TERMINATING] == {
            Instance.TERMINATED,
            Instance.TERMINATION_FAILED,
        }
        all_status.remove(Instance.TERMINATING)

        assert g[Instance.TERMINATION_FAILED] == {Instance.TERMINATING}
        all_status.remove(Instance.TERMINATION_FAILED)

        assert g[Instance.TERMINATED] == set()
        all_status.remove(Instance.TERMINATED)

        assert g[Instance.ALLOCATION_FAILED] == set()
        all_status.remove(Instance.ALLOCATION_FAILED)

        assert g[Instance.RAY_INSTALL_FAILED] == {
            Instance.TERMINATED,
            Instance.TERMINATING,
        }
        all_status.remove(Instance.RAY_INSTALL_FAILED)

        assert g[Instance.UNKNOWN] == set()
        all_status.remove(Instance.UNKNOWN)

        assert len(all_status) == 0

    @patch("time.time_ns")
    def test_status_time(self, mock_time):
        mock_time.return_value = 1
        instance = InstanceUtil.new_instance("i-123", "type_1", Instance.QUEUED)
        # OK
        assert (
            InstanceUtil.get_status_transition_times_ns(instance, Instance.QUEUED)[0]
            == 1
        )
        # No filter.
        assert InstanceUtil.get_status_transition_times_ns(
            instance,
        ) == [1]

        # Missing status returns empty list
        assert (
            InstanceUtil.get_status_transition_times_ns(instance, Instance.REQUESTED)
            == []
        )

        # Multiple status.
        mock_time.return_value = 2
        InstanceUtil.set_status(instance, Instance.REQUESTED)
        mock_time.return_value = 3
        InstanceUtil.set_status(instance, Instance.QUEUED)
        mock_time.return_value = 4
        InstanceUtil.set_status(instance, Instance.REQUESTED)
        assert InstanceUtil.get_status_transition_times_ns(
            instance, Instance.QUEUED
        ) == [1, 3]

    @patch("time.time_ns")
    def test_get_last_status_transition(self, mock_time):
        mock_time.return_value = 1
        instance = InstanceUtil.new_instance("i-123", "type_1", Instance.QUEUED)
        assert (
            InstanceUtil.get_last_status_transition(instance).instance_status
            == Instance.QUEUED
        )
        assert InstanceUtil.get_last_status_transition(instance).timestamp_ns == 1

        mock_time.return_value = 2
        InstanceUtil.set_status(instance, Instance.REQUESTED)
        assert (
            InstanceUtil.get_last_status_transition(instance).instance_status
            == Instance.REQUESTED
        )
        assert InstanceUtil.get_last_status_transition(instance).timestamp_ns == 2

        mock_time.return_value = 3
        InstanceUtil.set_status(instance, Instance.QUEUED)
        assert (
            InstanceUtil.get_last_status_transition(instance).instance_status
            == Instance.QUEUED
        )
        assert InstanceUtil.get_last_status_transition(instance).timestamp_ns == 3

        assert (
            InstanceUtil.get_last_status_transition(
                instance, select_instance_status=Instance.REQUESTED
            ).instance_status
            == Instance.REQUESTED
        )
        assert (
            InstanceUtil.get_last_status_transition(
                instance, select_instance_status=Instance.REQUESTED
            ).timestamp_ns
            == 2
        )

        assert (
            InstanceUtil.get_last_status_transition(
                instance, select_instance_status=Instance.RAY_RUNNING
            )
            is None
        )

    def test_is_cloud_instance_allocated(self):
        all_status = set(Instance.InstanceStatus.values())
        instance = InstanceUtil.new_instance("i-123", "type_1", Instance.QUEUED)
        positive_status = {
            Instance.ALLOCATED,
            Instance.RAY_INSTALLING,
            Instance.RAY_INSTALL_FAILED,
            Instance.RAY_RUNNING,
            Instance.RAY_STOP_REQUESTED,
            Instance.RAY_STOPPING,
            Instance.RAY_STOPPED,
            Instance.TERMINATING,
            Instance.TERMINATION_FAILED,
        }
        for s in positive_status:
            instance.status = s
            assert InstanceUtil.is_cloud_instance_allocated(instance.status)
            all_status.remove(s)

        # Unknown not possible.
        all_status.remove(Instance.UNKNOWN)
        for s in all_status:
            instance.status = s
            assert not InstanceUtil.is_cloud_instance_allocated(instance.status)

    def test_is_ray_running(self):
        all_statuses = set(Instance.InstanceStatus.values())
        positive_statuses = {
            Instance.RAY_RUNNING,
            Instance.RAY_STOP_REQUESTED,
            Instance.RAY_STOPPING,
        }
        all_statuses.remove(Instance.UNKNOWN)
        for s in positive_statuses:
            assert InstanceUtil.is_ray_running(s)
            all_statuses.remove(s)

        for s in all_statuses:
            assert not InstanceUtil.is_ray_running(s)

    def test_is_ray_pending(self):
        all_statuses = set(Instance.InstanceStatus.values())
        all_statuses.remove(Instance.UNKNOWN)
        positive_statuses = {
            Instance.QUEUED,
            Instance.REQUESTED,
            Instance.RAY_INSTALLING,
            Instance.ALLOCATED,
        }
        for s in positive_statuses:
            assert InstanceUtil.is_ray_pending(s), Instance.InstanceStatus.Name(s)
            all_statuses.remove(s)

        for s in all_statuses:
            assert not InstanceUtil.is_ray_pending(s), Instance.InstanceStatus.Name(s)

    def test_is_ray_running_reachable(self):
        all_status = set(Instance.InstanceStatus.values())
        positive_status = {
            Instance.QUEUED,
            Instance.REQUESTED,
            Instance.ALLOCATED,
            Instance.RAY_INSTALLING,
            Instance.RAY_RUNNING,
            Instance.RAY_STOP_REQUESTED,
        }
        for s in positive_status:
            assert InstanceUtil.is_ray_running_reachable(
                s
            ), Instance.InstanceStatus.Name(s)
            all_status.remove(s)

        # Unknown not possible.
        all_status.remove(Instance.UNKNOWN)
        for s in all_status:
            assert not InstanceUtil.is_ray_running_reachable(
                s
            ), Instance.InstanceStatus.Name(s)

    def test_reachable_from(self):
        def add_reachable_from(reachable, src, transitions):
            reachable[src] = set()
            for dst in transitions[src]:
                reachable[src].add(dst)
                reachable[src] |= (
                    reachable[dst] if reachable[dst] is not None else set()
                )

        expected_reachable = {s: None for s in Instance.InstanceStatus.values()}

        # Error status and terminal status.
        expected_reachable[Instance.ALLOCATION_FAILED] = set()
        expected_reachable[Instance.UNKNOWN] = set()
        expected_reachable[Instance.TERMINATED] = set()

        transitions = InstanceUtil.get_valid_transitions()

        # Recursively build the reachable set from terminal statuses.
        add_reachable_from(expected_reachable, Instance.TERMINATION_FAILED, transitions)
        add_reachable_from(expected_reachable, Instance.TERMINATING, transitions)
        # Add TERMINATION_FAILED again since it's also reachable from TERMINATING.
        add_reachable_from(expected_reachable, Instance.TERMINATION_FAILED, transitions)
        add_reachable_from(expected_reachable, Instance.RAY_STOPPED, transitions)
        add_reachable_from(expected_reachable, Instance.RAY_STOPPING, transitions)
        add_reachable_from(expected_reachable, Instance.RAY_STOP_REQUESTED, transitions)
        add_reachable_from(expected_reachable, Instance.RAY_RUNNING, transitions)
        # Add RAY_STOP_REQUESTED again since it's also reachable from RAY_RUNNING.
        add_reachable_from(expected_reachable, Instance.RAY_STOP_REQUESTED, transitions)
        add_reachable_from(expected_reachable, Instance.RAY_INSTALL_FAILED, transitions)
        add_reachable_from(expected_reachable, Instance.RAY_INSTALLING, transitions)
        add_reachable_from(expected_reachable, Instance.ALLOCATED, transitions)
        add_reachable_from(expected_reachable, Instance.REQUESTED, transitions)
        add_reachable_from(expected_reachable, Instance.QUEUED, transitions)
        # Add REQUESTED again since it's also reachable from QUEUED.
        add_reachable_from(expected_reachable, Instance.REQUESTED, transitions)

        for s, expected_reachable in expected_reachable.items():
            assert InstanceUtil.get_reachable_statuses(s) == expected_reachable, (
                f"reachable_from({s}) = {InstanceUtil.get_reachable_statuses(s)} "
                f"!= {expected_reachable}"
            )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
