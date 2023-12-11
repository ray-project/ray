import os
import sys
import unittest

# coding: utf-8
from typing import Dict, List, Optional
from unittest.mock import patch

import pytest

from ray.autoscaler.v2.instance_manager.instance_manager import InstanceUtil
from ray.core.generated.instance_manager_pb2 import Instance


class InstanceUtilTest(unittest.TestCase):
    def test_status_transitions(self):
        # Happy paths :)
        instance = InstanceUtil.new_instance("i-123", "type_1", {"CPU": 1}, "rq-1")
        assert instance.status == Instance.QUEUED
        InstanceUtil.set_status(instance, Instance.REQUESTED)
        InstanceUtil.set_status(instance, Instance.ALLOCATED)
        InstanceUtil.set_status(instance, Instance.RAY_INSTALLING)
        InstanceUtil.set_status(instance, Instance.RAY_RUNNING)
        InstanceUtil.set_status(instance, Instance.RAY_STOPPING)
        InstanceUtil.set_status(instance, Instance.RAY_STOPPED)

        # Allocation failed.
        instance = InstanceUtil.new_instance("i-123", "type_1", {"CPU": 1}, "rq-1")
        instance.status = Instance.REQUESTED
        InstanceUtil.set_status(instance, Instance.ALLOCATION_FAILED)

        # Ray install failed.
        instance = InstanceUtil.new_instance("i-123", "type_1", {"CPU": 1}, "rq-1")
        instance.status = Instance.RAY_INSTALLING
        InstanceUtil.set_status(instance, Instance.RAY_INSTALL_FAILED)

        # Ray instance failed.
        instance = InstanceUtil.new_instance("i-123", "type_1", {"CPU": 1}, "rq-1")
        instance.status = Instance.RAY_RUNNING
        InstanceUtil.set_status(instance, Instance.RAY_STOPPED)

        # Instance crashed.
        instance = InstanceUtil.new_instance("i-123", "type_1", {"CPU": 1}, "rq-1")
        instance.status = Instance.ALLOCATED
        InstanceUtil.set_status(instance, Instance.STOPPED)
        instance.status = Instance.RAY_INSTALLING
        InstanceUtil.set_status(instance, Instance.STOPPED)
        instance.status = Instance.RAY_RUNNING
        InstanceUtil.set_status(instance, Instance.STOPPED)

        # No random status changes
        instance = InstanceUtil.new_instance("i-123", "type_1", {"CPU": 1}, "rq-1")
        # No setting to unknown
        with pytest.raises(ValueError):
            InstanceUtil.set_status(instance, Instance.UNKNOWN)
        # No setting to queued
        with pytest.raises(ValueError):
            InstanceUtil.set_status(instance, Instance.QUEUED)
        # No setting to requested from running
        instance.status = Instance.RAY_RUNNING
        with pytest.raises(ValueError):
            InstanceUtil.set_status(instance, Instance.REQUESTED)
        # No setting backwards
        instance.status = Instance.RAY_RUNNING
        with pytest.raises(ValueError):
            InstanceUtil.set_status(instance, Instance.RAY_INSTALLING)

    @patch("time.time_ns")
    def test_status_time(self, mock_time):
        ns_to_ms = 1000 * 1000
        mock_time.return_value = 1 * ns_to_ms
        instance = InstanceUtil.new_instance("i-123", "type_1", {"CPU": 1}, "rq-1")
        # OK
        assert InstanceUtil.get_status_time_ms(instance, Instance.QUEUED) == 1

        # Missing status returns None
        assert InstanceUtil.get_status_time_ms(instance, Instance.REQUESTED) == None

        # Multiple status.
        mock_time.return_value = 2 * ns_to_ms
        InstanceUtil.set_status(instance, Instance.REQUESTED)
        mock_time.return_value = 3 * ns_to_ms
        InstanceUtil.set_status(instance, Instance.ALLOCATION_FAILED)
        mock_time.return_value = 4 * ns_to_ms
        InstanceUtil.set_status(instance, Instance.QUEUED)
        assert InstanceUtil.get_status_time_ms(instance, Instance.QUEUED) == 1
        assert (
            InstanceUtil.get_status_time_ms(instance, Instance.QUEUED, reverse=True)
            == 4
        )

        # Invalid timestamps.
        mock_time.return_value = 1 * ns_to_ms
        instance = InstanceUtil.new_instance("i-123", "type_1", {"CPU": 1}, "rq-1")
        mock_time.return_value = 2 * ns_to_ms
        InstanceUtil.set_status(instance, Instance.REQUESTED)
        mock_time.return_value = 1 * ns_to_ms
        with pytest.raises(ValueError, match="Invalid timestamp"):
            InstanceUtil.set_status(instance, Instance.ALLOCATION_FAILED)

    def test_is_ray_pending(self):
        instance = InstanceUtil.new_instance("i-123", "type_1", {"CPU": 1}, "rq-1")
        assert InstanceUtil.is_ray_pending(instance)
        instance.status = Instance.QUEUED
        assert InstanceUtil.is_ray_pending(instance)
        instance.status = Instance.REQUESTED
        assert InstanceUtil.is_ray_pending(instance)
        instance.status = Instance.ALLOCATED
        assert InstanceUtil.is_ray_pending(instance)
        instance.status = Instance.RAY_INSTALLING
        assert InstanceUtil.is_ray_pending(instance)
        instance.status = Instance.ALLOCATION_FAILED
        assert InstanceUtil.is_ray_pending(instance)
        instance.status = Instance.RAY_INSTALL_FAILED
        assert InstanceUtil.is_ray_pending(instance)

        instance.status = Instance.RAY_RUNNING
        assert not InstanceUtil.is_ray_pending(instance)
        instance.status = Instance.RAY_STOPPING
        assert not InstanceUtil.is_ray_pending(instance)
        instance.status = Instance.RAY_STOPPED
        assert not InstanceUtil.is_ray_pending(instance)
        instance.status = Instance.STOPPED
        assert not InstanceUtil.is_ray_pending(instance)
        instance.status = Instance.UNKNOWN
        with pytest.raises(AssertionError):
            InstanceUtil.is_ray_pending(instance)

    def test_is_cloud_instance_allocated(self):
        instance = InstanceUtil.new_instance("i-123", "type_1", {"CPU": 1}, "rq-1")
        assert not InstanceUtil.is_cloud_instance_allocated(instance)
        instance.status = Instance.REQUESTED
        assert not InstanceUtil.is_cloud_instance_allocated(instance)
        instance.status = Instance.STOPPED
        assert not InstanceUtil.is_cloud_instance_allocated(instance)
        instance.status = Instance.ALLOCATION_FAILED
        assert not InstanceUtil.is_cloud_instance_allocated(instance)

        instance.status = Instance.ALLOCATED
        assert InstanceUtil.is_cloud_instance_allocated(instance)
        instance.status = Instance.RAY_INSTALLING
        assert InstanceUtil.is_cloud_instance_allocated(instance)
        instance.status = Instance.RAY_INSTALL_FAILED
        assert InstanceUtil.is_cloud_instance_allocated(instance)
        instance.status = Instance.RAY_RUNNING
        assert InstanceUtil.is_cloud_instance_allocated(instance)
        instance.status = Instance.RAY_STOPPING
        assert InstanceUtil.is_cloud_instance_allocated(instance)
        instance.status = Instance.RAY_STOPPED
        assert InstanceUtil.is_cloud_instance_allocated(instance)

        instance.status = Instance.UNKNOWN
        with pytest.raises(AssertionError):
            InstanceUtil.is_cloud_instance_allocated(instance)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
