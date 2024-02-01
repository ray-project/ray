# coding: utf-8
import os
import sys
import time

import pytest

import mock

from ray.autoscaler.v2.instance_manager.config import InstanceReconcileConfig
from ray.autoscaler.v2.instance_manager.node_provider import (  # noqa
    CloudInstance,
    LaunchNodeError,
)
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler, logger
from ray.autoscaler.v2.tests.util import create_instance
from ray.core.generated.autoscaler_pb2 import ClusterResourceState
from ray.core.generated.instance_manager_pb2 import Instance

s_to_ns = 1 * 1_000_000_000

logger.setLevel("DEBUG")


class TestReconciler:
    @staticmethod
    def test_requested_instance_no_op():
        # Request no timeout yet.
        instances = [
            create_instance(
                "i-1",
                Instance.REQUESTED,
                launch_request_id="l1",
                instance_type="type-1",
                status_times=[(Instance.REQUESTED, time.time_ns())],
            ),
        ]

        results = Reconciler.reconcile(
            instances,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            config=InstanceReconcileConfig(),
        )
        # No ops
        assert len(results) == 0

    @staticmethod
    def test_requested_instance_to_allocated():
        # When there's a matching cloud instance for the requested instance.
        # The requested instance should be moved to ALLOCATED.
        instances = [
            create_instance(
                "i-1",
                status=Instance.REQUESTED,
                instance_type="type-1",
                status_times=[(Instance.REQUESTED, time.time_ns())],
            ),
            create_instance(
                "i-2",
                status=Instance.REQUESTED,
                instance_type="type-2",
                status_times=[(Instance.REQUESTED, time.time_ns())],
            ),
        ]

        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", "", True),
            "c-2": CloudInstance("c-2", "type-999", "", True),
        }

        results = Reconciler.reconcile(
            instances,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            config=InstanceReconcileConfig(),
        )

        assert len(results) == 1
        assert results["i-1"].new_instance_status == Instance.ALLOCATED
        assert results["i-1"].cloud_instance_id == "c-1"

    @staticmethod
    @mock.patch("time.time_ns")
    def test_requested_instance_to_queued(cur_time_ns):
        """
        When retry for request timeout happens, the instance should be
        moved to QUEUED.
        """
        cur_time_ns.return_value = 100 * s_to_ns
        instances = [
            create_instance(
                "i-1",
                status=Instance.REQUESTED,
                instance_type="type-1",
                status_times=[(Instance.REQUESTED, cur_time_ns - 10 * s_to_ns)],
            ),
        ]

        results = Reconciler.reconcile(
            instances,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            config=InstanceReconcileConfig(
                request_status_timeout_s=1, max_num_retry_request_to_allocate=3
            ),
        )

        assert len(results) == 1
        assert results["i-1"].new_instance_status == Instance.QUEUED

    @staticmethod
    @mock.patch("time.time_ns")
    def test_requested_instance_to_allocation_failed(cur_time_ns):
        """
        Test that the instance should be transitioned to ALLOCATION_FAILED
        iff:
            1. request timeout happens and retry count hits.
            2. launch error happens.
        """
        timeout_s = 5
        cur_time_ns.return_value = 20 * s_to_ns

        instances = [
            # Should fail due to launch error.
            create_instance(
                "i-1",
                status=Instance.REQUESTED,
                instance_type="type-1",
                status_times=[
                    (Instance.REQUESTED, cur_time_ns - (timeout_s - 1) * s_to_ns)
                ],
                launch_request_id="l1",
            ),
            # Should fail due to timeout and retry count hits.
            create_instance(
                "i-2",
                status=Instance.REQUESTED,
                instance_type="type-1",
                status_times=[
                    (Instance.REQUESTED, cur_time_ns - (timeout_s + 2) * s_to_ns),
                    (Instance.REQUESTED, cur_time_ns - (timeout_s + 1) * s_to_ns),
                ],
                launch_request_id="l2",
            ),
            # Still OK - but retrying
            create_instance(
                "i-3",
                status=Instance.REQUESTED,
                instance_type="type-1",
                status_times=[
                    (Instance.REQUESTED, cur_time_ns - (timeout_s + 1) * s_to_ns)
                ],
            ),
        ]

        launch_errors = [
            LaunchNodeError(
                request_id="l1",
                count=1,  # The request failed.
                node_type="type-1",
                timestamp_ns=1,
                exception=None,
                details="fail",
            )
        ]

        results = Reconciler.reconcile(
            instances,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={},
            cloud_provider_errors=launch_errors,
            config=InstanceReconcileConfig(
                request_status_timeout_s=timeout_s, max_num_retry_request_to_allocate=1
            ),
        )

        assert len(results) == 3
        assert (
            results["i-1"].new_instance_status == Instance.ALLOCATION_FAILED
        ), results["i-1"]
        assert (
            results["i-2"].new_instance_status == Instance.ALLOCATION_FAILED
        ), results["i-2"]
        assert results["i-3"].new_instance_status == Instance.QUEUED, results["i-3"]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
