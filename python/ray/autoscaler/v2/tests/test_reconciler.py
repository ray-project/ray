# coding: utf-8
import os
import sys
import time

import pytest

import mock

from ray.autoscaler.v2.instance_manager.config import InstanceReconcileConfig
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.node_provider import (  # noqa
    CloudInstance,
    LaunchNodeError,
)
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler, logger
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.tests.util import create_instance
from ray.core.generated.instance_manager_pb2 import Instance

s_to_ns = 1 * 1_000_000_000

logger.setLevel("DEBUG")


@pytest.fixture()
def setup():
    instance_storage = InstanceStorage(
        cluster_id="test_cluster_id",
        storage=InMemoryStorage(),
    )
    instance_manager = InstanceManager(
        instance_storage=instance_storage,
    )

    yield instance_manager, instance_storage


class TestReconciler:
    @staticmethod
    def _add_instances(instance_storage, instances):
        for instance in instances:
            ok, _ = instance_storage.upsert_instance(instance)
            assert ok

    @staticmethod
    def test_requested_instance_no_op(setup):
        instance_manager, instance_storage = setup
        # Request no timeout yet.
        TestReconciler._add_instances(
            instance_storage,
            [
                create_instance(
                    "i-1",
                    Instance.REQUESTED,
                    launch_request_id="l1",
                    instance_type="type-1",
                    status_times=[(Instance.REQUESTED, time.time_ns())],
                ),
            ],
        )

        Reconciler.sync_from(
            instance_manager,
            ray_nodes=[],
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            ray_install_errors=[],
            config=InstanceReconcileConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 1
        assert instances["i-1"].status == Instance.REQUESTED

    @staticmethod
    def test_requested_instance_to_allocated(setup):
        # When there's a matching cloud instance for the requested instance.
        # The requested instance should be moved to ALLOCATED.
        instance_manager, instance_storage = setup
        TestReconciler._add_instances(
            instance_storage,
            [
                create_instance(
                    "i-1",
                    status=Instance.REQUESTED,
                    instance_type="type-1",
                    status_times=[(Instance.REQUESTED, time.time_ns())],
                    launch_request_id="l1",
                ),
                create_instance(
                    "i-2",
                    status=Instance.REQUESTED,
                    instance_type="type-2",
                    status_times=[(Instance.REQUESTED, time.time_ns())],
                    launch_request_id="l2",
                ),
            ],
        )

        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", "", True),
            "c-2": CloudInstance("c-2", "type-999", "", True),
        }

        Reconciler.sync_from(
            instance_manager,
            ray_nodes=[],
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            config=InstanceReconcileConfig(),
        )

        instances, _ = instance_storage.get_instances()

        assert len(instances) == 2
        assert instances["i-1"].status == Instance.ALLOCATED
        assert instances["i-1"].cloud_instance_id == "c-1"
        assert instances["i-2"].status == Instance.REQUESTED

    @staticmethod
    @mock.patch("time.time_ns")
    def test_requested_instance_to_queued(cur_time_ns, setup):
        """
        When retry for request timeout happens, the instance should be
        moved to QUEUED.
        """
        instance_manager, instance_storage = setup
        cur_time_ns.return_value = 100 * s_to_ns

        instances = [
            create_instance(
                "i-1",
                status=Instance.REQUESTED,
                instance_type="type-1",
                status_times=[(Instance.REQUESTED, cur_time_ns - 10 * s_to_ns)],
                launch_request_id="l1",
            ),
        ]

        TestReconciler._add_instances(
            instance_storage,
            instances,
        )

        Reconciler.sync_from(
            instance_manager,
            ray_nodes=[],
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            ray_install_errors=[],
            config=InstanceReconcileConfig(
                request_status_timeout_s=1, max_num_retry_request_to_allocate=3
            ),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 1
        assert instances["i-1"].status == Instance.QUEUED

    @staticmethod
    @mock.patch("time.time_ns")
    def test_requested_instance_to_allocation_failed(cur_time_ns, setup):
        """
        Test that the instance should be transitioned to ALLOCATION_FAILED
        iff:
            1. request timeout happens and retry count hits.
            2. launch error happens.
        """
        timeout_s = 5
        cur_time_ns.return_value = 20 * s_to_ns
        instance_manager, instance_storage = setup

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
                launch_request_id="l2",
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

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

        Reconciler.sync_from(
            instance_manager,
            ray_nodes=[],
            non_terminated_cloud_instances={},
            cloud_provider_errors=launch_errors,
            ray_install_errors=[],
            config=InstanceReconcileConfig(
                request_status_timeout_s=timeout_s, max_num_retry_request_to_allocate=1
            ),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 3
        assert instances["i-1"].status == Instance.ALLOCATION_FAILED
        assert instances["i-2"].status == Instance.ALLOCATION_FAILED
        assert instances["i-3"].status == Instance.QUEUED


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
