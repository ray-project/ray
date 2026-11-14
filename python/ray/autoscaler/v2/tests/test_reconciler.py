import math
import os
import sys
import time

# coding: utf-8
from collections import defaultdict
from typing import Optional
from unittest import mock
from unittest.mock import MagicMock

import pytest

from ray._common.utils import binary_to_hex
from ray.autoscaler.v2.instance_manager.config import (
    InstanceReconcileConfig,
    NodeTypeConfig,
    Provider,
    WorkerReplicaConfig,
)
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.node_provider import (  # noqa
    CloudInstance,
    LaunchNodeError,
    TerminateNodeError,
)
from ray.autoscaler.v2.instance_manager.reconciler import (
    _BOOTING_INSTANCE_STATUSES,
    _DRAINING_INSTANCE_STATUSES,
    _TEARDOWN_INSTANCE_STATUSES,
    _TERMINAL_INSTANCE_STATUSES,
    _UNALLOCATED_INSTANCE_STATUSES,
    Reconciler,
    _build_multihost_replica_view,
    _get_replica_key,
    logger,
)
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.cloud_instance_updater import (
    CloudInstanceUpdater,
)
from ray.autoscaler.v2.instance_manager.subscribers.cloud_resource_monitor import (
    CloudResourceMonitor,
)
from ray.autoscaler.v2.instance_manager.subscribers.ray_stopper import RayStopError
from ray.autoscaler.v2.instance_manager.subscribers.threaded_ray_installer import (
    RayInstallError,
)
from ray.autoscaler.v2.scheduler import (
    IResourceScheduler,
    ResourceDemandScheduler,
    SchedulingReply,
)
from ray.autoscaler.v2.tests.util import MockSubscriber, create_instance
from ray.autoscaler.v2.utils import ResourceRequestUtil
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceState,
    GangResourceRequest,
    NodeState,
    NodeStatus,
    ResourceRequest,
    ResourceRequestByCount,
)
from ray.core.generated.instance_manager_pb2 import (
    Instance,
    InstanceUpdateEvent,
    LaunchRequest,
    NodeKind,
    TerminationRequest,
)

s_to_ns = 1 * 1_000_000_000

logger.setLevel("DEBUG")


class MockAutoscalingConfig:
    def __init__(self, configs=None):
        if configs is None:
            configs = {}
        self._configs = configs

    def get_node_type_configs(self):
        return self._configs.get("node_type_configs", {})

    def get_max_num_worker_nodes(self):
        return self._configs.get("max_num_worker_nodes")

    def get_max_num_nodes(self):
        n = self._configs.get("max_num_worker_nodes")
        return n + 1 if n is not None else None

    def get_upscaling_speed(self):
        return self._configs.get("upscaling_speed", 0.0)

    def get_max_concurrent_launches(self):
        return self._configs.get("max_concurrent_launches", 100)

    def get_instance_reconcile_config(self):
        return self._configs.get("instance_reconcile_config", InstanceReconcileConfig())

    def disable_node_updaters(self):
        return self._configs.get("disable_node_updaters", True)

    def disable_launch_config_check(self):
        return self._configs.get("disable_launch_config_check", False)

    def get_idle_timeout_s(self):
        return self._configs.get("idle_timeout_s", 999)

    def get_pending_idle_timeout_s(self):
        return self._configs.get("pending_idle_timeout_s", 999)

    def get_provider_instance_type(self, ray_node_type):
        return self._configs.get("provider_instance_types", {}).get(ray_node_type, "")

    @property
    def provider(self):
        return self._configs.get("provider", Provider.UNKNOWN)


class MockScheduler(IResourceScheduler):
    def __init__(self, to_launch=None, to_terminate=None):
        if to_launch is None:
            to_launch = []

        if to_terminate is None:
            to_terminate = []

        self.to_launch = to_launch
        self.to_terminate = to_terminate

    def schedule(self, req):
        return SchedulingReply(
            to_launch=self.to_launch,
            to_terminate=self.to_terminate,
        )


@pytest.fixture()
def setup():
    instance_storage = InstanceStorage(
        cluster_id="test_cluster_id",
        storage=InMemoryStorage(),
    )

    mock_subscriber = MockSubscriber()

    instance_manager = InstanceManager(
        instance_storage=instance_storage,
        instance_status_update_subscribers=[mock_subscriber],
    )

    cloud_resource_monitor = CloudResourceMonitor()

    yield instance_manager, instance_storage, mock_subscriber, cloud_resource_monitor


class TestReconciler:
    @staticmethod
    def _add_instances(instance_storage, instances):
        for instance in instances:
            ok, _ = instance_storage.upsert_instance(instance)
            assert ok

    @staticmethod
    def test_requested_instance_no_op(setup):
        instance_manager, instance_storage, _, cloud_resource_monitor = setup
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

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 1
        assert instances["i-1"].status == Instance.REQUESTED

    @staticmethod
    def test_requested_instance_to_allocated(setup):
        # When there's a matching cloud instance for the requested instance.
        # The requested instance should be moved to ALLOCATED.
        instance_manager, instance_storage, _, cloud_resource_monitor = setup
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
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
        }

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()

        assert len(instances) == 2
        assert instances["i-1"].status == Instance.ALLOCATED
        assert instances["i-1"].cloud_instance_id == "c-1"
        assert instances["i-2"].status == Instance.REQUESTED

    @staticmethod
    def test_requested_instance_to_allocation_failed(setup):
        """
        Test that the instance should be transitioned to ALLOCATION_FAILED
        when launch error happens.
        """
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        instances = [
            # Should succeed with instance matched.
            create_instance(
                "i-1",
                status=Instance.REQUESTED,
                instance_type="type-1",
                launch_request_id="l1",
            ),
            # Should fail due to launch error.
            create_instance(
                "i-2",
                status=Instance.REQUESTED,
                instance_type="type-2",
                launch_request_id="l1",
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

        launch_errors = [
            LaunchNodeError(
                request_id="l1",
                count=1,  # The request failed.
                node_type="type-2",
                timestamp_ns=1,
            )
        ]

        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
        }
        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=launch_errors,
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 2
        assert instances["i-1"].status == Instance.ALLOCATED
        assert instances["i-1"].cloud_instance_id == "c-1"
        assert instances["i-2"].status == Instance.ALLOCATION_FAILED

    @staticmethod
    def test_multiple_node_types_share_launch_request_id(setup):
        """
        Test that all node types of a launch request are transitioned to
        ALLOCATION_FAILED when each of them fails, i.e. launch errors of the same
        launch request don't overwrite each other.
        """
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        instances = [
            create_instance(
                "i-1",
                status=Instance.REQUESTED,
                instance_type="type-1",
                launch_request_id="l1",
            ),
            create_instance(
                "i-2",
                status=Instance.REQUESTED,
                instance_type="type-2",
                launch_request_id="l1",
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

        # Both node types of the same launch request failed.
        launch_errors = [
            LaunchNodeError(
                request_id="l1",
                count=1,
                node_type="type-1",
                timestamp_ns=1,
            ),
            LaunchNodeError(
                request_id="l1",
                count=1,
                node_type="type-2",
                timestamp_ns=1,
            ),
        ]

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={},
            cloud_provider_errors=launch_errors,
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 2
        assert instances["i-1"].status == Instance.ALLOCATION_FAILED
        assert instances["i-2"].status == Instance.ALLOCATION_FAILED

    @staticmethod
    def test_launch_error_count_fails_only_rejected_instances(setup):
        """
        A LaunchNodeError.count should fail only that many REQUESTED instances
        of the same (request id, type). Remaining instances stay REQUESTED so a
        partial maxReplicas cap can still bind later pods.
        """
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        instances = [
            create_instance(
                "i-1",
                status=Instance.REQUESTED,
                instance_type="type-1",
                launch_request_id="l1",
                status_times=[(Instance.REQUESTED, 1)],
            ),
            create_instance(
                "i-2",
                status=Instance.REQUESTED,
                instance_type="type-1",
                launch_request_id="l1",
                status_times=[(Instance.REQUESTED, 2)],
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

        launch_errors = [
            LaunchNodeError(
                request_id="l1",
                count=1,
                node_type="type-1",
                timestamp_ns=1,
            )
        ]

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={},
            cloud_provider_errors=launch_errors,
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert instances["i-1"].status == Instance.ALLOCATION_FAILED
        assert instances["i-2"].status == Instance.REQUESTED

    @staticmethod
    def test_launch_error_count_allocates_pod_then_fails_leftover(setup):
        """
        When a pod is already present, assign it first and fail only the
        leftover REQUESTED instance from LaunchNodeError.count.
        """
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        instances = [
            create_instance(
                "i-1",
                status=Instance.REQUESTED,
                instance_type="type-1",
                launch_request_id="l1",
                status_times=[(Instance.REQUESTED, 1)],
            ),
            create_instance(
                "i-2",
                status=Instance.REQUESTED,
                instance_type="type-1",
                launch_request_id="l1",
                status_times=[(Instance.REQUESTED, 2)],
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

        launch_errors = [
            LaunchNodeError(
                request_id="l1",
                count=1,
                node_type="type-1",
                timestamp_ns=1,
            )
        ]
        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
        }

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=launch_errors,
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        allocated = [
            inst for inst in instances.values() if inst.status == Instance.ALLOCATED
        ]
        failed = [
            inst
            for inst in instances.values()
            if inst.status == Instance.ALLOCATION_FAILED
        ]
        assert len(allocated) == 1
        assert allocated[0].cloud_instance_id == "c-1"
        assert len(failed) == 1

    @staticmethod
    def test_duplicate_launch_errors_sum_failure_counts(setup):
        """
        Multiple LaunchNodeError events with the same (request id, type)
        should add their counts. NodeProviderAdapter emits one error per
        launch batch, so overwriting would leave extra REQUESTED instances.
        """
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        instances = [
            create_instance(
                "i-1",
                status=Instance.REQUESTED,
                instance_type="type-1",
                launch_request_id="l1",
                status_times=[(Instance.REQUESTED, 1)],
            ),
            create_instance(
                "i-2",
                status=Instance.REQUESTED,
                instance_type="type-1",
                launch_request_id="l1",
                status_times=[(Instance.REQUESTED, 2)],
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

        launch_errors = [
            LaunchNodeError(
                request_id="l1",
                count=1,
                node_type="type-1",
                timestamp_ns=1,
            ),
            LaunchNodeError(
                request_id="l1",
                count=1,
                node_type="type-1",
                timestamp_ns=2,
            ),
        ]

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={},
            cloud_provider_errors=launch_errors,
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert instances["i-1"].status == Instance.ALLOCATION_FAILED
        assert instances["i-2"].status == Instance.ALLOCATION_FAILED

    @staticmethod
    def test_reconcile_terminated_cloud_instances(setup):

        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup

        instances = [
            create_instance(
                "i-1",
                status=Instance.ALLOCATED,
                instance_type="type-1",
                cloud_instance_id="c-1",
            ),
            create_instance(
                "i-2",
                status=Instance.TERMINATING,  # pending terminated, no update
                instance_type="type-2",
                cloud_instance_id="c-2",
            ),
            create_instance(
                "i-3",
                status=Instance.TERMINATED,  # Already terminated, no update
                instance_type="type-2",
                cloud_instance_id="c-3",
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

        cloud_instances = {
            "c-2": CloudInstance("c-2", "type-2", False, NodeKind.WORKER),
        }

        termination_errors = [
            TerminateNodeError(
                cloud_instance_id="c-2",
                timestamp_ns=1,
                request_id="t1",
            )
        ]
        subscriber.clear()
        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=termination_errors,
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 3
        assert instances["i-1"].status == Instance.TERMINATED
        assert instances["i-2"].status == Instance.TERMINATING
        events = subscriber.events
        assert len(events) == 3

        events_i_1 = subscriber.events_by_id("i-1")
        assert len(events_i_1) == 1
        assert events_i_1[0].new_instance_status == Instance.TERMINATED
        assert events_i_1[0].instance_id == "i-1"
        assert events_i_1[0].cloud_instance_id == "c-1"

        events_i_2 = subscriber.events_by_id("i-2")
        assert len(events_i_2) == 2
        assert events_i_2[0].new_instance_status == Instance.TERMINATION_FAILED
        assert events_i_2[1].new_instance_status == Instance.TERMINATING

    @staticmethod
    def test_gc_terminated_instances_past_retention(setup):
        # TERMINATED records older than the GC retention window are deleted from
        # storage, while recently TERMINATED and non-terminal records are kept.
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        retention_s = InstanceReconcileConfig().terminated_instance_gc_retention_s
        now = time.time_ns()
        instances = [
            create_instance(
                "i-stale",
                status=Instance.TERMINATED,
                status_times=[
                    (Instance.TERMINATED, now - (retention_s + 60) * s_to_ns)
                ],
            ),
            create_instance(
                "i-fresh",
                status=Instance.TERMINATED,
                status_times=[(Instance.TERMINATED, now)],
            ),
            create_instance(
                "i-live",
                status=Instance.ALLOCATED,
                cloud_instance_id="c-live",
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={
                "c-live": CloudInstance(
                    "c-live", "worker_nodes1", False, NodeKind.WORKER
                )
            },
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        stored, _ = instance_storage.get_instances()
        assert "i-stale" not in stored
        assert "i-fresh" in stored
        assert "i-live" in stored

    @staticmethod
    def test_gc_terminated_instances_kept_while_referenced_by_gcs(setup):
        # A TERMINATED record whose node_id is still in the GCS node snapshot is
        # kept even past retention, so it is not resurrected as ALLOCATED by
        # _handle_extra_cloud_instances_from_ray_nodes.
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        retention_s = InstanceReconcileConfig().terminated_instance_gc_retention_s
        now = time.time_ns()
        instances = [
            create_instance(
                "i-referenced",
                status=Instance.TERMINATED,
                ray_node_id=binary_to_hex(b"r-term"),
                status_times=[
                    (Instance.TERMINATED, now - (retention_s + 60) * s_to_ns)
                ],
            ),
            create_instance(
                "i-gone",
                status=Instance.TERMINATED,
                ray_node_id=binary_to_hex(b"r-gone"),
                status_times=[
                    (Instance.TERMINATED, now - (retention_s + 60) * s_to_ns)
                ],
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

        # GCS still reports r-term but not r-gone.
        ray_nodes = [
            NodeState(node_id=b"r-term", status=NodeStatus.DEAD, instance_id="c-term"),
        ]

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        stored, _ = instance_storage.get_instances()
        assert "i-referenced" in stored
        assert "i-gone" not in stored

    @staticmethod
    def test_gc_reclaims_allocation_failed(setup):
        # ALLOCATION_FAILED is also a terminal state nothing else reclaims, so a
        # record past retention is GC'd like TERMINATED. It is reached from
        # REQUESTED and never has a node_id, so the GCS check does not apply.
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        retention_s = InstanceReconcileConfig().terminated_instance_gc_retention_s
        now = time.time_ns()
        instances = [
            create_instance(
                "i-stale-failed",
                status=Instance.ALLOCATION_FAILED,
                status_times=[
                    (Instance.ALLOCATION_FAILED, now - (retention_s + 60) * s_to_ns)
                ],
            ),
            create_instance(
                "i-fresh-failed",
                status=Instance.ALLOCATION_FAILED,
                status_times=[(Instance.ALLOCATION_FAILED, now)],
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        stored, _ = instance_storage.get_instances()
        assert "i-stale-failed" not in stored
        assert "i-fresh-failed" in stored

    @staticmethod
    def test_gc_terminated_instances_disabled_with_negative_retention(setup):
        # A negative retention disables GC so even old TERMINATED records remain.
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        now = time.time_ns()
        TestReconciler._add_instances(
            instance_storage,
            [
                create_instance(
                    "i-stale",
                    status=Instance.TERMINATED,
                    status_times=[
                        (Instance.TERMINATED, now - 10 * 24 * 3600 * s_to_ns)
                    ],
                )
            ],
        )

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                {
                    "instance_reconcile_config": InstanceReconcileConfig(
                        terminated_instance_gc_retention_s=-1
                    )
                }
            ),
        )

        stored, _ = instance_storage.get_instances()
        assert "i-stale" in stored

    @staticmethod
    def test_ray_reconciler_no_op(setup):
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup

        im_instances = [
            create_instance(
                "i-1",
                status=Instance.ALLOCATED,
                launch_request_id="l1",
                instance_type="type-1",
                cloud_instance_id="c-1",
            ),
        ]
        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
        }

        TestReconciler._add_instances(instance_storage, im_instances)
        subscriber.clear()

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        # Assert no changes.
        assert subscriber.events == []

        # Unknown ray node status - no action.
        ray_nodes = [
            NodeState(node_id=b"r-1", status=NodeStatus.UNSPECIFIED, instance_id="c-1"),
        ]

        with pytest.raises(ValueError):
            Reconciler.reconcile(
                instance_manager,
                scheduler=MockScheduler(),
                cloud_provider=MagicMock(),
                cloud_resource_monitor=cloud_resource_monitor,
                ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
                non_terminated_cloud_instances=cloud_instances,
                cloud_provider_errors=[],
                ray_install_errors=[],
                autoscaling_config=MockAutoscalingConfig(),
            )

        # Assert no changes.
        assert subscriber.events == []

    @staticmethod
    def test_ray_reconciler_new_ray(setup):
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        # A newly running ray node with matching cloud instance id
        node_states = [
            NodeState(node_id=b"r-1", status=NodeStatus.RUNNING, instance_id="c-1"),
        ]
        im_instances = [
            create_instance(
                "i-0", status=Instance.TERMINATED, cloud_instance_id="c-1"
            ),  # this should not be matched.
            create_instance("i-1", status=Instance.ALLOCATED, cloud_instance_id="c-1"),
        ]
        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
        }
        TestReconciler._add_instances(instance_storage, im_instances)
        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=node_states),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 2
        assert instances["i-0"].status == Instance.TERMINATED
        assert instances["i-1"].status == Instance.RAY_RUNNING
        assert instances["i-1"].node_id == binary_to_hex(b"r-1")

    @staticmethod
    def test_ray_reconciler_already_ray_running(setup):
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup
        # A running ray node already reconciled.
        TestReconciler._add_instances(
            instance_storage,
            [
                create_instance(
                    "i-1", status=Instance.RAY_RUNNING, cloud_instance_id="c-1"
                ),
                create_instance(
                    "i-2", status=Instance.TERMINATING, cloud_instance_id="c-2"
                ),  # Already reconciled.
            ],
        )
        ray_nodes = [
            NodeState(node_id=b"r-1", status=NodeStatus.IDLE, instance_id="c-1"),
            NodeState(
                node_id=b"r-2", status=NodeStatus.IDLE, instance_id="c-2"
            ),  # Already being stopped
        ]
        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
            "c-2": CloudInstance("c-2", "type-2", True, NodeKind.WORKER),
        }

        subscriber.clear()
        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        assert len(subscriber.events) == 0

    @staticmethod
    def test_ray_reconciler_stopping_ray(setup):
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        # draining ray nodes
        im_instances = [
            create_instance(
                "i-1", status=Instance.RAY_RUNNING, cloud_instance_id="c-1"
            ),  # To be reconciled.
            create_instance(
                "i-2", status=Instance.RAY_STOPPING, cloud_instance_id="c-2"
            ),  # Already reconciled.
            create_instance(
                "i-3", status=Instance.TERMINATING, cloud_instance_id="c-3"
            ),  # Already reconciled.
        ]

        TestReconciler._add_instances(instance_storage, im_instances)

        ray_nodes = [
            NodeState(node_id=b"r-1", status=NodeStatus.DRAINING, instance_id="c-1"),
            NodeState(node_id=b"r-2", status=NodeStatus.DRAINING, instance_id="c-2"),
            NodeState(node_id=b"r-3", status=NodeStatus.DRAINING, instance_id="c-3"),
        ]
        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
            "c-2": CloudInstance("c-2", "type-2", True, NodeKind.WORKER),
            "c-3": CloudInstance("c-3", "type-3", True, NodeKind.WORKER),
        }

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 3
        assert instances["i-1"].status == Instance.RAY_STOPPING
        assert instances["i-2"].status == Instance.RAY_STOPPING
        assert instances["i-3"].status == Instance.TERMINATING

    @staticmethod
    def test_ray_reconciler_stopped_ray(setup):
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        # dead ray nodes
        im_instances = [
            create_instance(
                "i-1", status=Instance.ALLOCATED, cloud_instance_id="c-1"
            ),  # To be reconciled.
            create_instance(
                "i-2", status=Instance.RAY_STOPPING, cloud_instance_id="c-2"
            ),  # To be reconciled.
            create_instance(
                "i-3", status=Instance.TERMINATING, cloud_instance_id="c-3"
            ),  # Already reconciled.
        ]
        TestReconciler._add_instances(instance_storage, im_instances)

        ray_nodes = [
            NodeState(node_id=b"r-1", status=NodeStatus.DEAD, instance_id="c-1"),
            NodeState(node_id=b"r-2", status=NodeStatus.DEAD, instance_id="c-2"),
            NodeState(node_id=b"r-3", status=NodeStatus.DEAD, instance_id="c-3"),
        ]

        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
            "c-2": CloudInstance("c-2", "type-2", True, NodeKind.WORKER),
            "c-3": CloudInstance("c-3", "type-3", True, NodeKind.WORKER),
        }

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 3
        assert instances["i-1"].status == Instance.TERMINATING
        assert instances["i-2"].status == Instance.TERMINATING
        assert instances["i-3"].status == Instance.TERMINATING

    @staticmethod
    def test_reconcile_ray_installer_failures(setup):
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        # dead ray nodes
        im_instances = [
            create_instance(
                "i-1", status=Instance.RAY_INSTALLING, cloud_instance_id="c-1"
            ),  # To be reconciled.
        ]
        TestReconciler._add_instances(instance_storage, im_instances)

        ray_install_errors = [
            RayInstallError(
                im_instance_id="i-1",
                details="failed to install",
            )
        ]

        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
        }

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=[]),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=ray_install_errors,
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 1
        assert instances["i-1"].status == Instance.TERMINATING

    @staticmethod
    def test_draining_ray_node_also_terminated(setup):
        """
        A draining ray node due to a cloud instance termination should also
        transition the instance to TERMINATED.
        """

        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        im_instances = [
            create_instance(
                "i-1",
                status=Instance.RAY_RUNNING,
                cloud_instance_id="c-1",
                ray_node_id=binary_to_hex(b"r-1"),
            ),  # To be reconciled.
            create_instance(
                "i-2",
                status=Instance.RAY_RUNNING,
                cloud_instance_id="c-2",
                ray_node_id=binary_to_hex(b"r-2"),
            ),  # To be reconciled.
        ]
        TestReconciler._add_instances(instance_storage, im_instances)

        ray_nodes = [
            NodeState(node_id=b"r-1", status=NodeStatus.DEAD, instance_id="c-1"),
            NodeState(node_id=b"r-2", status=NodeStatus.DRAINING, instance_id="c-2"),
        ]

        cloud_instances = {
            # Terminated cloud instance.
        }

        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 2
        assert instances["i-1"].status == Instance.TERMINATED
        assert instances["i-2"].status == Instance.TERMINATED

    @staticmethod
    @pytest.mark.parametrize(
        "max_concurrent_launches,num_allocated,num_requested,num_running",
        [
            (1, 0, 0, 0),
            (10, 0, 0, 0),
            (1, 0, 1, 1),
            (1, 1, 0, 1),
            (10, 1, 0, 1),
            (10, 0, 1, 1),
            (10, 5, 5, 5),
        ],
    )
    @pytest.mark.parametrize(
        "upscaling_speed",
        [0.0, 0.1, 0.5, 1.0, 100.0],
    )
    def test_max_concurrent_launches(
        max_concurrent_launches,
        num_allocated,
        num_requested,
        num_running,
        upscaling_speed,
        setup,
    ):
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup
        next_id = 0

        # Add some allocated instances.
        cloud_instances = {}
        for _ in range(num_allocated):
            instance = create_instance(
                str(next_id),
                status=Instance.ALLOCATED,
                instance_type="type-1",
                cloud_instance_id=f"c-{next_id}",
            )
            next_id += 1
            cloud_instances[instance.cloud_instance_id] = CloudInstance(
                instance.cloud_instance_id, "type-1", True, NodeKind.WORKER
            )
            TestReconciler._add_instances(instance_storage, [instance])

        # Add some requested instances.
        for _ in range(num_requested):
            instance = create_instance(
                str(next_id),
                status=Instance.REQUESTED,
                instance_type="type-1",
                launch_request_id="l-1",
            )
            TestReconciler._add_instances(instance_storage, [instance])
            next_id += 1

        # Add many queued instances.
        queued_instances = [
            create_instance(
                str(i + next_id), status=Instance.QUEUED, instance_type="type-1"
            )
            for i in range(1000)
        ]
        TestReconciler._add_instances(instance_storage, queued_instances)

        # Add some running instances.
        for _ in range(num_running):
            instance = create_instance(
                str(next_id),
                status=Instance.RAY_RUNNING,
                instance_type="type-1",
                launch_request_id="l-1",
            )
            TestReconciler._add_instances(instance_storage, [instance])
            next_id += 1

        num_desired_upscale = max(1, math.ceil(upscaling_speed * (max(num_running, 1))))
        expected_launch_num = min(
            num_desired_upscale,
            max(0, max_concurrent_launches - num_requested),  # global limit
        )

        subscriber.clear()
        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "upscaling_speed": upscaling_speed,
                    "max_concurrent_launches": max_concurrent_launches,
                }
            ),
        )
        instances, _ = instance_storage.get_instances()
        assert len(subscriber.events) == expected_launch_num
        for event in subscriber.events:
            assert event.new_instance_status == Instance.REQUESTED
            assert (
                event.launch_request_id
                == instances[event.instance_id].launch_request_id
            )
            assert event.instance_type == "type-1"

    @staticmethod
    @mock.patch("time.time_ns")
    def test_stuck_instances_requested(mock_time_ns, setup):
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup
        cur_time_s = 10
        mock_time_ns.return_value = cur_time_s * s_to_ns

        reconcile_config = InstanceReconcileConfig(
            request_status_timeout_s=5,
            max_num_retry_request_to_allocate=1,  # max 1 retry.
        )

        instances = [
            create_instance(
                "no-update",
                Instance.REQUESTED,
                status_times=[(Instance.REQUESTED, 9 * s_to_ns)],
                launch_request_id="l1",
            ),
            create_instance(
                "retry",
                Instance.REQUESTED,
                status_times=[(Instance.REQUESTED, 2 * s_to_ns)],
                launch_request_id="l2",
            ),
            create_instance(
                "failed",
                Instance.REQUESTED,
                status_times=[
                    (Instance.REQUESTED, 1 * s_to_ns),
                    (Instance.REQUESTED, 2 * s_to_ns),  # This is a retry.
                ],
                launch_request_id="l3",
            ),
        ]

        TestReconciler._add_instances(instance_storage, instances)

        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "instance_reconcile_config": reconcile_config,
                    "max_concurrent_launches": 0,  # prevent launches
                }
            ),
        )

        instances, _ = instance_storage.get_instances()
        assert instances["no-update"].status == Instance.REQUESTED
        assert instances["retry"].status == Instance.QUEUED
        assert instances["failed"].status == Instance.ALLOCATION_FAILED

    @staticmethod
    @mock.patch("time.time_ns")
    def test_stuck_instances_ray_stop_requested(mock_time_ns, setup):
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup
        timeout_s = 5
        cur_time_s = 20
        mock_time_ns.return_value = cur_time_s * s_to_ns

        config = InstanceReconcileConfig(
            ray_stop_requested_status_timeout_s=timeout_s,
        )
        cur_status = Instance.RAY_STOP_REQUESTED

        instances = [
            create_instance(
                "no-update",
                cur_status,
                status_times=[(cur_status, (cur_time_s - timeout_s + 1) * s_to_ns)],
                ray_node_id=binary_to_hex(b"r-1"),
            ),
            create_instance(
                "updated",
                cur_status,
                status_times=[(cur_status, (cur_time_s - timeout_s - 1) * s_to_ns)],
                ray_node_id=binary_to_hex(b"r-2"),
            ),
        ]
        # Both nodes are still RUNNING in GCS, so the drain is treated as having
        # genuinely failed: the stuck handler must fall back to RAY_RUNNING.
        ray_nodes = [
            NodeState(node_id=b"r-1", status=NodeStatus.RUNNING),
            NodeState(node_id=b"r-2", status=NodeStatus.RUNNING),
        ]

        TestReconciler._add_instances(instance_storage, instances)

        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "instance_reconcile_config": config,
                    "max_concurrent_launches": 0,  # prevent launches
                }
            ),
        )

        instances, _ = instance_storage.get_instances()
        assert instances["no-update"].status == cur_status
        assert instances["updated"].status == Instance.RAY_RUNNING

    @staticmethod
    @pytest.mark.parametrize(
        "cur_status",
        [
            Instance.RAY_RUNNING,
            Instance.RAY_STOP_REQUESTED,
            Instance.RAY_STOPPING,
        ],
        ids=["ray_running", "ray_stop_requested", "ray_stopping"],
    )
    def test_missing_ray_node_marks_ray_stopped_and_terminates(cur_status, setup):
        # If an instance that should have a ray node points at a node_id that no
        # longer appears in GCS, the ray node has either been GC'd from the DEAD
        # list or is otherwise no longer alive. Reconcile it to RAY_STOPPED so it
        # can move through the normal termination path instead of lingering with a
        # stale node_id (especially after RAY_STOP_REQUESTED fell back to
        # RAY_RUNNING in a previous iteration).
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup

        instances = [
            create_instance(
                "updated",
                cur_status,
                ray_node_id=binary_to_hex(b"r-stale"),
                cloud_instance_id="c-stale",
            ),
        ]

        TestReconciler._add_instances(instance_storage, instances)

        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=[]),
            non_terminated_cloud_instances={
                "c-stale": CloudInstance(
                    cloud_instance_id="c-stale",
                    node_type="worker_nodes1",
                    node_kind=NodeKind.WORKER,
                    is_running=True,
                ),
            },
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={"max_concurrent_launches": 0}
            ),
        )

        events = subscriber.events
        assert len(events) == 2
        assert events[0].new_instance_status == Instance.RAY_STOPPED
        assert events[0].ray_node_id == binary_to_hex(b"r-stale")
        assert events[1].new_instance_status == Instance.TERMINATING

        instances, _ = instance_storage.get_instances()
        assert instances["updated"].status == Instance.TERMINATING

    @staticmethod
    def test_ray_stopped_shared_cloud_terminates_stale_record_only(setup):
        instance_manager, instance_storage, subscriber, _ = setup

        TestReconciler._add_instances(
            instance_storage,
            [
                create_instance(
                    "stale",
                    Instance.RAY_STOPPED,
                    ray_node_id=binary_to_hex(b"r-stale"),
                    cloud_instance_id="c-shared",
                ),
                create_instance(
                    "healthy",
                    Instance.RAY_RUNNING,
                    ray_node_id=binary_to_hex(b"r-live"),
                    cloud_instance_id="c-shared",
                ),
            ],
        )

        Reconciler._terminate_instances(instance_manager)

        assert [e.new_instance_status for e in subscriber.events_by_id("stale")] == [
            Instance.TERMINATED
        ]
        assert all(
            e.new_instance_status != Instance.TERMINATING for e in subscriber.events
        )

        instances, _ = instance_storage.get_instances()
        assert instances["stale"].status == Instance.TERMINATED
        assert instances["healthy"].status == Instance.RAY_RUNNING

    @staticmethod
    @mock.patch("time.time_ns")
    def test_ray_stop_requested_fail(mock_time_ns, setup):
        # Test that the instance should be transitioned to RAY_RUNNING
        # when the ray stop request fails.

        instance_manager, instance_storage, _, cloud_resource_monitor = setup
        mock_time_ns.return_value = 10 * s_to_ns

        instances = [
            create_instance(
                "i-1",
                status=Instance.RAY_STOP_REQUESTED,
                ray_node_id=binary_to_hex(b"r-1"),
                cloud_instance_id="c-1",
                status_times=[(Instance.RAY_STOP_REQUESTED, 10 * s_to_ns)],
            ),
            create_instance(
                "i-2",
                status=Instance.RAY_STOP_REQUESTED,
                ray_node_id=binary_to_hex(b"r-2"),
                cloud_instance_id="c-2",
                status_times=[(Instance.RAY_STOP_REQUESTED, 10 * s_to_ns)],
            ),
        ]

        ray_nodes = [
            NodeState(node_id=b"r-1", status=NodeStatus.RUNNING, instance_id="c-1"),
            NodeState(node_id=b"r-2", status=NodeStatus.RUNNING, instance_id="c-2"),
        ]

        ray_stop_errors = [
            RayStopError(im_instance_id="i-1"),
        ]

        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
            "c-2": CloudInstance("c-2", "type-2", True, NodeKind.WORKER),
        }

        TestReconciler._add_instances(instance_storage, instances)

        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            ray_stop_errors=ray_stop_errors,
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 2
        assert instances["i-1"].status == Instance.RAY_RUNNING
        assert instances["i-2"].status == Instance.RAY_STOP_REQUESTED

    @staticmethod
    @mock.patch("time.time_ns")
    @pytest.mark.parametrize(
        "cur_status,expect_status",
        [
            (Instance.ALLOCATED, Instance.TERMINATING),
            (Instance.RAY_INSTALLING, Instance.TERMINATING),
            (Instance.TERMINATING, Instance.TERMINATING),
        ],
    )
    def test_stuck_instances(mock_time_ns, cur_status, expect_status, setup):
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup
        timeout_s = 5
        cur_time_s = 20
        mock_time_ns.return_value = cur_time_s * s_to_ns
        config = InstanceReconcileConfig(
            allocate_status_timeout_s=timeout_s,
            terminating_status_timeout_s=timeout_s,
            ray_install_status_timeout_s=timeout_s,
        )
        instances = [
            create_instance(
                "no-update",
                cur_status,
                status_times=[(cur_status, (cur_time_s - timeout_s + 1) * s_to_ns)],
                instance_type="type-1",
                cloud_instance_id="c-1",
            ),
            create_instance(
                "updated",
                cur_status,
                status_times=[(cur_status, (cur_time_s - timeout_s - 1) * s_to_ns)],
                instance_type="type-1",
                cloud_instance_id="c-2",
            ),
        ]

        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
            "c-2": CloudInstance("c-2", "type-1", True, NodeKind.WORKER),
        }

        TestReconciler._add_instances(instance_storage, instances)

        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "instance_reconcile_config": config,
                    "max_concurrent_launches": 0,  # prevent launches
                }
            ),
        )

        instances, _ = instance_storage.get_instances()
        assert instances["no-update"].status == cur_status
        assert instances["updated"].status == expect_status

    @staticmethod
    @mock.patch("time.time_ns")
    @pytest.mark.parametrize(
        "status",
        [
            Instance.InstanceStatus.Name(Instance.RAY_STOPPING),
            Instance.InstanceStatus.Name(Instance.QUEUED),
        ],
    )
    def test_warn_stuck_transient_instances(mock_time_ns, status, setup):
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup
        cur_time_s = 10
        mock_time_ns.return_value = cur_time_s * s_to_ns
        timeout_s = 5
        status = Instance.InstanceStatus.Value(status)

        config = InstanceReconcileConfig(
            transient_status_warn_interval_s=timeout_s,
        )
        # TODO: we are only mocking the necessary fields for testing.
        # Probably better to have a more comprehensive mocker for instance
        # of various statuses.
        instances = [
            create_instance(
                "no-warn",
                status,
                status_times=[(status, (cur_time_s - timeout_s + 1) * s_to_ns)],
                cloud_instance_id="c-1" if status != Instance.QUEUED else "",
                instance_type="type-1",
            ),
            create_instance(
                "warn",
                status,
                status_times=[(status, (cur_time_s - timeout_s - 1) * s_to_ns)],
                cloud_instance_id="c-2" if status != Instance.QUEUED else "",
                instance_type="type-1",
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)
        mock_logger = mock.MagicMock()

        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
            "c-2": CloudInstance("c-2", "type-1", True, NodeKind.WORKER),
        }

        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances=cloud_instances
            if status != Instance.QUEUED
            else {},
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "instance_reconcile_config": config,
                    "max_concurrent_launches": 0,  # prevent launches
                }
            ),
            _logger=mock_logger,
        )

        assert mock_logger.warning.call_count == 1

    @staticmethod
    @mock.patch("time.time_ns")
    def test_stuck_instances_no_op(mock_time_ns, setup):
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup
        # Large enough to not trigger any timeouts
        mock_time_ns.return_value = 999999 * s_to_ns

        config = InstanceReconcileConfig()

        all_status = set(Instance.InstanceStatus.values())
        reconciled_stuck_statuses = {
            Instance.REQUESTED,
            Instance.ALLOCATED,
            Instance.RAY_INSTALLING,
            Instance.TERMINATING,
            Instance.RAY_STOP_REQUESTED,
        }

        transient_statuses = {
            Instance.RAY_STOPPING,
            Instance.RAY_INSTALL_FAILED,
            Instance.RAY_STOPPED,
            Instance.TERMINATION_FAILED,
            Instance.QUEUED,
            Instance.ALLOCATION_TIMEOUT,
        }
        no_op_statuses = all_status - reconciled_stuck_statuses - transient_statuses

        for status in no_op_statuses:
            instances = [
                create_instance(
                    f"no-op-{status}",
                    status,
                    status_times=[(status, 1 * s_to_ns)],
                ),
            ]
            TestReconciler._add_instances(instance_storage, instances)

        subscriber.clear()
        mock_logger = mock.MagicMock()
        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "instance_reconcile_config": config,
                    "max_concurrent_launches": 0,  # prevent launches
                }
            ),
            _logger=mock_logger,
        )

        assert subscriber.events == []
        assert mock_logger.warning.call_count == 0

    @staticmethod
    @pytest.mark.parametrize(
        "status,expected_running",
        [(Instance.RAY_RUNNING, True), (Instance.ALLOCATED, False)],
        ids=["ray_running", "allocated"],
    )
    def test_is_head_node_running(status, expected_running, setup):
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup

        instances = [
            create_instance(
                "i-1",
                status=status,
                cloud_instance_id="c-1",
                node_kind=NodeKind.HEAD,
            ),
        ]

        TestReconciler._add_instances(instance_storage, instances)
        assert Reconciler._is_head_node_running(instance_manager) is expected_running

    @staticmethod
    def test_scaling_updates(setup):
        """
        Tests that new instances should be launched due to autoscaling
        decisions, and existing instances should be terminated if needed.
        """
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        im_instances = [
            create_instance(
                "head",
                status=Instance.RAY_RUNNING,
                cloud_instance_id="c-0",
                ray_node_id=binary_to_hex(b"r-0"),
                node_kind=NodeKind.HEAD,
            ),
            create_instance(
                "i-1",
                status=Instance.RAY_RUNNING,
                cloud_instance_id="c-1",
                ray_node_id=binary_to_hex(b"r-1"),
                node_kind=NodeKind.WORKER,
            ),  # To be reconciled.
        ]
        TestReconciler._add_instances(instance_storage, im_instances)

        ray_nodes = [
            NodeState(node_id=b"r-0", status=NodeStatus.RUNNING, instance_id="c-1"),
            NodeState(node_id=b"r-1", status=NodeStatus.RUNNING, instance_id="c-1"),
        ]

        cloud_instances = {
            "c-0": CloudInstance("c-0", "head", True, NodeKind.HEAD),
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
        }

        mock_scheduler = MagicMock()
        mock_scheduler.schedule.return_value = SchedulingReply(
            to_launch=[
                LaunchRequest(instance_type="type-1", count=2),
            ],
            to_terminate=[
                TerminationRequest(
                    id="t1",
                    ray_node_id="r-1",
                    instance_id="i-1",
                    cause=TerminationRequest.Cause.IDLE,
                    idle_duration_ms=1000,
                )
            ],
            infeasible_gang_resource_requests=[
                GangResourceRequest(
                    requests=[ResourceRequest(resources_bundle={"CPU": 1})]
                )
            ],
        )

        state = Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=mock_scheduler,
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "max_concurrent_launches": 0,  # don't launch anything.
                }
            ),
        )

        instances, _ = instance_storage.get_instances()

        assert len(instances) == 3 + 1  # for head node
        for id, instance in instances.items():
            if id == "head":
                assert instance.status == Instance.RAY_RUNNING
            elif id == "i-1":
                assert instance.status == Instance.RAY_STOP_REQUESTED
            else:
                assert instance.status == Instance.QUEUED
                assert instance.instance_type == "type-1"

        assert len(state.infeasible_gang_resource_requests) == 1

    @staticmethod
    def test_terminating_instances(setup):
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup

        instances = [
            create_instance(
                "i-1",
                status=Instance.RAY_STOPPED,
                cloud_instance_id="c-1",
            ),
            create_instance(
                "i-2",
                status=Instance.RAY_INSTALL_FAILED,
                cloud_instance_id="c-2",
            ),
            create_instance(
                "i-3",
                status=Instance.TERMINATION_FAILED,
                cloud_instance_id="c-3",
            ),
        ]

        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
            "c-2": CloudInstance("c-2", "type-2", True, NodeKind.WORKER),
            "c-3": CloudInstance("c-3", "type-3", True, NodeKind.WORKER),
        }

        TestReconciler._add_instances(instance_storage, instances)

        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert instances["i-1"].status == Instance.TERMINATING
        assert instances["i-2"].status == Instance.TERMINATING
        assert instances["i-3"].status == Instance.TERMINATING

    @staticmethod
    @pytest.mark.parametrize(
        "disable_node_updaters",
        [True, False],
    )
    @pytest.mark.parametrize(
        "cloud_instance_running",
        [True, False],
    )
    def test_ray_install(disable_node_updaters, cloud_instance_running, setup):
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        instances = [
            create_instance(
                "i-1",
                status=Instance.ALLOCATED,
                instance_type="type-1",
                launch_request_id="l1",
                cloud_instance_id="c-1",
            ),
        ]

        cloud_instances = {
            "c-1": CloudInstance(
                "c-1", "type-1", "", cloud_instance_running, NodeKind.WORKER
            ),
        }

        TestReconciler._add_instances(instance_storage, instances)

        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "disable_node_updaters": disable_node_updaters,
                }
            ),
        )

        instances, _ = instance_storage.get_instances()
        if disable_node_updaters or not cloud_instance_running:
            assert instances["i-1"].status == Instance.ALLOCATED
        else:
            assert instances["i-1"].status == Instance.RAY_INSTALLING

    @staticmethod
    @mock.patch("time.time_ns")
    def test_autoscaler_state(mock_time_ns, setup):
        instance_manager, instance_storage, _, cloud_resource_monitor = setup
        mock_time_ns.return_value = 5

        instances = [
            create_instance(
                "head",
                status=Instance.RAY_RUNNING,
                cloud_instance_id="c-0",
                ray_node_id=binary_to_hex(b"r-0"),
                node_kind=NodeKind.HEAD,
            ),
            create_instance(
                "i-1",
                status=Instance.ALLOCATED,
                instance_type="type-1",
                cloud_instance_id="c-1",
                launch_request_id="l1",
                status_times=[
                    (Instance.QUEUED, 0),
                    (Instance.REQUESTED, 1),
                    (Instance.ALLOCATED, 2),
                ],
            ),
            # requested instance
            create_instance(
                "i-2",
                status=Instance.REQUESTED,
                instance_type="type-2",
                launch_request_id="l2",
                status_times=[(Instance.QUEUED, 0), (Instance.REQUESTED, 1)],
            ),
            # queued instance
            create_instance(
                "i-3",
                status=Instance.QUEUED,
                instance_type="type-3",
                launch_request_id="l3",
                status_times=[
                    (Instance.QUEUED, 0),
                ],
            ),
            # allocation failed
            create_instance(
                "i-4",
                status=Instance.ALLOCATION_FAILED,
                instance_type="type-4",
                launch_request_id="l4",
                status_times=[
                    (Instance.QUEUED, 0),
                    (Instance.REQUESTED, 1),
                    (Instance.ALLOCATION_FAILED, 2),
                ],
            ),
            # ray installing
            create_instance(
                "i-5",
                status=Instance.RAY_INSTALLING,
                instance_type="type-5",
                launch_request_id="l5",
                cloud_instance_id="c-5",
                status_times=[
                    (Instance.QUEUED, 0),
                    (Instance.REQUESTED, 1),
                    (Instance.ALLOCATED, 2),
                    (Instance.RAY_INSTALLING, 3),
                ],
            ),
        ]

        cloud_instances = {
            "c-0": CloudInstance("c-0", "head", True, NodeKind.HEAD),
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
            "c-5": CloudInstance("c-5", "type-5", True, NodeKind.WORKER),
        }

        TestReconciler._add_instances(instance_storage, instances)

        mock_scheduler = MagicMock()
        mock_scheduler.schedule.return_value = SchedulingReply(
            to_launch=[],
            to_terminate=[],
            infeasible_gang_resource_requests=[
                GangResourceRequest(
                    requests=[ResourceRequest(resources_bundle={"CPU": 1})]
                )
            ],
            infeasible_resource_requests=[ResourceRequest(resources_bundle={"CPU": 1})],
        )

        autoscaling_state = Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=mock_scheduler,
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(
                cluster_resource_state_version=1,
            ),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "max_concurrent_launches": 0,  # don't launch anything.
                    "provider_instance_types": {
                        "type-1": "m5.large",
                        "type-2": "m5.xlarge",
                        "type-3": "c5.large",
                        "type-4": "g5.xlarge",
                        "type-5": "r5.large",
                    },
                }
            ),
        )

        assert autoscaling_state.last_seen_cluster_resource_state_version == 1
        assert len(autoscaling_state.infeasible_gang_resource_requests) == 1
        assert len(autoscaling_state.infeasible_resource_requests) == 1
        assert len(autoscaling_state.pending_instances) == 2
        pending_instances = {
            (i.instance_id, i.instance_type_name, i.ray_node_type_name)
            for i in autoscaling_state.pending_instances
        }
        assert pending_instances == {
            ("i-1", "m5.large", "type-1"),
            ("i-5", "r5.large", "type-5"),
        }
        pending_instance_requests = defaultdict(int)
        for r in autoscaling_state.pending_instance_requests:
            pending_instance_requests[
                (r.instance_type_name, r.ray_node_type_name)
            ] += r.count
        failed_instance_requests = defaultdict(int)
        for r in autoscaling_state.failed_instance_requests:
            failed_instance_requests[
                (r.instance_type_name, r.ray_node_type_name)
            ] += r.count
        assert pending_instance_requests == {
            ("m5.xlarge", "type-2"): 1,
            ("c5.large", "type-3"): 1,
        }
        assert failed_instance_requests == {("g5.xlarge", "type-4"): 1}

    @staticmethod
    def test_extra_cloud_instances_cloud_provider(setup):
        """
        Test that extra cloud instances should be terminated.
        """
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup

        im_instances = [
            create_instance(
                "i-1", status=Instance.RAY_RUNNING, cloud_instance_id="c-1"
            ),  # To be reconciled.
        ]
        TestReconciler._add_instances(instance_storage, im_instances)

        ray_nodes = [
            NodeState(node_id=b"r-1", status=NodeStatus.RUNNING, instance_id="c-1"),
            # Out of band ray nodes.
            NodeState(
                node_id=b"r-2",
                status=NodeStatus.RUNNING,
                instance_id="c-3",
                ray_node_type_name="type-1",
            ),
        ]

        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
            # Out of band cloud instance.
            "c-2": CloudInstance("c-2", "type-2", True, NodeKind.WORKER),
        }

        subscriber.clear()
        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )
        events = subscriber.events
        for e in events:
            if e.new_instance_status == Instance.ALLOCATED:
                assert e.cloud_instance_id in {"c-2", "c-3"}
            else:
                assert e.new_instance_status == Instance.RAY_RUNNING
                assert e.ray_node_id == binary_to_hex(b"r-2")

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 3
        statuses = {instance.status for instance in instances.values()}
        assert statuses == {Instance.RAY_RUNNING, Instance.ALLOCATED}

    @staticmethod
    def test_cloud_instance_reboot(setup):
        """
        Test that the case of booting up a previous stopped cloud instance.
        """
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup

        im_instances = [
            create_instance(
                "i-1",
                status=Instance.TERMINATED,
                cloud_instance_id="c-1",
                ray_node_id=binary_to_hex(b"r-1"),
            ),
        ]
        TestReconciler._add_instances(instance_storage, im_instances)

        ray_nodes = [
            NodeState(
                node_id=b"r-1",
                status=NodeStatus.DEAD,
                instance_id="c-1",
                ray_node_type_name="type-1",
            ),
        ]

        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
        }

        subscriber.clear()
        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )
        events = subscriber.events
        for e in events:
            assert e.new_instance_status == Instance.ALLOCATED
            assert e.cloud_instance_id == "c-1"

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 2
        statuses = {instance.status for instance in instances.values()}
        assert statuses == {Instance.ALLOCATED, Instance.TERMINATED}

    @staticmethod
    def test_ray_node_restarted_on_the_same_cloud_instance(setup):
        """
        Test that the case of reusing cloud instances.
        """
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup

        im_instances = [
            create_instance(
                "i-1",
                status=Instance.RAY_RUNNING,
                cloud_instance_id="c-1",
                ray_node_id=binary_to_hex(b"r-1"),
            ),
        ]
        TestReconciler._add_instances(instance_storage, im_instances)

        ray_nodes = [
            NodeState(
                node_id=b"r-2",
                status=NodeStatus.IDLE,
                instance_id="c-1",
                ray_node_type_name="type-1",
            ),
            NodeState(
                node_id=b"r-1",
                status=NodeStatus.DEAD,
                instance_id="c-1",
                ray_node_type_name="type-1",
            ),
        ]

        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
        }

        subscriber.clear()
        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )
        events = subscriber.events
        assert len(events) == 4
        assert events[0].new_instance_status == Instance.ALLOCATED
        assert events[0].cloud_instance_id == "c-1"
        assert events[0].ray_node_id == binary_to_hex(b"r-2")

        assert events[1].new_instance_status == Instance.RAY_RUNNING
        assert events[1].instance_id == events[0].instance_id
        assert events[1].ray_node_id == binary_to_hex(b"r-2")

        assert events[2].new_instance_status == Instance.RAY_STOPPED
        assert events[2].instance_id == "i-1"
        assert events[2].ray_node_id == binary_to_hex(b"r-1")
        # c-1 has been reused by the new ray node r-2 (now RAY_RUNNING), so the
        # stale i-1 row must not terminate c-1. It is marked TERMINATED to clean
        # up the record without issuing a cloud termination request.
        assert events[3].new_instance_status == Instance.TERMINATED
        assert events[3].instance_id == "i-1"

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 2
        statuses = {instance.status for instance in instances.values()}
        assert statuses == {Instance.RAY_RUNNING, Instance.TERMINATED}

    @staticmethod
    def test_ray_head_restarted_on_the_same_cloud_instance(setup):
        """
        Test that the case of restarting Head node with GCS FT.
        """
        instance_manager, instance_storage, subscriber, cloud_resource_monitor = setup

        ray_nodes = [
            NodeState(
                node_id=b"r-2",
                status=NodeStatus.IDLE,
                instance_id="c-1",
                ray_node_type_name="type-1",
            ),
            NodeState(
                node_id=b"r-1",
                status=NodeStatus.DEAD,
                instance_id="c-1",
                ray_node_type_name="type-1",
            ),
        ]

        cloud_instances = {
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.HEAD),
        }

        subscriber.clear()
        Reconciler.reconcile(
            instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )
        events = subscriber.events
        assert len(events) == 5
        assert events[0].new_instance_status == Instance.ALLOCATED
        assert events[0].cloud_instance_id == "c-1"
        assert events[0].ray_node_id == binary_to_hex(b"r-2")

        assert events[1].new_instance_status == Instance.ALLOCATED
        assert events[1].cloud_instance_id == "c-1"
        assert events[1].ray_node_id == binary_to_hex(b"r-1")

        assert events[1].instance_id != events[0].instance_id

        assert events[2].new_instance_status == Instance.RAY_RUNNING
        assert events[2].instance_id == events[0].instance_id
        assert events[2].ray_node_id == binary_to_hex(b"r-2")

        assert events[3].new_instance_status == Instance.RAY_STOPPED
        assert events[3].instance_id == events[1].instance_id
        assert events[3].ray_node_id == binary_to_hex(b"r-1")

        # c-1 has been reused by the new ray node r-2 (now RAY_RUNNING), so the
        # stale row must not terminate c-1. It is marked TERMINATED to clean up
        # the record without issuing a cloud termination request.
        assert events[4].new_instance_status == Instance.TERMINATED
        assert events[4].instance_id == events[1].instance_id

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 2
        statuses = {instance.status for instance in instances.values()}
        assert statuses == {Instance.RAY_RUNNING, Instance.TERMINATED}

    @staticmethod
    def test_reconcile_max_worker_nodes_limit_triggers_termination(setup):
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        instances = [
            create_instance(
                "head",
                status=Instance.RAY_RUNNING,
                node_kind=NodeKind.HEAD,
                cloud_instance_id="c-head",
                ray_node_id=binary_to_hex(b"r-head"),
            ),
            create_instance(
                "i-0",
                status=Instance.ALLOCATED,
                instance_type="type-1",
                cloud_instance_id="c-0",
                ray_node_id=binary_to_hex(b"r-0"),
            ),
            create_instance(
                "i-1",
                status=Instance.ALLOCATED,
                instance_type="type-1",
                cloud_instance_id="c-1",
                ray_node_id=binary_to_hex(b"r-1"),
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

        # Empty list of Ray nodes - i.e. when instances are pending but not scheduled
        ray_nodes = []

        # Cloud instances corresponding to the 3 IM instances
        cloud_instances = {
            "c-head": CloudInstance("c-head", "head", True, NodeKind.HEAD),
            "c-0": CloudInstance("c-0", "type-1", True, NodeKind.WORKER),
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
        }

        # Scheduler should add both workers to to_terminate due to max nodes
        mock_scheduler = MockScheduler(
            to_launch=[],
            to_terminate=[
                TerminationRequest(
                    id="t0",
                    ray_node_id="r-0",
                    instance_id="i-0",
                    instance_status=Instance.ALLOCATED,
                    cause=TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
                ),
                TerminationRequest(
                    id="t1",
                    ray_node_id="r-1",
                    instance_id="i-1",
                    instance_status=Instance.ALLOCATED,
                    cause=TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
                ),
            ],
        )

        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=mock_scheduler,
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(
                node_states=ray_nodes,
                cluster_resource_state_version=1,
            ),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "node_type_configs": {
                        "type-1": {
                            "name": "type-1",
                            "resources": {"CPU": 1},
                            "min_worker_nodes": 0,
                            "max_worker_nodes": 0,
                        }
                    },
                }
            ),
        )

        instances, _ = instance_storage.get_instances()

        assert instances["i-0"].status == Instance.TERMINATING
        assert instances["i-1"].status == Instance.TERMINATING

    @staticmethod
    def test_terminate_queued_instances_max_workers(setup):
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        instances = [
            create_instance(
                "head",
                status=Instance.RAY_RUNNING,
                node_kind=NodeKind.HEAD,
                cloud_instance_id="c-head",
                ray_node_id=binary_to_hex(b"r-head"),
            ),
            create_instance(
                "i-0",
                status=Instance.QUEUED,
                instance_type="type-1",
            ),
            create_instance(
                "i-1",
                status=Instance.QUEUED,
                instance_type="type-1",
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

        # Empty list of Ray nodes - QUEUED instances have not launched ray yet
        ray_nodes = []

        # Cloud instance for head node only
        cloud_instances = {
            "c-head": CloudInstance("c-head", "head", True, NodeKind.HEAD),
        }

        # Scheduler should terminate QUEUED instances due to max_workers limit
        mock_scheduler = MockScheduler(
            to_launch=[],
            to_terminate=[
                TerminationRequest(
                    id="t0",
                    instance_id="i-0",
                    instance_status=Instance.QUEUED,
                    cause=TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
                ),
                TerminationRequest(
                    id="t1",
                    instance_id="i-1",
                    instance_status=Instance.QUEUED,
                    cause=TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
                ),
            ],
        )

        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=mock_scheduler,
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(
                node_states=ray_nodes,
                cluster_resource_state_version=1,
            ),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "node_type_configs": {
                        "type-1": {
                            "name": "type-1",
                            "resources": {"CPU": 1},
                            "min_worker_nodes": 0,
                            "max_worker_nodes": 0,
                        }
                    },
                }
            ),
        )

        instances, _ = instance_storage.get_instances()

        assert instances["i-0"].status == Instance.TERMINATED
        assert instances["i-1"].status == Instance.TERMINATED

    @staticmethod
    def test_terminate_ray_installing_instances_max_workers(setup):
        instance_manager, instance_storage, _, cloud_resource_monitor = setup

        instances = [
            create_instance(
                "head",
                status=Instance.RAY_RUNNING,
                node_kind=NodeKind.HEAD,
                cloud_instance_id="c-head",
                ray_node_id=binary_to_hex(b"r-head"),
            ),
            create_instance(
                "i-0",
                status=Instance.RAY_INSTALLING,
                instance_type="type-1",
                cloud_instance_id="c-0",
            ),
            create_instance(
                "i-1",
                status=Instance.RAY_INSTALLING,
                instance_type="type-1",
                cloud_instance_id="c-1",
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

        # Empty list of Ray nodes - RAY_INSTALLING instances have not started ray yet
        ray_nodes = []

        # Cloud instances for head and installing workers
        cloud_instances = {
            "c-head": CloudInstance("c-head", "head", True, NodeKind.HEAD),
            "c-0": CloudInstance("c-0", "type-1", True, NodeKind.WORKER),
            "c-1": CloudInstance("c-1", "type-1", True, NodeKind.WORKER),
        }

        # Scheduler should terminate RAY_INSTALLING instances due to max_workers limit
        mock_scheduler = MockScheduler(
            to_launch=[],
            to_terminate=[
                TerminationRequest(
                    id="t0",
                    instance_id="i-0",
                    instance_status=Instance.RAY_INSTALLING,
                    cause=TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
                ),
                TerminationRequest(
                    id="t1",
                    instance_id="i-1",
                    instance_status=Instance.RAY_INSTALLING,
                    cause=TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE,
                ),
            ],
        )

        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=mock_scheduler,
            cloud_provider=MagicMock(),
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ClusterResourceState(
                node_states=ray_nodes,
                cluster_resource_state_version=1,
            ),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "node_type_configs": {
                        "type-1": {
                            "name": "type-1",
                            "resources": {"CPU": 1},
                            "min_worker_nodes": 0,
                            "max_worker_nodes": 0,
                        }
                    },
                }
            ),
        )

        instances, _ = instance_storage.get_instances()

        assert instances["i-0"].status == Instance.TERMINATING
        assert instances["i-1"].status == Instance.TERMINATING

    @staticmethod
    def _replica_nt_cfg(
        name: str,
        max_w: int,
        workers_per_replica: Optional[int],
        resources: Optional[dict] = None,
    ) -> NodeTypeConfig:
        return NodeTypeConfig(
            name=name,
            resources=resources or {"CPU": 8, "GPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=max_w,
            replica_config=(
                WorkerReplicaConfig(workers_per_replica=workers_per_replica)
                if workers_per_replica is not None
                else None
            ),
        )

    @staticmethod
    def _run_launch_tick(
        node_type_configs,
        queued_ids_by_type,
        requested_ids_by_type=None,
        upscaling_speed=1.0,
        max_concurrent_launches=10,
        queued_items=None,
    ):
        storage = InstanceStorage(
            cluster_id="tpu_launch_test", storage=InMemoryStorage()
        )
        sub = MockSubscriber()
        im = InstanceManager(
            instance_storage=storage, instance_status_update_subscribers=[sub]
        )
        if queued_items is not None:
            for nt, iid, ts in queued_items:
                TestReconciler._add_instances(
                    storage,
                    [
                        create_instance(
                            iid,
                            status=Instance.QUEUED,
                            instance_type=nt,
                            status_times=[(Instance.QUEUED, ts)],
                        )
                    ],
                )
        else:
            t = 1
            for nt, ids in queued_ids_by_type.items():
                for iid in ids:
                    TestReconciler._add_instances(
                        storage,
                        [
                            create_instance(
                                iid,
                                status=Instance.QUEUED,
                                instance_type=nt,
                                status_times=[(Instance.QUEUED, t)],
                            )
                        ],
                    )
                    t += 1
        for nt, ids in (requested_ids_by_type or {}).items():
            for iid in ids:
                TestReconciler._add_instances(
                    storage,
                    [
                        create_instance(
                            iid,
                            status=Instance.REQUESTED,
                            instance_type=nt,
                            launch_request_id="in-flight",
                            status_times=[(Instance.REQUESTED, time.time_ns())],
                        )
                    ],
                )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "node_type_configs": node_type_configs,
                    "upscaling_speed": upscaling_speed,
                    "max_concurrent_launches": max_concurrent_launches,
                }
            ),
        )
        instances, _ = storage.get_instances()
        return storage, im, sub, instances

    def test_multihost_replica_launch_batching_bounds_and_fifo(self):
        """Multi-worker replica groups launch whole replicas atomically without
        starving behind or blocking single-host types."""
        cpu_cfg = NodeTypeConfig(
            name="cpu",
            resources={"CPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=8,
        )
        v6e_64 = {"tpu64": self._replica_nt_cfg("tpu64", 32, 16)}
        v6e_16 = {"tpu16": self._replica_nt_cfg("tpu16", 16, 4)}

        # 16-host slice exceeds default max_concurrent_launches=10: launches one
        # whole 16-host slice when capacity remains and tpu64 has no in-flight
        # REQUESTED pods, even when unrelated CPU launches are in flight.
        _, _, sub, _ = self._run_launch_tick(
            v6e_64, {"tpu64": [f"q{i}" for i in range(16)]}
        )
        assert [e.instance_id for e in sub.events] == [f"q{i}" for i in range(16)]
        _, _, sub_with_cpu_req, _ = self._run_launch_tick(
            {**v6e_64, "cpu": cpu_cfg},
            {"tpu64": [f"q{i}" for i in range(16)]},
            requested_ids_by_type={"cpu": [f"r{i}" for i in range(4)]},
        )
        assert [e.instance_id for e in sub_with_cpu_req.events] == [
            f"q{i}" for i in range(16)
        ]

        # Negative bounds: max_concurrent_launches=0, fully saturated cap (10/10),
        # or an already in-flight tpu64 batch waits rather than stacking above cap.
        _, _, sub0, _ = self._run_launch_tick(
            v6e_64,
            {"tpu64": [f"q{i}" for i in range(16)]},
            max_concurrent_launches=0,
        )
        assert sub0.events == []
        _, _, sub_sat, _ = self._run_launch_tick(
            {**v6e_64, "cpu": cpu_cfg},
            {"tpu64": [f"q{i}" for i in range(16)]},
            requested_ids_by_type={"cpu": [f"r{i}" for i in range(10)]},
        )
        assert sub_sat.events == []
        _, _, sub_same_type_inflight, _ = self._run_launch_tick(
            v6e_64,
            {"tpu64": [f"q{i}" for i in range(16)]},
            requested_ids_by_type={"tpu64": [f"r{i}" for i in range(4)]},
        )
        assert sub_same_type_inflight.events == []

        # Cross-type FIFO order invariance: co-queued CPU + multi-host TPU produce
        # identical launches regardless of storage iteration order, and later CPU
        # arrivals never starve an earlier 4-host slice behind 8 CPU pods.
        t64_late = [("tpu64", f"t{i:02d}", 10 + i) for i in range(16)]
        for items in ([("cpu", "c0", 1), *t64_late], [*t64_late, ("cpu", "c0", 1)]):
            _, _, sub_co, _ = self._run_launch_tick(
                {**v6e_64, "cpu": cpu_cfg},
                {},
                upscaling_speed=100.0,
                queued_items=items,
            )
            assert [e.instance_id for e in sub_co.events] == [
                "c0",
                *(f"t{i:02d}" for i in range(16)),
            ]
        t64_early = [("tpu64", f"t{i:02d}", 1 + i) for i in range(16)]
        for items in ([("cpu", "c0", 50), *t64_early], [*t64_early, ("cpu", "c0", 50)]):
            _, _, sub_tpu_first, _ = self._run_launch_tick(
                {**v6e_64, "cpu": cpu_cfg},
                {},
                upscaling_speed=100.0,
                queued_items=items,
            )
            assert [e.instance_id for e in sub_tpu_first.events] == [
                f"t{i:02d}" for i in range(16)
            ]
        t16_early = [("tpu16", f"t{i}", 10 + i) for i in range(4)]
        cpu_late = [("cpu", f"cn{i}", 100 + i) for i in range(5)]
        for items in ([*cpu_late, *t16_early], [*t16_early, *cpu_late]):
            _, _, sub_livelock, _ = self._run_launch_tick(
                {**v6e_16, "cpu": cpu_cfg},
                {},
                requested_ids_by_type={"cpu": [f"cr{i}" for i in range(8)]},
                upscaling_speed=100.0,
                queued_items=items,
            )
            assert [e.instance_id for e in sub_livelock.events] == [
                f"t{i}" for i in range(4)
            ]

        # Single-instance node types (replica_config=None) do not apply multi-host
        # replica multiplication or rounding.
        gcp_tpu = {
            "gcp_v4_16": self._replica_nt_cfg(
                "gcp_v4_16",
                1,
                None,
                resources={"CPU": 8, "TPU": 4, "TPU-v4-16-head": 1},
            )
        }
        _, _, sub_gcp, _ = self._run_launch_tick(
            gcp_tpu,
            {"gcp_v4_16": ["gcp-pod-0", "gcp-pod-1"]},
            upscaling_speed=0.0,
        )
        assert [e.instance_id for e in sub_gcp.events] == ["gcp-pod-0"]

        gcp_storage = InstanceStorage(
            cluster_id="gcp_scale_test", storage=InMemoryStorage()
        )
        gcp_sub = MockSubscriber()
        gcp_im = InstanceManager(
            instance_storage=gcp_storage, instance_status_update_subscribers=[gcp_sub]
        )
        TestReconciler._add_instances(
            gcp_storage,
            [
                create_instance(
                    "head-0",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    ray_node_id=b"h".ljust(28, b"\0").hex(),
                )
            ],
        )
        Reconciler.reconcile(
            instance_manager=gcp_im,
            scheduler=ResourceDemandScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=ClusterResourceState(
                node_states=[
                    NodeState(
                        node_id=b"h".ljust(28, b"\0"),
                        ray_node_type_name="head",
                        status=NodeStatus.RUNNING,
                    )
                ],
                pending_resource_requests=[
                    ResourceRequestByCount(
                        request=ResourceRequestUtil.make({"TPU-v4-16-head": 1}),
                        count=1,
                    )
                ],
            ),
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "node_type_configs": {
                        **gcp_tpu,
                        "head": NodeTypeConfig(
                            name="head",
                            resources={},
                            min_worker_nodes=0,
                            max_worker_nodes=1,
                        ),
                    },
                    "provider": Provider.GCP,
                    "disable_launch_config_check": True,
                }
            ),
        )
        gcp_insts, _ = gcp_storage.get_instances()
        assert [
            (i.instance_type, i.status)
            for i in gcp_insts.values()
            if i.node_kind != NodeKind.HEAD
        ] == [("gcp_v4_16", Instance.REQUESTED)]

    def test_multihost_replica_launch_batching_alignment_and_budget(self):
        """Launches stay whole-replica aligned across ticks and enforce global
        concurrency and per-type upscaling budgets without double-counting."""
        v6e_16 = {"tpu16": self._replica_nt_cfg("tpu16", 16, 4)}

        # 3 x 4-host slices (12 QUEUED) with max_concurrent_launches=10: Tick 1
        # launches 8 (2 whole replicas), leaving 4 QUEUED for Tick 2.
        storage, im, sub1, instances = self._run_launch_tick(
            v6e_16,
            {"tpu16": [f"q{i}" for i in range(12)]},
            upscaling_speed=1000.0,
            max_concurrent_launches=10,
        )
        assert len(sub1.events) == 8
        assert sum(i.status == Instance.QUEUED for i in instances.values()) == 4
        cloud_8 = {
            f"pod-{idx}": CloudInstance(f"pod-{idx}", "tpu16", NodeKind.WORKER, True)
            for idx in range(len(sub1.events))
        }
        sub1.clear()
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances=cloud_8,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "node_type_configs": v6e_16,
                    "upscaling_speed": 1000.0,
                    "max_concurrent_launches": 10,
                }
            ),
        )
        instances2, _ = storage.get_instances()
        assert sum(i.status == Instance.ALLOCATED for i in instances2.values()) == 8
        assert sum(i.status == Instance.REQUESTED for i in instances2.values()) == 4
        assert sum(i.status == Instance.QUEUED for i in instances2.values()) == 0

        # Conservative mode (upscaling_speed=0) launches a full 4-host slice, not 1.
        _, _, sub_cons, _ = self._run_launch_tick(
            v6e_16,
            {"tpu16": [f"q{i}" for i in range(4)]},
            upscaling_speed=0.0,
            max_concurrent_launches=10,
        )
        assert len(sub_cons.events) == 4

        # Non-multiple QUEUED counts never split a multi-host replica or strand a
        # fractional REQUESTED instance.
        def _q(iid, ts, nt="tpu16"):
            return create_instance(
                iid,
                status=Instance.QUEUED,
                instance_type=nt,
                status_times=[(Instance.QUEUED, ts)],
            )

        r0_and_strays = [_q(f"r0-{i}", 1 + i) for i in range(4)] + [
            _q(f"s-{i}", 10 + i) for i in range(2)
        ]
        for max_c in (100, 5):
            assert [
                i.instance_id
                for i in Reconciler._compute_to_launch(
                    r0_and_strays, [], [], 1.0, max_c, {"tpu16": 4}
                )["tpu16"]
            ] == [f"r0-{i}" for i in range(4)]
        stray_first = [_q("stray", 1)] + [_q(f"r0-{i}", 5 + i) for i in range(4)]
        assert (
            len(
                Reconciler._compute_to_launch(
                    stray_first, [], [], 1.0, 4, {"tpu16": 4}
                )["tpu16"]
            )
            == 4
        )
        req_old = create_instance(
            "old",
            status=Instance.REQUESTED,
            instance_type="tpu16",
            launch_request_id="req-old",
            status_times=[(Instance.QUEUED, 1), (Instance.REQUESTED, 2)],
        )
        assert (
            Reconciler._compute_to_launch(
                r0_and_strays[:4], [req_old], [], 1.0, 2, {"tpu16": 4}
            )
            == {}
        )
        assert (
            len(
                Reconciler._compute_to_launch(
                    r0_and_strays[:4], [], [], 1.0, 2, {"tpu16": 4}
                )["tpu16"]
            )
            == 4
        )
        cpu_then_tpu = [_q(f"c{i}", 1 + i, "cpu") for i in range(4)] + [
            _q(f"t{i}", 10 + i) for i in range(4)
        ]
        res_ct = Reconciler._compute_to_launch(
            cpu_then_tpu, [], [], 4.0, 4, {"tpu16": 4}
        )
        assert [i.instance_id for i in res_ct["cpu"]] == [f"c{i}" for i in range(4)]
        assert "tpu16" not in res_ct

        # Only one multi-worker atomic floor launch may exceed max_concurrent_launches
        # in a single tick: once tpu_a consumes the remaining budget, tpu_b waits.
        two_types = [
            *(_q(f"a-{i}", 1 + i, "tpu_a") for i in range(4)),
            *(_q(f"b-{i}", 10 + i, "tpu_b") for i in range(4)),
        ]
        res_two = Reconciler._compute_to_launch(
            two_types, [], [], 1.0, 3, {"tpu_a": 4, "tpu_b": 4}
        )
        assert [i.instance_id for i in res_two["tpu_a"]] == [f"a-{i}" for i in range(4)]
        assert "tpu_b" not in res_two

        # Per-type upscaling_speed budget caps total selected replicas for a type
        # in one tick (with 1 running + upscaling_speed=1.0 -> 4 slots, not 8).
        running_4 = [
            create_instance(
                f"run-{i}", status=Instance.RAY_RUNNING, instance_type="tpu16"
            )
            for i in range(4)
        ]
        queued_8 = [_q(f"q-{i}", 1 + i) for i in range(8)]
        res_up = Reconciler._compute_to_launch(
            queued_8, [], running_4, 1.0, 100, {"tpu16": 4}
        )
        assert [i.instance_id for i in res_up["tpu16"]] == [f"q-{i}" for i in range(4)]

    @staticmethod
    def _setup_multihost_reconcile_env(
        cluster_id: str,
        instances,
        max_worker_nodes: int = 8,
        workers_per_replica: int = 4,
    ):
        cfg = MockAutoscalingConfig(
            configs={
                "node_type_configs": {
                    "tpu": NodeTypeConfig(
                        name="tpu",
                        resources={"CPU": 4, "TPU": 4},
                        min_worker_nodes=0,
                        max_worker_nodes=max_worker_nodes,
                        replica_config=WorkerReplicaConfig(
                            workers_per_replica=workers_per_replica
                        ),
                    )
                }
            }
        )
        store = InstanceStorage(cluster_id=cluster_id, storage=InMemoryStorage())
        sub = MockSubscriber()
        im = InstanceManager(store, instance_status_update_subscribers=[sub])
        TestReconciler._add_instances(store, instances)
        sub.clear()
        return cfg, store, sub, im

    @staticmethod
    def _replica_cloud(rep_ranges, include_head: bool = False):
        cloud = (
            {"c-head": CloudInstance("c-head", "head", NodeKind.HEAD, True)}
            if include_head
            else {}
        )
        for rep, indices in rep_ranges.items():
            for h in indices:
                cloud[f"c-{h}"] = CloudInstance(
                    f"c-{h}", "tpu", NodeKind.WORKER, True, replica_id=rep
                )
        return cloud

    def test_multihost_replica_drain_and_partial_termination_filtering(self):
        """Solicited drains, mixed crash+drain completion, and partial-replica
        termination filtering preserve replica atomicity without pod churn."""
        v6e_16 = {
            "tpu16": NodeTypeConfig(
                name="tpu16",
                resources={"CPU": 8, "GPU": 4},
                min_worker_nodes=0,
                max_worker_nodes=16,
                replica_config=WorkerReplicaConfig(workers_per_replica=4),
            )
        }
        rep_cloud = {
            "c-head": CloudInstance("c-head", "head", NodeKind.HEAD, True),
            **{
                f"c0-{i}": CloudInstance(
                    f"c0-{i}", "tpu16", NodeKind.WORKER, True, replica_id="rep-0"
                )
                for i in range(4)
            },
            **{
                f"c1-{i}": CloudInstance(
                    f"c1-{i}", "tpu16", NodeKind.WORKER, True, replica_id="rep-1"
                )
                for i in range(4)
            },
            "c-legacy": CloudInstance("c-legacy", "tpu16", NodeKind.WORKER, True),
        }
        nt_cfgs = {
            **v6e_16,
            "head": NodeTypeConfig(
                name="head", resources={}, min_worker_nodes=0, max_worker_nodes=1
            ),
        }

        # (a) Solicited drain of degraded slice (3 RAY_RUNNING + 1 TERMINATED) plus
        # solicited ALLOCATED termination while rep-1 is busy drains immediately.
        s7_store = InstanceStorage(cluster_id="repro7a", storage=InMemoryStorage())
        s7_im = InstanceManager(s7_store, instance_status_update_subscribers=[])
        TestReconciler._add_instances(
            s7_store,
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                    ray_node_id=binary_to_hex(b"h"),
                ),
                *(
                    create_instance(
                        f"s0-{i}",
                        status=Instance.RAY_RUNNING,
                        instance_type="tpu16",
                        cloud_instance_id=f"c0-{i}",
                        ray_node_id=binary_to_hex(f"n0{i}".encode()),
                    )
                    for i in range(3)
                ),
                create_instance(
                    "s0-3", status=Instance.TERMINATED, instance_type="tpu16"
                ),
                create_instance(
                    "alloc-0",
                    status=Instance.ALLOCATED,
                    instance_type="tpu16",
                    cloud_instance_id="c-legacy",
                ),
                *(
                    create_instance(
                        f"s1-{i}",
                        status=Instance.RAY_RUNNING,
                        instance_type="tpu16",
                        cloud_instance_id=f"c1-{i}",
                        ray_node_id=binary_to_hex(f"n1{i}".encode()),
                    )
                    for i in range(4)
                ),
            ],
        )
        ray_7 = [
            NodeState(node_id=b"h", status=NodeStatus.RUNNING),
            *(
                NodeState(
                    node_id=f"n0{i}".encode(),
                    status=NodeStatus.IDLE,
                    ray_node_type_name="tpu16",
                )
                for i in range(3)
            ),
            *(
                NodeState(
                    node_id=f"n1{i}".encode(),
                    status=NodeStatus.RUNNING,
                    ray_node_type_name="tpu16",
                )
                for i in range(4)
            ),
        ]
        term_7 = [
            *(
                TerminationRequest(
                    id=f"t{i}",
                    instance_id=f"s0-{i}",
                    ray_node_id=binary_to_hex(f"n0{i}".encode()),
                    instance_type="tpu16",
                    instance_status=Instance.RAY_RUNNING,
                    cause=TerminationRequest.Cause.IDLE,
                )
                for i in range(3)
            ),
            TerminationRequest(
                id="ta",
                instance_id="alloc-0",
                instance_type="tpu16",
                instance_status=Instance.ALLOCATED,
                cause=TerminationRequest.Cause.IDLE,
            ),
        ]
        Reconciler.reconcile(
            instance_manager=s7_im,
            scheduler=MockScheduler(to_terminate=term_7),
            cloud_provider=MagicMock(),
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_7),
            non_terminated_cloud_instances={
                k: v for k, v in rep_cloud.items() if k != "c0-3"
            },
            autoscaling_config=MockAutoscalingConfig(
                configs={"node_type_configs": nt_cfgs}
            ),
        )
        inst7, _ = s7_store.get_instances()
        assert all(
            inst7[f"s0-{i}"].status == Instance.RAY_STOP_REQUESTED for i in range(3)
        )
        assert inst7["alloc-0"].status == Instance.TERMINATING
        assert all(inst7[f"s1-{i}"].status == Instance.RAY_RUNNING for i in range(4))

        # (b) 3 solicited RAY_STOPPED + 1 unsolicited crashed RAY_STOPPED in
        # rep-0 terminate cleanly in 1 tick while rep-1 remains RAY_RUNNING.
        s8_store = InstanceStorage(cluster_id="repro8", storage=InMemoryStorage())
        s8_sub = MockSubscriber()
        s8_im = InstanceManager(s8_store, instance_status_update_subscribers=[s8_sub])
        TestReconciler._add_instances(
            s8_store,
            [
                *(
                    create_instance(
                        f"s0-{i}",
                        status=Instance.RAY_STOPPED,
                        instance_type="tpu16",
                        cloud_instance_id=f"c0-{i}",
                        status_times=[
                            (Instance.RAY_STOP_REQUESTED, 1),
                            (Instance.RAY_STOPPED, 2),
                        ],
                    )
                    for i in range(3)
                ),
                create_instance(
                    "s0-3",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu16",
                    cloud_instance_id="c0-3",
                    status_times=[(Instance.RAY_RUNNING, 1), (Instance.RAY_STOPPED, 2)],
                ),
                *(
                    create_instance(
                        f"s1-{i}",
                        status=Instance.RAY_RUNNING,
                        instance_type="tpu16",
                        cloud_instance_id=f"c1-{i}",
                    )
                    for i in range(4)
                ),
            ],
        )
        s8_sub.clear()
        Reconciler._terminate_instances(
            s8_im,
            autoscaling_config=MockAutoscalingConfig(
                configs={"node_type_configs": v6e_16}
            ),
            non_terminated_cloud_instances=rep_cloud,
        )
        assert {e.instance_id for e in s8_sub.events} == {f"s0-{i}" for i in range(4)}

    def test_multihost_partial_termination_filtering_and_occupancy_clamp(self):
        """Booting slices drain as exact KubeRay replicas, partial booting
        terminations defer, and single-host draining replacements are not delayed."""
        head_cfg = NodeTypeConfig(
            name="head", resources={"CPU": 1}, min_worker_nodes=0, max_worker_nodes=0
        )
        tpu_cap4 = {
            "head": head_cfg,
            "tpu16": NodeTypeConfig(
                name="tpu16",
                resources={"CPU": 4, "TPU": 4},
                min_worker_nodes=0,
                max_worker_nodes=4,
                replica_config=WorkerReplicaConfig(workers_per_replica=4),
            ),
        }
        b_store = InstanceStorage(cluster_id="b1_boot", storage=InMemoryStorage())
        b_sub = MockSubscriber()
        b_im = InstanceManager(b_store, instance_status_update_subscribers=[b_sub])
        TestReconciler._add_instances(
            b_store,
            [
                create_instance(
                    "h",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    cloud_instance_id="ch",
                    ray_node_id=(b"h" * 28).hex(),
                    node_kind=NodeKind.HEAD,
                ),
                *(
                    create_instance(
                        f"p{i}",
                        status=(
                            Instance.RAY_INSTALLING
                            if i in (1, 6)
                            else Instance.ALLOCATED
                        ),
                        instance_type="tpu16",
                        cloud_instance_id=f"cp-{i}",
                    )
                    for i in range(8)
                ),
            ],
        )
        b_cloud = {
            "ch": CloudInstance("ch", "head", NodeKind.HEAD, True),
            **{
                f"cp-{i}": CloudInstance(
                    f"cp-{i}",
                    "tpu16",
                    NodeKind.WORKER,
                    True,
                    replica_id="G1" if i < 4 else "G2",
                )
                for i in range(8)
            },
        }
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=b_im,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(
                node_states=[NodeState(node_id=b"h" * 28, status=NodeStatus.RUNNING)]
            ),
            scheduler=ResourceDemandScheduler(),
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "node_type_configs": tpu_cap4,
                    "disable_launch_config_check": True,
                }
            ),
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=b_cloud,
        )
        term_boot = {
            e.instance_id
            for e in b_sub.events
            if e.new_instance_status == Instance.TERMINATING
        }
        assert term_boot in (
            {f"p{i}" for i in range(4)},
            {f"p{i}" for i in range(4, 8)},
        )

        p_store = InstanceStorage(cluster_id="b1_part", storage=InMemoryStorage())
        p_sub = MockSubscriber()
        p_im = InstanceManager(p_store, instance_status_update_subscribers=[p_sub])
        outdated_inst = create_instance(
            "g2-outdated",
            status=Instance.RAY_RUNNING,
            instance_type="stale_tpu",
            cloud_instance_id="cp-4",
            ray_node_id=(b"o" * 28).hex(),
        )
        tpu_cap0 = {
            "head": head_cfg,
            "tpu16": NodeTypeConfig(
                name="tpu16",
                resources={"CPU": 4, "TPU": 4},
                min_worker_nodes=0,
                max_worker_nodes=0,
                replica_config=WorkerReplicaConfig(workers_per_replica=4),
            ),
            "tpu_active": NodeTypeConfig(
                name="tpu_active",
                resources={"CPU": 4, "TPU": 4},
                min_worker_nodes=8,
                max_worker_nodes=8,
                replica_config=WorkerReplicaConfig(workers_per_replica=4),
            ),
            "cpu": NodeTypeConfig(
                name="cpu",
                resources={"CPU": 2},
                min_worker_nodes=2,
                max_worker_nodes=2,
            ),
        }
        TestReconciler._add_instances(
            p_store,
            [
                create_instance(
                    "h",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    cloud_instance_id="ch",
                    ray_node_id=(b"h" * 28).hex(),
                    node_kind=NodeKind.HEAD,
                ),
                create_instance(
                    "g1-0",
                    status=Instance.ALLOCATED,
                    instance_type="tpu16",
                    cloud_instance_id="cp-0",
                    launch_request_id="req-g1",
                ),
                create_instance(
                    "g1-1",
                    status=Instance.ALLOCATED,
                    instance_type="tpu16",
                    cloud_instance_id="cp-1",
                    launch_request_id="req-g1",
                ),
                create_instance(
                    "r-2",
                    status=Instance.REQUESTED,
                    instance_type="tpu16",
                    launch_request_id="req-g1",
                ),
                create_instance(
                    "r-3",
                    status=Instance.REQUESTED,
                    instance_type="tpu16",
                    launch_request_id="req-g1",
                ),
                create_instance(
                    "r-active",
                    status=Instance.REQUESTED,
                    instance_type="tpu_active",
                    launch_request_id="req-active",
                ),
                create_instance(
                    "gpu-req",
                    status=Instance.REQUESTED,
                    instance_type="stale_gpu",
                    launch_request_id="req-gpu",
                ),
                outdated_inst,
            ],
        )
        part_log = MagicMock()
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=p_im,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(
                node_states=[
                    NodeState(node_id=b"h" * 28, status=NodeStatus.RUNNING),
                    NodeState(
                        node_id=b"o" * 28,
                        status=NodeStatus.RUNNING,
                        ray_node_type_name="stale_tpu",
                    ),
                ]
            ),
            scheduler=ResourceDemandScheduler(),
            autoscaling_config=MockAutoscalingConfig(
                configs={"node_type_configs": tpu_cap0, "max_num_worker_nodes": 11}
            ),
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=b_cloud,
            _logger=part_log,
        )
        # Only outdated g2-outdated drains; partial booting G1 + REQUESTED r-2/r-3/gpu-req
        # defer, whole-replica launch for tpu_active (4 hosts, with 1 existing REQUESTED
        # host and max_worker_nodes=8 -> 7 slots free) is preserved, and cpu (requested 2)
        # is clamped to 1 because g2-outdated is an outdated single-host type (not in
        # replica_sizes) newly marked RAY_STOP_REQUESTED while active nodes
        # (h + g1-0 + g1-1 + r-2 + r-3 + r-active + gpu-req + 4 tpu_active = 11)
        # leave 1 slot before max_num_nodes=12.
        assert [
            (e.instance_type or e.instance_id, e.new_instance_status)
            for e in p_sub.events
        ] == [
            ("g2-outdated", Instance.RAY_STOP_REQUESTED),
            *(("tpu_active", Instance.QUEUED) for _ in range(4)),
            ("cpu", Instance.QUEUED),
        ]
        assert any(
            "partial replica or in-flight REQUESTED pods, waiting for remaining replica pods"
            in str(c)
            and "G1" in str(c)
            for c in part_log.warning.call_args_list
        )
        for max_w, expected_queued in ((8, 0), (12, 4)):
            p_sub.clear()
            tpu_cap0["tpu_active"].max_worker_nodes = max_w
            mock_sched = MagicMock()
            mock_sched.schedule.return_value = SchedulingReply(
                to_terminate=[
                    TerminationRequest(
                        id="t-req",
                        instance_id="r-active",
                        instance_type="tpu_active",
                        instance_status=Instance.REQUESTED,
                        cause=TerminationRequest.Cause.OUTDATED,
                    )
                ],
                to_launch=[
                    LaunchRequest(
                        id="l-rep",
                        instance_type="tpu_active",
                        count=4,
                        request_ts_ms=1,
                    )
                ],
            )
            Reconciler._scale_cluster(
                autoscaling_state=MagicMock(),
                instance_manager=p_im,
                cloud_resource_monitor=CloudResourceMonitor(),
                ray_state=ClusterResourceState(),
                scheduler=mock_sched,
                autoscaling_config=MockAutoscalingConfig(
                    configs={"node_type_configs": tpu_cap0, "max_num_worker_nodes": 20}
                ),
                cloud_provider=MagicMock(),
                non_terminated_cloud_instances=b_cloud,
                _logger=part_log,
            )
            assert len(p_sub.events) == expected_queued

    def test_single_host_draining_replacement_not_clamped(self):
        """A draining single-host node (RAY_STOPPING) does not delay its
        replacement launch in either a pure single-host or mixed cluster."""
        head_cfg = NodeTypeConfig(
            name="head", resources={"CPU": 1}, min_worker_nodes=0, max_worker_nodes=0
        )
        cpu_cfg = NodeTypeConfig(
            name="cpu", resources={"CPU": 2}, min_worker_nodes=1, max_worker_nodes=1
        )
        tpu_cfg = NodeTypeConfig(
            name="tpu",
            resources={"CPU": 4, "TPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=4,
            replica_config=WorkerReplicaConfig(workers_per_replica=4),
        )
        for include_tpu in (False, True):
            nt_cfgs = {"head": head_cfg, "cpu": cpu_cfg}
            if include_tpu:
                nt_cfgs["tpu"] = tpu_cfg
            store = InstanceStorage(
                cluster_id=f"sh_drain_{include_tpu}", storage=InMemoryStorage()
            )
            sub = MockSubscriber()
            im = InstanceManager(store, instance_status_update_subscribers=[sub])
            TestReconciler._add_instances(
                store,
                [
                    create_instance(
                        "head",
                        status=Instance.RAY_RUNNING,
                        instance_type="head",
                        cloud_instance_id="c-head",
                        node_kind=NodeKind.HEAD,
                    ),
                    create_instance(
                        "cpu-0",
                        status=Instance.RAY_STOPPING,
                        instance_type="cpu",
                        cloud_instance_id="c-cpu-0",
                        ray_node_id="n-cpu-0",
                    ),
                ],
            )
            sub.clear()
            sched = MagicMock()
            sched.schedule.return_value = SchedulingReply(
                to_launch=[LaunchRequest(id="l-cpu", instance_type="cpu", count=1)]
            )
            Reconciler._scale_cluster(
                autoscaling_state=MagicMock(),
                instance_manager=im,
                cloud_resource_monitor=CloudResourceMonitor(),
                ray_state=ClusterResourceState(),
                scheduler=sched,
                autoscaling_config=MockAutoscalingConfig(
                    configs={
                        "node_type_configs": nt_cfgs,
                        "max_num_worker_nodes": 5 if include_tpu else 1,
                    }
                ),
                cloud_provider=MagicMock(),
                non_terminated_cloud_instances={
                    "c-head": CloudInstance("c-head", "head", NodeKind.HEAD, True),
                    "c-cpu-0": CloudInstance("c-cpu-0", "cpu", NodeKind.WORKER, True),
                },
            )
            assert [(e.instance_type, e.new_instance_status) for e in sub.events] == [
                ("cpu", Instance.QUEUED)
            ]

    def test_multihost_crashed_worker_terminates_replica_while_drained_worker_waits(
        self,
    ):
        cloud = self._replica_cloud({"rep-0": range(4)})
        now_ns = time.time_ns()

        # Spontaneous raylet exit (RAY_RUNNING -> RAY_STOPPED) terminates the
        # whole replica atomically in one batch.
        cfg, _, crash_sub, crash_im = self._setup_multihost_reconcile_env(
            "crash",
            [
                *(
                    create_instance(
                        f"w-{h}",
                        status=Instance.RAY_RUNNING,
                        instance_type="tpu",
                        cloud_instance_id=f"c-{h}",
                    )
                    for h in range(3)
                ),
                create_instance(
                    "w-3",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu",
                    cloud_instance_id="c-3",
                    status_times=[
                        (Instance.RAY_RUNNING, now_ns - 2),
                        (Instance.RAY_STOPPED, now_ns - 1),
                    ],
                ),
            ],
        )
        Reconciler._terminate_instances(
            crash_im, autoscaling_config=cfg, non_terminated_cloud_instances=cloud
        )
        assert sorted(
            (e.instance_id, e.new_instance_status) for e in crash_sub.events
        ) == [(f"w-{h}", Instance.TERMINATING) for h in range(4)]

        # Solicited stop (RAY_STOP_REQUESTED -> RAY_STOPPED) waits while siblings run.
        cfg, _, drain_sub, drain_im = self._setup_multihost_reconcile_env(
            "drain",
            [
                *(
                    create_instance(
                        f"w-{h}",
                        status=Instance.RAY_RUNNING,
                        instance_type="tpu",
                        cloud_instance_id=f"c-{h}",
                    )
                    for h in range(3)
                ),
                create_instance(
                    "w-3",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu",
                    cloud_instance_id="c-3",
                    status_times=[
                        (Instance.RAY_RUNNING, now_ns - 3),
                        (Instance.RAY_STOP_REQUESTED, now_ns - 2),
                        (Instance.RAY_STOPPED, now_ns - 1),
                    ],
                ),
            ],
        )
        Reconciler._terminate_instances(
            drain_im, autoscaling_config=cfg, non_terminated_cloud_instances=cloud
        )
        assert drain_sub.events == []

    def test_multihost_idle_scale_down_leaves_booting_worker_until_drain_accepted(self):
        workers = [
            *(
                create_instance(
                    f"w-{h}",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id=f"c-{h}",
                    ray_node_id=f"n-{h}",
                )
                for h in range(3)
            ),
            create_instance(
                "w-3",
                status=Instance.ALLOCATED,
                instance_type="tpu",
                cloud_instance_id="c-3",
            ),
        ]
        cfg, store, sub, im = self._setup_multihost_reconcile_env(
            "boot_hold",
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                ),
                *workers,
            ],
        )
        cloud = self._replica_cloud({"rep-0": range(4)}, include_head=True)
        sched = MagicMock()
        sched.schedule.return_value = SchedulingReply(
            to_launch=[],
            to_terminate=[
                TerminationRequest(
                    id=f"t-{h}",
                    instance_id=workers[h].instance_id,
                    ray_node_id=workers[h].node_id,
                    instance_type="tpu",
                    instance_status=workers[h].status,
                    cause=TerminationRequest.Cause.IDLE,
                )
                for h in range(4)
            ],
        )
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(),
            scheduler=sched,
            autoscaling_config=cfg,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud,
        )
        assert sorted((e.instance_id, e.new_instance_status) for e in sub.events) == [
            (f"w-{h}", Instance.RAY_STOP_REQUESTED) for h in range(3)
        ]
        assert store.get_instances()[0]["w-3"].status == Instance.ALLOCATED

    def test_multihost_replica_atomic_termination_on_drain_and_crash(self):
        cloud = self._replica_cloud({"rep-0": range(4)}, include_head=True)
        now_ns = time.time_ns()

        # Once all draining siblings reach RAY_STOPPED, _terminate_instances
        # terminates the held ALLOCATED sibling in the same batch.
        cfg, _, drained_sub, drained_im = self._setup_multihost_reconcile_env(
            "boot_done",
            [
                *(
                    create_instance(
                        f"w-{h}",
                        status=Instance.RAY_STOPPED,
                        instance_type="tpu",
                        cloud_instance_id=f"c-{h}",
                        status_times=[
                            (Instance.RAY_RUNNING, now_ns - 3),
                            (Instance.RAY_STOP_REQUESTED, now_ns - 2),
                            (Instance.RAY_STOPPED, now_ns - 1),
                        ],
                    )
                    for h in range(3)
                ),
                create_instance(
                    "w-3",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="c-3",
                ),
            ],
        )
        Reconciler._terminate_instances(
            drained_im, autoscaling_config=cfg, non_terminated_cloud_instances=cloud
        )
        assert sorted(
            (e.instance_id, e.new_instance_status) for e in drained_sub.events
        ) == [(f"w-{h}", Instance.TERMINATING) for h in range(4)]

        # Unsolicited crash of one worker also transitions its ALLOCATED sibling
        # to TERMINATING atomically since the replica tears down as a unit.
        cfg, _, crash_sub, crash_im = self._setup_multihost_reconcile_env(
            "crash_sib",
            [
                create_instance(
                    "w-0",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu",
                    cloud_instance_id="c-0",
                    status_times=[
                        (Instance.RAY_RUNNING, now_ns - 2),
                        (Instance.RAY_STOPPED, now_ns - 1),
                    ],
                ),
                create_instance(
                    "w-3",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="c-3",
                ),
            ],
        )
        Reconciler._terminate_instances(
            crash_im, autoscaling_config=cfg, non_terminated_cloud_instances=cloud
        )
        assert sorted(
            (e.instance_id, e.new_instance_status) for e in crash_sub.events
        ) == [("w-0", Instance.TERMINATING), ("w-3", Instance.TERMINATING)]

    @pytest.mark.parametrize(
        "sibling_status",
        [
            Instance.RAY_RUNNING,
            Instance.RAY_STOP_REQUESTED,
            Instance.RAY_STOPPING,
        ],
    )
    def test_multihost_drain_hold_timeout_terminates_transitionable_siblings(
        self, sibling_status
    ):
        now_ns = time.time_ns()
        cfg, store, sub, im = self._setup_multihost_reconcile_env("hold_timeout", [])
        hold_timeout_s = (
            cfg.get_instance_reconcile_config().terminating_status_timeout_s
        )
        expired_ns = now_ns - (hold_timeout_s + 60) * 10**9
        TestReconciler._add_instances(
            store,
            [
                # rep-0: w-0 timed out in solicited RAY_STOPPED while w-1 still runs/drains Ray.
                create_instance(
                    "w-0",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu",
                    cloud_instance_id="c-0",
                    status_times=[
                        (Instance.RAY_RUNNING, expired_ns - 2),
                        (Instance.RAY_STOP_REQUESTED, expired_ns - 1),
                        (Instance.RAY_STOPPED, expired_ns),
                    ],
                ),
                create_instance(
                    "w-1",
                    status=sibling_status,
                    instance_type="tpu",
                    cloud_instance_id="c-1",
                    ray_node_id="n-1",
                ),
                create_instance(
                    "w-2",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="c-2",
                ),
                create_instance(
                    "w-3",
                    status=Instance.RAY_INSTALLING,
                    instance_type="tpu",
                    cloud_instance_id="c-3",
                ),
                # rep-1: all running workers drained; unready w-7 terminates with them.
                *(
                    create_instance(
                        f"w-{h}",
                        status=Instance.RAY_STOPPED,
                        instance_type="tpu",
                        cloud_instance_id=f"c-{h}",
                        status_times=[
                            (Instance.RAY_RUNNING, now_ns - 3),
                            (Instance.RAY_STOP_REQUESTED, now_ns - 2),
                            (Instance.RAY_STOPPED, now_ns - 1),
                        ],
                    )
                    for h in range(4, 7)
                ),
                create_instance(
                    "w-7",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="c-7",
                ),
            ],
        )
        cloud = self._replica_cloud({"rep-0": range(4), "rep-1": range(4, 8)})
        sub.clear()
        Reconciler._terminate_instances(
            im, autoscaling_config=cfg, non_terminated_cloud_instances=cloud
        )
        # Every sibling whose status permits TERMINATING transitions atomically;
        # RAY_STOP_REQUESTED has no transition to TERMINATING and stays untouched.
        expected_rep0 = (
            ["w-0", "w-2", "w-3"]
            if sibling_status == Instance.RAY_STOP_REQUESTED
            else ["w-0", "w-1", "w-2", "w-3"]
        )
        assert sorted((e.instance_id, e.new_instance_status) for e in sub.events) == [
            (wid, Instance.TERMINATING)
            for wid in [*expected_rep0, "w-4", "w-5", "w-6", "w-7"]
        ]

        # Once the cloud provider removes rep-0, all four siblings converge to TERMINATED.
        sub.clear()
        Reconciler._handle_cloud_instance_terminated(
            im,
            non_terminated_cloud_instances={
                k: v for k, v in cloud.items() if v.replica_id != "rep-0"
            },
        )
        assert sorted((e.instance_id, e.new_instance_status) for e in sub.events) == [
            (f"w-{h}", Instance.TERMINATED) for h in range(4)
        ]

    def test_multihost_ray_stopping_sibling_defers_solicited_stop_but_not_unsolicited(
        self,
    ):
        now_ns = time.time_ns()
        cfg, store, sub, im = self._setup_multihost_reconcile_env(
            "stopping_hold",
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                ),
                create_instance(
                    "w-0",
                    status=Instance.RAY_STOPPING,
                    instance_type="tpu",
                    cloud_instance_id="c-0",
                    ray_node_id="n-0",
                ),
                create_instance(
                    "w-1",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="c-1",
                ),
                create_instance(
                    "w-2",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu",
                    cloud_instance_id="c-2",
                    status_times=[
                        (Instance.RAY_RUNNING, now_ns - 4),
                        (Instance.RAY_STOP_REQUESTED, now_ns - 3),
                        (Instance.RAY_STOPPING, now_ns - 2),
                        (Instance.RAY_STOPPED, now_ns - 1),
                    ],
                ),
            ],
        )
        cloud = self._replica_cloud({"rep-0": range(3)}, include_head=True)
        sched = MagicMock()
        sched.schedule.return_value = SchedulingReply(
            to_launch=[],
            to_terminate=[
                TerminationRequest(
                    id="t-1",
                    instance_id="w-1",
                    instance_type="tpu",
                    instance_status=Instance.ALLOCATED,
                    cause=TerminationRequest.Cause.IDLE,
                )
            ],
        )
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(),
            scheduler=sched,
            autoscaling_config=cfg,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud,
        )
        assert sub.events == []

        sub.clear()
        Reconciler._terminate_instances(
            im, autoscaling_config=cfg, non_terminated_cloud_instances=cloud
        )
        assert sub.events == []

        # Once w-0 finishes draining to RAY_STOPPED, all three terminate together.
        _, version = store.get_instances()
        Reconciler._update_instance_manager(
            im,
            version,
            {
                "w-0": InstanceUpdateEvent(
                    instance_id="w-0", new_instance_status=Instance.RAY_STOPPED
                )
            },
        )
        sub.clear()
        Reconciler._terminate_instances(
            im, autoscaling_config=cfg, non_terminated_cloud_instances=cloud
        )
        assert sorted((e.instance_id, e.new_instance_status) for e in sub.events) == [
            ("w-0", Instance.TERMINATING),
            ("w-1", Instance.TERMINATING),
            ("w-2", Instance.TERMINATING),
        ]

        # Counter-case: unsolicited RAY_RUNNING -> RAY_STOPPING -> RAY_STOPPED
        # (without RAY_STOP_REQUESTED) is NOT held while siblings run/drain.
        _, _, unsol_sub, unsol_im = self._setup_multihost_reconcile_env(
            "unsolicited_stopping",
            [
                create_instance(
                    "w-0",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="c-0",
                    ray_node_id="n-0",
                ),
                create_instance(
                    "w-1",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="c-1",
                ),
                create_instance(
                    "w-2",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu",
                    cloud_instance_id="c-2",
                    status_times=[
                        (Instance.RAY_RUNNING, now_ns - 3),
                        (Instance.RAY_STOPPING, now_ns - 2),
                        (Instance.RAY_STOPPED, now_ns - 1),
                    ],
                ),
            ],
        )
        Reconciler._terminate_instances(
            unsol_im, autoscaling_config=cfg, non_terminated_cloud_instances=cloud
        )
        assert sorted(
            (e.instance_id, e.new_instance_status) for e in unsol_sub.events
        ) == [
            ("w-0", Instance.TERMINATING),
            ("w-1", Instance.TERMINATING),
            ("w-2", Instance.TERMINATING),
        ]

    def test_multihost_draining_replica_clamps_replacement_launches(self):
        # Healthy 4-host outdated replica at max_worker_nodes=4 (min=4): replacement
        # launches must stay clamped across Tick 1 (RAY_RUNNING -> RAY_STOP_REQUESTED),
        # Tick 2 (still RAY_STOP_REQUESTED), Tick 3 (RAY_STOPPING), and Tick 4
        # (3 solicited RAY_STOPPED + 1 RAY_STOPPING), then launch on Tick 5 once all
        # 4 reach RAY_STOPPED and transition to TERMINATING in the same reconcile.
        head_cfg = NodeTypeConfig(
            name="head", resources={"CPU": 1}, min_worker_nodes=0, max_worker_nodes=0
        )

        def _make_cfg(max_w: int, max_num_w: Optional[int] = None):
            cfgs = {
                "node_type_configs": {
                    "head": head_cfg,
                    "tpu": NodeTypeConfig(
                        name="tpu",
                        resources={"CPU": 4, "TPU": 4},
                        min_worker_nodes=4,
                        max_worker_nodes=max_w,
                        launch_config_hash="new-hash",
                        replica_config=WorkerReplicaConfig(workers_per_replica=4),
                    ),
                },
                "disable_launch_config_check": False,
                "max_concurrent_launches": 0,
            }
            if max_num_w is not None:
                cfgs["max_num_worker_nodes"] = max_num_w
            return MockAutoscalingConfig(configs=cfgs)

        def _make_outdated_worker(h: int):
            inst = create_instance(
                f"w-{h}",
                status=Instance.RAY_RUNNING,
                instance_type="tpu",
                cloud_instance_id=f"c-{h}",
                ray_node_id=(bytes([h + 1]) * 28).hex(),
            )
            inst.launch_config_hash = "old-hash"
            return inst

        store = InstanceStorage(cluster_id="drain_clamp", storage=InMemoryStorage())
        sub = MockSubscriber()
        im = InstanceManager(store, instance_status_update_subscribers=[sub])
        TestReconciler._add_instances(
            store,
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                    ray_node_id=(b"h" * 28).hex(),
                ),
                *(_make_outdated_worker(h) for h in range(4)),
            ],
        )
        cloud = self._replica_cloud({"rep-0": range(4)}, include_head=True)
        ray_running_state = ClusterResourceState(
            node_states=[
                NodeState(node_id=b"h" * 28, status=NodeStatus.RUNNING),
                *(
                    NodeState(
                        node_id=bytes([h + 1]) * 28,
                        status=NodeStatus.RUNNING,
                        ray_node_type_name="tpu",
                    )
                    for h in range(4)
                ),
            ]
        )
        cfg_cap4 = _make_cfg(max_w=4)
        sched = ResourceDemandScheduler()

        # Tick 1: all 4 move to RAY_STOP_REQUESTED; 0 replacement QUEUED created.
        sub.clear()
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ray_running_state,
            scheduler=sched,
            autoscaling_config=cfg_cap4,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud,
        )
        assert sorted((e.instance_id, e.new_instance_status) for e in sub.events) == [
            (f"w-{h}", Instance.RAY_STOP_REQUESTED) for h in range(4)
        ]

        # Tick 2: still RAY_STOP_REQUESTED; still 0 replacement QUEUED.
        sub.clear()
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ray_running_state,
            scheduler=sched,
            autoscaling_config=cfg_cap4,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud,
        )
        assert sub.events == []

        # Also below per-type maxReplicas (max_w=8), tighter max_num_worker_nodes=4
        # clamps the duplicate slice to 0 while rep-0 is RAY_STOP_REQUESTED.
        sub.clear()
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ray_running_state,
            scheduler=sched,
            autoscaling_config=_make_cfg(max_w=8, max_num_w=4),
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud,
        )
        assert sub.events == []

        # Tick 3: all 4 move to RAY_STOPPING; scheduler no longer sees them, yet
        # occupancy clamp still prevents premature launches at max_worker_nodes=4.
        _, ver = store.get_instances()
        Reconciler._update_instance_manager(
            im,
            ver,
            {
                f"w-{h}": InstanceUpdateEvent(
                    instance_id=f"w-{h}", new_instance_status=Instance.RAY_STOPPING
                )
                for h in range(4)
            },
        )
        sub.clear()
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(
                node_states=[
                    NodeState(node_id=b"h" * 28, status=NodeStatus.RUNNING),
                    *(
                        NodeState(
                            node_id=bytes([h + 1]) * 28,
                            status=NodeStatus.DRAINING,
                            ray_node_type_name="tpu",
                        )
                        for h in range(4)
                    ),
                ]
            ),
            scheduler=sched,
            autoscaling_config=cfg_cap4,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud,
        )
        assert sub.events == []

        # Tick 4: 3 reach RAY_STOPPED (held by w-3 still RAY_STOPPING); whole-replica
        # occupancy keeps launches clamped to 0.
        _, ver = store.get_instances()
        Reconciler._update_instance_manager(
            im,
            ver,
            {
                f"w-{h}": InstanceUpdateEvent(
                    instance_id=f"w-{h}", new_instance_status=Instance.RAY_STOPPED
                )
                for h in range(3)
            },
        )
        sub.clear()
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(
                node_states=[
                    NodeState(node_id=b"h" * 28, status=NodeStatus.RUNNING),
                    NodeState(
                        node_id=bytes([4]) * 28,
                        status=NodeStatus.DRAINING,
                        ray_node_type_name="tpu",
                    ),
                ]
            ),
            scheduler=sched,
            autoscaling_config=cfg_cap4,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud,
        )
        assert sub.events == []

        # Tick 5: w-3 finishes stopping too -> all 4 terminate and replacement launches.
        _, ver = store.get_instances()
        Reconciler._update_instance_manager(
            im,
            ver,
            {
                "w-3": InstanceUpdateEvent(
                    instance_id="w-3", new_instance_status=Instance.RAY_STOPPED
                )
            },
        )
        sub.clear()
        Reconciler._step_next(
            autoscaling_state=MagicMock(),
            instance_manager=im,
            cloud_provider=MagicMock(),
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=ClusterResourceState(
                node_states=[NodeState(node_id=b"h" * 28, status=NodeStatus.RUNNING)]
            ),
            non_terminated_cloud_instances=cloud,
            scheduler=sched,
            autoscaling_config=cfg_cap4,
        )
        queued = [e for e in sub.events if e.new_instance_status == Instance.QUEUED]
        terminating = {
            e.instance_id
            for e in sub.events
            if e.new_instance_status == Instance.TERMINATING
        }
        assert len(queued) == 4
        assert terminating == {f"w-{h}" for h in range(4)}

        # Negative counter-case: below both max_worker_nodes=8 and max_num_nodes=9,
        # rolling replacement of outdated rep-0 queues 4 new workers immediately.
        s2 = InstanceStorage(cluster_id="drain_surge", storage=InMemoryStorage())
        sub2 = MockSubscriber()
        im2 = InstanceManager(s2, instance_status_update_subscribers=[sub2])
        TestReconciler._add_instances(
            s2,
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                    ray_node_id=(b"h" * 28).hex(),
                ),
                *(_make_outdated_worker(h) for h in range(4)),
            ],
        )
        sub2.clear()
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im2,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ray_running_state,
            scheduler=sched,
            autoscaling_config=_make_cfg(max_w=8, max_num_w=8),
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud,
        )
        assert sorted(
            (e.instance_type or e.instance_id, e.new_instance_status)
            for e in sub2.events
        ) == [
            *(("tpu", Instance.QUEUED) for _ in range(4)),
            *((f"w-{h}", Instance.RAY_STOP_REQUESTED) for h in range(4)),
        ]

        # N >= 2 degraded replicas (rep-0 2/4 RAY_STOP_REQUESTED + rep-1 2/4
        # RAY_RUNNING) already fill max_worker_nodes=8 (2 replicas) -> clamp 3rd slice.
        s3 = InstanceStorage(cluster_id="drain_2deg", storage=InMemoryStorage())
        sub3 = MockSubscriber()
        im3 = InstanceManager(s3, instance_status_update_subscribers=[sub3])
        TestReconciler._add_instances(
            s3,
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                    ray_node_id=(b"h" * 28).hex(),
                ),
                *(
                    create_instance(
                        f"r0-{h}",
                        status=Instance.RAY_STOP_REQUESTED,
                        instance_type="tpu",
                        cloud_instance_id=f"c-{h}",
                        ray_node_id=f"n0-{h}",
                        launch_request_id="req-old-0",
                    )
                    for h in range(2)
                ),
                *(
                    create_instance(
                        f"r1-{h}",
                        status=Instance.RAY_RUNNING,
                        instance_type="tpu",
                        cloud_instance_id=f"c-{4 + h}",
                        ray_node_id=f"n1-{h}",
                        launch_request_id="req-old-1",
                    )
                    for h in range(2)
                ),
            ],
        )
        cloud3 = self._replica_cloud(
            {"rep-0": range(2), "rep-1": range(4, 6)}, include_head=True
        )
        mock_sched3 = MagicMock()
        mock_sched3.schedule.return_value = SchedulingReply(
            to_launch=[LaunchRequest(id="l-3", instance_type="tpu", count=4)]
        )
        sub3.clear()
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im3,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(),
            scheduler=mock_sched3,
            autoscaling_config=_make_cfg(max_w=8),
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud3,
        )
        assert sub3.events == []

        # Partial replica with TERMINATING + RAY_INSTALLING siblings ("req-old") plus
        # an unrelated fresh 4-pod REQUESTED slice ("req-new") already occupies all 8
        # slots at max_worker_nodes=8; rep-0 must not consume req-new's REQUESTED pods.
        s4 = InstanceStorage(
            cluster_id="drain_boot_disjoint", storage=InMemoryStorage()
        )
        sub4 = MockSubscriber()
        im4 = InstanceManager(s4, instance_status_update_subscribers=[sub4])
        TestReconciler._add_instances(
            s4,
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                    ray_node_id=(b"h" * 28).hex(),
                ),
                *(
                    create_instance(
                        f"t-{h}",
                        status=Instance.TERMINATING,
                        instance_type="tpu",
                        cloud_instance_id=f"c-{h}",
                        launch_request_id="req-old",
                    )
                    for h in range(2)
                ),
                *(
                    create_instance(
                        f"b-{h}",
                        status=Instance.RAY_INSTALLING,
                        instance_type="tpu",
                        cloud_instance_id=f"c-{h}",
                        launch_request_id="req-old",
                    )
                    for h in range(2, 4)
                ),
                *(
                    create_instance(
                        f"r-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-new",
                    )
                    for h in range(4)
                ),
            ],
        )
        cloud4 = self._replica_cloud({"rep-0": range(4)}, include_head=True)
        mock_sched4 = MagicMock()
        mock_sched4.schedule.return_value = SchedulingReply(
            to_launch=[LaunchRequest(id="l-4", instance_type="tpu", count=4)]
        )
        sub4.clear()
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im4,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(),
            scheduler=mock_sched4,
            autoscaling_config=_make_cfg(max_w=8),
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud4,
        )
        assert sub4.events == []

    def test_multihost_mid_materialization_running_replica_defers_drain(self):
        # 1. Incomplete replica with 2 RAY_RUNNING pods + 2 REQUESTED siblings
        # (no ALLOCATED/RAY_INSTALLING pods) must defer draining.
        cfg, _, sub1, im1 = self._setup_multihost_reconcile_env(
            "mid_mat_run",
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                ),
                *(
                    create_instance(
                        f"w-{h}",
                        status=Instance.RAY_RUNNING,
                        instance_type="tpu",
                        cloud_instance_id=f"c-{h}",
                        ray_node_id=f"n-{h}",
                        launch_request_id="req-mid",
                    )
                    for h in range(2)
                ),
                *(
                    create_instance(
                        f"r-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-mid",
                    )
                    for h in range(2, 4)
                ),
            ],
        )
        cloud1 = self._replica_cloud({"rep-0": range(2)}, include_head=True)
        sched1 = MagicMock()
        sched1.schedule.return_value = SchedulingReply(
            to_terminate=[
                TerminationRequest(
                    id=f"t-{h}",
                    instance_id=f"w-{h}",
                    ray_node_id=f"n-{h}",
                    instance_type="tpu",
                    instance_status=Instance.RAY_RUNNING,
                    cause=TerminationRequest.Cause.IDLE,
                )
                for h in range(2)
            ]
        )
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im1,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(),
            scheduler=sched1,
            autoscaling_config=cfg,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud1,
        )
        assert sub1.events == []

        # 2. N >= 2 concurrent half-running replicas where total REQUESTED pods == 4
        # (multiple of replica size) still defer via matching launch_request_id.
        cfg2, _, sub2, im2 = self._setup_multihost_reconcile_env(
            "mid_mat_two",
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                ),
                *(
                    create_instance(
                        f"w-{h}",
                        status=Instance.RAY_RUNNING,
                        instance_type="tpu",
                        cloud_instance_id=f"c-{h}",
                        ray_node_id=f"n-{h}",
                        launch_request_id="req-a" if h < 2 else "req-b",
                    )
                    for h in (0, 1, 4, 5)
                ),
                *(
                    create_instance(
                        f"r-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-a" if h < 4 else "req-b",
                    )
                    for h in (2, 3, 6, 7)
                ),
            ],
        )
        cloud2 = self._replica_cloud(
            {"rep-0": range(2), "rep-1": range(4, 6)}, include_head=True
        )
        sched2 = MagicMock()
        sched2.schedule.return_value = SchedulingReply(
            to_terminate=[
                TerminationRequest(
                    id=f"t-{h}",
                    instance_id=f"w-{h}",
                    ray_node_id=f"n-{h}",
                    instance_type="tpu",
                    instance_status=Instance.RAY_RUNNING,
                    cause=TerminationRequest.Cause.IDLE,
                )
                for h in (0, 1, 4, 5)
            ]
        )
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im2,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(),
            scheduler=sched2,
            autoscaling_config=cfg2,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud2,
        )
        assert sub2.events == []

        # 3. Negative counter-case: genuinely degraded old 3/4 RAY_RUNNING slice
        # alongside an unrelated fresh 4-pod REQUESTED slice drains immediately.
        cfg3, _, sub3, im3 = self._setup_multihost_reconcile_env(
            "deg_plus_new",
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                ),
                *(
                    create_instance(
                        f"w-{h}",
                        status=Instance.RAY_RUNNING,
                        instance_type="tpu",
                        cloud_instance_id=f"c-{h}",
                        ray_node_id=f"n-{h}",
                        launch_request_id="req-old",
                    )
                    for h in range(3)
                ),
                *(
                    create_instance(
                        f"r-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-new",
                    )
                    for h in range(4)
                ),
            ],
        )
        cloud3 = self._replica_cloud({"rep-0": range(3)}, include_head=True)
        sched3 = MagicMock()
        sched3.schedule.return_value = SchedulingReply(
            to_terminate=[
                TerminationRequest(
                    id=f"t-{h}",
                    instance_id=f"w-{h}",
                    ray_node_id=f"n-{h}",
                    instance_type="tpu",
                    instance_status=Instance.RAY_RUNNING,
                    cause=TerminationRequest.Cause.IDLE,
                )
                for h in range(3)
            ]
        )
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im3,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(),
            scheduler=sched3,
            autoscaling_config=cfg3,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud3,
        )
        assert sorted((e.instance_id, e.new_instance_status) for e in sub3.events) == [
            (f"w-{h}", Instance.RAY_STOP_REQUESTED) for h in range(3)
        ]

    def test_multihost_replica_batch_affinity_and_unaligned_retry_isolation(self):
        # 1. Interleaved pod arrival across N >= 2 concurrent batches keeps 1:1
        # replica_id <-> launch_request_id affinity and never steals healed pods
        # from existing degraded or externally adopted replicas.
        cfg, store, _, im = self._setup_multihost_reconcile_env(
            "affinity",
            [
                *(
                    create_instance(
                        f"old-{h}",
                        status=Instance.RAY_RUNNING,
                        instance_type="tpu",
                        cloud_instance_id=f"c-old-{h}",
                        launch_request_id="req-old",
                    )
                    for h in range(3)
                ),
                *(
                    create_instance(
                        f"ext-{h}",
                        status=Instance.RAY_RUNNING,
                        instance_type="tpu",
                        cloud_instance_id=f"c-ext-{h}",
                    )
                    for h in range(2)
                ),
                *(
                    create_instance(
                        f"r0-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-0",
                        status_times=[(Instance.REQUESTED, 100)],
                    )
                    for h in range(4)
                ),
                *(
                    create_instance(
                        f"r1-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-1",
                        status_times=[(Instance.REQUESTED, 200)],
                    )
                    for h in range(4)
                ),
            ],
        )
        cloud = {
            **{
                f"c-old-{h}": CloudInstance(
                    f"c-old-{h}", "tpu", NodeKind.WORKER, True, replica_id="rep-old"
                )
                for h in range(4)
            },
            **{
                f"c-ext-{h}": CloudInstance(
                    f"c-ext-{h}", "tpu", NodeKind.WORKER, True, replica_id="rep-ext"
                )
                for h in range(3)
            },
            "c-a-0": CloudInstance(
                "c-a-0", "tpu", NodeKind.WORKER, True, replica_id="rep-a"
            ),
            "c-b-0": CloudInstance(
                "c-b-0", "tpu", NodeKind.WORKER, True, replica_id="rep-b"
            ),
            "c-a-1": CloudInstance(
                "c-a-1", "tpu", NodeKind.WORKER, True, replica_id="rep-a"
            ),
            "c-b-1": CloudInstance(
                "c-b-1", "tpu", NodeKind.WORKER, True, replica_id="rep-b"
            ),
        }
        Reconciler._handle_cloud_instance_allocation(
            im, cloud, [], autoscaling_config=cfg
        )
        Reconciler._handle_extra_cloud_instances_from_cloud_provider(im, cloud)
        # Tick 2: remaining pods arrive interleaved in reverse order.
        cloud.update(
            {
                "c-b-2": CloudInstance(
                    "c-b-2", "tpu", NodeKind.WORKER, True, replica_id="rep-b"
                ),
                "c-a-2": CloudInstance(
                    "c-a-2", "tpu", NodeKind.WORKER, True, replica_id="rep-a"
                ),
                "c-b-3": CloudInstance(
                    "c-b-3", "tpu", NodeKind.WORKER, True, replica_id="rep-b"
                ),
                "c-a-3": CloudInstance(
                    "c-a-3", "tpu", NodeKind.WORKER, True, replica_id="rep-a"
                ),
            }
        )
        Reconciler._handle_cloud_instance_allocation(
            im, cloud, [], autoscaling_config=cfg
        )
        Reconciler._handle_extra_cloud_instances_from_cloud_provider(im, cloud)
        insts, _ = store.get_instances()
        by_rep = defaultdict(set)
        for inst in insts.values():
            by_rep[cloud[inst.cloud_instance_id].replica_id].add(inst.launch_request_id)
        assert by_rep["rep-old"] == {"req-old", ""}
        assert by_rep["rep-ext"] == {""}
        assert len(by_rep["rep-a"]) == 1 and len(by_rep["rep-b"]) == 1
        assert by_rep["rep-a"] | by_rep["rep-b"] == {"req-0", "req-1"}

        # 2. LaunchNodeError for an already-started replica batch fails that batch
        # without stealing a fresh unrelated replica's pods.
        _, err_store, _, err_im = self._setup_multihost_reconcile_env(
            "affinity_err",
            [
                create_instance(
                    "a0",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="ca-0",
                    launch_request_id="req-a",
                ),
                *(
                    create_instance(
                        f"a{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-a",
                        status_times=[(Instance.REQUESTED, 100)],
                    )
                    for h in range(1, 4)
                ),
                *(
                    create_instance(
                        f"b{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-b",
                        status_times=[(Instance.REQUESTED, 200)],
                    )
                    for h in range(4)
                ),
            ],
        )
        err_cloud = {
            "ca-0": CloudInstance(
                "ca-0", "tpu", NodeKind.WORKER, True, replica_id="rep-a"
            ),
            "cb-0": CloudInstance(
                "cb-0", "tpu", NodeKind.WORKER, True, replica_id="rep-b"
            ),
        }
        Reconciler._handle_cloud_instance_allocation(
            err_im,
            err_cloud,
            [
                LaunchNodeError(
                    request_id="req-a",
                    node_type="tpu",
                    count=2,
                    timestamp_ns=1,
                    details="quota",
                )
            ],
            autoscaling_config=cfg,
        )
        err_insts, _ = err_store.get_instances()
        assert (
            sum(
                1
                for h in range(1, 4)
                if err_insts[f"a{h}"].status == Instance.ALLOCATION_FAILED
            )
            == 2
        )
        assert any(err_insts[f"b{h}"].cloud_instance_id == "cb-0" for h in range(4))

    def test_multihost_degraded_replica_drain_and_teardown_isolation(self):
        cfg, _, _, _ = self._setup_multihost_reconcile_env("cfg_only", [])
        # 3. Degraded slice ("req-old", 3/4 RAY_RUNNING) beside an unrelated
        # unaligned partial retry ("req-new", 2 REQUESTED) drains immediately
        # and does not absorb req-new's slots in launch occupancy clamping.
        cfg_cap8 = MockAutoscalingConfig(
            configs={
                "node_type_configs": {
                    "head": NodeTypeConfig(
                        name="head",
                        resources={},
                        min_worker_nodes=0,
                        max_worker_nodes=0,
                    ),
                    "tpu": NodeTypeConfig(
                        name="tpu",
                        resources={"CPU": 4, "TPU": 4},
                        min_worker_nodes=0,
                        max_worker_nodes=8,
                        replica_config=WorkerReplicaConfig(workers_per_replica=4),
                    ),
                },
                "max_concurrent_launches": 16,
            }
        )
        _, _, sub3, im3 = self._setup_multihost_reconcile_env(
            "unaligned_retry",
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                ),
                *(
                    create_instance(
                        f"w-{h}",
                        status=Instance.RAY_RUNNING,
                        instance_type="tpu",
                        cloud_instance_id=f"c-{h}",
                        ray_node_id=f"n-{h}",
                        launch_request_id="req-old",
                    )
                    for h in range(3)
                ),
                *(
                    create_instance(
                        f"r-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-new",
                    )
                    for h in range(2)
                ),
            ],
        )
        cloud3 = self._replica_cloud({"rep-old": range(3)}, include_head=True)
        sched3 = MagicMock()
        sched3.schedule.return_value = SchedulingReply(
            to_terminate=[
                TerminationRequest(
                    id=f"t-{h}",
                    instance_id=f"w-{h}",
                    ray_node_id=f"n-{h}",
                    instance_type="tpu",
                    instance_status=Instance.RAY_RUNNING,
                    cause=TerminationRequest.Cause.IDLE,
                )
                for h in range(3)
            ],
            to_launch=[LaunchRequest(id="l-1", instance_type="tpu", count=4)],
        )
        sub3.clear()
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im3,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(),
            scheduler=sched3,
            autoscaling_config=cfg_cap8,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud3,
        )
        # rep-old drains immediately (not deferred by req-new's 2 % 4 != 0) and
        # occupies 4 + 2 = 6 slots, clamping the extra 4-pod launch at max_worker_nodes=8.
        assert sorted((e.instance_id, e.new_instance_status) for e in sub3.events) == [
            (f"w-{h}", Instance.RAY_STOP_REQUESTED) for h in range(3)
        ]

        # 4. Incomplete batch rep-0 (1 ALLOCATED + 1 REQUESTED "req-0") beside
        # 4 REQUESTED "req-1" does not steal req-1's slots -> occupies 4 + 4 = 8.
        _, _, sub4, im4 = self._setup_multihost_reconcile_env(
            "no_cross_batch_steal",
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                ),
                create_instance(
                    "w-0",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="c-0",
                    launch_request_id="req-0",
                ),
                create_instance(
                    "r0-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="req-0",
                ),
                *(
                    create_instance(
                        f"r1-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-1",
                    )
                    for h in range(4)
                ),
            ],
        )
        cloud4 = self._replica_cloud({"rep-0": range(1)}, include_head=True)
        sched4 = MagicMock()
        sched4.schedule.return_value = SchedulingReply(
            to_launch=[LaunchRequest(id="l-2", instance_type="tpu", count=4)]
        )
        sub4.clear()
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im4,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(),
            scheduler=sched4,
            autoscaling_config=cfg_cap8,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud4,
        )
        assert sub4.events == []

        # 5. Teardown/timed-out replicas (TERMINATING, ALLOCATION_TIMEOUT) stay
        # protected in assigned_reps so neither their own REQUESTED siblings nor
        # unrelated batches bind lingering pods onto them; a multi-replica batch
        # still claims its second fresh replica, and stale RAY_STOPPED duplicates
        # shadowed by an active RAY_RUNNING pod do not block filling a live slice.
        _, td_store, _, td_im = self._setup_multihost_reconcile_env(
            "affinity_teardown",
            [
                # req-a1 (rep-term) has 1 TERMINATING, 2 ALLOCATED, and 1 REQUESTED
                # sibling; req-a2 is the second replica's own 4-slot REQUESTED batch.
                create_instance(
                    "t0",
                    status=Instance.TERMINATING,
                    instance_type="tpu",
                    cloud_instance_id="ct-0",
                    launch_request_id="req-a1",
                ),
                *(
                    create_instance(
                        f"t{h}",
                        status=Instance.ALLOCATED,
                        instance_type="tpu",
                        cloud_instance_id=f"ct-{h}",
                        launch_request_id="req-a1",
                    )
                    for h in range(1, 3)
                ),
                create_instance(
                    "ta-0",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="req-a1",
                    status_times=[(Instance.REQUESTED, 100)],
                ),
                *(
                    create_instance(
                        f"ta-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-a2",
                        status_times=[(Instance.REQUESTED, 100)],
                    )
                    for h in range(1, 5)
                ),
                # rep-timeout has an ALLOCATION_TIMEOUT pod from req-old.
                create_instance(
                    "to-0",
                    status=Instance.ALLOCATION_TIMEOUT,
                    instance_type="tpu",
                    cloud_instance_id="co-0",
                    launch_request_id="req-old",
                ),
                # req-b is an unrelated fresh batch.
                create_instance(
                    "b-0",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="req-b",
                    status_times=[(Instance.REQUESTED, 200)],
                ),
                # rep-live has a stale RAY_STOPPED row shadowed by live RAY_RUNNING
                # on the same cloud_instance_id cl-0 -> remains fillable by req-live.
                create_instance(
                    "l-stale",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu",
                    cloud_instance_id="cl-0",
                    launch_request_id="req-live",
                ),
                create_instance(
                    "l-0",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="cl-0",
                    ray_node_id="nl-0",
                    launch_request_id="req-live",
                ),
                create_instance(
                    "l-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="req-live",
                    status_times=[(Instance.REQUESTED, 300)],
                ),
            ],
        )
        td_cloud = {
            "ct-0": CloudInstance(
                "ct-0", "tpu", NodeKind.WORKER, True, replica_id="rep-term"
            ),
            "ct-1": CloudInstance(
                "ct-1", "tpu", NodeKind.WORKER, True, replica_id="rep-term"
            ),
            "ct-2": CloudInstance(
                "ct-2", "tpu", NodeKind.WORKER, True, replica_id="rep-term"
            ),
            "ct-3": CloudInstance(
                "ct-3", "tpu", NodeKind.WORKER, True, replica_id="rep-term"
            ),
            "co-0": CloudInstance(
                "co-0", "tpu", NodeKind.WORKER, True, replica_id="rep-timeout"
            ),
            "co-1": CloudInstance(
                "co-1", "tpu", NodeKind.WORKER, True, replica_id="rep-timeout"
            ),
            "ca2-0": CloudInstance(
                "ca2-0", "tpu", NodeKind.WORKER, True, replica_id="rep-a2"
            ),
            "cl-0": CloudInstance(
                "cl-0", "tpu", NodeKind.WORKER, True, replica_id="rep-live"
            ),
            "cl-1": CloudInstance(
                "cl-1", "tpu", NodeKind.WORKER, True, replica_id="rep-live"
            ),
        }
        Reconciler._handle_cloud_instance_allocation(
            td_im, td_cloud, [], autoscaling_config=cfg
        )
        td_insts, _ = td_store.get_instances()
        assigned_cids_after = {
            i.cloud_instance_id for i in td_insts.values() if i.cloud_instance_id
        }
        # Neither lingering ct-3 (TERMINATING rep-term) nor co-1 (ALLOCATION_TIMEOUT
        # rep-timeout) may be claimed by any REQUESTED instance.
        assert "ct-3" not in assigned_cids_after
        assert "co-1" not in assigned_cids_after
        # ta-0 (same replica batch as TERMINATING t0) fails to ALLOCATION_FAILED,
        # while second replica req-a2 skips rep-term and claims fresh rep-a2.
        assert td_insts["ta-0"].status == Instance.ALLOCATION_FAILED
        assert any(
            td_insts[f"ta-{h}"].cloud_instance_id == "ca2-0" for h in range(1, 5)
        )
        # req-b stays REQUESTED rather than stealing co-1 or ct-3.
        assert td_insts["b-0"].status == Instance.REQUESTED
        # Live rep-live with shadowed stale RAY_STOPPED still binds cl-1.
        assert td_insts["l-1"].cloud_instance_id == "cl-1"

        # 6. Exhaustive disjoint partition of all non-UNKNOWN InstanceStatus values.
        buckets = [
            _TERMINAL_INSTANCE_STATUSES,
            _UNALLOCATED_INSTANCE_STATUSES,
            _BOOTING_INSTANCE_STATUSES,
            {Instance.RAY_RUNNING},
            _DRAINING_INSTANCE_STATUSES,
            _TEARDOWN_INSTANCE_STATUSES,
        ]
        all_non_unknown = {
            v for v in Instance.InstanceStatus.values() if v != Instance.UNKNOWN
        }
        assert sum(len(b) for b in buckets) == len(all_non_unknown)
        assert set().union(*buckets) == all_non_unknown

    def test_multihost_healed_and_non_fillable_allocation_isolation(self):
        cfg, _, _, _ = self._setup_multihost_reconcile_env("cfg_heal", [])
        cfg_cap8 = MockAutoscalingConfig(
            configs={
                "node_type_configs": {
                    "head": NodeTypeConfig(
                        name="head",
                        resources={},
                        min_worker_nodes=0,
                        max_worker_nodes=0,
                    ),
                    "tpu": NodeTypeConfig(
                        name="tpu",
                        resources={"CPU": 4, "TPU": 4},
                        min_worker_nodes=0,
                        max_worker_nodes=8,
                        replica_config=WorkerReplicaConfig(workers_per_replica=4),
                    ),
                },
                "max_concurrent_launches": 16,
            }
        )
        # 7. Sub-replica (< size) stale REQUESTED leftover after a vanished replica
        # cannot claim a fresh replica; a valid 4-pod batch claims it instead.
        _, sr_store, _, sr_im = self._setup_multihost_reconcile_env(
            "stale_partial_leftover",
            [
                create_instance(
                    "old-0",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="cold-0",
                    launch_request_id="req-stale",
                ),
                create_instance(
                    "old-1",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="cold-1",
                    launch_request_id="req-stale",
                ),
                *(
                    create_instance(
                        f"sr-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-stale",
                        status_times=[(Instance.REQUESTED, 50)],
                    )
                    for h in range(2)
                ),
                *(
                    create_instance(
                        f"fr-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-fresh",
                        status_times=[(Instance.REQUESTED, 100)],
                    )
                    for h in range(4)
                ),
            ],
        )
        sr_cloud = {
            "cf-0": CloudInstance(
                "cf-0", "tpu", NodeKind.WORKER, True, replica_id="rep-fresh"
            )
        }
        Reconciler._handle_cloud_instance_allocation(
            sr_im, sr_cloud, [], autoscaling_config=cfg
        )
        sr_insts, _ = sr_store.get_instances()
        assert all(sr_insts[f"sr-{h}"].status == Instance.REQUESTED for h in range(2))
        assert any(sr_insts[f"fr-{h}"].cloud_instance_id == "cf-0" for h in range(4))

        # 8. Two replica batches: rep-1 already bound 4/4 pods (one later
        # preempted) while rep-2 is still booting; healed pod on rep-1 is never
        # stolen by rep-2 even after rep-2 binds its first pod in the same tick.
        _, mr_store, _, mr_im = self._setup_multihost_reconcile_env(
            "multirep_healed",
            [
                *(
                    create_instance(
                        f"r1-{h}",
                        status=Instance.RAY_RUNNING,
                        instance_type="tpu",
                        cloud_instance_id=f"c1-{h}",
                        ray_node_id=f"n1-{h}",
                        launch_request_id="req-r1",
                    )
                    for h in range(3)
                ),
                create_instance(
                    "r1-3",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="c1-3",
                    launch_request_id="req-r1",
                ),
                *(
                    create_instance(
                        f"r2-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-r2",
                        status_times=[(Instance.REQUESTED, 100 + h)],
                    )
                    for h in range(4)
                ),
            ],
        )
        mr_cloud = {
            **{
                f"c1-{h}": CloudInstance(
                    f"c1-{h}", "tpu", NodeKind.WORKER, True, replica_id="rep-1"
                )
                for h in range(3)
            },
            # Place healed pod last so reverse iteration sees it first on every slot.
            "c2-0": CloudInstance(
                "c2-0", "tpu", NodeKind.WORKER, True, replica_id="rep-2"
            ),
            "c2-1": CloudInstance(
                "c2-1", "tpu", NodeKind.WORKER, True, replica_id="rep-2"
            ),
            "c1-healed": CloudInstance(
                "c1-healed", "tpu", NodeKind.WORKER, True, replica_id="rep-1"
            ),
        }
        Reconciler._handle_cloud_instance_allocation(
            mr_im, mr_cloud, [], autoscaling_config=cfg
        )
        mr_insts, _ = mr_store.get_instances()
        r2_cids = {
            mr_insts[f"r2-{h}"].cloud_instance_id
            for h in range(4)
            if mr_insts[f"r2-{h}"].cloud_instance_id
        }
        assert r2_cids == {"c2-0", "c2-1"}
        assert "c1-healed" not in r2_cids

        # 9. Raylet restart mid-replica-boot: old tagged row became TERMINATED while
        # an untagged RAY_RUNNING row owns the same live pod -> sibling still binds.
        _, rr_store, _, rr_im = self._setup_multihost_reconcile_env(
            "raylet_restart_boot",
            [
                create_instance(
                    "rr-old",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="cr-0",
                    launch_request_id="req-rr",
                ),
                create_instance(
                    "rr-new",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="cr-0",
                    ray_node_id="nr-0",
                ),
                create_instance(
                    "rr-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="req-rr",
                    status_times=[(Instance.REQUESTED, 100)],
                ),
            ],
        )
        rr_cloud = {
            "cr-0": CloudInstance(
                "cr-0", "tpu", NodeKind.WORKER, True, replica_id="rep-rr"
            ),
            "cr-1": CloudInstance(
                "cr-1", "tpu", NodeKind.WORKER, True, replica_id="rep-rr"
            ),
        }
        Reconciler._handle_cloud_instance_allocation(
            rr_im, rr_cloud, [], autoscaling_config=cfg
        )
        rr_insts, _ = rr_store.get_instances()
        assert rr_insts["rr-1"].cloud_instance_id == "cr-1"

        # 10. ALLOCATION_TIMEOUT/RAY_INSTALL_FAILED replica is non-fillable in
        # _scale_cluster and _filter_partial_replica_terminations: it drains
        # immediately despite same-batch REQUESTED slots and occupies full size.
        _, _, sub_to, im_to = self._setup_multihost_reconcile_env(
            "non_fillable_timeout",
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                ),
                create_instance(
                    "to-run-0",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="cto-0",
                    ray_node_id="nto-0",
                    launch_request_id="req-to1",
                ),
                create_instance(
                    "to-run-1",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="cto-1",
                    ray_node_id="nto-1",
                    launch_request_id="req-to1",
                ),
                create_instance(
                    "to-fail-2",
                    status=Instance.ALLOCATION_TIMEOUT,
                    instance_type="tpu",
                    cloud_instance_id="cto-2",
                    launch_request_id="req-to1",
                ),
                *(
                    create_instance(
                        f"to-req-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="req-to2",
                    )
                    for h in range(4)
                ),
            ],
        )
        cloud_to = {
            "c-head": CloudInstance("c-head", "head", NodeKind.HEAD, True),
            **{
                f"cto-{h}": CloudInstance(
                    f"cto-{h}", "tpu", NodeKind.WORKER, True, replica_id="rep-to"
                )
                for h in range(3)
            },
        }
        sched_to = MagicMock()
        sched_to.schedule.return_value = SchedulingReply(
            to_terminate=[
                TerminationRequest(
                    id=f"tto-{h}",
                    instance_id=f"to-run-{h}",
                    ray_node_id=f"nto-{h}",
                    instance_type="tpu",
                    instance_status=Instance.RAY_RUNNING,
                    cause=TerminationRequest.Cause.IDLE,
                )
                for h in range(2)
            ],
            to_launch=[LaunchRequest(id="lto-1", instance_type="tpu", count=4)],
        )
        sub_to.clear()
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im_to,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(),
            scheduler=sched_to,
            autoscaling_config=cfg_cap8,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud_to,
        )
        assert sorted(
            (e.instance_id, e.new_instance_status) for e in sub_to.events
        ) == [
            ("to-run-0", Instance.RAY_STOP_REQUESTED),
            ("to-run-1", Instance.RAY_STOP_REQUESTED),
        ]

    def test_multihost_allocation_orphan_cleanup_and_unlabeled_fallback_guards(self):
        """Surplus REQUESTED rows fail immediately when their claimed replica is
        already full via ray-node adoption, and Step 3 unlabeled fallback never
        steals from claimed labeled batches or LaunchNodeError batches."""
        # 1. Orphaned REQUESTED row fails immediately when claimed replica already
        # holds size assigned pods (e.g. second pod adopted via untagged ray node),
        # while an incomplete claimed replica keeps its REQUESTED row waiting.
        cfg, store, _, im = self._setup_multihost_reconcile_env(
            "orphan_adopted_cleanup",
            [
                create_instance(
                    "a-0",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="ca-0",
                    launch_request_id="lid-a",
                ),
                create_instance(
                    "a-adopted",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="ca-1",
                    ray_node_id="na-1",
                ),
                create_instance(
                    "a-orphan",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-a",
                    status_times=[(Instance.REQUESTED, 10)],
                ),
                create_instance(
                    "b-0",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="cb-0",
                    launch_request_id="lid-b",
                ),
                create_instance(
                    "b-wait",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-b",
                    status_times=[(Instance.REQUESTED, 20)],
                ),
            ],
            max_worker_nodes=4,
            workers_per_replica=2,
        )
        Reconciler._handle_cloud_instance_allocation(
            im,
            {
                "ca-0": CloudInstance(
                    "ca-0", "tpu", NodeKind.WORKER, True, replica_id="rep-a"
                ),
                "ca-1": CloudInstance(
                    "ca-1", "tpu", NodeKind.WORKER, True, replica_id="rep-a"
                ),
                "cb-0": CloudInstance(
                    "cb-0", "tpu", NodeKind.WORKER, True, replica_id="rep-b"
                ),
            },
            [],
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        assert insts["a-orphan"].status == Instance.ALLOCATION_FAILED
        assert insts["b-wait"].status == Instance.REQUESTED

        # 2. Step 3 unlabeled pod fallback skips both a claimed labeled batch and
        # a batch with a LaunchNodeError, binding only to an unclaimed healthy batch.
        _, u_store, _, u_im = self._setup_multihost_reconcile_env(
            "unlabeled_fallback_guards",
            [
                create_instance(
                    "c-0",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="cc-0",
                    launch_request_id="lid-claimed",
                ),
                create_instance(
                    "c-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-claimed",
                    status_times=[(Instance.REQUESTED, 10)],
                ),
                create_instance(
                    "e-0",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-err",
                    status_times=[(Instance.REQUESTED, 20)],
                ),
                create_instance(
                    "e-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-err",
                    status_times=[(Instance.REQUESTED, 21)],
                ),
                create_instance(
                    "f-0",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-free",
                    status_times=[(Instance.REQUESTED, 30)],
                ),
                create_instance(
                    "f-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-free",
                    status_times=[(Instance.REQUESTED, 31)],
                ),
            ],
            max_worker_nodes=8,
            workers_per_replica=2,
        )
        Reconciler._handle_cloud_instance_allocation(
            u_im,
            {
                "cc-0": CloudInstance(
                    "cc-0", "tpu", NodeKind.WORKER, True, replica_id="rep-c"
                ),
                "u-0": CloudInstance("u-0", "tpu", NodeKind.WORKER, True),
            },
            [
                LaunchNodeError(
                    request_id="lid-err",
                    node_type="tpu",
                    count=2,
                    timestamp_ns=1,
                    details="quota",
                )
            ],
            autoscaling_config=cfg,
        )
        u_insts, _ = u_store.get_instances()
        assert u_insts["c-1"].status == Instance.REQUESTED
        assert u_insts["e-0"].status == Instance.ALLOCATION_FAILED
        assert u_insts["e-1"].status == Instance.ALLOCATION_FAILED
        assert {u_insts["f-0"].cloud_instance_id, u_insts["f-1"].cloud_instance_id} == {
            "u-0",
            "",
        }

    def test_multihost_replica_id_persistence_and_per_replica_accounting(self):
        # 1. Per-replica launch_request_id preserves vanished-pod accounting
        # across pod disappearance before siblings arrive, and GC retains
        # terminal siblings while their replica batch has unallocated rows.
        cfg2, store, _, im = self._setup_multihost_reconcile_env(
            "persist_rid",
            [
                create_instance(
                    "r1-0",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-a",
                    status_times=[(Instance.REQUESTED, 10)],
                ),
                create_instance(
                    "r1-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-a",
                    status_times=[(Instance.REQUESTED, 11)],
                ),
                create_instance(
                    "done-0",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="c-done",
                    launch_request_id="lid-done",
                    status_times=[(Instance.TERMINATED, 1)],
                ),
            ],
            max_worker_nodes=4,
            workers_per_replica=2,
        )
        Reconciler._handle_cloud_instance_allocation(
            im,
            {
                "c1-0": CloudInstance(
                    "c1-0", "tpu", NodeKind.WORKER, True, replica_id="rep-1"
                )
            },
            [],
            autoscaling_config=cfg2,
        )
        # Pod c1-0 disappears before c1-1 arrives -> r1-0 transitions to TERMINATED
        # while staying protected from GC until lid-a finishes.
        Reconciler._handle_cloud_instance_terminated(im, {})
        Reconciler._gc_terminated_instances(
            im, InstanceReconcileConfig(terminated_instance_gc_retention_s=0), []
        )
        insts, _ = store.get_instances()
        assert insts["r1-0"].status == Instance.TERMINATED
        assert "done-0" not in insts
        # Sibling c1-1 arrives later and binds to r1-1 via vanished_cids_by_lid.
        Reconciler._handle_cloud_instance_allocation(
            im,
            {
                "c1-1": CloudInstance(
                    "c1-1", "tpu", NodeKind.WORKER, True, replica_id="rep-1"
                )
            },
            [],
            autoscaling_config=cfg2,
        )
        insts, _ = store.get_instances()
        assert insts["r1-1"].cloud_instance_id == "c1-1"

        # 2. Healed pod on booting replica rep-1 (already bound 2/2, 1 evicted)
        # is never stolen by another replica batch even while c1-0 is ALLOCATED.
        _, boot_store, _, boot_im = self._setup_multihost_reconcile_env(
            "boot_heal",
            [
                create_instance(
                    "r1-0",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="c1-0",
                    launch_request_id="lid-r1",
                ),
                create_instance(
                    "r1-1",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="c1-1",
                    launch_request_id="lid-r1",
                ),
                create_instance(
                    "r2-0",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-r2",
                    status_times=[(Instance.REQUESTED, 20)],
                ),
                create_instance(
                    "r2-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-r2",
                    status_times=[(Instance.REQUESTED, 21)],
                ),
            ],
            max_worker_nodes=4,
            workers_per_replica=2,
        )
        Reconciler._handle_cloud_instance_allocation(
            boot_im,
            {
                "c1-0": CloudInstance(
                    "c1-0", "tpu", NodeKind.WORKER, True, replica_id="rep-1"
                ),
                "c1-healed": CloudInstance(
                    "c1-healed", "tpu", NodeKind.WORKER, True, replica_id="rep-1"
                ),
                "c2-0": CloudInstance(
                    "c2-0", "tpu", NodeKind.WORKER, True, replica_id="rep-2"
                ),
            },
            [],
            autoscaling_config=cfg2,
        )
        boot_insts, _ = boot_store.get_instances()
        assert boot_insts["r2-0"].cloud_instance_id == "c2-0"
        assert boot_insts["r2-1"].status == Instance.REQUESTED

        # 3. Vanished replica rep-a1 and non-fillable rep-timeout never corrupt
        # per-replica owed counts for other replica batches.
        _, m3_store, _, m3_im = self._setup_multihost_reconcile_env(
            "multi_vanished",
            [
                create_instance(
                    "a1-0",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="ca1-0",
                    launch_request_id="lid-a1",
                ),
                create_instance(
                    "a1-1",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="ca1-1",
                    launch_request_id="lid-a1",
                ),
                create_instance(
                    "a2-0",
                    status=Instance.ALLOCATION_TIMEOUT,
                    instance_type="tpu",
                    cloud_instance_id="ca2-0",
                    launch_request_id="lid-a2",
                ),
                create_instance(
                    "a3-0",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="ca3-0",
                    ray_node_id="na30",
                    launch_request_id="lid-a3",
                ),
                create_instance(
                    "a3-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-a3",
                    status_times=[(Instance.REQUESTED, 10)],
                ),
                create_instance(
                    "b-0",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-b",
                    status_times=[(Instance.REQUESTED, 20)],
                ),
                create_instance(
                    "b-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-b",
                    status_times=[(Instance.REQUESTED, 21)],
                ),
            ],
            max_worker_nodes=8,
            workers_per_replica=2,
        )
        Reconciler._handle_cloud_instance_allocation(
            m3_im,
            {
                "ca2-0": CloudInstance(
                    "ca2-0", "tpu", NodeKind.WORKER, True, replica_id="rep-a2"
                ),
                "ca3-0": CloudInstance(
                    "ca3-0", "tpu", NodeKind.WORKER, True, replica_id="rep-a3"
                ),
                "ca3-1": CloudInstance(
                    "ca3-1", "tpu", NodeKind.WORKER, True, replica_id="rep-a3"
                ),
                "cb-0": CloudInstance(
                    "cb-0", "tpu", NodeKind.WORKER, True, replica_id="rep-b"
                ),
            },
            [],
            autoscaling_config=cfg2,
        )
        m3_insts, _ = m3_store.get_instances()
        assert m3_insts["a3-1"].cloud_instance_id == "ca3-1"
        assert m3_insts["b-0"].cloud_instance_id == "cb-0"

        # 4. Evicted replica rep-1 (2/2 previously bound) drains immediately and
        # occupies its full replica slot in _scale_cluster without absorbing
        # sibling REQUESTED slots meant for rep-2.
        _, _, sub6, im6 = self._setup_multihost_reconcile_env(
            "evicted_drain_and_cap",
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                ),
                create_instance(
                    "r1-0",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="c1-0",
                    ray_node_id="n10",
                    launch_request_id="lid-r1",
                ),
                create_instance(
                    "r1-1",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="c1-1",
                    launch_request_id="lid-r1",
                ),
                create_instance(
                    "r2-0",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-r2",
                ),
                create_instance(
                    "r2-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-r2",
                ),
            ],
            max_worker_nodes=4,
            workers_per_replica=2,
        )
        sched6 = MagicMock()
        sched6.schedule.return_value = SchedulingReply(
            to_terminate=[
                TerminationRequest(
                    id="t1",
                    instance_id="r1-0",
                    ray_node_id="n10",
                    instance_type="tpu",
                    instance_status=Instance.RAY_RUNNING,
                    cause=TerminationRequest.Cause.IDLE,
                )
            ],
            to_launch=[LaunchRequest(id="l1", instance_type="tpu", count=2)],
        )
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im6,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(),
            scheduler=sched6,
            autoscaling_config=cfg2,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances={
                "c-head": CloudInstance("c-head", "head", NodeKind.HEAD, True),
                "c1-0": CloudInstance(
                    "c1-0", "tpu", NodeKind.WORKER, True, replica_id="rep-1"
                ),
            },
        )
        assert [(e.instance_id, e.new_instance_status) for e in sub6.events] == [
            ("r1-0", Instance.RAY_STOP_REQUESTED)
        ]

        # 6. Torn-down historical replica retained within GC window never blocks
        # a later scale-up that reuses the same replica_id across multiple ticks.
        _, reuse_store, _, reuse_im = self._setup_multihost_reconcile_env(
            "reuse_replica_id",
            [
                create_instance(
                    "old-0",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="cold-0",
                    launch_request_id="lid-old",
                    status_times=[
                        (Instance.ALLOCATED, 1),
                        (Instance.TERMINATING, 2),
                        (Instance.TERMINATED, 3),
                    ],
                ),
                create_instance(
                    "old-1",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="cold-1",
                    launch_request_id="lid-old",
                    status_times=[
                        (Instance.ALLOCATED, 1),
                        (Instance.TERMINATING, 2),
                        (Instance.TERMINATED, 3),
                    ],
                ),
                create_instance(
                    "new-0",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-new",
                    status_times=[(Instance.REQUESTED, 100)],
                ),
                create_instance(
                    "new-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-new",
                    status_times=[(Instance.REQUESTED, 101)],
                ),
            ],
            max_worker_nodes=4,
            workers_per_replica=2,
        )
        Reconciler._handle_cloud_instance_allocation(
            reuse_im,
            {
                "cnew-0": CloudInstance(
                    "cnew-0", "tpu", NodeKind.WORKER, True, replica_id="rep-0"
                )
            },
            [],
            autoscaling_config=cfg2,
        )
        Reconciler._handle_cloud_instance_allocation(
            reuse_im,
            {
                "cnew-0": CloudInstance(
                    "cnew-0", "tpu", NodeKind.WORKER, True, replica_id="rep-0"
                ),
                "cnew-1": CloudInstance(
                    "cnew-1", "tpu", NodeKind.WORKER, True, replica_id="rep-0"
                ),
            },
            [],
            autoscaling_config=cfg2,
        )
        reuse_insts, _ = reuse_store.get_instances()
        assert reuse_insts["new-0"].cloud_instance_id == "cnew-0"
        assert reuse_insts["new-1"].cloud_instance_id == "cnew-1"

        # 7. Mid-boot preemption of one pod while another remains live keeps
        # owed slots aligned so remaining pods cpnew-2 and cpnew-3 still bind.
        cfg4, preempt_store, _, preempt_im = self._setup_multihost_reconcile_env(
            "mid_boot_preempt",
            [
                *(
                    create_instance(
                        f"p-new-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="lid-p",
                        status_times=[(Instance.REQUESTED, 10 + h)],
                    )
                    for h in range(4)
                ),
            ],
            max_worker_nodes=8,
            workers_per_replica=4,
        )
        Reconciler._handle_cloud_instance_allocation(
            preempt_im,
            {
                "cpnew-0": CloudInstance(
                    "cpnew-0", "tpu", NodeKind.WORKER, True, replica_id="rep-0"
                ),
                "cpnew-1": CloudInstance(
                    "cpnew-1", "tpu", NodeKind.WORKER, True, replica_id="rep-0"
                ),
            },
            [],
            autoscaling_config=cfg4,
        )
        Reconciler._handle_cloud_instance_terminated(
            preempt_im,
            {
                "cpnew-0": CloudInstance(
                    "cpnew-0", "tpu", NodeKind.WORKER, True, replica_id="rep-0"
                )
            },
        )
        Reconciler._handle_cloud_instance_allocation(
            preempt_im,
            {
                "cpnew-0": CloudInstance(
                    "cpnew-0", "tpu", NodeKind.WORKER, True, replica_id="rep-0"
                ),
                "cpnew-2": CloudInstance(
                    "cpnew-2", "tpu", NodeKind.WORKER, True, replica_id="rep-0"
                ),
                "cpnew-3": CloudInstance(
                    "cpnew-3", "tpu", NodeKind.WORKER, True, replica_id="rep-0"
                ),
            },
            [],
            autoscaling_config=cfg4,
        )
        preempt_insts, _ = preempt_store.get_instances()
        assert {preempt_insts[f"p-new-{h}"].cloud_instance_id for h in range(4)} == {
            "cpnew-0",
            "cpnew-1",
            "cpnew-2",
            "cpnew-3",
        }

    def test_multihost_vanished_mid_boot_reattach_and_teardown_cleanup(self):
        # 8. Sole allocated pod evicted mid-boot (no live pods yet): staggered
        # remaining pods reattach across ticks, fresh full batches take priority
        # over partial-vanished batches, and unclaimed vanished pods count toward
        # _scale_cluster max_worker_nodes occupancy clamp.
        cfg5, mid2_store, mid2_sub, mid2_im = self._setup_multihost_reconcile_env(
            "vanished_mid_boot_reattach",
            [
                create_instance(
                    "head",
                    status=Instance.RAY_RUNNING,
                    instance_type="head",
                    node_kind=NodeKind.HEAD,
                    cloud_instance_id="c-head",
                ),
                create_instance(
                    "d-new-0",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="cdnew-0",
                    launch_request_id="lid-d",
                    status_times=[
                        (Instance.ALLOCATED, 100),
                        (Instance.TERMINATED, 105),
                    ],
                ),
                *(
                    create_instance(
                        f"d-new-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="lid-d",
                        status_times=[(Instance.REQUESTED, 90 + h)],
                    )
                    for h in range(1, 4)
                ),
                *(
                    create_instance(
                        f"fresh-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="lid-fresh",
                        status_times=[(Instance.REQUESTED, 200 + h)],
                    )
                    for h in range(4)
                ),
            ],
            max_worker_nodes=8,
            workers_per_replica=4,
        )
        # Occupancy clamp sees lid-d (3 REQUESTED + 1 vanished = 4) + lid-fresh (4)
        # == 8 == max_worker_nodes, so an extra 4-worker launch is clamped to 0.
        sched_clamp = MagicMock()
        sched_clamp.schedule.return_value = SchedulingReply(
            to_launch=[LaunchRequest(id="extra", instance_type="tpu", count=4)]
        )
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=mid2_im,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ClusterResourceState(),
            scheduler=sched_clamp,
            autoscaling_config=cfg5,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances={
                "c-head": CloudInstance("c-head", "head", NodeKind.HEAD, True)
            },
        )
        assert mid2_sub.events == []

        # Tick 1: one pod of rep-fresh arrives first. Even though lid-d has an
        # older REQUESTED timestamp (90 vs 200), lid-fresh is sorted first and
        # claims rep-fresh without lid-d stealing it.
        Reconciler._handle_cloud_instance_allocation(
            mid2_im,
            {
                "cfresh-0": CloudInstance(
                    "cfresh-0", "tpu", NodeKind.WORKER, True, replica_id="rep-fresh"
                ),
            },
            [],
            autoscaling_config=cfg5,
        )
        # Tick 2: remaining pods of rep-fresh and staggered first pod cdnew-1 of
        # rep-0 arrive -> lid-d reattaches to rep-0 via vanished_slots.
        Reconciler._handle_cloud_instance_allocation(
            mid2_im,
            {
                **{
                    f"cfresh-{h}": CloudInstance(
                        f"cfresh-{h}",
                        "tpu",
                        NodeKind.WORKER,
                        True,
                        replica_id="rep-fresh",
                    )
                    for h in range(4)
                },
                "cdnew-1": CloudInstance(
                    "cdnew-1", "tpu", NodeKind.WORKER, True, replica_id="rep-0"
                ),
            },
            [],
            autoscaling_config=cfg5,
        )
        # Tick 3: remaining pods cdnew-2 and cdnew-3 arrive and finish rep-0.
        Reconciler._handle_cloud_instance_allocation(
            mid2_im,
            {
                **{
                    f"cfresh-{h}": CloudInstance(
                        f"cfresh-{h}",
                        "tpu",
                        NodeKind.WORKER,
                        True,
                        replica_id="rep-fresh",
                    )
                    for h in range(4)
                },
                **{
                    f"cdnew-{h}": CloudInstance(
                        f"cdnew-{h}",
                        "tpu",
                        NodeKind.WORKER,
                        True,
                        replica_id="rep-0",
                    )
                    for h in range(1, 4)
                },
            },
            [],
            autoscaling_config=cfg5,
        )
        mid2_insts, _ = mid2_store.get_instances()
        assert {mid2_insts[f"d-new-{h}"].cloud_instance_id for h in range(4)} == {
            "cdnew-0",
            "cdnew-1",
            "cdnew-2",
            "cdnew-3",
        }
        assert {mid2_insts[f"fresh-{h}"].cloud_instance_id for h in range(4)} == {
            f"cfresh-{h}" for h in range(4)
        }

        # Same-tick simultaneous arrival: full 4-pod rep-fresh and 3-pod rep-0
        # arrive together; cand_counts >= avail_req routes rep-fresh to 4-slot
        # lid-fresh and rep-0 to 3-slot partial-vanished lid-d.
        _, sim_store, _, sim_im = self._setup_multihost_reconcile_env(
            "vanished_same_tick",
            [
                create_instance(
                    "sd-0",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="csd-0",
                    launch_request_id="lid-sd",
                    status_times=[
                        (Instance.ALLOCATED, 100),
                        (Instance.TERMINATED, 105),
                    ],
                ),
                *(
                    create_instance(
                        f"sd-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="lid-sd",
                        status_times=[(Instance.REQUESTED, 90 + h)],
                    )
                    for h in range(1, 4)
                ),
                *(
                    create_instance(
                        f"sf-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id="lid-sf",
                        status_times=[(Instance.REQUESTED, 200 + h)],
                    )
                    for h in range(4)
                ),
            ],
            max_worker_nodes=8,
            workers_per_replica=4,
        )
        Reconciler._handle_cloud_instance_allocation(
            sim_im,
            {
                **{
                    f"csf-{h}": CloudInstance(
                        f"csf-{h}", "tpu", NodeKind.WORKER, True, replica_id="rep-sf"
                    )
                    for h in range(4)
                },
                **{
                    f"csd-{h}": CloudInstance(
                        f"csd-{h}", "tpu", NodeKind.WORKER, True, replica_id="rep-sd"
                    )
                    for h in range(1, 4)
                },
            },
            [],
            autoscaling_config=cfg5,
        )
        sim_insts, _ = sim_store.get_instances()
        assert {sim_insts[f"sf-{h}"].cloud_instance_id for h in range(4)} == {
            f"csf-{h}" for h in range(4)
        }
        assert {sim_insts[f"sd-{h}"].cloud_instance_id for h in range(4)} == {
            f"csd-{h}" for h in range(4)
        }

        # 9. Torn-down partial batch fails unallocated REQUESTED -> ALLOCATION_FAILED
        # in _terminate_instances (including when triggered by an untagged pod on
        # the same incomplete replica), while RAY_STOP_REQUESTED does NOT poison siblings.
        _, td_store, _, td_im = self._setup_multihost_reconcile_env(
            "teardown_sibling_cleanup",
            [
                create_instance(
                    "a-stop",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu",
                    cloud_instance_id="ca-0",
                    launch_request_id="lid-td",
                    status_times=[
                        (Instance.RAY_RUNNING, 1),
                        (Instance.RAY_STOPPED, 2),
                    ],
                ),
                create_instance(
                    "a-req",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-td",
                ),
                # Untagged stopped pod on rep-untag alongside tagged sibling
                # u-alloc (lid-untag) and unallocated u-req (lid-untag).
                create_instance(
                    "u-stop",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu",
                    cloud_instance_id="cu-0",
                    status_times=[
                        (Instance.RAY_RUNNING, 1),
                        (Instance.RAY_STOPPED, 2),
                    ],
                ),
                create_instance(
                    "u-alloc",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="cu-1",
                    launch_request_id="lid-untag",
                ),
                create_instance(
                    "u-req",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-untag",
                ),
                create_instance(
                    "b-drain",
                    status=Instance.RAY_STOP_REQUESTED,
                    instance_type="tpu",
                    cloud_instance_id="cb-0",
                    ray_node_id="nb-0",
                    launch_request_id="lid-drain",
                ),
                create_instance(
                    "b-req",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-drain",
                ),
            ],
            max_worker_nodes=12,
            workers_per_replica=4,
        )
        Reconciler._terminate_instances(
            td_im,
            autoscaling_config=cfg5,
            non_terminated_cloud_instances={
                "ca-0": CloudInstance(
                    "ca-0", "tpu", NodeKind.WORKER, True, replica_id="rep-td"
                ),
                "cu-0": CloudInstance(
                    "cu-0", "tpu", NodeKind.WORKER, True, replica_id="rep-untag"
                ),
                "cu-1": CloudInstance(
                    "cu-1", "tpu", NodeKind.WORKER, True, replica_id="rep-untag"
                ),
                "cb-0": CloudInstance(
                    "cb-0", "tpu", NodeKind.WORKER, True, replica_id="rep-drain"
                ),
            },
        )
        td_insts, _ = td_store.get_instances()
        assert td_insts["a-stop"].status == Instance.TERMINATING
        assert td_insts["a-req"].status == Instance.ALLOCATION_FAILED
        assert td_insts["u-stop"].status == Instance.TERMINATING
        assert td_insts["u-alloc"].status == Instance.TERMINATING
        assert td_insts["u-req"].status == Instance.ALLOCATION_FAILED
        assert td_insts["b-req"].status == Instance.REQUESTED

    def test_multihost_atomic_request_timeout_fails_batch_together(self):
        """Multi-worker REQUESTED batches wait until all remaining REQUESTED rows
        time out, then fail atomically to ALLOCATION_FAILED instead of requeuing
        to QUEUED, while single-host REQUESTED rows still requeue to QUEUED."""
        now_ns = time.time_ns()
        timeout_s = InstanceReconcileConfig().request_status_timeout_s
        expired_ns = now_ns - (timeout_s + 60) * 10**9
        fresh_ns = now_ns - 5 * 10**9

        cfg = MockAutoscalingConfig(
            configs={
                "node_type_configs": {
                    "tpu": NodeTypeConfig(
                        name="tpu",
                        resources={"CPU": 4, "TPU": 4},
                        min_worker_nodes=0,
                        max_worker_nodes=8,
                        replica_config=WorkerReplicaConfig(workers_per_replica=4),
                    ),
                    "cpu": NodeTypeConfig(
                        name="cpu",
                        resources={"CPU": 4},
                        min_worker_nodes=0,
                        max_worker_nodes=4,
                    ),
                }
            }
        )
        store = InstanceStorage(cluster_id="atomic_timeout", storage=InMemoryStorage())
        im = InstanceManager(store, instance_status_update_subscribers=[])
        TestReconciler._add_instances(
            store,
            [
                # Batch lid-split straddles timeout (2 expired, 2 fresh): must NOT split.
                create_instance(
                    "s-0",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-split",
                    status_times=[(Instance.REQUESTED, expired_ns)],
                ),
                create_instance(
                    "s-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-split",
                    status_times=[(Instance.REQUESTED, expired_ns)],
                ),
                create_instance(
                    "s-2",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-split",
                    status_times=[(Instance.REQUESTED, fresh_ns)],
                ),
                create_instance(
                    "s-3",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-split",
                    status_times=[(Instance.REQUESTED, fresh_ns)],
                ),
                # Batch lid-expired has 2 allocated + 2 expired REQUESTED: both fail
                # atomically to ALLOCATION_FAILED (never QUEUED).
                create_instance(
                    "e-0",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="ce-0",
                    launch_request_id="lid-expired",
                ),
                create_instance(
                    "e-1",
                    status=Instance.ALLOCATED,
                    instance_type="tpu",
                    cloud_instance_id="ce-1",
                    launch_request_id="lid-expired",
                ),
                create_instance(
                    "e-2",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-expired",
                    status_times=[(Instance.REQUESTED, expired_ns)],
                ),
                create_instance(
                    "e-3",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-expired",
                    status_times=[(Instance.REQUESTED, expired_ns)],
                ),
                # Single-host expired REQUESTED row still requeues to QUEUED.
                create_instance(
                    "c-0",
                    status=Instance.REQUESTED,
                    instance_type="cpu",
                    launch_request_id="lid-cpu",
                    status_times=[(Instance.REQUESTED, expired_ns)],
                ),
            ],
        )
        Reconciler._handle_stuck_instances(
            im,
            reconcile_config=cfg.get_instance_reconcile_config(),
            _logger=MagicMock(),
            replica_sizes={"tpu": 4},
        )
        insts, _ = store.get_instances()
        assert all(insts[f"s-{h}"].status == Instance.REQUESTED for h in range(4))
        assert insts["e-2"].status == Instance.ALLOCATION_FAILED
        assert insts["e-3"].status == Instance.ALLOCATION_FAILED
        assert insts["c-0"].status == Instance.QUEUED

    def test_multihost_already_terminating_replica_sweeps_late_siblings(self):
        """A replica that already has a TERMINATING member immediately transitions
        late-arriving or drain-rejected siblings and skips solicited RAY_STOPPED
        holds, while leaving RAY_STOP_REQUESTED siblings and other replicas intact."""
        now_ns = time.time_ns()
        cfg = MockAutoscalingConfig(
            configs={
                "node_type_configs": {
                    "tpu": NodeTypeConfig(
                        name="tpu",
                        resources={"CPU": 4, "TPU": 4},
                        min_worker_nodes=0,
                        max_worker_nodes=8,
                        replica_config=WorkerReplicaConfig(workers_per_replica=4),
                    )
                }
            }
        )
        store = InstanceStorage(cluster_id="sweep_term", storage=InMemoryStorage())
        im = InstanceManager(store, instance_status_update_subscribers=[])
        TestReconciler._add_instances(
            store,
            [
                # rep-1 already has w-0 TERMINATING from a prior tick; late-arriving
                # w-1 (RAY_RUNNING) and solicited w-2 (RAY_STOPPED) must both enter
                # TERMINATING, while w-3 (RAY_STOP_REQUESTED) stays untouched.
                create_instance(
                    "w-0",
                    status=Instance.TERMINATING,
                    instance_type="tpu",
                    cloud_instance_id="c-0",
                ),
                create_instance(
                    "w-1",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="c-1",
                    ray_node_id="n-1",
                ),
                create_instance(
                    "w-2",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu",
                    cloud_instance_id="c-2",
                    status_times=[
                        (Instance.RAY_STOP_REQUESTED, now_ns - 2_000_000_000),
                        (Instance.RAY_STOPPED, now_ns - 1_000_000_000),
                    ],
                ),
                create_instance(
                    "w-3",
                    status=Instance.RAY_STOP_REQUESTED,
                    instance_type="tpu",
                    cloud_instance_id="c-3",
                    ray_node_id="n-3",
                ),
                # Negative counter-case: sibling replica rep-2 has no TERMINATING
                # member, so b-1 (solicited RAY_STOPPED) stays held while b-0 runs.
                create_instance(
                    "b-0",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="cb-0",
                    ray_node_id="nb-0",
                ),
                create_instance(
                    "b-1",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu",
                    cloud_instance_id="cb-1",
                    status_times=[
                        (Instance.RAY_STOP_REQUESTED, now_ns - 2_000_000_000),
                        (Instance.RAY_STOPPED, now_ns - 1_000_000_000),
                    ],
                ),
            ],
        )
        cloud = {
            **{
                f"c-{h}": CloudInstance(
                    f"c-{h}", "tpu", NodeKind.WORKER, True, replica_id="rep-1"
                )
                for h in range(4)
            },
            "cb-0": CloudInstance(
                "cb-0", "tpu", NodeKind.WORKER, True, replica_id="rep-2"
            ),
            "cb-1": CloudInstance(
                "cb-1", "tpu", NodeKind.WORKER, True, replica_id="rep-2"
            ),
        }
        Reconciler._terminate_instances(
            im,
            autoscaling_config=cfg,
            non_terminated_cloud_instances=cloud,
            replica_sizes={"tpu": 4},
        )
        insts, _ = store.get_instances()
        assert insts["w-0"].status == Instance.TERMINATING
        assert insts["w-1"].status == Instance.TERMINATING
        assert insts["w-2"].status == Instance.TERMINATING
        assert insts["w-3"].status == Instance.RAY_STOP_REQUESTED
        assert insts["b-0"].status == Instance.RAY_RUNNING
        assert insts["b-1"].status == Instance.RAY_STOPPED

    @staticmethod
    def _make_kuberay_e2e_env(
        cluster_id: str,
        worker_groups,
        node_type_configs,
        max_num_worker_nodes: int = 8,
        initial_pods=None,
    ):
        from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.cloud_provider import (
            KUBERAY_LABEL_KEY_REPLICA_NAME,
            KubeRayProvider,
        )
        from ray.autoscaler.v2.instance_manager.subscribers.cloud_instance_updater import (
            CloudInstanceUpdater,
        )
        from ray.autoscaler.v2.tests.test_node_provider import (
            MockKubernetesHttpApiClient,
        )
        from ray.tests.kuberay.test_autoscaling_config import get_basic_ray_cr

        class _StatefulK8sClient(MockKubernetesHttpApiClient):
            def __init__(self, pod_list, ray_cluster):
                super().__init__(pod_list, ray_cluster)
                self.get_calls = 0
                self.patch_calls = []

            def get(self, path: str):
                self.get_calls += 1
                return super().get(path)

            def patch(
                self,
                path: str,
                patches,
                content_type: str = "application/json-patch+json",
            ):
                self.patch_calls.append(list(patches))
                for op in patches:
                    parts = op["path"].split("/")
                    idx, field = int(parts[3]), parts[4]
                    self._ray_cluster["spec"]["workerGroupSpecs"][idx][field] = op[
                        "value"
                    ]
                return self._ray_cluster

        def _make_pod(name: str, group: str, rep: Optional[str] = None, kind="worker"):
            labels = {
                "ray.io/cluster": "test",
                "ray.io/node-type": kind,
                "ray.io/group": group,
            }
            if rep is not None:
                labels[KUBERAY_LABEL_KEY_REPLICA_NAME] = rep
            return {
                "metadata": {"name": name, "labels": labels},
                "status": {"containerStatuses": [{"state": {"running": {}}}]},
            }

        cr = get_basic_ray_cr()
        cr["spec"]["workerGroupSpecs"] = worker_groups
        pods = {
            "metadata": {"resourceVersion": "1"},
            "items": [_make_pod("head-0", "headgroup", kind="head")]
            + list(initial_pods or []),
        }
        client = _StatefulK8sClient(pods, cr)
        provider = KubeRayProvider(
            cluster_name="test",
            provider_config={"namespace": "default", "head_node_type": "headgroup"},
            gcs_client=MagicMock(),
            k8s_api_client=client,
        )
        cfg = MockAutoscalingConfig(
            configs={
                "node_type_configs": {
                    "headgroup": NodeTypeConfig(
                        name="headgroup",
                        resources={"CPU": 1},
                        min_worker_nodes=0,
                        max_worker_nodes=1,
                    ),
                    **node_type_configs,
                },
                "max_num_worker_nodes": max_num_worker_nodes,
                "upscaling_speed": 10.0,
            }
        )
        store = InstanceStorage(cluster_id=cluster_id, storage=InMemoryStorage())
        store.batch_upsert_instances(
            updates=[
                create_instance(
                    "head-inst",
                    status=Instance.RAY_RUNNING,
                    instance_type="headgroup",
                    cloud_instance_id="head-0",
                    ray_node_id=(b"h" * 28).hex(),
                    node_kind=NodeKind.HEAD,
                )
            ],
            expected_storage_version=0,
        )
        im = InstanceManager(
            instance_storage=store,
            instance_status_update_subscribers=[CloudInstanceUpdater(provider)],
        )
        head_state = ClusterResourceState(
            node_states=[
                NodeState(
                    node_id=b"h" * 28,
                    instance_id="head-0",
                    ray_node_type_name="headgroup",
                    status=NodeStatus.RUNNING,
                )
            ]
        )
        return cr, pods, client, provider, cfg, store, im, head_state, _make_pod

    def test_multihost_reconciler_kuberay_end_to_end_lifecycle(self):
        """End-to-end Reconciler + KubeRayProvider + CloudInstanceUpdater lifecycle:
        per-replica UUID assignment, single-sync multi-replica launch, atomic
        REQUESTED timeout + idempotent relaunch, and same-tick pending-delete deferral."""
        (
            cr,
            pods,
            client,
            provider,
            cfg,
            store,
            im,
            head_state,
            _pod,
        ) = self._make_kuberay_e2e_env(
            cluster_id="e2e_kuberay",
            worker_groups=[
                {
                    "groupName": "tpu",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 4,
                    "numOfHosts": 2,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            node_type_configs={
                "tpu": NodeTypeConfig(
                    name="tpu",
                    resources={"CPU": 4},
                    min_worker_nodes=0,
                    max_worker_nodes=8,
                    replica_config=WorkerReplicaConfig(workers_per_replica=2),
                )
            },
        )

        # Tick 1: Launch 2 replicas (4 workers). _handle_instances_launch assigns
        # 2 distinct per-replica UUIDs, and KubeRayProvider syncs once (1 CR GET
        # + 1 pod LIST) while patching replicas 0 -> 1 -> 2.
        sched = MagicMock()
        sched.schedule.return_value = SchedulingReply(
            to_launch=[LaunchRequest(id="l-2rep", instance_type="tpu", count=4)]
        )
        cloud = provider.get_non_terminated()
        client.get_calls = 0
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=cloud,
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        tpu_reqs = [i for i in insts.values() if i.instance_type == "tpu"]
        assert all(
            i.status == Instance.REQUESTED and i.launch_request_id for i in tpu_reqs
        )
        lids = {i.launch_request_id for i in tpu_reqs}
        assert len(tpu_reqs) == 4
        assert len(lids) == 2
        assert all(
            sum(1 for i in tpu_reqs if i.launch_request_id == lid) == 2 for lid in lids
        )
        assert client.get_calls == 2
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 2
        assert provider.poll_errors() == []

        # Tick 2: First replica boots 2 pods; second replica stays unclaimed, times
        # out atomically to ALLOCATION_FAILED, and when the scheduler re-requests it
        # after CR dropped externally to 1, KubeRay heals CR 1 -> 2 cleanly.
        pods["items"].extend(
            [_pod("r0-0", "tpu", "rep-0"), _pod("r0-1", "tpu", "rep-0")]
        )
        sched.schedule.return_value = SchedulingReply()
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, ver = store.get_instances()
        unclaimed = [
            i
            for i in insts.values()
            if i.instance_type == "tpu" and i.status == Instance.REQUESTED
        ]
        assert len(unclaimed) == 2
        expired_ns = (
            time.time_ns()
            - (cfg.get_instance_reconcile_config().request_status_timeout_s + 60)
            * 10**9
        )
        store.batch_upsert_instances(
            updates=[
                create_instance(
                    i.instance_id,
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id=i.launch_request_id,
                    status_times=[(Instance.REQUESTED, expired_ns)],
                )
                for i in unclaimed
            ],
            expected_storage_version=ver,
        )
        cr["spec"]["workerGroupSpecs"][0]["replicas"] = 1
        client.patch_calls.clear()
        sched.schedule.return_value = SchedulingReply(
            to_launch=[LaunchRequest(id="l-retry", instance_type="tpu", count=2)]
        )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        assert all(
            insts[i.instance_id].status == Instance.ALLOCATION_FAILED for i in unclaimed
        )
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 2
        assert len(client.patch_calls) == 1

        # Tick 3: Same-tick terminate of rep-0 defers new QUEUED tpu launch until
        # pending delete completes (zero LaunchNodeError).
        alloc_r0 = [
            i for i in insts.values() if i.cloud_instance_id in {"r0-0", "r0-1"}
        ]
        sched.schedule.return_value = SchedulingReply(
            to_terminate=[
                TerminationRequest(
                    id=f"term-{i.instance_id}",
                    instance_id=i.instance_id,
                    instance_type="tpu",
                    instance_status=i.status,
                    cause=TerminationRequest.Cause.OUTDATED,
                )
                for i in alloc_r0
            ],
            to_launch=[LaunchRequest(id="l-new", instance_type="tpu", count=2)],
        )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        queued_tpu = [
            i
            for i in insts.values()
            if i.instance_type == "tpu" and i.status == Instance.QUEUED
        ]
        assert len(queued_tpu) == 2
        assert provider.poll_errors() == []

    def test_kuberay_legacy_multihost_timeout_does_not_inflate_replicas(self):
        """A legacy multi-host group (numOfHosts=4, no replica_config) keeps
        spec.replicas=1 across repeated REQUESTED timeouts without inflating."""
        cr, _, _, provider, cfg, store, im, head_state, _ = self._make_kuberay_e2e_env(
            cluster_id="e2e_legacy_timeout",
            worker_groups=[
                {
                    "groupName": "tpu",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 4,
                    "numOfHosts": 4,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            node_type_configs={
                "tpu": NodeTypeConfig(
                    name="tpu",
                    resources={"CPU": 4},
                    min_worker_nodes=0,
                    max_worker_nodes=16,
                )
            },
            max_num_worker_nodes=16,
        )
        sched = MagicMock()
        sched.schedule.return_value = SchedulingReply(
            to_launch=[LaunchRequest(id="l-1", instance_type="tpu", count=4)]
        )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 1

        sched.schedule.return_value = SchedulingReply()
        expired_ns = (
            time.time_ns()
            - (cfg.get_instance_reconcile_config().request_status_timeout_s + 60)
            * 10**9
        )
        for _ in range(2):
            insts, ver = store.get_instances()
            reqs = [
                i
                for i in insts.values()
                if i.instance_type == "tpu" and i.status == Instance.REQUESTED
            ]
            assert len(reqs) == 4
            store.batch_upsert_instances(
                updates=[
                    create_instance(
                        i.instance_id,
                        status=Instance.REQUESTED,
                        instance_type="tpu",
                        launch_request_id=i.launch_request_id,
                        status_times=[(Instance.REQUESTED, expired_ns)],
                    )
                    for i in reqs
                ],
                expected_storage_version=ver,
            )
            Reconciler.reconcile(
                instance_manager=im,
                scheduler=sched,
                cloud_provider=provider,
                cloud_resource_monitor=CloudResourceMonitor(),
                ray_cluster_resource_state=head_state,
                non_terminated_cloud_instances=provider.get_non_terminated(),
                autoscaling_config=cfg,
            )
            assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 1

    def test_kuberay_unlabeled_multihost_idle_scaledown_converges(self):
        """Unlabeled multi-host pods (no replica-name label) converge to
        spec.replicas=0 even when RAY_STOPPED arrives across multiple ticks."""
        (
            cr,
            pods,
            _,
            provider,
            cfg,
            store,
            im,
            head_state,
            _pod,
        ) = self._make_kuberay_e2e_env(
            cluster_id="e2e_unlabeled_sd",
            worker_groups=[
                {
                    "groupName": "tpu",
                    "replicas": 1,
                    "minReplicas": 0,
                    "maxReplicas": 2,
                    "numOfHosts": 4,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            node_type_configs={
                "tpu": NodeTypeConfig(
                    name="tpu",
                    resources={"CPU": 4},
                    min_worker_nodes=0,
                    max_worker_nodes=8,
                    replica_config=WorkerReplicaConfig(workers_per_replica=4),
                )
            },
        )
        pods["items"].extend(_pod(f"u-{h}", "tpu", rep=None) for h in range(4))
        now_ns = time.time_ns()
        TestReconciler._add_instances(
            store,
            [
                *(
                    create_instance(
                        f"w-{h}",
                        status=Instance.RAY_STOPPED,
                        instance_type="tpu",
                        cloud_instance_id=f"u-{h}",
                        status_times=[
                            (Instance.RAY_RUNNING, now_ns - 3),
                            (Instance.RAY_STOP_REQUESTED, now_ns - 2),
                            (Instance.RAY_STOPPED, now_ns - 1),
                        ],
                    )
                    for h in range(3)
                ),
                create_instance(
                    "w-3",
                    status=Instance.RAY_STOPPING,
                    instance_type="tpu",
                    cloud_instance_id="u-3",
                ),
            ],
        )
        sched = MagicMock()
        sched.schedule.return_value = SchedulingReply()
        # Tick 1: first 3 stopped pods terminate -> spec.replicas drops 1 -> 0.
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 0
        # KubeRay deletes u-0..u-2 and clears workersToDelete; w-3 finishes stopping.
        pods["items"] = [
            p
            for p in pods["items"]
            if p["metadata"]["name"] not in {"u-0", "u-1", "u-2"}
        ]
        cr["spec"]["workerGroupSpecs"][0]["scaleStrategy"] = {"workersToDelete": []}
        insts, ver = store.get_instances()
        store.batch_upsert_instances(
            updates=[
                create_instance(
                    "w-3",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu",
                    cloud_instance_id="u-3",
                    status_times=[
                        (Instance.RAY_RUNNING, now_ns - 3),
                        (Instance.RAY_STOP_REQUESTED, now_ns - 2),
                        (Instance.RAY_STOPPED, now_ns - 1),
                    ],
                )
            ],
            expected_storage_version=ver,
        )
        # Tick 2: final pod u-3 terminates cleanly with replicas staying 0.
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 0
        assert cr["spec"]["workerGroupSpecs"][0]["scaleStrategy"][
            "workersToDelete"
        ] == ["u-3"]

    def test_kuberay_single_host_in_step_multi_tick_lifecycle(self):
        """Real KubeRayProvider + Reconciler single-host multi-tick lifecycle:
        consecutive launches before pods appear, REQUESTED timeout retry,
        terminate with launch in flight, and externally created pod adoption."""
        (
            cr,
            pods,
            _,
            provider,
            cfg,
            store,
            im,
            head_state,
            _pod,
        ) = self._make_kuberay_e2e_env(
            cluster_id="e2e_single_host",
            worker_groups=[
                {
                    "groupName": "cpu",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 5,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            node_type_configs={
                "cpu": NodeTypeConfig(
                    name="cpu",
                    resources={"CPU": 2},
                    min_worker_nodes=0,
                    max_worker_nodes=5,
                )
            },
            max_num_worker_nodes=5,
        )
        sched = MagicMock()
        # Tick 1 & Tick 2: Two consecutive launches before any pods appear -> replicas 1 -> 2.
        for expected_rep in (1, 2):
            sched.schedule.return_value = SchedulingReply(
                to_launch=[
                    LaunchRequest(id=f"l-{expected_rep}", instance_type="cpu", count=1)
                ]
            )
            Reconciler.reconcile(
                instance_manager=im,
                scheduler=sched,
                cloud_provider=provider,
                cloud_resource_monitor=CloudResourceMonitor(),
                ray_cluster_resource_state=head_state,
                non_terminated_cloud_instances=provider.get_non_terminated(),
                autoscaling_config=cfg,
            )
            assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == expected_rep

        # Tick 3: Expire reqs[1] while no new pod arrives -> single-host timeout
        # requeues and retries under the same launch_request_id without
        # over-scaling spec.replicas past 2.
        insts, ver = store.get_instances()
        reqs = [
            i
            for i in insts.values()
            if i.instance_type == "cpu" and i.status == Instance.REQUESTED
        ]
        retried_id = reqs[1].instance_id
        retried_lid = reqs[1].launch_request_id
        expired_ns = (
            time.time_ns()
            - (cfg.get_instance_reconcile_config().request_status_timeout_s + 60)
            * 10**9
        )
        store.batch_upsert_instances(
            updates=[
                create_instance(
                    retried_id,
                    status=Instance.REQUESTED,
                    instance_type="cpu",
                    launch_request_id=retried_lid,
                    status_times=[(Instance.REQUESTED, expired_ns)],
                )
            ],
            expected_storage_version=ver,
        )
        sched.schedule.return_value = SchedulingReply()
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        assert insts[retried_id].status == Instance.REQUESTED
        assert insts[retried_id].launch_request_id == retried_lid
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 2

        # Tick 3b: One pod appears and binds cleanly; replicas stays 2.
        pods["items"].append(_pod("cpu-0", "cpu"))
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 2

        # Tick 4: Terminate cpu-0 while a new cpu launch is queued -> pending
        # delete defers the new launch cleanly without LaunchNodeError.
        insts, _ = store.get_instances()
        cpu0 = next(i for i in insts.values() if i.cloud_instance_id == "cpu-0")
        sched.schedule.return_value = SchedulingReply(
            to_terminate=[
                TerminationRequest(
                    id="t-cpu0",
                    instance_id=cpu0.instance_id,
                    instance_type="cpu",
                    instance_status=cpu0.status,
                    cause=TerminationRequest.Cause.OUTDATED,
                )
            ],
            to_launch=[LaunchRequest(id="l-after-del", instance_type="cpu", count=1)],
        )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 1
        assert provider.poll_errors() == []
        insts, _ = store.get_instances()
        assert any(
            i.instance_type == "cpu" and i.status == Instance.QUEUED
            for i in insts.values()
        )

        # Tick 5: cpu-0 finishes deleting and an external KubeRay pod appears
        # alongside the fulfilled pod -> adopted cleanly and deferred launch runs.
        pods["items"] = [
            p for p in pods["items"] if p["metadata"]["name"] != "cpu-0"
        ] + [_pod("cpu-1", "cpu"), _pod("cpu-ext", "cpu")]
        cr["spec"]["workerGroupSpecs"][0]["scaleStrategy"] = {"workersToDelete": []}
        sched.schedule.return_value = SchedulingReply()
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        allocated_cids = {
            i.cloud_instance_id
            for i in insts.values()
            if i.instance_type == "cpu" and i.status == Instance.ALLOCATED
        }
        assert {"cpu-1", "cpu-ext"} <= allocated_cids
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 3
        assert provider.poll_errors() == []

    def test_kuberay_pod_loss_recreates_new_replica_end_to_end(self):
        """Matches kuberay#5526 pod-loss behavior: when a mid-boot replica loses
        a pod, KubeRay deletes the entire replica and creates a fresh replica with
        a new replica name and pod names; Ray recovers via the vanished-batch path."""
        (
            cr,
            pods,
            _,
            provider,
            cfg,
            store,
            im,
            head_state,
            _pod,
        ) = self._make_kuberay_e2e_env(
            cluster_id="e2e_kuberay_5526",
            worker_groups=[
                {
                    "groupName": "tpu",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 4,
                    "numOfHosts": 2,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            node_type_configs={
                "tpu": NodeTypeConfig(
                    name="tpu",
                    resources={"CPU": 4},
                    min_worker_nodes=0,
                    max_worker_nodes=8,
                    replica_config=WorkerReplicaConfig(workers_per_replica=2),
                )
            },
        )
        sched = MagicMock()
        # Tick 1: Request 1 replica (2 workers).
        sched.schedule.return_value = SchedulingReply(
            to_launch=[LaunchRequest(id="l-1", instance_type="tpu", count=2)]
        )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 1

        # Tick 2: First pod r0-0 of rep-0 materializes and binds.
        pods["items"].append(_pod("r0-0", "tpu", "rep-0"))
        sched.schedule.return_value = SchedulingReply()
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        # Tick 3: Pod loss causes KubeRay to delete all of rep-0.
        pods["items"] = [p for p in pods["items"] if p["metadata"]["name"] != "r0-0"]
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        # Tick 4: KubeRay creates replacement replica rep-1 with new pod names r1-0, r1-1.
        pods["items"].extend(
            [_pod("r1-0", "tpu", "rep-1"), _pod("r1-1", "tpu", "rep-1")]
        )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=head_state,
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        active_tpu = [
            i
            for i in insts.values()
            if i.instance_type == "tpu" and i.status == Instance.ALLOCATED
        ]
        assert {i.cloud_instance_id for i in active_tpu} == {"r1-0", "r1-1"}
        assert not any(
            i.instance_type == "tpu" and i.status == Instance.REQUESTED
            for i in insts.values()
        )
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 1
        assert provider.poll_errors() == []

    @pytest.mark.parametrize("replica_id", ["rep-0", None])
    @pytest.mark.parametrize("num_restarts", [1, 3])
    @pytest.mark.parametrize("max_replicas", [1, 2])
    @pytest.mark.parametrize(
        "teardown_status",
        [
            Instance.RAY_STOPPED,
            Instance.ALLOCATION_TIMEOUT,
            Instance.RAY_INSTALL_FAILED,
            Instance.TERMINATION_FAILED,
            Instance.RAY_STOPPING,
        ],
    )
    def test_kuberay_untagged_teardown_after_raylet_restart_cleans_siblings(
        self, replica_id, num_restarts, max_replicas, teardown_status
    ):
        """When a labeled or unlabeled pod undergoes 1x to 3x mid-boot raylet
        restarts (leaving prior rows TERMINATED without teardown history and an
        untagged successor owning the pod), entering any teardown status on the
        untagged row must fail sibling REQUESTED rows atomically, prevent zombie
        vanished_cids_by_lid, and allow a clean replacement launch without a
        wedge at maxReplicas=1 or 2x over-provisioning at maxReplicas=2."""
        (
            cr,
            pods,
            _,
            provider,
            cfg,
            store,
            im,
            head_state,
            _pod,
        ) = self._make_kuberay_e2e_env(
            cluster_id=(
                f"e2e_untagged_td_{replica_id}_{num_restarts}_"
                f"{max_replicas}_{teardown_status}"
            ),
            worker_groups=[
                {
                    "groupName": "tpu",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": max_replicas,
                    "numOfHosts": 4,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            node_type_configs={
                "tpu": NodeTypeConfig(
                    name="tpu",
                    resources={"CPU": 4},
                    min_worker_nodes=0,
                    max_worker_nodes=4 * max_replicas,
                    replica_config=WorkerReplicaConfig(workers_per_replica=4),
                )
            },
            max_num_worker_nodes=4 * max_replicas,
        )

        recorded_unclaimed = []
        orig_begin_step = provider.begin_reconcile_step

        def _spy_begin_step(
            known_cloud_instance_ids,
            single_host_in_flight,
            unclaimed_replicas_by_lid,
        ):
            recorded_unclaimed.append(
                {nt: dict(lids) for nt, lids in unclaimed_replicas_by_lid.items()}
            )
            return orig_begin_step(
                known_cloud_instance_ids,
                single_host_in_flight,
                unclaimed_replicas_by_lid,
            )

        provider.begin_reconcile_step = _spy_begin_step

        def _cluster_state(*extra_nodes):
            return ClusterResourceState(
                node_states=[head_state.node_states[0], *extra_nodes]
            )

        sched = MagicMock()

        # Tick 1: Launch 1 multi-host replica (4 workers).
        sched.schedule.return_value = SchedulingReply(
            to_launch=[LaunchRequest(id="l-1", instance_type="tpu", count=4)]
        )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=_cluster_state(),
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        tpu_reqs = [
            i
            for i in insts.values()
            if i.instance_type == "tpu" and i.status == Instance.REQUESTED
        ]
        assert len(tpu_reqs) == 4
        lids = {i.launch_request_id for i in tpu_reqs}
        assert len(lids) == 1
        lid1 = next(iter(lids))
        assert lid1
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 1
        assert provider.poll_errors() == []

        # Tick 2: Pod p0 boots and joins Ray; 3 siblings stay REQUESTED.
        pods["items"].append(_pod("p0", "tpu", replica_id))
        sched.schedule.return_value = SchedulingReply()
        ray_nodes = [bytes([ord("a") + i]) * 28 for i in range(num_restarts + 1)]
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=_cluster_state(
                NodeState(
                    node_id=ray_nodes[0],
                    instance_id="p0",
                    ray_node_type_name="tpu",
                    status=NodeStatus.RUNNING,
                )
            ),
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        p0_rows = [i for i in insts.values() if i.cloud_instance_id == "p0"]
        assert len(p0_rows) == 1
        orig_row_id = p0_rows[0].instance_id
        assert p0_rows[0].status == Instance.RAY_RUNNING
        assert p0_rows[0].launch_request_id == lid1
        assert (
            sum(
                1
                for i in insts.values()
                if i.instance_type == "tpu" and i.status == Instance.REQUESTED
            )
            == 3
        )
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 1

        # Tick 3: 1x or 3x mid-boot raylet restarts on p0. Each prior row cleans
        # up directly to TERMINATED without teardown history, leaving one active
        # untagged row owning p0.
        for restart_idx in range(1, num_restarts + 1):
            node_states = [
                NodeState(
                    node_id=ray_nodes[j],
                    instance_id="p0",
                    ray_node_type_name="tpu",
                    status=(
                        NodeStatus.RUNNING if j == restart_idx else NodeStatus.DEAD
                    ),
                )
                for j in range(restart_idx + 1)
            ]
            Reconciler.reconcile(
                instance_manager=im,
                scheduler=sched,
                cloud_provider=provider,
                cloud_resource_monitor=CloudResourceMonitor(),
                ray_cluster_resource_state=_cluster_state(*node_states),
                non_terminated_cloud_instances=provider.get_non_terminated(),
                autoscaling_config=cfg,
            )

        insts, ver = store.get_instances()
        assert insts[orig_row_id].status == Instance.TERMINATED
        assert all(
            h.instance_status != Instance.TERMINATING
            for h in insts[orig_row_id].status_history
        )
        active_p0_rows = [
            i
            for i in insts.values()
            if i.cloud_instance_id == "p0" and i.status != Instance.TERMINATED
        ]
        assert len(active_p0_rows) == 1
        restarted_row = active_p0_rows[0]
        restarted_id = restarted_row.instance_id
        assert restarted_row.status == Instance.RAY_RUNNING
        assert restarted_row.launch_request_id == ""
        assert (
            sum(
                1
                for i in insts.values()
                if i.cloud_instance_id == "p0" and i.status == Instance.TERMINATED
            )
            == num_restarts
        )
        assert (
            sum(
                1
                for i in insts.values()
                if i.instance_type == "tpu" and i.status == Instance.REQUESTED
            )
            == 3
        )
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 1
        assert provider.poll_errors() == []

        # Tick 4: Trigger the parametrized teardown status on the untagged row.
        dead_prior_nodes = [
            NodeState(
                node_id=ray_nodes[j],
                instance_id="p0",
                ray_node_type_name="tpu",
                status=NodeStatus.DEAD,
            )
            for j in range(num_restarts)
        ]
        all_dead_nodes = [
            *dead_prior_nodes,
            NodeState(
                node_id=ray_nodes[num_restarts],
                instance_id="p0",
                ray_node_type_name="tpu",
                status=NodeStatus.DEAD,
            ),
        ]

        if teardown_status == Instance.RAY_STOPPED:
            Reconciler.reconcile(
                instance_manager=im,
                scheduler=sched,
                cloud_provider=provider,
                cloud_resource_monitor=CloudResourceMonitor(),
                ray_cluster_resource_state=_cluster_state(*all_dead_nodes),
                non_terminated_cloud_instances=provider.get_non_terminated(),
                autoscaling_config=cfg,
            )
        elif teardown_status == Instance.RAY_STOPPING:
            draining_nodes = [
                *dead_prior_nodes,
                NodeState(
                    node_id=ray_nodes[num_restarts],
                    instance_id="p0",
                    ray_node_type_name="tpu",
                    status=NodeStatus.DRAINING,
                ),
            ]
            Reconciler.reconcile(
                instance_manager=im,
                scheduler=sched,
                cloud_provider=provider,
                cloud_resource_monitor=CloudResourceMonitor(),
                ray_cluster_resource_state=_cluster_state(*draining_nodes),
                non_terminated_cloud_instances=provider.get_non_terminated(),
                autoscaling_config=cfg,
            )
            insts, _ = store.get_instances()
            assert insts[restarted_id].status == Instance.RAY_STOPPING
            assert (
                sum(
                    1
                    for i in insts.values()
                    if i.instance_type == "tpu" and i.status == Instance.REQUESTED
                )
                == 0
            )
            assert (
                sum(
                    1
                    for i in insts.values()
                    if i.launch_request_id == lid1
                    and i.status == Instance.ALLOCATION_FAILED
                )
                == 3
            )
            # Follow-up tick: draining raylet stops, driving pod termination.
            Reconciler.reconcile(
                instance_manager=im,
                scheduler=sched,
                cloud_provider=provider,
                cloud_resource_monitor=CloudResourceMonitor(),
                ray_cluster_resource_state=_cluster_state(*all_dead_nodes),
                non_terminated_cloud_instances=provider.get_non_terminated(),
                autoscaling_config=cfg,
            )
        elif teardown_status == Instance.ALLOCATION_TIMEOUT:
            prior_hist = [
                (h.instance_status, h.timestamp_ns)
                for h in restarted_row.status_history
            ]
            now_ns = max(time.time_ns(), prior_hist[-1][1] + 1)
            ok, _ = store.batch_upsert_instances(
                updates=[
                    create_instance(
                        restarted_id,
                        status=Instance.ALLOCATION_TIMEOUT,
                        instance_type="tpu",
                        cloud_instance_id="p0",
                        launch_request_id="",
                        status_times=[
                            (Instance.ALLOCATED, prior_hist[0][1]),
                            (Instance.ALLOCATION_TIMEOUT, now_ns),
                        ],
                    )
                ],
                expected_storage_version=ver,
            )
            assert ok
            Reconciler.reconcile(
                instance_manager=im,
                scheduler=sched,
                cloud_provider=provider,
                cloud_resource_monitor=CloudResourceMonitor(),
                ray_cluster_resource_state=_cluster_state(*dead_prior_nodes),
                non_terminated_cloud_instances=provider.get_non_terminated(),
                autoscaling_config=cfg,
            )
        elif teardown_status == Instance.RAY_INSTALL_FAILED:
            prior_hist = [
                (h.instance_status, h.timestamp_ns)
                for h in restarted_row.status_history
            ]
            now_ns = max(time.time_ns(), prior_hist[-1][1] + 1)
            ok, _ = store.batch_upsert_instances(
                updates=[
                    create_instance(
                        restarted_id,
                        status=Instance.RAY_INSTALLING,
                        instance_type="tpu",
                        cloud_instance_id="p0",
                        launch_request_id="",
                        status_times=[
                            (Instance.ALLOCATED, prior_hist[0][1]),
                            (Instance.RAY_INSTALLING, now_ns),
                        ],
                    )
                ],
                expected_storage_version=ver,
            )
            assert ok
            Reconciler.reconcile(
                instance_manager=im,
                scheduler=sched,
                cloud_provider=provider,
                cloud_resource_monitor=CloudResourceMonitor(),
                ray_cluster_resource_state=_cluster_state(*dead_prior_nodes),
                non_terminated_cloud_instances=provider.get_non_terminated(),
                ray_install_errors=[
                    RayInstallError(
                        im_instance_id=restarted_id, details="install failed"
                    )
                ],
                autoscaling_config=cfg,
            )
        elif teardown_status == Instance.TERMINATION_FAILED:
            prior_hist = [
                (h.instance_status, h.timestamp_ns)
                for h in restarted_row.status_history
            ]
            now_ns = max(time.time_ns(), prior_hist[-1][1] + 1)
            ok, _ = store.batch_upsert_instances(
                updates=[
                    create_instance(
                        restarted_id,
                        status=Instance.TERMINATING,
                        instance_type="tpu",
                        cloud_instance_id="p0",
                        launch_request_id="",
                        ray_node_id=restarted_row.node_id,
                        status_times=[*prior_hist, (Instance.TERMINATING, now_ns)],
                    )
                ],
                expected_storage_version=ver,
            )
            assert ok
            Reconciler.reconcile(
                instance_manager=im,
                scheduler=sched,
                cloud_provider=provider,
                cloud_resource_monitor=CloudResourceMonitor(),
                ray_cluster_resource_state=_cluster_state(*all_dead_nodes),
                non_terminated_cloud_instances=provider.get_non_terminated(),
                cloud_provider_errors=[
                    TerminateNodeError(
                        cloud_instance_id="p0",
                        timestamp_ns=now_ns,
                        request_id="t-1",
                    )
                ],
                autoscaling_config=cfg,
            )

        insts, _ = store.get_instances()
        assert insts[restarted_id].status == Instance.TERMINATING
        assert (
            sum(
                1
                for i in insts.values()
                if i.instance_type == "tpu" and i.status == Instance.REQUESTED
            )
            == 0
        )
        failed_siblings = [
            i
            for i in insts.values()
            if i.instance_type == "tpu"
            and i.launch_request_id == lid1
            and i.status == Instance.ALLOCATION_FAILED
        ]
        assert len(failed_siblings) == 3
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 0
        assert cr["spec"]["workerGroupSpecs"][0]["scaleStrategy"][
            "workersToDelete"
        ] == ["p0"]
        assert provider.poll_errors() == []

        # Tick 5: KubeRay deletes p0 and clears workersToDelete; demand persists.
        # Replacement launches cleanly with 4 fresh REQUESTED rows and replicas=1
        # (never wedging at 0 for maxReplicas=1 or over-provisioning to 2 for
        # maxReplicas=2).
        pods["items"] = [p for p in pods["items"] if p["metadata"]["name"] != "p0"]
        cr["spec"]["workerGroupSpecs"][0]["scaleStrategy"] = {"workersToDelete": []}
        sched.schedule.return_value = SchedulingReply(
            to_launch=[LaunchRequest(id="l-2", instance_type="tpu", count=4)]
        )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=_cluster_state(),
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        assert insts[restarted_id].status == Instance.TERMINATED
        view = _build_multihost_replica_view(
            list(insts.values()), provider.get_non_terminated(), {"tpu": 4}
        )
        assert view.vanished_cids_by_lid == {}
        assert recorded_unclaimed[-1].get("tpu", {}) == {}
        fresh_reqs = [
            i
            for i in insts.values()
            if i.instance_type == "tpu" and i.status == Instance.REQUESTED
        ]
        assert len(fresh_reqs) == 4
        fresh_lids = {i.launch_request_id for i in fresh_reqs}
        assert len(fresh_lids) == 1
        assert next(iter(fresh_lids)) != lid1
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 1
        assert provider.poll_errors() == []

    @pytest.mark.parametrize("replica_id", ["rep-0", None])
    @pytest.mark.parametrize("max_replicas", [1, 2])
    def test_kuberay_untagged_teardown_post_delete_skipped_tick4_recovers(
        self, replica_id, max_replicas
    ):
        """If p0 disappears before sibling REQUESTED rows fail (e.g. autoscaler
        crash or skipped Tick 4 after an untagged restarted row entered
        TERMINATING), Tick 5 must still propagate teardown for both labeled and
        unlabeled pods, fail all 3 siblings, export zero phantom unclaimed
        replicas, and launch a clean replacement."""
        (
            cr,
            pods,
            _,
            provider,
            cfg,
            store,
            im,
            head_state,
            _pod,
        ) = self._make_kuberay_e2e_env(
            cluster_id=f"e2e_untagged_post_del_{replica_id}_{max_replicas}",
            worker_groups=[
                {
                    "groupName": "tpu",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": max_replicas,
                    "numOfHosts": 4,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            node_type_configs={
                "tpu": NodeTypeConfig(
                    name="tpu",
                    resources={"CPU": 4},
                    min_worker_nodes=0,
                    max_worker_nodes=4 * max_replicas,
                    replica_config=WorkerReplicaConfig(workers_per_replica=4),
                )
            },
            max_num_worker_nodes=4 * max_replicas,
        )

        recorded_unclaimed = []
        orig_begin_step = provider.begin_reconcile_step

        def _spy_begin_step(
            known_cloud_instance_ids,
            single_host_in_flight,
            unclaimed_replicas_by_lid,
        ):
            recorded_unclaimed.append(
                {nt: dict(lids) for nt, lids in unclaimed_replicas_by_lid.items()}
            )
            return orig_begin_step(
                known_cloud_instance_ids,
                single_host_in_flight,
                unclaimed_replicas_by_lid,
            )

        provider.begin_reconcile_step = _spy_begin_step

        def _cluster_state(*extra_nodes):
            return ClusterResourceState(
                node_states=[head_state.node_states[0], *extra_nodes]
            )

        sched = MagicMock()

        # Tick 1: Launch 1 replica (lid1, 4 REQUESTED).
        sched.schedule.return_value = SchedulingReply(
            to_launch=[LaunchRequest(id="l-1", instance_type="tpu", count=4)]
        )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=_cluster_state(),
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        lid1 = next(
            i.launch_request_id
            for i in insts.values()
            if i.instance_type == "tpu" and i.status == Instance.REQUESTED
        )

        # Tick 2: p0 joins Ray; 3 siblings remain REQUESTED.
        pods["items"].append(_pod("p0", "tpu", replica_id))
        sched.schedule.return_value = SchedulingReply()
        ray_node_1 = b"a" * 28
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=_cluster_state(
                NodeState(
                    node_id=ray_node_1,
                    instance_id="p0",
                    ray_node_type_name="tpu",
                    status=NodeStatus.RUNNING,
                )
            ),
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )

        # Tick 3: Raylet restarts on p0 -> orig TERMINATED, untagged RAY_RUNNING.
        ray_node_2 = b"b" * 28
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=_cluster_state(
                NodeState(
                    node_id=ray_node_1,
                    instance_id="p0",
                    ray_node_type_name="tpu",
                    status=NodeStatus.DEAD,
                ),
                NodeState(
                    node_id=ray_node_2,
                    instance_id="p0",
                    ray_node_type_name="tpu",
                    status=NodeStatus.RUNNING,
                ),
            ),
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, ver = store.get_instances()
        restarted_row = next(
            i
            for i in insts.values()
            if i.cloud_instance_id == "p0" and i.status == Instance.RAY_RUNNING
        )
        restarted_id = restarted_row.instance_id
        assert restarted_row.launch_request_id == ""

        # Simulate skipped Tick 4: untagged row entered TERMINATING and p0 was
        # deleted from Kubernetes while the 3 sibling REQUESTED rows were not
        # yet transitioned to ALLOCATION_FAILED.
        prior_hist = [
            (h.instance_status, h.timestamp_ns) for h in restarted_row.status_history
        ]
        now_ns = max(time.time_ns(), prior_hist[-1][1] + 1)
        ok, _ = store.batch_upsert_instances(
            updates=[
                create_instance(
                    restarted_id,
                    status=Instance.TERMINATING,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                    ray_node_id=restarted_row.node_id,
                    status_times=[*prior_hist, (Instance.TERMINATING, now_ns)],
                )
            ],
            expected_storage_version=ver,
        )
        assert ok
        pods["items"] = [p for p in pods["items"] if p["metadata"]["name"] != "p0"]
        cr["spec"]["workerGroupSpecs"][0]["replicas"] = 0

        # Tick 5: Reconcile after p0 is already gone. Untagged row terminates,
        # all 3 lid1 siblings fail atomically, zero vanished_cids_by_lid or
        # phantom unclaimed replicas leak, and replacement launches cleanly.
        sched.schedule.return_value = SchedulingReply(
            to_launch=[LaunchRequest(id="l-2", instance_type="tpu", count=4)]
        )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=_cluster_state(),
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        assert insts[restarted_id].status == Instance.TERMINATED
        assert (
            sum(
                1
                for i in insts.values()
                if i.launch_request_id == lid1
                and i.status == Instance.ALLOCATION_FAILED
            )
            == 3
        )
        assert (
            sum(
                1
                for i in insts.values()
                if i.launch_request_id == lid1 and i.status == Instance.REQUESTED
            )
            == 0
        )
        view = _build_multihost_replica_view(
            list(insts.values()), provider.get_non_terminated(), {"tpu": 4}
        )
        assert view.vanished_cids_by_lid == {}
        assert recorded_unclaimed[-1].get("tpu", {}) == {}
        fresh_reqs = [
            i
            for i in insts.values()
            if i.instance_type == "tpu" and i.status == Instance.REQUESTED
        ]
        assert len(fresh_reqs) == 4
        assert {i.launch_request_id for i in fresh_reqs} != {lid1}
        assert cr["spec"]["workerGroupSpecs"][0]["replicas"] == 1
        assert provider.poll_errors() == []

    @pytest.mark.parametrize("replica_id", ["rep-0", None])
    def test_kuberay_untagged_same_tick_shadowed_stopped_and_draining_cleans_siblings(
        self, replica_id
    ):
        """Same-tick race: when a raylet restarts on p0 and the new raylet
        immediately reports DRAINING while the prior tagged row is still in
        shadowed RAY_STOPPED (not yet TERMINATED), teardown must propagate to
        lid1 and fail all 3 sibling REQUESTED rows in that same tick."""
        (
            cr,
            pods,
            _,
            provider,
            cfg,
            store,
            im,
            head_state,
            _pod,
        ) = self._make_kuberay_e2e_env(
            cluster_id=f"e2e_same_tick_drain_{replica_id}",
            worker_groups=[
                {
                    "groupName": "tpu",
                    "replicas": 0,
                    "minReplicas": 0,
                    "maxReplicas": 1,
                    "numOfHosts": 4,
                    "template": {"spec": {"containers": [{}]}},
                }
            ],
            node_type_configs={
                "tpu": NodeTypeConfig(
                    name="tpu",
                    resources={"CPU": 4},
                    min_worker_nodes=0,
                    max_worker_nodes=4,
                    replica_config=WorkerReplicaConfig(workers_per_replica=4),
                )
            },
            max_num_worker_nodes=4,
        )

        def _cluster_state(*extra_nodes):
            return ClusterResourceState(
                node_states=[head_state.node_states[0], *extra_nodes]
            )

        sched = MagicMock()

        # Tick 1: Launch 1 replica (lid1).
        sched.schedule.return_value = SchedulingReply(
            to_launch=[LaunchRequest(id="l-1", instance_type="tpu", count=4)]
        )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=_cluster_state(),
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        lid1 = next(
            i.launch_request_id
            for i in insts.values()
            if i.instance_type == "tpu" and i.status == Instance.REQUESTED
        )

        # Tick 2: Pod p0 joins Ray.
        pods["items"].append(_pod("p0", "tpu", replica_id))
        sched.schedule.return_value = SchedulingReply()
        ray_node_1 = b"a" * 28
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=_cluster_state(
                NodeState(
                    node_id=ray_node_1,
                    instance_id="p0",
                    ray_node_type_name="tpu",
                    status=NodeStatus.RUNNING,
                )
            ),
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        orig_row_id = next(
            i.instance_id for i in insts.values() if i.cloud_instance_id == "p0"
        )

        # Tick 3 (same-tick race): ray_node_1 reports DEAD while replacement
        # ray_node_2 on p0 immediately reports DRAINING.
        ray_node_2 = b"b" * 28
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=sched,
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=_cluster_state(
                NodeState(
                    node_id=ray_node_1,
                    instance_id="p0",
                    ray_node_type_name="tpu",
                    status=NodeStatus.DEAD,
                ),
                NodeState(
                    node_id=ray_node_2,
                    instance_id="p0",
                    ray_node_type_name="tpu",
                    status=NodeStatus.DRAINING,
                ),
            ),
            non_terminated_cloud_instances=provider.get_non_terminated(),
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        assert insts[orig_row_id].status == Instance.TERMINATED
        assert all(
            h.instance_status != Instance.TERMINATING
            for h in insts[orig_row_id].status_history
        )
        draining_rows = [
            i
            for i in insts.values()
            if i.cloud_instance_id == "p0" and i.instance_id != orig_row_id
        ]
        assert len(draining_rows) == 1
        assert draining_rows[0].status == Instance.RAY_STOPPING
        assert draining_rows[0].launch_request_id == ""
        assert (
            sum(
                1
                for i in insts.values()
                if i.instance_type == "tpu" and i.status == Instance.REQUESTED
            )
            == 0
        )
        assert (
            sum(
                1
                for i in insts.values()
                if i.launch_request_id == lid1
                and i.status == Instance.ALLOCATION_FAILED
            )
            == 3
        )
        assert provider.poll_errors() == []

    @pytest.mark.parametrize(
        "teardown_status",
        [
            Instance.RAY_STOPPED,
            Instance.ALLOCATION_TIMEOUT,
            Instance.RAY_INSTALL_FAILED,
            Instance.TERMINATION_FAILED,
            Instance.RAY_STOPPING,
        ],
    )
    @pytest.mark.parametrize(
        "replica_id,pod_present",
        [("rep-0", True), (None, True), (None, False)],
    )
    def test_multihost_replica_view_untagged_teardown_reattaches(
        self, teardown_status, replica_id, pod_present
    ):
        """All live pre-termination teardown statuses on an untagged row
        propagate teardown to the prior tagged batch for labeled pods,
        unlabeled pods, and pods already absent from cloud_instances."""
        cloud_instances = (
            {
                "p0": CloudInstance(
                    cloud_instance_id="p0",
                    node_type="tpu",
                    is_running=True,
                    node_kind=NodeKind.WORKER,
                    replica_id=replica_id,
                )
            }
            if pod_present
            else {}
        )
        now_ns = time.time_ns()

        view = _build_multihost_replica_view(
            [
                create_instance(
                    "i-orig",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-1",
                    status_times=[
                        (Instance.RAY_RUNNING, now_ns - 2),
                        (Instance.TERMINATED, now_ns - 1),
                    ],
                ),
                create_instance(
                    "i-untagged-td",
                    status=teardown_status,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                ),
                create_instance(
                    "i-sib",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-1",
                ),
            ],
            cloud_instances,
            {"tpu": 4},
        )
        assert ("tpu", "lid-1") in view.torn_down_lids
        assert ("tpu", "lid-1") not in view.vanished_cids_by_lid
        if pod_present and replica_id is not None:
            assert "lid-1" in view.rep_lids[("tpu", replica_id)]
            assert ("tpu", replica_id) in view.non_fillable_reps

    @pytest.mark.parametrize(
        "history_status",
        [
            Instance.TERMINATING,
            Instance.TERMINATION_FAILED,
            Instance.ALLOCATION_TIMEOUT,
            Instance.RAY_INSTALL_FAILED,
            Instance.RAY_STOPPING,
        ],
    )
    @pytest.mark.parametrize(
        "replica_id,pod_present",
        [("rep-0", True), (None, True), (None, False)],
    )
    def test_multihost_replica_view_terminal_untagged_teardown_propagates(
        self, history_status, replica_id, pod_present
    ):
        """An already-TERMINATED untagged row with any teardown history status
        must propagate teardown to its prior tagged launch batch and keep that
        batch out of vanished_cids_by_lid."""
        cloud_instances = (
            {
                "p0": CloudInstance(
                    cloud_instance_id="p0",
                    node_type="tpu",
                    is_running=True,
                    node_kind=NodeKind.WORKER,
                    replica_id=replica_id,
                )
            }
            if pod_present
            else {}
        )
        now_ns = time.time_ns()

        view = _build_multihost_replica_view(
            [
                create_instance(
                    "i-orig",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-1",
                    status_times=[
                        (Instance.RAY_RUNNING, now_ns - 4),
                        (Instance.TERMINATED, now_ns - 3),
                    ],
                ),
                create_instance(
                    "i-untagged-term-td",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                    status_times=[
                        (Instance.ALLOCATED, now_ns - 3),
                        (history_status, now_ns - 2),
                        (Instance.TERMINATED, now_ns - 1),
                    ],
                ),
                create_instance(
                    "i-sib",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-1",
                ),
            ],
            cloud_instances,
            {"tpu": 4},
        )
        assert ("tpu", "lid-1") in view.torn_down_lids
        assert ("tpu", "lid-1") not in view.vanished_cids_by_lid

    @pytest.mark.parametrize("replica_id", ["rep-0", None])
    def test_multihost_replica_view_shadowed_untagged_row_does_not_reattach(
        self, replica_id
    ):
        """A stale untagged RAY_STOPPED row shadowed by a tagged active row must
        not pollute untagged_active_cids, propagate teardown, or reattach an
        unrelated prior batch for either labeled or unlabeled pods."""
        cloud_instances = {
            "p0": CloudInstance(
                cloud_instance_id="p0",
                node_type="tpu",
                is_running=True,
                node_kind=NodeKind.WORKER,
                replica_id=replica_id,
            )
        }
        now_ns = time.time_ns()

        shadowed_view = _build_multihost_replica_view(
            [
                create_instance(
                    "i-old-term",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-old",
                    status_times=[
                        (Instance.RAY_RUNNING, now_ns - 3),
                        (Instance.TERMINATED, now_ns - 2),
                    ],
                ),
                create_instance(
                    "i-shadowed-untagged",
                    status=Instance.RAY_STOPPED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                ),
                create_instance(
                    "i-tagged-active",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-new",
                ),
            ],
            cloud_instances,
            {"tpu": 4},
        )
        if replica_id is not None:
            assert shadowed_view.rep_lids[("tpu", replica_id)] == {"lid-new"}
        assert ("tpu", "lid-old") not in shadowed_view.torn_down_lids
        assert ("tpu", "lid-new") not in shadowed_view.torn_down_lids

    @pytest.mark.parametrize("replica_id", [None, "rep-2"])
    def test_multihost_replica_view_reused_cid_and_non_teardown_isolation(
        self, replica_id
    ):
        """Verify incarnation-timestamp boundaries when a cloud_instance_id is
        reused across batches (B1/B2/B3) on unlabeled and labeled pods,
        multi-incarnation ordering, and that RAY_STOP_REQUESTED or external pod
        loss without teardown history do not falsely mark batches torn down."""
        live_cloud = {
            "p0": CloudInstance(
                cloud_instance_id="p0",
                node_type="tpu",
                is_running=True,
                node_kind=NodeKind.WORKER,
                replica_id=replica_id,
            )
        }

        # B1: Old incarnation (r1 + u1 terminal teardown) vs newer r2 reusing p0
        # even when r2 was REQUESTED before r1 allocated.
        b1_rows = [
            create_instance(
                "r1",
                status=Instance.TERMINATED,
                instance_type="tpu",
                cloud_instance_id="p0",
                launch_request_id="lid-1",
                status_times=[
                    (Instance.REQUESTED, 50),
                    (Instance.ALLOCATED, 100),
                    (Instance.RAY_RUNNING, 110),
                    (Instance.TERMINATED, 120),
                ],
            ),
            create_instance(
                "u1",
                status=Instance.TERMINATED,
                instance_type="tpu",
                cloud_instance_id="p0",
                launch_request_id="",
                status_times=[
                    (Instance.ALLOCATED, 120),
                    (Instance.TERMINATING, 140),
                    (Instance.TERMINATED, 150),
                ],
            ),
            create_instance(
                "r2",
                status=Instance.RAY_RUNNING,
                instance_type="tpu",
                cloud_instance_id="p0",
                launch_request_id="lid-2",
                status_times=[
                    (Instance.REQUESTED, 80),
                    (Instance.ALLOCATED, 200),
                    (Instance.RAY_RUNNING, 210),
                ],
            ),
            create_instance(
                "r2-req",
                status=Instance.REQUESTED,
                instance_type="tpu",
                launch_request_id="lid-2",
                status_times=[(Instance.REQUESTED, 80)],
            ),
        ]
        b1_view = _build_multihost_replica_view(b1_rows, live_cloud, {"tpu": 4})
        assert ("tpu", "lid-1") in b1_view.torn_down_lids
        assert ("tpu", "lid-2") not in b1_view.torn_down_lids
        if replica_id is not None:
            assert b1_view.rep_lids[("tpu", replica_id)] == {"lid-2"}
            assert ("tpu", "lid-1") not in b1_view.claimed_reps_by_lid

        # B2: Old r1 already GC'd while stale u1 terminal teardown stays in IM;
        # newer r2 reusing p0 must stay unaffected both live and if r2 vanishes.
        b2_rows = [r for r in b1_rows if r.instance_id != "r1"]
        b2_live = _build_multihost_replica_view(b2_rows, live_cloud, {"tpu": 4})
        assert ("tpu", "lid-2") not in b2_live.torn_down_lids
        b2_vanished = _build_multihost_replica_view(b2_rows, {}, {"tpu": 4})
        assert ("tpu", "lid-2") not in b2_vanished.torn_down_lids
        assert b2_vanished.vanished_cids_by_lid == {("tpu", "lid-2"): {"p0"}}

        # B3: Old non-teardown r1 stays in IM with open lid-1 REQUESTED while
        # newer r2 reuses p0, restarts raylet, and untagged u2 runs healthy or
        # tears down (live Tick 4 and post-delete Tick 5). Only lid-2 owns
        # replica_id and is marked torn down; lid-1 stays preserved in
        # vanished_cids_by_lid.
        b3_base = [
            create_instance(
                "r1",
                status=Instance.TERMINATED,
                instance_type="tpu",
                cloud_instance_id="p0",
                launch_request_id="lid-1",
                status_times=[
                    (Instance.ALLOCATED, 100),
                    (Instance.RAY_RUNNING, 110),
                    (Instance.TERMINATED, 120),
                ],
            ),
            create_instance(
                "r1-req",
                status=Instance.REQUESTED,
                instance_type="tpu",
                launch_request_id="lid-1",
            ),
            create_instance(
                "r2",
                status=Instance.TERMINATED,
                instance_type="tpu",
                cloud_instance_id="p0",
                launch_request_id="lid-2",
                status_times=[
                    (Instance.ALLOCATED, 200),
                    (Instance.RAY_RUNNING, 210),
                    (Instance.TERMINATED, 220),
                ],
            ),
            create_instance(
                "r2-req",
                status=Instance.REQUESTED,
                instance_type="tpu",
                launch_request_id="lid-2",
            ),
        ]
        b3_healthy_u2 = create_instance(
            "u2-healthy",
            status=Instance.RAY_RUNNING,
            instance_type="tpu",
            cloud_instance_id="p0",
            launch_request_id="",
            status_times=[
                (Instance.ALLOCATED, 220),
                (Instance.RAY_RUNNING, 230),
            ],
        )
        b3_healthy = _build_multihost_replica_view(
            [*b3_base, b3_healthy_u2], live_cloud, {"tpu": 4}
        )
        assert ("tpu", "lid-1") not in b3_healthy.torn_down_lids
        assert ("tpu", "lid-2") not in b3_healthy.torn_down_lids
        if replica_id is not None:
            assert b3_healthy.rep_lids[("tpu", replica_id)] == {"lid-2"}
            assert ("tpu", "lid-1") not in b3_healthy.claimed_reps_by_lid
            assert b3_healthy.vanished_cids_by_lid == {("tpu", "lid-1"): {"p0"}}

        b3_live_u2 = create_instance(
            "u2-live",
            status=Instance.RAY_STOPPED,
            instance_type="tpu",
            cloud_instance_id="p0",
            launch_request_id="",
            status_times=[
                (Instance.ALLOCATED, 220),
                (Instance.RAY_RUNNING, 230),
                (Instance.RAY_STOPPED, 240),
            ],
        )
        b3_tick4 = _build_multihost_replica_view(
            [*b3_base, b3_live_u2], live_cloud, {"tpu": 4}
        )
        assert ("tpu", "lid-2") in b3_tick4.torn_down_lids
        assert ("tpu", "lid-1") not in b3_tick4.torn_down_lids
        if replica_id is not None:
            assert b3_tick4.rep_lids[("tpu", replica_id)] == {"lid-2"}
            assert ("tpu", "lid-1") not in b3_tick4.claimed_reps_by_lid
            assert b3_tick4.vanished_cids_by_lid == {("tpu", "lid-1"): {"p0"}}

        b3_term_u2 = create_instance(
            "u2-term",
            status=Instance.TERMINATED,
            instance_type="tpu",
            cloud_instance_id="p0",
            launch_request_id="",
            status_times=[
                (Instance.ALLOCATED, 220),
                (Instance.TERMINATING, 240),
                (Instance.TERMINATED, 250),
            ],
        )
        b3_tick5 = _build_multihost_replica_view([*b3_base, b3_term_u2], {}, {"tpu": 4})
        assert ("tpu", "lid-2") in b3_tick5.torn_down_lids
        assert ("tpu", "lid-2") not in b3_tick5.vanished_cids_by_lid
        assert ("tpu", "lid-1") not in b3_tick5.torn_down_lids
        assert b3_tick5.vanished_cids_by_lid == {("tpu", "lid-1"): {"p0"}}

        # Multiple sequential torn-down incarnations on one reused cid propagate
        # to both batches regardless of IM row iteration order.
        multi_td_rows = [
            *b3_base,
            create_instance(
                "u1-term",
                status=Instance.TERMINATED,
                instance_type="tpu",
                cloud_instance_id="p0",
                launch_request_id="",
                status_times=[
                    (Instance.ALLOCATED, 120),
                    (Instance.TERMINATING, 140),
                    (Instance.TERMINATED, 150),
                ],
            ),
            b3_term_u2,
        ]
        for ordered in (multi_td_rows, list(reversed(multi_td_rows))):
            multi_view = _build_multihost_replica_view(ordered, {}, {"tpu": 4})
            assert ("tpu", "lid-1") in multi_view.torn_down_lids
            assert ("tpu", "lid-2") in multi_view.torn_down_lids
            assert multi_view.vanished_cids_by_lid == {}

        # Non-teardown states: RAY_STOP_REQUESTED and external pod loss without
        # teardown history must NOT mark lid-1 torn down.
        stop_req_view = _build_multihost_replica_view(
            [
                b3_base[0],
                b3_base[1],
                create_instance(
                    "u-stop-req",
                    status=Instance.RAY_STOP_REQUESTED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                    status_times=[
                        (Instance.ALLOCATED, 120),
                        (Instance.RAY_STOP_REQUESTED, 130),
                    ],
                ),
            ],
            live_cloud,
            {"tpu": 4},
        )
        assert ("tpu", "lid-1") not in stop_req_view.torn_down_lids

        ext_loss_view = _build_multihost_replica_view(
            [
                b3_base[0],
                b3_base[1],
                create_instance(
                    "u-ext-loss",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                    status_times=[
                        (Instance.ALLOCATED, 120),
                        (Instance.RAY_RUNNING, 130),
                        (Instance.TERMINATED, 140),
                    ],
                ),
            ],
            {},
            {"tpu": 4},
        )
        assert ("tpu", "lid-1") not in ext_loss_view.torn_down_lids
        assert ext_loss_view.vanished_cids_by_lid == {("tpu", "lid-1"): {"p0"}}

    def test_gc_retains_untagged_teardown_and_deletes_non_teardown_restarts(self):
        """When terminated_instance_gc_retention_s < request_status_timeout_s,
        an open launch batch retains its untagged terminal teardown row on the
        same cid while still collecting intermediate non-teardown restart rows
        and unrelated terminal rows, then collects everything in one pass once
        the batch closes."""
        now_ns = time.time_ns()
        expired_ns = now_ns - 60 * s_to_ns
        gc_cfg = InstanceReconcileConfig(
            terminated_instance_gc_retention_s=10,
            request_status_timeout_s=600,
        )
        _, store, _, im = self._setup_multihost_reconcile_env(
            "gc_untagged_td_open_batch",
            [
                create_instance(
                    "r0-req",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-0",
                    status_times=[(Instance.REQUESTED, now_ns - 5 * s_to_ns)],
                ),
                create_instance(
                    "r0",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-0",
                    status_times=[
                        (Instance.ALLOCATED, expired_ns - 30),
                        (Instance.TERMINATED, expired_ns - 20),
                    ],
                ),
                create_instance(
                    "u-mid",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                    status_times=[
                        (Instance.ALLOCATED, expired_ns - 20),
                        (Instance.RAY_RUNNING, expired_ns - 15),
                        (Instance.TERMINATED, expired_ns - 10),
                    ],
                ),
                create_instance(
                    "u-td",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                    status_times=[
                        (Instance.ALLOCATED, expired_ns - 10),
                        (Instance.TERMINATING, expired_ns - 5),
                        (Instance.TERMINATED, expired_ns),
                    ],
                ),
                create_instance(
                    "done-0",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p-other",
                    launch_request_id="lid-done",
                    status_times=[(Instance.TERMINATED, expired_ns)],
                ),
            ],
            max_worker_nodes=4,
            workers_per_replica=4,
        )

        Reconciler._gc_terminated_instances(im, gc_cfg, [])
        insts, ver = store.get_instances()
        assert "r0" in insts
        assert "u-td" in insts
        assert "u-mid" not in insts
        assert "done-0" not in insts

        # Seam check: surviving records still propagate teardown to lid-0.
        view = _build_multihost_replica_view(list(insts.values()), {}, {"tpu": 4})
        assert ("tpu", "lid-0") in view.torn_down_lids
        assert view.vanished_cids_by_lid == {}

        # Close lid-0: a single GC pass deletes all expired terminal rows.
        ok, _ = store.batch_upsert_instances(
            updates=[
                create_instance(
                    "r0-req",
                    status=Instance.ALLOCATION_FAILED,
                    instance_type="tpu",
                    launch_request_id="lid-0",
                    status_times=[
                        (Instance.REQUESTED, now_ns - 5 * s_to_ns),
                        (Instance.ALLOCATION_FAILED, now_ns),
                    ],
                )
            ],
            expected_storage_version=ver,
        )
        assert ok
        Reconciler._gc_terminated_instances(im, gc_cfg, [])
        insts, _ = store.get_instances()
        assert "r0" not in insts
        assert "u-td" not in insts

    @pytest.mark.parametrize("replica_id", [None, "rep-1"])
    @pytest.mark.parametrize(
        "u1_status",
        [Instance.TERMINATED, Instance.RAY_RUNNING, Instance.TERMINATING],
    )
    def test_gc_retains_reused_cid_tagged_guard_rows_for_open_batch(
        self, u1_status, replica_id
    ):
        """On a reused cid where older batch lid-0 is still open and newer
        closed batch lid-1 has an untagged teardown or live untagged successor
        on unlabeled or labeled pods, GC must retain r1 and u1 while lid-0 is
        open so u1 cannot hijack or poison lid-0, then delete all expired
        terminal rows in one pass once lid-0 closes."""
        now_ns = time.time_ns()
        expired_ns = now_ns - 60 * s_to_ns
        gc_cfg = InstanceReconcileConfig(
            terminated_instance_gc_retention_s=10,
            request_status_timeout_s=600,
        )
        if u1_status == Instance.TERMINATED:
            u1_times = [
                (Instance.ALLOCATED, expired_ns - 10),
                (Instance.TERMINATING, expired_ns - 5),
                (Instance.TERMINATED, expired_ns),
            ]
        elif u1_status == Instance.RAY_RUNNING:
            u1_times = [
                (Instance.ALLOCATED, expired_ns - 10),
                (Instance.RAY_RUNNING, expired_ns - 5),
            ]
        else:
            u1_times = [
                (Instance.ALLOCATED, expired_ns - 10),
                (Instance.TERMINATING, expired_ns - 5),
            ]

        _, store, _, im = self._setup_multihost_reconcile_env(
            f"gc_reused_cid_guard_{u1_status}_{replica_id}",
            [
                create_instance(
                    "r0-req",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-0",
                    status_times=[(Instance.REQUESTED, now_ns - 5 * s_to_ns)],
                ),
                create_instance(
                    "r0",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-0",
                    status_times=[
                        (Instance.ALLOCATED, expired_ns - 50),
                        (Instance.TERMINATED, expired_ns - 40),
                    ],
                ),
                create_instance(
                    "r1",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-1",
                    status_times=[
                        (Instance.ALLOCATED, expired_ns - 30),
                        (Instance.TERMINATED, expired_ns - 20),
                    ],
                ),
                create_instance(
                    "u-mid",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                    status_times=[
                        (Instance.ALLOCATED, expired_ns - 20),
                        (Instance.TERMINATED, expired_ns - 15),
                    ],
                ),
                create_instance(
                    "u1",
                    status=u1_status,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                    status_times=u1_times,
                ),
                create_instance(
                    "done-0",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p-other",
                    launch_request_id="lid-done",
                    status_times=[(Instance.TERMINATED, expired_ns)],
                ),
            ],
            max_worker_nodes=4,
            workers_per_replica=4,
        )

        Reconciler._gc_terminated_instances(im, gc_cfg, [])
        insts, ver = store.get_instances()
        assert "r0" in insts
        assert "r1" in insts
        assert "u1" in insts
        assert "u-mid" not in insts
        assert "done-0" not in insts

        # Seam check: u1's healthy state and teardown (immediate or subsequent
        # crash from RAY_RUNNING) isolate replica ownership and mark lid-1 only,
        # never hijacking or poisoning open lid-0.
        live_cloud = {
            "p0": CloudInstance(
                cloud_instance_id="p0",
                node_type="tpu",
                is_running=True,
                node_kind=NodeKind.WORKER,
                replica_id=replica_id,
            )
        }
        if u1_status == Instance.TERMINATED:
            view = _build_multihost_replica_view(list(insts.values()), {}, {"tpu": 4})
            assert ("tpu", "lid-1") in view.torn_down_lids
            assert ("tpu", "lid-0") not in view.torn_down_lids
            assert view.vanished_cids_by_lid == {("tpu", "lid-0"): {"p0"}}
            if replica_id is not None:
                r2_old = create_instance(
                    "r2-old",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-2",
                    status_times=[
                        (Instance.ALLOCATED, expired_ns + 5),
                        (Instance.TERMINATED, expired_ns + 8),
                    ],
                )
                u2_live = create_instance(
                    "u2-live",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                    status_times=[
                        (Instance.ALLOCATED, expired_ns + 8),
                        (Instance.RAY_RUNNING, expired_ns + 10),
                    ],
                )
                view_3rd = _build_multihost_replica_view(
                    [*insts.values(), r2_old, u2_live], live_cloud, {"tpu": 4}
                )
                assert view_3rd.rep_lids[("tpu", replica_id)] == {"lid-2"}
                assert ("tpu", "lid-0") not in view_3rd.claimed_reps_by_lid
                assert ("tpu", "lid-1") not in view_3rd.claimed_reps_by_lid
                assert ("tpu", "lid-1") in view_3rd.torn_down_lids
                assert ("tpu", "lid-0") not in view_3rd.torn_down_lids
                assert ("tpu", "lid-2") not in view_3rd.torn_down_lids
                assert view_3rd.vanished_cids_by_lid == {("tpu", "lid-0"): {"p0"}}
        else:
            if u1_status == Instance.RAY_RUNNING:
                healthy_view = _build_multihost_replica_view(
                    list(insts.values()), live_cloud, {"tpu": 4}
                )
                assert ("tpu", "lid-0") not in healthy_view.torn_down_lids
                assert ("tpu", "lid-1") not in healthy_view.torn_down_lids
                if replica_id is not None:
                    assert healthy_view.rep_lids[("tpu", replica_id)] == {"lid-1"}
                    assert ("tpu", "lid-0") not in healthy_view.claimed_reps_by_lid
                    assert healthy_view.vanished_cids_by_lid == {
                        ("tpu", "lid-0"): {"p0"}
                    }
            overrides = (
                {"u1": Instance.RAY_STOPPED}
                if u1_status == Instance.RAY_RUNNING
                else None
            )
            view = _build_multihost_replica_view(
                list(insts.values()),
                live_cloud,
                {"tpu": 4},
                status_overrides=overrides,
            )
            assert ("tpu", "lid-1") in view.torn_down_lids
            assert ("tpu", "lid-0") not in view.torn_down_lids
            if replica_id is not None:
                assert view.rep_lids[("tpu", replica_id)] == {"lid-1"}
                assert ("tpu", "lid-0") not in view.claimed_reps_by_lid

        # Close lid-0: a single GC pass deletes all expired terminal rows.
        ok, _ = store.batch_upsert_instances(
            updates=[
                create_instance(
                    "r0-req",
                    status=Instance.ALLOCATION_FAILED,
                    instance_type="tpu",
                    launch_request_id="lid-0",
                    status_times=[
                        (Instance.REQUESTED, now_ns - 5 * s_to_ns),
                        (Instance.ALLOCATION_FAILED, now_ns),
                    ],
                )
            ],
            expected_storage_version=ver,
        )
        assert ok
        Reconciler._gc_terminated_instances(im, gc_cfg, [])
        insts, _ = store.get_instances()
        assert "r0" not in insts
        assert "r1" not in insts
        if u1_status == Instance.TERMINATED:
            assert "u1" not in insts
        else:
            assert "u1" in insts

    @pytest.mark.parametrize("reused_replica_id", ["rep-0", "rep-2"])
    def test_multihost_reused_cid_healthy_pod_theft_e2e(self, reused_replica_id):
        """When a reused cloud_instance_id belongs to healthy labeled replica
        rep-0 or rep-2 via an untagged raylet-restart row (u2-0), older batch
        lid-1's superseded TERMINATED row must not hijack the replica or steal
        arriving sibling pod p1 from lid-2."""
        now_ns = time.time_ns()
        node_a = b"a" * 28
        node_b = b"b" * 28
        cfg, store, _, im = self._setup_multihost_reconcile_env(
            f"reused_cid_theft_{reused_replica_id}",
            [
                create_instance(
                    "r1-old",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-1",
                    status_times=[
                        (Instance.REQUESTED, now_ns - 55 * s_to_ns),
                        (Instance.ALLOCATED, now_ns - 50 * s_to_ns),
                        (Instance.TERMINATED, now_ns - 40 * s_to_ns),
                    ],
                ),
                create_instance(
                    "r1-req-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-1",
                    status_times=[(Instance.REQUESTED, now_ns - 55 * s_to_ns)],
                ),
                create_instance(
                    "r2-0",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-2",
                    status_times=[
                        (Instance.REQUESTED, now_ns - 35 * s_to_ns),
                        (Instance.ALLOCATED, now_ns - 30 * s_to_ns),
                        (Instance.TERMINATED, now_ns - 20 * s_to_ns),
                    ],
                ),
                create_instance(
                    "u2-0",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                    ray_node_id=binary_to_hex(node_a),
                    status_times=[
                        (Instance.ALLOCATED, now_ns - 20 * s_to_ns),
                        (Instance.RAY_RUNNING, now_ns - 15 * s_to_ns),
                    ],
                ),
                create_instance(
                    "r2-req-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-2",
                    status_times=[(Instance.REQUESTED, now_ns - 35 * s_to_ns)],
                ),
            ],
            max_worker_nodes=8,
            workers_per_replica=2,
        )
        cloud = {
            "p0": CloudInstance(
                "p0", "tpu", NodeKind.WORKER, True, replica_id=reused_replica_id
            ),
            "p1": CloudInstance(
                "p1", "tpu", NodeKind.WORKER, True, replica_id=reused_replica_id
            ),
        }
        provider = MagicMock()
        provider.get_node_types_with_pending_deletes.return_value = set()

        # Tick 1: Sibling pod p1 arrives on reused_replica_id and must bind to
        # lid-2's r2-req-1 without lid-1 stealing it or failing lid-2.
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=MockScheduler(),
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=ClusterResourceState(
                node_states=[
                    NodeState(
                        node_id=node_a,
                        instance_id="p0",
                        ray_node_type_name="tpu",
                        status=NodeStatus.RUNNING,
                    )
                ]
            ),
            non_terminated_cloud_instances=cloud,
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        assert insts["r2-req-1"].status == Instance.ALLOCATED
        assert insts["r2-req-1"].cloud_instance_id == "p1"
        assert insts["u2-0"].status == Instance.RAY_RUNNING
        assert insts["r1-req-1"].status == Instance.REQUESTED
        assert insts["r1-req-1"].cloud_instance_id == ""
        assert all(
            i.status != Instance.ALLOCATION_FAILED
            for i in insts.values()
            if i.launch_request_id == "lid-2"
        )

        # Tick 2: p1 joins Ray cleanly under lid-2 while lid-1 stays open.
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=MockScheduler(),
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=ClusterResourceState(
                node_states=[
                    NodeState(
                        node_id=node_a,
                        instance_id="p0",
                        ray_node_type_name="tpu",
                        status=NodeStatus.RUNNING,
                    ),
                    NodeState(
                        node_id=node_b,
                        instance_id="p1",
                        ray_node_type_name="tpu",
                        status=NodeStatus.RUNNING,
                    ),
                ]
            ),
            non_terminated_cloud_instances=cloud,
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        assert insts["r2-req-1"].status == Instance.RAY_RUNNING
        assert insts["r2-req-1"].cloud_instance_id == "p1"
        assert insts["u2-0"].status == Instance.RAY_RUNNING
        assert insts["r1-req-1"].status == Instance.REQUESTED

    @pytest.mark.parametrize(
        "scenario", ["healthy_full_replica", "teardown_on_other_replica"]
    )
    def test_multihost_reused_cid_does_not_fail_unrelated_batch_e2e(self, scenario):
        """A full healthy replica or a tearing-down untagged row on a reused
        cloud_instance_id must leave the older batch's REQUESTED rows intact
        instead of failing them to ALLOCATION_FAILED."""
        now_ns = time.time_ns()
        node_a = b"a" * 28
        node_b = b"b" * 28
        if scenario == "healthy_full_replica":
            u2 = create_instance(
                "u2-0",
                status=Instance.RAY_RUNNING,
                instance_type="tpu",
                cloud_instance_id="p0",
                launch_request_id="",
                ray_node_id=binary_to_hex(node_a),
                status_times=[
                    (Instance.ALLOCATED, now_ns - 20 * s_to_ns),
                    (Instance.RAY_RUNNING, now_ns - 15 * s_to_ns),
                ],
            )
            node_states = [
                NodeState(
                    node_id=node_a,
                    instance_id="p0",
                    ray_node_type_name="tpu",
                    status=NodeStatus.RUNNING,
                ),
                NodeState(
                    node_id=node_b,
                    instance_id="p1",
                    ray_node_type_name="tpu",
                    status=NodeStatus.RUNNING,
                ),
            ]
        else:
            u2 = create_instance(
                "u2-0",
                status=Instance.TERMINATING,
                instance_type="tpu",
                cloud_instance_id="p0",
                launch_request_id="",
                status_times=[
                    (Instance.ALLOCATED, now_ns - 20 * s_to_ns),
                    (Instance.TERMINATING, now_ns - 15 * s_to_ns),
                ],
            )
            node_states = [
                NodeState(
                    node_id=node_b,
                    instance_id="p1",
                    ray_node_type_name="tpu",
                    status=NodeStatus.RUNNING,
                ),
            ]

        cfg, store, _, im = self._setup_multihost_reconcile_env(
            f"reused_cid_no_fail_{scenario}",
            [
                create_instance(
                    "r1-old",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-1",
                    status_times=[
                        (Instance.REQUESTED, now_ns - 55 * s_to_ns),
                        (Instance.ALLOCATED, now_ns - 50 * s_to_ns),
                        (Instance.TERMINATED, now_ns - 40 * s_to_ns),
                    ],
                ),
                create_instance(
                    "r1-req-1",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-1",
                    status_times=[(Instance.REQUESTED, now_ns - 55 * s_to_ns)],
                ),
                create_instance(
                    "r2-0",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-2",
                    status_times=[
                        (Instance.REQUESTED, now_ns - 35 * s_to_ns),
                        (Instance.ALLOCATED, now_ns - 30 * s_to_ns),
                        (Instance.TERMINATED, now_ns - 20 * s_to_ns),
                    ],
                ),
                u2,
                create_instance(
                    "r2-1",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="p1",
                    launch_request_id="lid-2",
                    ray_node_id=binary_to_hex(node_b),
                    status_times=[
                        (Instance.REQUESTED, now_ns - 35 * s_to_ns),
                        (Instance.ALLOCATED, now_ns - 30 * s_to_ns),
                        (Instance.RAY_RUNNING, now_ns - 25 * s_to_ns),
                    ],
                ),
            ],
            max_worker_nodes=8,
            workers_per_replica=2,
        )
        cloud = {
            "p0": CloudInstance("p0", "tpu", NodeKind.WORKER, True, replica_id="rep-2"),
            "p1": CloudInstance("p1", "tpu", NodeKind.WORKER, True, replica_id="rep-2"),
        }
        provider = MagicMock()
        provider.get_node_types_with_pending_deletes.return_value = set()

        Reconciler.reconcile(
            instance_manager=im,
            scheduler=MockScheduler(),
            cloud_provider=provider,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=ClusterResourceState(node_states=node_states),
            non_terminated_cloud_instances=cloud,
            autoscaling_config=cfg,
        )
        insts, _ = store.get_instances()
        assert insts["r1-req-1"].status == Instance.REQUESTED
        if scenario == "healthy_full_replica":
            assert insts["u2-0"].status == Instance.RAY_RUNNING
            assert insts["r2-1"].status == Instance.RAY_RUNNING
        else:
            assert insts["u2-0"].status == Instance.TERMINATING
            assert insts["r2-1"].status == Instance.TERMINATING

    @pytest.mark.parametrize("reused_node_type", ["tpu-a", "tpu-b"])
    def test_multihost_reused_cid_preserves_vanished_slot_across_replicas_and_node_types(
        self, reused_node_type
    ):
        """When p0 from (tpu-a, rep-1, lid-1) vanishes and is reused on another
        replica (same or different node type), rep-1 must keep p0 in
        rep_bound_cids_by_lid so owed_batch_slots stays exact and lid-1 never
        claims rep-2."""
        instances = [
            create_instance(
                "r1-0",
                status=Instance.TERMINATED,
                instance_type="tpu-a",
                cloud_instance_id="p0",
                launch_request_id="lid-1",
                status_times=[
                    (Instance.ALLOCATED, 100),
                    (Instance.TERMINATED, 120),
                ],
            ),
            create_instance(
                "r1-1",
                status=Instance.RAY_RUNNING,
                instance_type="tpu-a",
                cloud_instance_id="p1",
                launch_request_id="lid-1",
                status_times=[
                    (Instance.ALLOCATED, 100),
                    (Instance.RAY_RUNNING, 110),
                ],
            ),
            create_instance(
                "r2-0",
                status=Instance.RAY_RUNNING,
                instance_type=reused_node_type,
                cloud_instance_id="p0",
                launch_request_id="lid-2",
                status_times=[
                    (Instance.ALLOCATED, 200),
                    (Instance.RAY_RUNNING, 210),
                ],
            ),
        ]
        cloud = {
            "p0": CloudInstance(
                "p0", reused_node_type, NodeKind.WORKER, True, replica_id="rep-2"
            ),
            "p1": CloudInstance(
                "p1", "tpu-a", NodeKind.WORKER, True, replica_id="rep-1"
            ),
        }
        view = _build_multihost_replica_view(instances, cloud, {"tpu-a": 4, "tpu-b": 4})
        assert view.rep_bound_cids_by_lid[(("tpu-a", "rep-1"), "lid-1")] == {"p0", "p1"}
        assert view.owed_batch_slots("tpu-a", "rep-1", "lid-1", 4) == 2
        assert view.claimed_reps_by_lid[("tpu-a", "lid-1")] == {"rep-1"}
        assert view.rep_lids[(reused_node_type, "rep-2")] == {"lid-2"}
        assert view.rep_bound_cids_by_lid[((reused_node_type, "rep-2"), "lid-2")] == {
            "p0"
        }
        if reused_node_type == "tpu-b":
            assert ("tpu-a", "rep-2") not in view.rep_lids

    def test_multihost_reused_cid_live_row_cross_node_type_isolation(self):
        """Cross-node-type live and teardown rows referencing a cloud_instance_id
        now owned by another node type must not attach to the foreign replica,
        must not suppress teardown propagation via RAY_STOPPED shadowing, and
        must never terminate the other node type's live physical pod."""
        cloud_p0_b = {
            "p0": CloudInstance(
                "p0", "tpu-b", NodeKind.WORKER, True, replica_id="rep-0"
            )
        }
        rb_live = create_instance(
            "rb-live",
            status=Instance.RAY_RUNNING,
            instance_type="tpu-b",
            cloud_instance_id="p0",
            launch_request_id="lid-b",
            status_times=[
                (Instance.ALLOCATED, 200),
                (Instance.RAY_RUNNING, 210),
            ],
        )

        # Case 1: Stale RAY_RUNNING row on tpu-a referencing tpu-b's pod p0.
        ra_stale = create_instance(
            "ra-stale",
            status=Instance.RAY_RUNNING,
            instance_type="tpu-a",
            cloud_instance_id="p0",
            launch_request_id="lid-a",
            status_times=[
                (Instance.ALLOCATED, 100),
                (Instance.RAY_RUNNING, 110),
            ],
        )
        live_view = _build_multihost_replica_view(
            [ra_stale, rb_live], cloud_p0_b, {"tpu-a": 2, "tpu-b": 2}
        )
        assert ("tpu-a", "rep-0") not in live_view.rep_lids
        assert ("tpu-a", "lid-a") not in live_view.claimed_reps_by_lid
        assert live_view.rep_lids[("tpu-b", "rep-0")] == {"lid-b"}
        assert live_view.claimed_reps_by_lid[("tpu-b", "lid-b")] == {"rep-0"}

        # Case 2: Stale TERMINATING row on tpu-a referencing tpu-b's pod p0.
        ra_stale_td = create_instance(
            "ra-stale-td",
            status=Instance.TERMINATING,
            instance_type="tpu-a",
            cloud_instance_id="p0",
            launch_request_id="lid-a",
            status_times=[
                (Instance.ALLOCATED, 100),
                (Instance.TERMINATING, 120),
            ],
        )
        td_view = _build_multihost_replica_view(
            [ra_stale_td, rb_live], cloud_p0_b, {"tpu-a": 2, "tpu-b": 2}
        )
        assert ("tpu-a", "rep-0") not in td_view.rep_lids
        assert ("tpu-a", "lid-a") not in td_view.claimed_reps_by_lid
        assert ("tpu-a", "lid-a") in td_view.torn_down_lids
        assert td_view.rep_lids[("tpu-b", "rep-0")] == {"lid-b"}
        assert ("tpu-b", "lid-b") not in td_view.torn_down_lids

        # Case 3: Stale RAY_STOPPED row on tpu-a with open sibling ra-req while
        # tpu-b owns live pod p0. tpu-b's active pod must NOT shadow tpu-a's
        # teardown propagation, yet _terminate_instances must mark ra TERMINATED
        # without terminating physical pod p0.
        ra_stopped = create_instance(
            "ra-stopped",
            status=Instance.RAY_STOPPED,
            instance_type="tpu-a",
            cloud_instance_id="p0",
            launch_request_id="lid-a",
            status_times=[
                (Instance.ALLOCATED, 100),
                (Instance.RAY_STOPPED, 120),
            ],
        )
        ra_req = create_instance(
            "ra-req",
            status=Instance.REQUESTED,
            instance_type="tpu-a",
            launch_request_id="lid-a",
            status_times=[(Instance.REQUESTED, 90)],
        )
        stopped_view = _build_multihost_replica_view(
            [ra_stopped, ra_req, rb_live], cloud_p0_b, {"tpu-a": 2, "tpu-b": 2}
        )
        assert ("tpu-a", "lid-a") in stopped_view.torn_down_lids
        assert ("tpu-b", "lid-b") not in stopped_view.torn_down_lids
        assert "p0" in stopped_view.active_cids
        assert stopped_view.rep_lids[("tpu-b", "rep-0")] == {"lid-b"}

        cfg = MockAutoscalingConfig(
            configs={
                "node_type_configs": {
                    "tpu-a": NodeTypeConfig(
                        name="tpu-a",
                        resources={"CPU": 4},
                        min_worker_nodes=0,
                        max_worker_nodes=4,
                        replica_config=WorkerReplicaConfig(workers_per_replica=2),
                    ),
                    "tpu-b": NodeTypeConfig(
                        name="tpu-b",
                        resources={"CPU": 4},
                        min_worker_nodes=0,
                        max_worker_nodes=4,
                        replica_config=WorkerReplicaConfig(workers_per_replica=2),
                    ),
                }
            }
        )
        store = InstanceStorage(
            cluster_id="reused_cid_cross_nt_ray_stopped", storage=InMemoryStorage()
        )
        sub = MockSubscriber()
        mock_provider = MagicMock()
        im = InstanceManager(
            store,
            instance_status_update_subscribers=[
                sub,
                CloudInstanceUpdater(mock_provider),
            ],
        )
        self._add_instances(store, [ra_stopped, ra_req, rb_live])
        sub.clear()

        Reconciler._terminate_instances(
            im, autoscaling_config=cfg, non_terminated_cloud_instances=cloud_p0_b
        )
        insts, _ = store.get_instances()
        assert insts["ra-stopped"].status == Instance.TERMINATED
        assert insts["ra-req"].status == Instance.ALLOCATION_FAILED
        assert insts["rb-live"].status == Instance.RAY_RUNNING
        assert all(e.new_instance_status != Instance.TERMINATING for e in sub.events)
        mock_provider.terminate.assert_not_called()

    def test_multihost_reused_cid_replica_key_and_partial_term_cross_nt_isolation(
        self,
    ):
        """Verify _get_replica_key rejects cross-node-type and HEAD cloud
        instances (preventing unintended sibling termination sweeps) and
        _filter_partial_replica_terminations scopes live/done/term cid sets by
        node type."""
        replica_sizes = {"tpu-a": 2, "tpu-b": 2}
        a_inst = create_instance(
            "a-inst",
            status=Instance.RAY_RUNNING,
            instance_type="tpu-a",
            cloud_instance_id="p0",
        )
        # Positive control: matching worker node type returns replica key.
        assert _get_replica_key(
            a_inst,
            {
                "p0": CloudInstance(
                    "p0", "tpu-a", NodeKind.WORKER, True, replica_id="rep-0"
                )
            },
            replica_sizes,
        ) == ("tpu-a", "rep-0")
        # Negative 1: foreign node_type returns None.
        assert (
            _get_replica_key(
                a_inst,
                {
                    "p0": CloudInstance(
                        "p0", "tpu-b", NodeKind.WORKER, True, replica_id="rep-0"
                    )
                },
                replica_sizes,
            )
            is None
        )
        # Negative 2: HEAD cloud instance returns None even with matching node_type,
        # and _build_multihost_replica_view preserves vanished_cids_by_lid when a
        # terminated worker pod's cloud_instance_id is reused by a HEAD pod.
        head_cloud = {
            "p0": CloudInstance("p0", "tpu-a", NodeKind.HEAD, True, replica_id="rep-0")
        }
        assert _get_replica_key(a_inst, head_cloud, replica_sizes) is None
        head_reused_view = _build_multihost_replica_view(
            [
                create_instance(
                    "a-term",
                    status=Instance.TERMINATED,
                    instance_type="tpu-a",
                    cloud_instance_id="p0",
                    launch_request_id="lid-1",
                    status_times=[
                        (Instance.ALLOCATED, 100),
                        (Instance.TERMINATED, 120),
                    ],
                ),
                create_instance(
                    "a-req",
                    status=Instance.REQUESTED,
                    instance_type="tpu-a",
                    launch_request_id="lid-1",
                    status_times=[(Instance.REQUESTED, 90)],
                ),
            ],
            head_cloud,
            replica_sizes,
        )
        assert head_reused_view.vanished_cids_by_lid[("tpu-a", "lid-1")] == {"p0"}
        assert ("tpu-a", "rep-0") not in head_reused_view.assigned_reps

        # _terminate_instances seam: stale TERMINATING row on tpu-a referencing
        # p0 (now owned by tpu-b rep-0) must NOT sweep unrelated healthy replica
        # (tpu-a, rep-0).
        cfg = MockAutoscalingConfig(
            configs={
                "node_type_configs": {
                    "tpu-a": NodeTypeConfig(
                        name="tpu-a",
                        resources={"CPU": 4},
                        min_worker_nodes=0,
                        max_worker_nodes=4,
                        replica_config=WorkerReplicaConfig(workers_per_replica=2),
                    ),
                    "tpu-b": NodeTypeConfig(
                        name="tpu-b",
                        resources={"CPU": 4},
                        min_worker_nodes=0,
                        max_worker_nodes=4,
                        replica_config=WorkerReplicaConfig(workers_per_replica=2),
                    ),
                }
            }
        )
        store = InstanceStorage(
            cluster_id="reused_cid_rep_key_sweep", storage=InMemoryStorage()
        )
        im = InstanceManager(store, instance_status_update_subscribers=[])
        self._add_instances(
            store,
            [
                create_instance(
                    "a-stale",
                    status=Instance.TERMINATING,
                    instance_type="tpu-a",
                    cloud_instance_id="p0",
                ),
                create_instance(
                    "pa0",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu-a",
                    cloud_instance_id="pa0",
                ),
                create_instance(
                    "pa1",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu-a",
                    cloud_instance_id="pa1",
                ),
                create_instance(
                    "b-live",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu-b",
                    cloud_instance_id="p0",
                ),
            ],
        )
        sweep_cloud = {
            "p0": CloudInstance(
                "p0", "tpu-b", NodeKind.WORKER, True, replica_id="rep-0"
            ),
            "pa0": CloudInstance(
                "pa0", "tpu-a", NodeKind.WORKER, True, replica_id="rep-0"
            ),
            "pa1": CloudInstance(
                "pa1", "tpu-a", NodeKind.WORKER, True, replica_id="rep-0"
            ),
        }
        Reconciler._terminate_instances(
            im, autoscaling_config=cfg, non_terminated_cloud_instances=sweep_cloud
        )
        insts, _ = store.get_instances()
        assert insts["pa0"].status == Instance.RAY_RUNNING
        assert insts["pa1"].status == Instance.RAY_RUNNING
        assert insts["b-live"].status == Instance.RAY_RUNNING

        # Partial-replica termination filter cross-node-type isolation:
        # Subcase A (term_cids): stale tpu-b termination on p1 must NOT satisfy
        # (tpu-a, rep-a)'s partial-replica gate while a1 on p1 is still running.
        rep_a_cloud = {
            "p0": CloudInstance(
                "p0", "tpu-a", NodeKind.WORKER, True, replica_id="rep-a"
            ),
            "p1": CloudInstance(
                "p1", "tpu-a", NodeKind.WORKER, True, replica_id="rep-a"
            ),
        }
        a0 = create_instance(
            "a0",
            status=Instance.RAY_RUNNING,
            instance_type="tpu-a",
            cloud_instance_id="p0",
        )
        a1 = create_instance(
            "a1",
            status=Instance.RAY_RUNNING,
            instance_type="tpu-a",
            cloud_instance_id="p1",
        )
        b_stale_run = create_instance(
            "b-stale",
            status=Instance.RAY_RUNNING,
            instance_type="tpu-b",
            cloud_instance_id="p1",
        )
        req_a0 = TerminationRequest(
            id="t-a0",
            instance_id="a0",
            instance_type="tpu-a",
            instance_status=Instance.RAY_RUNNING,
            cause=TerminationRequest.Cause.IDLE,
        )
        req_a1 = TerminationRequest(
            id="t-a1",
            instance_id="a1",
            instance_type="tpu-a",
            instance_status=Instance.RAY_RUNNING,
            cause=TerminationRequest.Cause.IDLE,
        )
        req_b_stale = TerminationRequest(
            id="t-b-stale",
            instance_id="b-stale",
            instance_type="tpu-b",
            instance_status=Instance.RAY_RUNNING,
            cause=TerminationRequest.Cause.IDLE,
        )
        im_a = [a0, a1, b_stale_run]
        kept_term = Reconciler._filter_partial_replica_terminations(
            [req_a0, req_b_stale],
            im_a,
            {i.instance_id: i for i in im_a},
            rep_a_cloud,
            replica_sizes,
        )
        assert [r.instance_id for r in kept_term] == ["b-stale"]

        # Subcase B (done_cids): stale tpu-b TERMINATING row on p1 must NOT
        # satisfy (tpu-a, rep-a)'s partial-replica gate when p1 has no tpu-a
        # teardown row yet.
        b_stale_term = create_instance(
            "b-term",
            status=Instance.TERMINATING,
            instance_type="tpu-b",
            cloud_instance_id="p1",
        )
        im_b = [a0, b_stale_term]
        kept_done = Reconciler._filter_partial_replica_terminations(
            [req_a0],
            im_b,
            {i.instance_id: i for i in im_b},
            rep_a_cloud,
            replica_sizes,
        )
        assert kept_done == []

        # Positive control: terminating both a0 and a1 on tpu-a is accepted.
        kept_whole = Reconciler._filter_partial_replica_terminations(
            [req_a0, req_a1],
            [a0, a1],
            {"a0": a0, "a1": a1},
            rep_a_cloud,
            replica_sizes,
        )
        assert {r.instance_id for r in kept_whole} == {"a0", "a1"}

    def test_gc_retains_reused_cid_with_non_terminal_open_row_and_cross_nt_isolation(
        self,
    ):
        """Verify _gc_terminated_instances protects expired untagged teardown
        and reused-cid guard rows even when the open batch's row on that cid is
        non-terminal, and scopes active_lids by (instance_type, launch_request_id)."""
        now_ns = time.time_ns()
        expired_ns = now_ns - 60 * s_to_ns
        gc_cfg = InstanceReconcileConfig(
            terminated_instance_gc_retention_s=10,
            request_status_timeout_s=600,
        )

        # Part 1: Non-terminal open row retains expired u-td (GC-view commutativity).
        _, store1, _, im1 = self._setup_multihost_reconcile_env(
            "gc_non_terminal_utd",
            [
                create_instance(
                    "r0-req",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-0",
                    status_times=[(Instance.REQUESTED, now_ns - 5 * s_to_ns)],
                ),
                create_instance(
                    "r0-live",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-0",
                    status_times=[
                        (Instance.ALLOCATED, expired_ns - 30),
                        (Instance.RAY_RUNNING, expired_ns - 25),
                    ],
                ),
                create_instance(
                    "u-td",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                    status_times=[
                        (Instance.ALLOCATED, expired_ns - 15),
                        (Instance.TERMINATING, expired_ns - 10),
                        (Instance.TERMINATED, expired_ns),
                    ],
                ),
            ],
            max_worker_nodes=4,
            workers_per_replica=4,
        )
        before_insts, _ = store1.get_instances()
        view_before = _build_multihost_replica_view(
            list(before_insts.values()), {}, {"tpu": 4}
        )
        assert ("tpu", "lid-0") in view_before.torn_down_lids

        Reconciler._gc_terminated_instances(im1, gc_cfg, [])
        after_insts, _ = store1.get_instances()
        assert "u-td" in after_insts
        view_after = _build_multihost_replica_view(
            list(after_insts.values()), {}, {"tpu": 4}
        )
        assert view_after.torn_down_lids == view_before.torn_down_lids

        # Part 2: Non-terminal open row + expired r1 + live u1 retains r1 so
        # u1 teardown marks lid-1 only, then cleans r1 once lid-0 closes.
        _, store2, _, im2 = self._setup_multihost_reconcile_env(
            "gc_non_terminal_reused_guard",
            [
                create_instance(
                    "r0-req",
                    status=Instance.REQUESTED,
                    instance_type="tpu",
                    launch_request_id="lid-0",
                    status_times=[(Instance.REQUESTED, now_ns - 5 * s_to_ns)],
                ),
                create_instance(
                    "r0-live",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-0",
                    status_times=[
                        (Instance.ALLOCATED, expired_ns - 50),
                        (Instance.RAY_RUNNING, expired_ns - 45),
                    ],
                ),
                create_instance(
                    "r1",
                    status=Instance.TERMINATED,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="lid-1",
                    status_times=[
                        (Instance.ALLOCATED, expired_ns - 30),
                        (Instance.TERMINATED, expired_ns - 20),
                    ],
                ),
                create_instance(
                    "u1",
                    status=Instance.RAY_RUNNING,
                    instance_type="tpu",
                    cloud_instance_id="p0",
                    launch_request_id="",
                    status_times=[
                        (Instance.ALLOCATED, expired_ns - 10),
                        (Instance.RAY_RUNNING, expired_ns - 5),
                    ],
                ),
            ],
            max_worker_nodes=4,
            workers_per_replica=4,
        )
        Reconciler._gc_terminated_instances(im2, gc_cfg, [])
        insts2, ver2 = store2.get_instances()
        assert "r1" in insts2
        assert "u1" in insts2
        td_view = _build_multihost_replica_view(
            list(insts2.values()),
            {"p0": CloudInstance("p0", "tpu", NodeKind.WORKER, True, replica_id=None)},
            {"tpu": 4},
            status_overrides={"u1": Instance.TERMINATING},
        )
        assert ("tpu", "lid-1") in td_view.torn_down_lids
        assert ("tpu", "lid-0") not in td_view.torn_down_lids

        ok, _ = store2.batch_upsert_instances(
            updates=[
                create_instance(
                    "r0-req",
                    status=Instance.ALLOCATION_FAILED,
                    instance_type="tpu",
                    launch_request_id="lid-0",
                    status_times=[
                        (Instance.REQUESTED, now_ns - 5 * s_to_ns),
                        (Instance.ALLOCATION_FAILED, now_ns),
                    ],
                )
            ],
            expected_storage_version=ver2,
        )
        assert ok
        Reconciler._gc_terminated_instances(im2, gc_cfg, [])
        insts2, _ = store2.get_instances()
        assert "r1" not in insts2
        assert "u1" in insts2

        # Part 3: Cross-node-type active_lids isolation: open (tpu-b, shared-lid)
        # retains b-term but does NOT block GC of expired (tpu-a, shared-lid).
        store3 = InstanceStorage(
            cluster_id="gc_cross_nt_active_lids", storage=InMemoryStorage()
        )
        im3 = InstanceManager(store3, instance_status_update_subscribers=[])
        self._add_instances(
            store3,
            [
                create_instance(
                    "b-req",
                    status=Instance.REQUESTED,
                    instance_type="tpu-b",
                    launch_request_id="shared-lid",
                    status_times=[(Instance.REQUESTED, now_ns - 5 * s_to_ns)],
                ),
                create_instance(
                    "a-term",
                    status=Instance.TERMINATED,
                    instance_type="tpu-a",
                    cloud_instance_id="p0",
                    launch_request_id="shared-lid",
                    status_times=[(Instance.TERMINATED, expired_ns)],
                ),
                create_instance(
                    "b-term",
                    status=Instance.TERMINATED,
                    instance_type="tpu-b",
                    cloud_instance_id="p1",
                    launch_request_id="shared-lid",
                    status_times=[(Instance.TERMINATED, expired_ns)],
                ),
            ],
        )
        Reconciler._gc_terminated_instances(im3, gc_cfg, [])
        insts3, _ = store3.get_instances()
        assert "a-term" not in insts3
        assert "b-term" in insts3

    def test_scale_cluster_forwards_cloud_instance_replica_id(self):
        """Reconciler._scale_cluster forwards CloudInstance.replica_id in memory
        onto AutoscalerInstance.cloud_instance_replica_id and normalizes empty
        strings and missing entries to None.
        """
        captured_requests = []
        forbidden_proto_accesses = []

        class _ZeroProtoInstanceProxy:
            """Strict proxy exposing only upstream Instance proto fields.

            Local generated pb2 bindings in this worktree may still expose
            stale fields removed from src/ray/protobuf/instance_manager.proto.
            Blocking cloud_instance_replica_id guarantees local pytest fails
            if _scale_cluster reads im_instance.cloud_instance_replica_id
            directly or via getattr(..., default).
            """

            def __init__(self, wrapped):
                self._wrapped = wrapped

            def __getattr__(self, name):
                if name == "cloud_instance_replica_id":
                    forbidden_proto_accesses.append(name)
                    raise AttributeError(
                        "Instance proto has no 'cloud_instance_replica_id' field"
                    )
                return getattr(self._wrapped, name)

        class _ZeroProtoInstanceManagerProxy:
            def __init__(self, wrapped):
                self._wrapped = wrapped

            def get_instance_manager_state(self, request=None):
                reply = self._wrapped.get_instance_manager_state(request=request)
                proxied_state = MagicMock(
                    instances=[
                        _ZeroProtoInstanceProxy(inst) for inst in reply.state.instances
                    ],
                    version=reply.state.version,
                )
                return MagicMock(status=reply.status, state=proxied_state)

        class SpyScheduler(IResourceScheduler):
            def schedule(self, request):
                captured_requests.append(request)
                return SchedulingReply()

        store = InstanceStorage(
            cluster_id="test",
            storage=InMemoryStorage(),
        )
        im = InstanceManager(
            instance_storage=store,
            instance_status_update_subscribers=[],
        )
        im_proxy = _ZeroProtoInstanceManagerProxy(im)
        run_node_id = b"r" * 28
        run_node_hex = binary_to_hex(run_node_id)
        instances = [
            create_instance(
                "i-rep",
                status=Instance.ALLOCATED,
                cloud_instance_id="c-rep",
                instance_type="tpu",
            ),
            create_instance(
                "i-rep-run",
                status=Instance.RAY_RUNNING,
                cloud_instance_id="c-rep-run",
                ray_node_id=run_node_hex,
                instance_type="tpu",
            ),
            create_instance(
                "i-norep",
                status=Instance.ALLOCATED,
                cloud_instance_id="c-norep",
                instance_type="tpu",
            ),
            create_instance(
                "i-emptyrep",
                status=Instance.ALLOCATED,
                cloud_instance_id="c-emptyrep",
                instance_type="tpu",
            ),
            create_instance(
                "i-missing",
                status=Instance.ALLOCATED,
                cloud_instance_id="c-missing",
                instance_type="tpu",
            ),
            create_instance(
                "i-requested",
                status=Instance.REQUESTED,
                cloud_instance_id="",
                instance_type="tpu",
            ),
            create_instance(
                "i-foreign-nt",
                status=Instance.ALLOCATED,
                cloud_instance_id="c-foreign-nt",
                instance_type="tpu",
            ),
            create_instance(
                "i-head-cloud",
                status=Instance.ALLOCATED,
                cloud_instance_id="c-head-cloud",
                instance_type="tpu",
            ),
        ]
        for inst in instances:
            ok, _ = store.upsert_instance(inst)
            assert ok

        cloud_instances = {
            "c-rep": CloudInstance(
                "c-rep", "tpu", NodeKind.WORKER, True, replica_id="rep-0"
            ),
            "c-rep-run": CloudInstance(
                "c-rep-run", "tpu", NodeKind.WORKER, True, replica_id="rep-1"
            ),
            "c-norep": CloudInstance(
                "c-norep", "tpu", NodeKind.WORKER, True, replica_id=None
            ),
            "c-emptyrep": CloudInstance(
                "c-emptyrep", "tpu", NodeKind.WORKER, True, replica_id=""
            ),
            "c-foreign-nt": CloudInstance(
                "c-foreign-nt", "other-type", NodeKind.WORKER, True, replica_id="rep-x"
            ),
            "c-head-cloud": CloudInstance(
                "c-head-cloud", "tpu", NodeKind.HEAD, True, replica_id="rep-head"
            ),
            "": CloudInstance("", "tpu", NodeKind.WORKER, True, replica_id="bogus"),
        }
        cfg = MockAutoscalingConfig(
            configs={
                "node_type_configs": {
                    "tpu": NodeTypeConfig(
                        name="tpu",
                        min_worker_nodes=0,
                        max_worker_nodes=8,
                        resources={"CPU": 4},
                    )
                },
                "disable_launch_config_check": True,
            }
        )
        ray_state = ClusterResourceState(
            node_states=[NodeState(node_id=run_node_id, status=NodeStatus.RUNNING)]
        )

        # Pass 1: populated cloud_instances map.
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im_proxy,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ray_state,
            scheduler=SpyScheduler(),
            autoscaling_config=cfg,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=cloud_instances,
        )
        assert len(captured_requests) == 1
        by_id = {
            inst.im_instance.instance_id: inst
            for inst in captured_requests[0].current_instances
        }
        assert by_id["i-rep"].cloud_instance_id == "c-rep"
        assert by_id["i-rep"].cloud_instance_replica_id == "rep-0"
        assert by_id["i-rep-run"].cloud_instance_id == "c-rep-run"
        assert by_id["i-rep-run"].cloud_instance_replica_id == "rep-1"
        assert by_id["i-rep-run"].ray_node is not None
        assert by_id["i-norep"].cloud_instance_id == "c-norep"
        assert by_id["i-norep"].cloud_instance_replica_id is None
        assert by_id["i-emptyrep"].cloud_instance_id == "c-emptyrep"
        assert by_id["i-emptyrep"].cloud_instance_replica_id is None
        assert by_id["i-missing"].cloud_instance_id == "c-missing"
        assert by_id["i-missing"].cloud_instance_replica_id is None
        assert by_id["i-requested"].cloud_instance_id is None
        assert by_id["i-requested"].cloud_instance_replica_id is None
        assert by_id["i-foreign-nt"].cloud_instance_id == "c-foreign-nt"
        assert by_id["i-foreign-nt"].cloud_instance_replica_id is None
        assert by_id["i-head-cloud"].cloud_instance_id == "c-head-cloud"
        assert by_id["i-head-cloud"].cloud_instance_replica_id is None
        assert not forbidden_proto_accesses

        # Pass 2: non_terminated_cloud_instances=None produces None everywhere.
        Reconciler._scale_cluster(
            autoscaling_state=MagicMock(),
            instance_manager=im_proxy,
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_state=ray_state,
            scheduler=SpyScheduler(),
            autoscaling_config=cfg,
            cloud_provider=MagicMock(),
            non_terminated_cloud_instances=None,
        )
        assert len(captured_requests) == 2
        assert len(captured_requests[1].current_instances) == 8
        assert all(
            inst.cloud_instance_replica_id is None
            for inst in captured_requests[1].current_instances
        )
        assert not forbidden_proto_accesses
        # Self-verify that the zero-proto proxy actually rejects the field.
        with pytest.raises(AttributeError):
            _ = by_id["i-rep"].im_instance.cloud_instance_replica_id
        assert forbidden_proto_accesses == ["cloud_instance_replica_id"]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
