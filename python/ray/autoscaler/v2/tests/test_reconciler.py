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
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler, logger
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
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
    def test_multihost_replica_launch_batching():
        """Multi-worker replica groups (`workers_per_replica > 1`) launch whole
        replicas atomically without starving behind or blocking single-host types."""

        def _replica_nt_cfg(
            name: str,
            max_w: int,
            workers_per_replica: Optional[int],
            resources: Optional[dict] = None,
        ):
            res = resources or {"CPU": 8, "GPU": 4}
            head_keys = tuple(
                sorted(k for k in res if k.startswith("TPU-") and k.endswith("-head"))
            )
            return NodeTypeConfig(
                name=name,
                resources=res,
                min_worker_nodes=0,
                max_worker_nodes=max_w,
                replica_config=(
                    WorkerReplicaConfig(
                        workers_per_replica=workers_per_replica,
                        head_resources=head_keys,
                    )
                    if workers_per_replica is not None
                    else None
                ),
            )

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

        cpu_cfg = NodeTypeConfig(
            name="cpu",
            resources={"CPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=8,
        )
        v6e_64 = {"tpu64": _replica_nt_cfg("tpu64", 32, 16)}
        v6e_16 = {"tpu16": _replica_nt_cfg("tpu16", 16, 4)}

        # 16-host v6e-64 slice exceeds default max_concurrent_launches=10: launches
        # one whole 16-host slice when capacity remains and tpu64 has no in-flight
        # REQUESTED pods, even when unrelated CPU launches are in flight.
        _, _, sub, _ = _run_launch_tick(v6e_64, {"tpu64": [f"q{i}" for i in range(16)]})
        assert [e.instance_id for e in sub.events] == [f"q{i}" for i in range(16)]
        _, _, sub_with_cpu_req, _ = _run_launch_tick(
            {**v6e_64, "cpu": cpu_cfg},
            {"tpu64": [f"q{i}" for i in range(16)]},
            requested_ids_by_type={"cpu": [f"r{i}" for i in range(4)]},
        )
        assert [e.instance_id for e in sub_with_cpu_req.events] == [
            f"q{i}" for i in range(16)
        ]

        # Negative bounds: max_concurrent_launches=0, fully saturated cap (10/10),
        # or an already in-flight tpu64 batch waits rather than stacking above cap.
        _, _, sub0, _ = _run_launch_tick(
            v6e_64,
            {"tpu64": [f"q{i}" for i in range(16)]},
            max_concurrent_launches=0,
        )
        assert sub0.events == []
        _, _, sub_sat, _ = _run_launch_tick(
            {**v6e_64, "cpu": cpu_cfg},
            {"tpu64": [f"q{i}" for i in range(16)]},
            requested_ids_by_type={"cpu": [f"r{i}" for i in range(10)]},
        )
        assert sub_sat.events == []
        _, _, sub_same_type_inflight, _ = _run_launch_tick(
            v6e_64,
            {"tpu64": [f"q{i}" for i in range(16)]},
            requested_ids_by_type={"tpu64": [f"r{i}" for i in range(4)]},
        )
        assert sub_same_type_inflight.events == []

        # Cross-type FIFO order invariance: co-queued CPU + multi-host TPU produce
        # identical launches regardless of storage iteration order, and later CPU
        # arrivals never starve an earlier v6e-16 (4-host) slice behind 8 CPU pods.
        t64_late = [("tpu64", f"t{i:02d}", 10 + i) for i in range(16)]
        for items in ([("cpu", "c0", 1), *t64_late], [*t64_late, ("cpu", "c0", 1)]):
            _, _, sub_co, _ = _run_launch_tick(
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
            _, _, sub_tpu_first, _ = _run_launch_tick(
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
            _, _, sub_livelock, _ = _run_launch_tick(
                {**v6e_16, "cpu": cpu_cfg},
                {},
                requested_ids_by_type={"cpu": [f"cr{i}" for i in range(8)]},
                upscaling_speed=100.0,
                queued_items=items,
            )
            assert [e.instance_id for e in sub_livelock.events] == [
                f"t{i}" for i in range(4)
            ]

        # Single-instance node types (`replica_config=None`, e.g. GCE TPU VMs)
        # do not apply multi-host replica multiplication or rounding.
        gcp_tpu = {
            "gcp_v4_16": _replica_nt_cfg(
                "gcp_v4_16",
                1,
                None,
                resources={"CPU": 8, "TPU": 4, "TPU-v4-16-head": 1},
            )
        }
        _, _, sub_gcp, _ = _run_launch_tick(
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

        # 3 x v6e-16 (12 QUEUED) with max_concurrent_launches=10: Tick 1 launches 8
        # (2 whole replicas, 0 fractional REQUESTED), leaving 4 QUEUED for Tick 2.
        storage, im, sub1, instances = _run_launch_tick(
            v6e_16,
            {"tpu16": [f"q{i}" for i in range(12)]},
            upscaling_speed=1000.0,
            max_concurrent_launches=10,
        )
        assert len(sub1.events) == 8
        assert sum(i.status == Instance.QUEUED for i in instances.values()) == 4
        # Once the 8 REQUESTED pods transition to ALLOCATED on KubeRay, Tick 2
        # launches the remaining 4-host slice cleanly.
        cloud_8 = {}
        for idx, e in enumerate(sub1.events):
            cid = f"pod-{idx}"
            cloud_8[cid] = CloudInstance(cid, "tpu16", NodeKind.WORKER, True)
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
        _, _, sub_cons, _ = _run_launch_tick(
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

        # Retried multi-host QUEUED stragglers (launch_request_id != "") re-enter
        # REQUESTED without % size rounding <4 stragglers to 0 or mixing them into
        # fresh QUEUED replica batches.
        retried_2 = [
            create_instance(
                f"ret-{i}",
                status=Instance.QUEUED,
                instance_type="tpu16",
                launch_request_id="req-old",
                status_times=[(Instance.QUEUED, 1 + i)],
            )
            for i in range(2)
        ]
        assert [
            i.instance_id
            for i in Reconciler._compute_to_launch(
                retried_2, [], [], 1.0, 100, {"tpu16": 4}
            )["tpu16"]
        ] == ["ret-0", "ret-1"]
        assert [
            i.instance_id
            for i in Reconciler._compute_to_launch(
                retried_2 + r0_and_strays[:4], [], [], 1.0, 100, {"tpu16": 4}
            )["tpu16"]
        ] == ["ret-0", "ret-1", *(f"r0-{i}" for i in range(4))]

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

        # (c) _scale_cluster plumbs cloud_instance_replica_id so reshuffled booting
        # slices drain as exact KubeRay replicas and defers partial booting scale-downs.
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
                ),
                create_instance(
                    "g1-1",
                    status=Instance.ALLOCATED,
                    instance_type="tpu16",
                    cloud_instance_id="cp-1",
                ),
                create_instance(
                    "r-2", status=Instance.REQUESTED, instance_type="tpu16"
                ),
                create_instance(
                    "r-3", status=Instance.REQUESTED, instance_type="tpu16"
                ),
                create_instance(
                    "r-active", status=Instance.REQUESTED, instance_type="tpu_active"
                ),
                create_instance(
                    "gpu-req", status=Instance.REQUESTED, instance_type="stale_gpu"
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
        # host and max_worker_nodes=8 -> 7 slots free) is preserved, and neither
        # min_worker_nodes cpu launch is queued because active nodes
        # (h + g1-0 + g1-1 + r-2 + r-3 + r-active + gpu-req + 4 tpu_active = 11) plus
        # the still-draining g2-outdated (RAY_STOP_REQUESTED) reach max_num_nodes=12.
        assert [
            (e.instance_type or e.instance_id, e.new_instance_status)
            for e in p_sub.events
        ] == [
            ("g2-outdated", Instance.RAY_STOP_REQUESTED),
            *(("tpu_active", Instance.QUEUED) for _ in range(4)),
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

    def test_multihost_crashed_worker_terminates_while_drained_worker_waits(self):
        cloud = self._replica_cloud({"rep-0": range(4)})
        now_ns = time.time_ns()

        # Spontaneous raylet exit (RAY_RUNNING -> RAY_STOPPED) terminates immediately.
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
        assert [(e.instance_id, e.new_instance_status) for e in crash_sub.events] == [
            ("w-3", Instance.TERMINATING)
        ]

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

    def test_multihost_solicited_drain_terminates_unready_siblings_but_crash_does_not(
        self,
    ):
        cloud = self._replica_cloud({"rep-0": range(4)}, include_head=True)
        now_ns = time.time_ns()

        # Once all draining siblings reach RAY_STOPPED, _terminate_instances must
        # terminate the held ALLOCATED sibling in the same batch rather than orphaning it.
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

        # Unsolicited crash of one worker must not terminate an ALLOCATED sibling.
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
        ) == [("w-0", Instance.TERMINATING)]

    @pytest.mark.parametrize(
        "sibling_status",
        [
            Instance.RAY_RUNNING,
            Instance.RAY_STOP_REQUESTED,
            Instance.RAY_STOPPING,
        ],
    )
    def test_multihost_drain_hold_timeout_leaves_unready_siblings(self, sibling_status):
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
                # rep-1: all running workers drained; unready w-7 must terminate with them.
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
        assert sorted((e.instance_id, e.new_instance_status) for e in sub.events) == [
            ("w-0", Instance.TERMINATING),
            ("w-4", Instance.TERMINATING),
            ("w-5", Instance.TERMINATING),
            ("w-6", Instance.TERMINATING),
            ("w-7", Instance.TERMINATING),
        ]

        # Once the cloud provider removes rep-0, remaining siblings converge to TERMINATED.
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

    def test_multihost_ray_stopping_sibling_defers_unready_and_stopped_siblings(self):
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
                        (Instance.RAY_RUNNING, now_ns - 3),
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
                    )
                    for h in range(2)
                ),
                *(
                    create_instance(
                        f"r-{h}",
                        status=Instance.REQUESTED,
                        instance_type="tpu",
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


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
