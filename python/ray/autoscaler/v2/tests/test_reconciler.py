import math
import os
import sys
import time

# coding: utf-8
from collections import defaultdict
from unittest import mock
from unittest.mock import MagicMock

import pytest

from ray._common.utils import binary_to_hex
from ray.autoscaler.v2.instance_manager.config import InstanceReconcileConfig, Provider
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.node_provider import (  # noqa
    CloudInstance,
    LaunchNodeError,
    TerminateNodeError,
)
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler, logger
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.ray_stopper import RayStopError
from ray.autoscaler.v2.instance_manager.subscribers.threaded_ray_installer import (
    RayInstallError,
)
from ray.autoscaler.v2.scheduler import IResourceScheduler, SchedulingReply
from ray.autoscaler.v2.tests.util import MockSubscriber, create_instance
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceState,
    GangResourceRequest,
    NodeState,
    NodeStatus,
    ResourceRequest,
)
from ray.core.generated.instance_manager_pb2 import (
    Instance,
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

    @property
    def provider(self):
        return Provider.UNKNOWN


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

    yield instance_manager, instance_storage, mock_subscriber


class TestReconciler:
    @staticmethod
    def _add_instances(instance_storage, instances):
        for instance in instances:
            ok, _ = instance_storage.upsert_instance(instance)
            assert ok

    @staticmethod
    def test_requested_instance_no_op(setup):
        instance_manager, instance_storage, _ = setup
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
        instance_manager, instance_storage, _ = setup
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
        instance_manager, instance_storage, _ = setup

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
    def test_reconcile_terminated_cloud_instances(setup):

        instance_manager, instance_storage, subscriber = setup

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

        events_i_2 = subscriber.events_by_id("i-2")
        assert len(events_i_2) == 2
        assert events_i_2[0].new_instance_status == Instance.TERMINATION_FAILED
        assert events_i_2[1].new_instance_status == Instance.TERMINATING

    @staticmethod
    def test_ray_reconciler_no_op(setup):
        instance_manager, instance_storage, subscriber = setup

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
        instance_manager, instance_storage, _ = setup

        # A newly running ray node with matching cloud instance id
        node_states = [
            NodeState(node_id=b"r-1", status=NodeStatus.RUNNING, instance_id="c-1"),
        ]
        im_instances = [
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
            ray_cluster_resource_state=ClusterResourceState(node_states=node_states),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        instances, _ = instance_storage.get_instances()
        assert len(instances) == 1
        assert instances["i-1"].status == Instance.RAY_RUNNING
        assert instances["i-1"].node_id == binary_to_hex(b"r-1")

    @staticmethod
    def test_ray_reconciler_already_ray_running(setup):
        instance_manager, instance_storage, subscriber = setup
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
            ray_cluster_resource_state=ClusterResourceState(node_states=ray_nodes),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(),
        )

        assert len(subscriber.events) == 0

    @staticmethod
    def test_ray_reconciler_stopping_ray(setup):
        instance_manager, instance_storage, _ = setup

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
        instance_manager, instance_storage, _ = setup

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
        instance_manager, instance_storage, _ = setup

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

        instance_manager, instance_storage, _ = setup

        im_instances = [
            create_instance(
                "i-1", status=Instance.RAY_RUNNING, cloud_instance_id="c-1"
            ),  # To be reconciled.
            create_instance(
                "i-2", status=Instance.RAY_RUNNING, cloud_instance_id="c-2"
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
        instance_manager, instance_storage, subscriber = setup
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

        num_desired_upscale = max(1, math.ceil(upscaling_speed * (num_running)))
        expected_launch_num = min(
            num_desired_upscale,
            max(0, max_concurrent_launches - num_requested),  # global limit
        )

        subscriber.clear()
        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
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
        instance_manager, instance_storage, subscriber = setup
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
        instance_manager, instance_storage, subscriber = setup
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
                ray_node_id="r-1",
            ),
            create_instance(
                "updated",
                cur_status,
                status_times=[(cur_status, (cur_time_s - timeout_s - 1) * s_to_ns)],
                ray_node_id="r-2",
            ),
        ]

        TestReconciler._add_instances(instance_storage, instances)

        Reconciler.reconcile(
            instance_manager=instance_manager,
            scheduler=MockScheduler(),
            cloud_provider=MagicMock(),
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
        )

        instances, _ = instance_storage.get_instances()
        assert instances["no-update"].status == cur_status
        assert instances["updated"].status == Instance.RAY_RUNNING

    @staticmethod
    @mock.patch("time.time_ns")
    def test_ray_stop_requested_fail(mock_time_ns, setup):
        # Test that the instance should be transitioned to RAY_RUNNING
        # when the ray stop request fails.

        instance_manager, instance_storage, _ = setup
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
        instance_manager, instance_storage, subscriber = setup
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
        instance_manager, instance_storage, subscriber = setup
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
        instance_manager, instance_storage, subscriber = setup
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
        instance_manager, instance_storage, subscriber = setup

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
        instance_manager, instance_storage, _ = setup

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
        instance_manager, instance_storage, subscriber = setup

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
        instance_manager, instance_storage, _ = setup

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
        instance_manager, instance_storage, _ = setup
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
            ray_cluster_resource_state=ClusterResourceState(
                cluster_resource_state_version=1,
            ),
            non_terminated_cloud_instances=cloud_instances,
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=MockAutoscalingConfig(
                configs={
                    "max_concurrent_launches": 0,  # don't launch anything.
                }
            ),
        )

        assert autoscaling_state.last_seen_cluster_resource_state_version == 1
        assert len(autoscaling_state.infeasible_gang_resource_requests) == 1
        assert len(autoscaling_state.infeasible_resource_requests) == 1
        assert len(autoscaling_state.pending_instances) == 2
        pending_instances = {i.instance_id for i in autoscaling_state.pending_instances}
        assert pending_instances == {"i-1", "i-5"}
        pending_instance_requests = defaultdict(int)
        for r in autoscaling_state.pending_instance_requests:
            pending_instance_requests[r.ray_node_type_name] += r.count
        failed_instance_requests = defaultdict(int)
        for r in autoscaling_state.failed_instance_requests:
            failed_instance_requests[r.ray_node_type_name] += r.count
        assert pending_instance_requests == {"type-2": 1, "type-3": 1}
        assert failed_instance_requests == {"type-4": 1}

    @staticmethod
    def test_extra_cloud_instances_cloud_provider(setup):
        """
        Test that extra cloud instances should be terminated.
        """
        instance_manager, instance_storage, subscriber = setup

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
    def test_reconcile_max_worker_nodes_limit_triggers_termination(setup):
        instance_manager, instance_storage, _ = setup

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


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
