# coding: utf-8
import os
import sys
import time

import pytest

from mock import MagicMock

from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.node_provider import (  # noqa
    CloudInstance,
    LaunchNodeError,
    TerminateNodeError,
)
from ray.autoscaler.v2.instance_manager.ray_installer import RayInstallError
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler, logger
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.scheduler import IResourceScheduler, SchedulingReply
from ray.autoscaler.v2.tests.util import MockSubscriber, create_instance
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceState,
    NodeState,
    NodeStatus,
)
from ray.core.generated.instance_manager_pb2 import Instance, NodeKind

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

    def get_upscaling_speed(self):
        return self._configs.get("upscaling_speed", 0.0)

    def get_max_concurrent_launches(self):
        return self._configs.get("max_concurrent_launches", 100)


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
            "c-1": CloudInstance("c-1", "type-1", "", True, NodeKind.WORKER),
            "c-2": CloudInstance("c-2", "type-999", "", True, NodeKind.WORKER),
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
            "c-1": CloudInstance("c-1", "type-1", "", True, NodeKind.WORKER),
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
        instance_manager, instance_storage, _ = setup

        instances = [
            create_instance(
                "i-1",
                status=Instance.ALLOCATED,
                instance_type="type-1",
                cloud_instance_id="c-1",
            ),
            create_instance(
                "i-2",
                status=Instance.TERMINATING,
                instance_type="type-2",
                cloud_instance_id="c-2",
            ),
        ]
        TestReconciler._add_instances(instance_storage, instances)

        cloud_instances = {
            "c-2": CloudInstance("c-2", "type-2", "", False, NodeKind.WORKER),
        }

        termination_errors = [
            TerminateNodeError(
                cloud_instance_id="c-2",
                timestamp_ns=1,
                request_id="t1",
            )
        ]

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
        assert len(instances) == 2
        assert instances["i-1"].status == Instance.TERMINATED
        assert not instances["i-1"].cloud_instance_id
        assert instances["i-2"].status == Instance.TERMINATION_FAILED

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
            "c-1": CloudInstance("c-1", "type-1", "", True, NodeKind.WORKER),
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
            NodeState(
                node_id=b"r-2", status=NodeStatus.RUNNING, instance_id="c-unknown"
            ),
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
            "c-1": CloudInstance("c-1", "type-1", "", True, NodeKind.WORKER),
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
        assert instances["i-1"].node_id == "r-1"

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
            "c-1": CloudInstance("c-1", "type-1", "", True, NodeKind.WORKER),
            "c-2": CloudInstance("c-2", "type-2", "", True, NodeKind.WORKER),
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
            "c-1": CloudInstance("c-1", "type-1", "", True, NodeKind.WORKER),
            "c-2": CloudInstance("c-2", "type-2", "", True, NodeKind.WORKER),
            "c-3": CloudInstance("c-3", "type-3", "", True, NodeKind.WORKER),
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
            "c-1": CloudInstance("c-1", "type-1", "", True, NodeKind.WORKER),
            "c-2": CloudInstance("c-2", "type-2", "", True, NodeKind.WORKER),
            "c-3": CloudInstance("c-3", "type-3", "", True, NodeKind.WORKER),
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
        assert instances["i-1"].status == Instance.RAY_STOPPED
        assert instances["i-2"].status == Instance.RAY_STOPPED
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
            "c-1": CloudInstance("c-1", "type-1", "", True, NodeKind.WORKER),
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
        assert instances["i-1"].status == Instance.RAY_INSTALL_FAILED

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
        "max_concurrent_launches,num_allocated,num_requested",
        [
            (1, 0, 0),
            (10, 0, 0),
            (1, 0, 1),
            (1, 1, 0),
            (10, 1, 0),
            (10, 0, 1),
            (10, 5, 5),
        ],
    )
    @pytest.mark.parametrize(
        "upscaling_speed",
        [0.0, 0.1, 0.5, 1.0, 100.0],
    )
    def test_max_concurrent_launches(
        max_concurrent_launches, num_allocated, num_requested, upscaling_speed, setup
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
                instance.cloud_instance_id, "type-1", "", True, NodeKind.WORKER
            )
            TestReconciler._add_instances(instance_storage, [instance])

        # Add some requested instances.
        for _ in range(num_requested):
            instance = create_instance(
                str(next_id), status=Instance.REQUESTED, instance_type="type-1"
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

        num_desired_upscale = max(1, upscaling_speed * (num_requested + num_allocated))
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


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
