# coding: utf-8
"""
Test recovery behavior for the ALLOCATION_TIMEOUT scenario.

Verifies that when an instance reaches ALLOCATION_TIMEOUT, the autoscaler can:
1. Terminate the timed-out old instance.
2. Launch a replacement instance.
3. Avoid a QUEUED->REQUESTED->QUEUED loop.

Test design principles:
- Pure Python, mocking only the k8s client.
- Validate instance state rather than log output.
- Run reconcile multiple times to verify the state does not get stuck.
"""

import time
from typing import Any, Dict, List

import pytest

from ray.autoscaler.v2.instance_manager.config import InstanceReconcileConfig, Provider
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    ICloudInstanceProvider,
)
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.cloud_instance_updater import (
    CloudInstanceUpdater,
)
from ray.autoscaler.v2.instance_manager.subscribers.cloud_resource_monitor import (
    CloudResourceMonitor,
)
from ray.autoscaler.v2.scheduler import (
    ResourceDemandScheduler,
)
from ray.autoscaler.v2.tests.util import create_instance
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceState,
    NodeState,
    NodeStatus,
)
from ray.core.generated.instance_manager_pb2 import Instance, NodeKind

s_to_ns = 1 * 1_000_000_000


class MockAutoscalingConfig:
    """Mock autoscaling config for testing"""

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


class EventCapturingSubscriber:
    """
    Subscriber that captures events for verification and delegates to CloudInstanceUpdater.

    This wraps the real CloudInstanceUpdater to capture events for test assertions
    while using the actual launch/terminate logic.
    """

    def __init__(self, cloud_provider: ICloudInstanceProvider):
        self.cloud_provider = cloud_provider
        self.updater = CloudInstanceUpdater(cloud_provider=cloud_provider)
        self.events = []

    def notify(self, events):
        self.events.extend(events)
        # Delegate to real updater which calls cloud_provider.launch/terminate
        self.updater.notify(events)

    def clear(self):
        self.events.clear()

    def events_by_id(self, instance_id):
        return [e for e in self.events if e.instance_id == instance_id]


class MockK8sClient:
    """
    Mock Kubernetes API client that simulates RayCluster behavior.

    Tracks:
    - replicas: current replica count
    - workers_to_delete: list of worker pod names to delete
    - patch_history: history of all patches for verification
    """

    def __init__(self, max_replicas=3, initial_replicas=3, worker_pod_names=None):
        self.max_replicas = max_replicas
        self.replicas = initial_replicas
        self.workers_to_delete: List[str] = []
        self.patch_history: List[Dict] = []
        self._resource_version = 100
        # Worker pod names - defaults to worker-001, worker-002, worker-003
        self.worker_pod_names = worker_pod_names or ["worker-001", "worker-002", "worker-003"]

    def get(self, path: str) -> Dict[str, Any]:
        """Handle GET requests"""
        if "rayclusters" in path:
            return {
                "metadata": {
                    "name": "test-ray-cluster",
                    "namespace": "default",
                    "resourceVersion": str(self._resource_version),
                },
                "spec": {
                    "workerGroupSpecs": [
                        {
                            "groupName": "default-worker-group",
                            "replicas": self.replicas,
                            "minReplicas": 0,
                            "maxReplicas": self.max_replicas,
                            "scaleStrategy": {
                                "workersToDelete": list(self.workers_to_delete)
                            },
                            "numOfHosts": 1,
                        }
                    ]
                },
            }
        elif "pods" in path:
            # Return pods based on current replicas (excluding workers_to_delete)
            # Use worker_pod_names to match the cloud_instances in the test
            items = []
            for i in range(min(self.replicas, len(self.worker_pod_names))):
                pod_name = self.worker_pod_names[i]
                if pod_name not in self.workers_to_delete:
                    # worker-003 is in ALLOCATION_TIMEOUT state, so it should not be running
                    if pod_name == "worker-003":
                        # Pod is pending/failed - not running
                        container_state = {"waiting": {"reason": "ContainerCreating"}}
                    else:
                        container_state = {"running": {}}

                    items.append(
                        {
                            "metadata": {
                                "name": pod_name,
                                "labels": {
                                    "ray.io/cluster": "test-ray-cluster",
                                    "ray.io/node-type": "worker",
                                    "ray.io/group": "default-worker-group",
                                },
                            },
                            "status": {
                                "containerStatuses": [{"state": container_state}]
                            },
                        }
                    )
            return {
                "metadata": {"resourceVersion": str(self._resource_version)},
                "items": items,
            }
        return {}

    def patch(self, path: str, payload: List[Dict]) -> Dict[str, Any]:
        """Handle PATCH requests and update internal state"""
        self.patch_history.append({"path": path, "payload": payload})
        self._resource_version += 1

        for op in payload:
            if op["op"] == "replace" and "replicas" in op["path"]:
                self.replicas = op["value"]
            elif op["op"] == "replace" and "scaleStrategy" in op["path"]:
                self.workers_to_delete = op["value"].get("workersToDelete", [])

        return {}




class TestAllocationTimeoutRecovery:
    """Test ALLOCATION_TIMEOUT instance recovery"""

    @staticmethod
    def _add_instances(instance_storage, instances):
        for instance in instances:
            ok, _ = instance_storage.upsert_instance(instance)
            assert ok

    def test_no_queued_requested_loop(self):
        """
        Minimal reproduction path:

        Preconditions:
        - maxReplicas = 3
        - 3 worker instances: 2 ALLOCATED (healthy), 1 ALLOCATION_TIMEOUT
        - idle_worker_nodes = 1

        Steps:
        1. Run Reconciler.reconcile() multiple times to simulate repeated
           reconciler cycles.

        Expected results:
        1. The new instance does not enter a QUEUED->REQUESTED->QUEUED loop.
        2. The new instance can eventually transition into REQUESTED.
        3. Terminate is submitted to Kubernetes before launch.
        """
        # ===== Preconditions =====
        instance_storage = InstanceStorage(
            cluster_id="test_cluster_id",
            storage=InMemoryStorage(),
        )

        cloud_resource_monitor = CloudResourceMonitor()

        # Mock K8s client: maxReplicas=3, initial_replicas=3
        # Worker pod names match the cloud_instance_id in cloud_instances
        mock_k8s = MockK8sClient(
            max_replicas=3,
            initial_replicas=3,
            worker_pod_names=["worker-001", "worker-002", "worker-003"]
        )

        # Create real cloud provider with mock k8s client
        from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.cloud_provider import (
            KubeRayProvider,
        )

        cloud_provider = KubeRayProvider(
            cluster_name="test-ray-cluster",
            provider_config={"namespace": "default"},
            k8s_api_client=mock_k8s,
        )

        # Use EventCapturingSubscriber that wraps real CloudInstanceUpdater
        # and captures events for test verification
        mock_subscriber = EventCapturingSubscriber(cloud_provider=cloud_provider)

        instance_manager = InstanceManager(
            instance_storage=instance_storage,
            instance_status_update_subscribers=[mock_subscriber],
        )

        # Create 2 ALLOCATED instances and 1 ALLOCATION_TIMEOUT instance.
        current_time = time.time_ns()
        timeout_time = current_time - 200 * s_to_ns  # 200s ago, beyond the timeout threshold.

        from ray._common.utils import binary_to_hex

        instances = [
            # Head node
            create_instance(
                "head",
                status=Instance.RAY_RUNNING,
                cloud_instance_id="head-001",
                node_kind=NodeKind.HEAD,
                instance_type="head",
                ray_node_id=binary_to_hex(b"head"),
            ),
            # Worker 1: healthy
            create_instance(
                "worker-1",
                status=Instance.ALLOCATED,
                instance_type="default-worker-group",
                cloud_instance_id="worker-001",
                node_kind=NodeKind.WORKER,
                ray_node_id=binary_to_hex(b"wkr1"),
            ),
            # Worker 2: healthy
            create_instance(
                "worker-2",
                status=Instance.ALLOCATED,
                instance_type="default-worker-group",
                cloud_instance_id="worker-002",
                node_kind=NodeKind.WORKER,
                ray_node_id=binary_to_hex(b"wkr2"),
            ),
            # Worker 3: ALLOCATION_TIMEOUT (startup timed out)
            create_instance(
                "worker-3",
                status=Instance.ALLOCATION_TIMEOUT,
                instance_type="default-worker-group",
                cloud_instance_id="worker-003",
                node_kind=NodeKind.WORKER,
                status_times=[(Instance.ALLOCATION_TIMEOUT, timeout_time)],
            ),
        ]

        TestAllocationTimeoutRecovery._add_instances(instance_storage, instances)

        # Mock ray nodes: head + 2 healthy workers.
        # ray_node_id must match the instance node_id.
        # available_resources=0 means resources are fully consumed, so scale-up is needed.
        ray_nodes = [
            NodeState(
                node_id=b"head",
                status=NodeStatus.RUNNING,
                instance_id="head-001",
                total_resources={"CPU": 0},
                available_resources={"CPU": 0},
            ),
            NodeState(
                node_id=b"wkr1",
                status=NodeStatus.RUNNING,
                instance_id="worker-001",
                total_resources={"CPU": 4},
                available_resources={"CPU": 0},  # Resources are fully consumed.
            ),
            NodeState(
                node_id=b"wkr2",
                status=NodeStatus.RUNNING,
                instance_id="worker-002",
                total_resources={"CPU": 4},
                available_resources={"CPU": 0},  # Resources are fully consumed.
            ),
        ]

        # Mock cloud instances
        # worker-003 must be included because the cloud instance still exists
        # while the instance is in ALLOCATION_TIMEOUT.
        # is_running=False means the pod is not running normally.
        cloud_instances = {
            "head-001": CloudInstance("head-001", "head", True, NodeKind.HEAD),
            "worker-001": CloudInstance(
                "worker-001", "default-worker-group", True, NodeKind.WORKER
            ),
            "worker-002": CloudInstance(
                "worker-002", "default-worker-group", True, NodeKind.WORKER
            ),
            "worker-003": CloudInstance(
                "worker-003", "default-worker-group", False, NodeKind.WORKER  # is_running=False
            ),
        }

        # Use the real ResourceDemandScheduler.

        scheduler = ResourceDemandScheduler()

        # Add pending_resource_requests to simulate resource demand.
        # Request 4 CPU while current available CPU is 0, so one more worker is needed.
        from ray.core.generated.autoscaler_pb2 import (
            ResourceRequest,
            ResourceRequestByCount,
        )
        ray_cluster_resource_state = ClusterResourceState(
            node_states=ray_nodes,
            pending_resource_requests=[
                ResourceRequestByCount(
                    request=ResourceRequest(resources_bundle={"CPU": 4}),
                    count=1
                ),
            ],
        )

        # Mock autoscaling config. Use MockAutoscalingConfig to avoid schema validation.
        # Build real NodeTypeConfig objects.
        from ray.autoscaler.v2.instance_manager.config import NodeTypeConfig

        node_type_configs = {
            "default-worker-group": NodeTypeConfig(
                name="default-worker-group",
                min_worker_nodes=0,
                max_worker_nodes=3,
                idle_worker_nodes=0,
                resources={"CPU": 4},
                labels={},
                launch_config_hash="hash1",
                idle_timeout_s=None,
            ),
            "head": NodeTypeConfig(
                name="head",
                min_worker_nodes=0,
                max_worker_nodes=1,
                idle_worker_nodes=0,
                resources={"CPU": 0},
                labels={},
                launch_config_hash="hash1",
                idle_timeout_s=None,
            ),
        }

        autoscaling_config = MockAutoscalingConfig(
            configs={
                "node_type_configs": node_type_configs,
                "max_num_worker_nodes": 3,
                "upscaling_speed": 1.0,
                "max_concurrent_launches": 100,
                "instance_reconcile_config": InstanceReconcileConfig(
                    request_status_timeout_s=10,
                    allocate_status_timeout_s=300,
                ),
                "disable_node_updaters": True,
                "disable_launch_config_check": True,
                "idle_timeout_s": 999,
            }
        )

        # ===== Steps: run reconcile multiple times =====
        # Run 3 reconcile cycles to simulate repeated reconciler execution.
        for cycle in range(3):
            # Clear subscriber events so each iteration is checked independently.
            mock_subscriber.events.clear()

            Reconciler.reconcile(
                instance_manager=instance_manager,
                scheduler=scheduler,
                cloud_provider=cloud_provider,
                cloud_resource_monitor=cloud_resource_monitor,
                ray_cluster_resource_state=ray_cluster_resource_state,
                non_terminated_cloud_instances=cloud_instances,
                cloud_provider_errors=[],
                ray_install_errors=[],
                autoscaling_config=autoscaling_config,
            )

            # Fetch current instance state to ensure repeated reconcile calls do not fail.
            instance_storage.get_instances()

        # ===== Expected result checks =====

        # Get final instance states.
        all_instances, _ = instance_storage.get_instances()

        # 1. Verify the ALLOCATION_TIMEOUT instance transitions to TERMINATING.
        worker_3 = all_instances.get("worker-3")
        assert worker_3 is not None, "Expected worker-3 instance to exist"
        assert worker_3.status == Instance.TERMINATING, (
            f"Expected worker-3 status to be TERMINATING, "
            f"got {Instance.InstanceStatus.Name(worker_3.status)}"
        )

        # 2. Verify at least one new instance is created (QUEUED or REQUESTED).
        new_instances = [
            i
            for i in all_instances.values()
            if i.status in (Instance.QUEUED, Instance.REQUESTED)
        ]
        assert len(new_instances) >= 1, (
            f"Expected at least 1 new instance (QUEUED/REQUESTED), "
            f"got {len(new_instances)}"
        )

        # 3. Verify K8s patch history shows terminate before launch.
        # Each patch may contain both replicas and scaleStrategy updates.
        # Key checks:
        #   a) There should be two patches (terminate + launch).
        #   b) The first patch should contain workersToDelete.
        #   c) The second patch should increase replicas.

        # Verify there are two patches (terminate + launch).
        assert len(mock_k8s.patch_history) == 2, (
            f"Expected 2 patches (terminate + launch), got {len(mock_k8s.patch_history)}. "
            "If only 1 patch, launch may have failed due to maxReplicas bug."
        )

        # Verify the first patch contains workersToDelete.
        first_patch = mock_k8s.patch_history[0]
        first_patch_payload_str = str(first_patch.get("payload", []))
        assert "workersToDelete" in first_patch_payload_str, (
            f"Expected first patch to contain workersToDelete (terminate before launch). "
            f"First patch: {first_patch}"
        )

        # Verify workersToDelete contains the timed-out instance.
        for op in first_patch["payload"]:
            if "scaleStrategy" in op.get("path", ""):
                workers_to_delete = op["value"].get("workersToDelete", [])
                assert "worker-003" in workers_to_delete, (
                    f"Expected worker-003 in workersToDelete, got {workers_to_delete}"
                )
                break

        # Verify the second patch increases replicas.
        second_patch = mock_k8s.patch_history[1]
        for op in second_patch["payload"]:
            if "replicas" in op.get("path", ""):
                assert op["value"] == 3, (
                    f"Expected replicas=3 after launch, got {op['value']}"
                )
                break

        # 4. Verify final replicas do not exceed maxReplicas.
        assert mock_k8s.replicas <= 3, (
            f"Expected replicas <= 3, got {mock_k8s.replicas}"
        )

        # 5. Verify no instance enters a REQUESTED->QUEUED->REQUESTED loop.
        # Check whether status_history contains a repeated transition pattern.
        for instance in all_instances.values():
            status_sequence = [h.instance_status for h in instance.status_history]
            # Check for a REQUESTED->QUEUED->REQUESTED pattern.
            for i in range(len(status_sequence) - 2):
                assert not (
                    status_sequence[i] == Instance.REQUESTED
                    and status_sequence[i + 1] == Instance.QUEUED
                    and status_sequence[i + 2] == Instance.REQUESTED
                ), (
                    f"Instance {instance.instance_id} entered REQUESTED->QUEUED->REQUESTED loop. "
                    f"Status sequence: {[Instance.InstanceStatus.Name(s) for s in status_sequence]}"
                )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
