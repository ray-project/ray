import asyncio
from typing import Dict, List, Optional, Tuple
from unittest import mock

import pytest

from ray.serve._private.common import (
    DeploymentID,
    DeploymentStatus,
    DeploymentStatusTrigger,
    ReplicaID,
    ReplicaState,
    RequestProtocol,
    RunningReplicaInfo,
)
from ray.serve._private.controller import ServeController
from ray.serve._private.node_port_manager import NodePortManager
from ray.serve.config import HTTPOptions, gRPCOptions
from ray.serve.schema import (
    DeploymentDetails,
    DeploymentSchema,
    ReplicaDetails,
    Target,
    TargetGroup,
)


# Simple test KV store implementation
class FakeKVStore:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def put(self, key, value):
        self.data[key] = value

    def delete(self, key):
        if key in self.data:
            del self.data[key]


# Simplified LongPollHost for tests
class FakeLongPollHost:
    def __init__(self):
        self.notified_changes = {}

    def notify_changed(self, changes):
        self.notified_changes.update(changes)


# Application State Manager for dependency injection
class FakeApplicationStateManager:
    def __init__(self, app_statuses, route_prefixes, ingress_deployments):
        self.app_statuses = app_statuses
        self.route_prefixes = route_prefixes
        self.ingress_deployments = ingress_deployments

    def list_app_statuses(self):
        return self.app_statuses

    def get_route_prefix(self, app_name):
        return self.route_prefixes.get(app_name, f"/{app_name}")

    def get_ingress_deployment_name(self, app_name):
        return self.ingress_deployments.get(app_name, f"{app_name}_ingress")


class FakeDeploymentReplica:
    def __init__(self, node_id, replica_id: ReplicaID):
        self.actor_node_id = node_id
        self.replica_id = replica_id


class FakeProxyState:
    def __init__(self, node_id, node_instance_id, node_ip, actor_name):
        self.node_id = node_id
        self.node_ip = node_ip
        self.node_instance_id = node_instance_id
        self.actor_name = actor_name


class FakeProxyStateManager:
    def __init__(self):
        self.proxy_details = {}
        self.fallback_proxy_details = None
        self._http_options = HTTPOptions()
        self._grpc_options = gRPCOptions(
            grpc_servicer_functions=["f1"],
        )

    def add_proxy_details(self, node_id, node_instance_id, node_ip, actor_name):

        self.proxy_details[node_id] = FakeProxyState(
            node_id=node_id,
            node_ip=node_ip,
            node_instance_id=node_instance_id,
            actor_name=actor_name,
        )

    def add_fallback_proxy_details(self, node_id, node_instance_id, node_ip, actor_name):
        self.fallback_proxy_details = FakeProxyState(
            node_id=node_id,
            node_instance_id=node_instance_id,
            node_ip=node_ip,
            actor_name=actor_name,
        )

    def get_proxy_details(self):
        return self.proxy_details

    def get_targets(self, protocol: RequestProtocol):
        if protocol == RequestProtocol.HTTP:
            port = self._http_options.port
        else:
            port = self._grpc_options.port
        return [
            Target(
                ip=proxy_details.node_ip,
                port=port,
                instance_id=proxy_details.node_instance_id,
                name=proxy_details.actor_name,
            )
            for node_id, proxy_details in self.proxy_details.items()
        ]

    def get_grpc_config(self):
        return self._grpc_options

    def get_fallback_proxy_details(self):
        return self.fallback_proxy_details


class FakeReplicaStateContainer:
    def __init__(self, replica_infos: List[RunningReplicaInfo]):
        self.replica_infos = replica_infos

    def get(self):
        return [
            FakeDeploymentReplica(replica_info.node_id, replica_info.replica_id)
            for replica_info in self.replica_infos
        ]


class FakeDeploymentState:
    def __init__(self, id, replica_infos):
        self.id = id
        self._replicas = FakeReplicaStateContainer(replica_infos)


# Deployment State Manager for dependency injection
class FakeDeploymentStateManager:
    def __init__(
        self,
        running_replica_infos: Dict[DeploymentID, List[RunningReplicaInfo]],
    ):
        self.running_replica_infos = running_replica_infos

        self._deployment_states = {}
        for deployment_id, replica_infos in self.running_replica_infos.items():
            deployment_state = FakeDeploymentState(deployment_id, replica_infos)
            self._deployment_states[deployment_id] = deployment_state

    def get_running_replica_infos(self):
        return self.running_replica_infos

    def get_replica_details(self, replica_info: RunningReplicaInfo) -> ReplicaDetails:
        return ReplicaDetails(
            replica_id=replica_info.replica_id.unique_id,
            node_id=replica_info.node_id,
            node_ip=replica_info.node_ip,
            node_instance_id="",
            start_time_s=0,
            state=ReplicaState.RUNNING,
            actor_name=replica_info.replica_id.unique_id,
        )

    def get_deployment_details(self, id: DeploymentID) -> Optional[DeploymentDetails]:
        if id not in self.running_replica_infos:
            return None
        replica_details = [
            self.get_replica_details(replica_info)
            for replica_info in self.running_replica_infos[id]
        ]
        return DeploymentDetails(
            name=id.name,
            status=DeploymentStatus.HEALTHY,
            status_trigger=DeploymentStatusTrigger.UNSPECIFIED,
            message="",
            deployment_config=mock.Mock(spec=DeploymentSchema),
            target_num_replicas=1,
            required_resources={},
            replicas=replica_details,
        )

    def get_ingress_replicas_info(self) -> List[Tuple[str, str, int, int]]:
        return []


# Test Controller that overrides methods and dependencies
class FakeDirectIngressController(ServeController):
    def __init__(
        self,
        kv_store,
        long_poll_host,
        application_state_manager,
        deployment_state_manager,
        proxy_state_manager,
    ):
        # Skip parent __init__ since we'll set dependencies directly
        self.kv_store = kv_store
        self.long_poll_host = long_poll_host
        self.application_state_manager = application_state_manager
        self.deployment_state_manager = deployment_state_manager
        self.proxy_state_manager = proxy_state_manager
        self._direct_ingress_enabled = True
        self._ha_proxy_enabled = False
        self._controller_node_id = "head_node_id"

        self._shutting_down = False
        self.done_recovering_event = asyncio.Event()
        self.done_recovering_event.set()

        self.node_update_duration_gauge_s = mock.Mock()

    def _update_proxy_nodes(self):
        pass


@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    monkeypatch.setenv("RAY_SERVE_ENABLE_DIRECT_INGRESS", "1")
    monkeypatch.setenv("RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT", "30000")
    monkeypatch.setenv("RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT", "30100")
    monkeypatch.setenv("RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT", "40000")
    monkeypatch.setenv("RAY_SERVE_DIRECT_INGRESS_MAX_GRPC_PORT", "40100")

    yield


# Fixture to clear NodePortManager between tests
@pytest.fixture(autouse=True)
def reset_node_port_manager():
    # Save original managers
    original_managers = NodePortManager._node_managers.copy()

    # Clear all managers
    NodePortManager._node_managers = {}

    yield

    # Restore original managers
    NodePortManager._node_managers = original_managers


# Fixture for a minimal controller setup
@pytest.fixture
def direct_ingress_controller():
    # Setup test dependencies
    kv_store = FakeKVStore()
    long_poll_host = FakeLongPollHost()
    app_state_manager = FakeApplicationStateManager({}, {}, {})
    deployment_state_manager = FakeDeploymentStateManager({})
    proxy_state_manager = FakeProxyStateManager()
    # Create controller with test dependencies
    controller = FakeDirectIngressController(
        kv_store=kv_store,
        long_poll_host=long_poll_host,
        application_state_manager=app_state_manager,
        deployment_state_manager=deployment_state_manager,
        proxy_state_manager=proxy_state_manager,
    )

    yield controller


def test_direct_ingress_is_disabled(
    direct_ingress_controller: FakeDirectIngressController,
):
    """Test that get_target_groups returns empty list when direct ingress is disabled."""
    direct_ingress_controller._direct_ingress_enabled = False
    target_groups = direct_ingress_controller.get_target_groups()
    assert target_groups == []

    # proxy has nodes
    direct_ingress_controller.proxy_state_manager.add_proxy_details(
        "node1", "instance1", "10.0.0.1", "proxy1"
    )
    direct_ingress_controller.proxy_state_manager.add_proxy_details(
        "node2", "instance2", "10.0.0.2", "proxy2"
    )
    target_groups = direct_ingress_controller.get_target_groups()
    expected_target_groups = [
        TargetGroup(
            protocol=RequestProtocol.HTTP,
            route_prefix="/",
            targets=[
                Target(ip="10.0.0.1", port=8000, instance_id="instance1", name="proxy1"),
                Target(ip="10.0.0.2", port=8000, instance_id="instance2", name="proxy2"),
            ],
        ),
        TargetGroup(
            protocol=RequestProtocol.GRPC,
            route_prefix="/",
            targets=[
                Target(ip="10.0.0.1", port=9000, instance_id="instance1", name="proxy1"),
                Target(ip="10.0.0.2", port=9000, instance_id="instance2", name="proxy2"),
            ],
        ),
    ]
    assert target_groups == expected_target_groups


# Test with empty applications
def test_get_target_groups_empty_when_no_apps(
    direct_ingress_controller: FakeDirectIngressController,
):
    """Test that get_target_groups returns empty list when no apps are running."""
    target_groups = direct_ingress_controller.get_target_groups()
    assert target_groups == []

    # proxy has nodes
    direct_ingress_controller.proxy_state_manager.add_proxy_details(
        "node1", "instance1", "10.0.0.1", "proxy1"
    )
    direct_ingress_controller.proxy_state_manager.add_proxy_details(
        "node2", "instance2", "10.0.0.2", "proxy2"
    )
    target_groups = direct_ingress_controller.get_target_groups()
    expected_target_groups = [
        TargetGroup(
            protocol=RequestProtocol.HTTP,
            route_prefix="/",
            targets=[
                Target(ip="10.0.0.1", port=8000, instance_id="instance1", name="proxy1"),
                Target(ip="10.0.0.2", port=8000, instance_id="instance2", name="proxy2"),
            ],
        ),
        TargetGroup(
            protocol=RequestProtocol.GRPC,
            route_prefix="/",
            targets=[
                Target(ip="10.0.0.1", port=9000, instance_id="instance1", name="proxy1"),
                Target(ip="10.0.0.2", port=9000, instance_id="instance2", name="proxy2"),
            ],
        ),
    ]
    assert target_groups == expected_target_groups


# Test with running applications
def test_get_target_groups_with_running_apps(
    direct_ingress_controller: FakeDirectIngressController,
):
    """Test that get_target_groups properly returns target groups for running apps."""
    # Setup test data
    app_statuses = {
        "app1": {},
        "app2": {},
    }
    route_prefixes = {
        "app1": "/app1",
        "app2": "/app2",
    }
    ingress_deployments = {
        "app1": "app1_ingress",
        "app2": "app2_ingress",
    }

    deployment_id1 = DeploymentID(name="app1_ingress", app_name="app1")
    deployment_id2 = DeploymentID(name="app2_ingress", app_name="app2")

    # Create replica info
    replica_id1 = ReplicaID(unique_id="replica1", deployment_id=deployment_id1)
    replica_id2 = ReplicaID(unique_id="replica2", deployment_id=deployment_id2)

    replica_info1 = RunningReplicaInfo(
        replica_id=replica_id1,
        node_id="node1",
        node_ip="10.0.0.1",
        availability_zone="az1",
        actor_name="replica1",
        max_ongoing_requests=100,
    )
    replica_info2 = RunningReplicaInfo(
        replica_id=replica_id2,
        node_id="node2",
        node_ip="10.0.0.2",
        availability_zone="az2",
        actor_name="replica2",
        max_ongoing_requests=100,
    )

    running_replica_infos = {
        deployment_id1: [replica_info1],
        deployment_id2: [replica_info2],
    }

    # Setup test application state manager
    direct_ingress_controller.application_state_manager = FakeApplicationStateManager(
        app_statuses=app_statuses,
        route_prefixes=route_prefixes,
        ingress_deployments=ingress_deployments,
    )

    # Setup test deployment state manager
    direct_ingress_controller.deployment_state_manager = FakeDeploymentStateManager(
        running_replica_infos=running_replica_infos,
    )

    # setup proxy state manager
    direct_ingress_controller.proxy_state_manager.add_proxy_details(
        "node1", "instance1", "10.0.0.1", "proxy1"
    )
    direct_ingress_controller.proxy_state_manager.add_proxy_details(
        "node2", "instance2", "10.0.0.2", "proxy2"
    )

    # Allocate ports for replicas using controller's methods
    http_port1 = direct_ingress_controller.allocate_replica_port(
        "node1", replica_id1.unique_id, RequestProtocol.HTTP
    )
    grpc_port1 = direct_ingress_controller.allocate_replica_port(
        "node1", replica_id1.unique_id, RequestProtocol.GRPC
    )
    http_port2 = direct_ingress_controller.allocate_replica_port(
        "node2", replica_id2.unique_id, RequestProtocol.HTTP
    )
    grpc_port2 = direct_ingress_controller.allocate_replica_port(
        "node2", replica_id2.unique_id, RequestProtocol.GRPC
    )

    # Call get_target_groups
    target_groups = direct_ingress_controller.get_target_groups()

    # Create expected target groups for direct comparison
    expected_target_groups = [
        TargetGroup(
            protocol=RequestProtocol.HTTP,
            route_prefix="/app1",
            app_name="app1",
            targets=[
                Target(ip="10.0.0.1", port=http_port1, instance_id="", name="replica1"),
            ],
        ),
        TargetGroup(
            protocol=RequestProtocol.GRPC,
            route_prefix="/app1",
            app_name="app1",
            targets=[
                Target(ip="10.0.0.1", port=grpc_port1, instance_id="", name="replica1"),
            ],
        ),
        TargetGroup(
            protocol=RequestProtocol.HTTP,
            route_prefix="/app2",
            app_name="app2",
            targets=[
                Target(ip="10.0.0.2", port=http_port2, instance_id="", name="replica2"),
            ],
        ),
        TargetGroup(
            protocol=RequestProtocol.GRPC,
            route_prefix="/app2",
            app_name="app2",
            targets=[
                Target(ip="10.0.0.2", port=grpc_port2, instance_id="", name="replica2"),
            ],
        ),
    ]

    # Sort both lists to ensure consistent comparison
    target_groups.sort(key=lambda g: (g.protocol, g.route_prefix))
    expected_target_groups.sort(key=lambda g: (g.protocol, g.route_prefix))

    # Direct comparison
    assert target_groups == expected_target_groups

    # now release some ports
    direct_ingress_controller.release_replica_port(
        "node1", replica_id1.unique_id, http_port1, RequestProtocol.HTTP
    )
    direct_ingress_controller.release_replica_port(
        "node2", replica_id2.unique_id, grpc_port2, RequestProtocol.GRPC
    )

    # verify the ports are released
    assert not direct_ingress_controller._is_port_allocated(
        direct_ingress_controller.deployment_state_manager.get_replica_details(
            replica_info1
        ),
        RequestProtocol.HTTP,
    )
    assert not direct_ingress_controller._is_port_allocated(
        direct_ingress_controller.deployment_state_manager.get_replica_details(
            replica_info2
        ),
        RequestProtocol.GRPC,
    )

    # get the target groups again
    target_groups = direct_ingress_controller.get_target_groups()
    expected_target_groups = [
        TargetGroup(
            protocol=RequestProtocol.GRPC,
            route_prefix="/app1",
            app_name="app1",
            targets=[
                Target(ip="10.0.0.1", port=grpc_port1, instance_id="", name="replica1"),
            ],
        ),
        TargetGroup(
            protocol=RequestProtocol.HTTP,
            route_prefix="/app2",
            app_name="app2",
            targets=[
                Target(ip="10.0.0.2", port=http_port2, instance_id="", name="replica2"),
            ],
        ),
    ]

    # Sort both lists again for the second comparison
    target_groups.sort(key=lambda g: (g.protocol, g.route_prefix))
    expected_target_groups.sort(key=lambda g: (g.protocol, g.route_prefix))

    assert target_groups == expected_target_groups


def test_get_target_groups_with_port_not_allocated(
    direct_ingress_controller: FakeDirectIngressController,
):
    """Test that get_target_groups correctly filters out targets with unallocated ports."""
    # Setup test data
    app_statuses = {"app1": {}}
    route_prefixes = {"app1": "/app1"}
    ingress_deployments = {"app1": "app1_ingress"}

    # Create replica info for two replicas
    deployment_id1 = DeploymentID(name="app1_ingress", app_name="app1")
    replica_id1 = ReplicaID(unique_id="replica1", deployment_id=deployment_id1)
    replica_id2 = ReplicaID(unique_id="replica2", deployment_id=deployment_id1)

    replica_info1 = RunningReplicaInfo(
        replica_id=replica_id1,
        node_id="node1",
        node_ip="10.0.0.1",
        availability_zone="az1",
        actor_name="replica1",
        max_ongoing_requests=100,
    )
    replica_info2 = RunningReplicaInfo(
        replica_id=replica_id2,
        node_id="node2",
        node_ip="10.0.0.2",
        availability_zone="az2",
        actor_name="replica2",
        max_ongoing_requests=100,
    )

    running_replica_infos = {
        deployment_id1: [replica_info1, replica_info2],
    }

    # Setup test application state manager
    direct_ingress_controller.application_state_manager = FakeApplicationStateManager(
        app_statuses=app_statuses,
        route_prefixes=route_prefixes,
        ingress_deployments=ingress_deployments,
    )

    # Setup test deployment state manager
    direct_ingress_controller.deployment_state_manager = FakeDeploymentStateManager(
        running_replica_infos=running_replica_infos,
    )

    # Only allocate ports for the first replica, leave the second one without ports
    http_port1 = direct_ingress_controller.allocate_replica_port(
        "node1", replica_id1.unique_id, RequestProtocol.HTTP
    )
    grpc_port1 = direct_ingress_controller.allocate_replica_port(
        "node1", replica_id1.unique_id, RequestProtocol.GRPC
    )

    # Call get_target_groups
    target_groups = direct_ingress_controller.get_target_groups()

    # Create expected target groups
    expected_target_groups = [
        TargetGroup(
            protocol=RequestProtocol.HTTP,
            route_prefix="/app1",
            app_name="app1",
            targets=[
                Target(ip="10.0.0.1", port=http_port1, instance_id="", name="replica1"),
            ],
        ),
        TargetGroup(
            protocol=RequestProtocol.GRPC,
            route_prefix="/app1",
            app_name="app1",
            targets=[
                Target(ip="10.0.0.1", port=grpc_port1, instance_id="", name="replica1"),
            ],
        ),
    ]

    # Sort both lists to ensure consistent comparison
    target_groups.sort(key=lambda g: (g.protocol, g.route_prefix))
    expected_target_groups.sort(key=lambda g: (g.protocol, g.route_prefix))

    # Direct comparison
    assert target_groups == expected_target_groups


def test_get_target_groups_only_includes_ingress_deployments(
    direct_ingress_controller: FakeDirectIngressController,
):
    """Test that get_target_groups only includes ingress deployments and not regular deployments."""
    # Setup test data
    app_statuses = {"app1": {}}
    route_prefixes = {"app1": "/app1"}
    ingress_deployments = {"app1": "app1_ingress"}

    # Create replica info for ingress deployment
    ingress_deployment_id = DeploymentID(name="app1_ingress", app_name="app1")
    ingress_replica_id = ReplicaID(
        unique_id="ingress_replica", deployment_id=ingress_deployment_id
    )
    ingress_replica_info = RunningReplicaInfo(
        replica_id=ingress_replica_id,
        node_id="node1",
        node_ip="10.0.0.1",
        availability_zone="az1",
        actor_name="ingress_replica",
        max_ongoing_requests=100,
    )

    # Create replica info for regular non-ingress deployment
    regular_deployment_id = DeploymentID(name="app1_model", app_name="app1")
    regular_replica_id = ReplicaID(
        unique_id="regular_replica", deployment_id=regular_deployment_id
    )
    regular_replica_info = RunningReplicaInfo(
        replica_id=regular_replica_id,
        node_id="node2",
        node_ip="10.0.0.2",
        availability_zone="az2",
        actor_name="regular_replica",
        max_ongoing_requests=100,
    )

    # Set up running replica infos for both deployments
    running_replica_infos = {
        ingress_deployment_id: [ingress_replica_info],
        regular_deployment_id: [regular_replica_info],
    }

    # Setup test application state manager
    direct_ingress_controller.application_state_manager = FakeApplicationStateManager(
        app_statuses=app_statuses,
        route_prefixes=route_prefixes,
        ingress_deployments=ingress_deployments,
    )

    # Setup test deployment state manager
    direct_ingress_controller.deployment_state_manager = FakeDeploymentStateManager(
        running_replica_infos=running_replica_infos,
    )

    # Allocate ports for both replicas
    ingress_http_port = direct_ingress_controller.allocate_replica_port(
        "node1", ingress_replica_id.unique_id, RequestProtocol.HTTP
    )
    ingress_grpc_port = direct_ingress_controller.allocate_replica_port(
        "node1", ingress_replica_id.unique_id, RequestProtocol.GRPC
    )
    _ = direct_ingress_controller.allocate_replica_port(
        "node2", regular_replica_id.unique_id, RequestProtocol.HTTP
    )
    _ = direct_ingress_controller.allocate_replica_port(
        "node2", regular_replica_id.unique_id, RequestProtocol.GRPC
    )

    # Call get_target_groups
    target_groups = direct_ingress_controller.get_target_groups()

    # Create expected target groups - only including the ingress deployment
    expected_target_groups = [
        TargetGroup(
            protocol=RequestProtocol.HTTP,
            route_prefix="/app1",
            app_name="app1",
            targets=[
                Target(
                    ip="10.0.0.1",
                    port=ingress_http_port,
                    instance_id="",
                    name="ingress_replica",
                ),
            ],
        ),
        TargetGroup(
            protocol=RequestProtocol.GRPC,
            route_prefix="/app1",
            app_name="app1",
            targets=[
                Target(
                    ip="10.0.0.1",
                    port=ingress_grpc_port,
                    instance_id="",
                    name="ingress_replica",
                ),
            ],
        ),
    ]

    # Sort both lists to ensure consistent comparison
    target_groups.sort(key=lambda g: (g.protocol, g.route_prefix))
    expected_target_groups.sort(key=lambda g: (g.protocol, g.route_prefix))

    # Direct comparison - regular deployment should not be included
    assert target_groups == expected_target_groups

    # Verify all ports are still allocated even though not all are included in target groups
    assert direct_ingress_controller._is_port_allocated(
        direct_ingress_controller.deployment_state_manager.get_replica_details(
            ingress_replica_info
        ),
        RequestProtocol.HTTP,
    )
    assert direct_ingress_controller._is_port_allocated(
        direct_ingress_controller.deployment_state_manager.get_replica_details(
            ingress_replica_info
        ),
        RequestProtocol.GRPC,
    )
    assert direct_ingress_controller._is_port_allocated(
        direct_ingress_controller.deployment_state_manager.get_replica_details(
            regular_replica_info
        ),
        RequestProtocol.HTTP,
    )
    assert direct_ingress_controller._is_port_allocated(
        direct_ingress_controller.deployment_state_manager.get_replica_details(
            regular_replica_info
        ),
        RequestProtocol.GRPC,
    )


def test_get_target_groups_app_with_no_running_replicas(
    direct_ingress_controller: FakeDirectIngressController,
):
    """Test that get_target_groups correctly handles apps with no running replicas."""
    # Setup test data for two apps - one with replicas, one without
    app_statuses = {
        "app1": {},  # App with replicas
        "app2": {},  # App with no replicas
    }
    route_prefixes = {
        "app1": "/app1",
        "app2": "/app2",
    }
    ingress_deployments = {
        "app1": "app1_ingress",
        "app2": "app2_ingress",
    }

    # Create replica info for app1's ingress deployment
    deployment_id1 = DeploymentID(name="app1_ingress", app_name="app1")
    replica_id1 = ReplicaID(unique_id="replica1", deployment_id=deployment_id1)
    replica_info1 = RunningReplicaInfo(
        replica_id=replica_id1,
        node_id="node1",
        node_ip="10.0.0.1",
        availability_zone="az1",
        actor_name="replica1",
        max_ongoing_requests=100,
    )

    # Note: No replicas for app2_ingress

    # Running replica infos only for app1
    running_replica_infos = {
        deployment_id1: [replica_info1],
        # No entry for app2_ingress
    }

    # Setup test application state manager
    direct_ingress_controller.application_state_manager = FakeApplicationStateManager(
        app_statuses=app_statuses,
        route_prefixes=route_prefixes,
        ingress_deployments=ingress_deployments,
    )

    # Setup test deployment state manager
    direct_ingress_controller.deployment_state_manager = FakeDeploymentStateManager(
        running_replica_infos=running_replica_infos,
    )

    # setup proxy state manager
    direct_ingress_controller.proxy_state_manager.add_proxy_details(
        "node1", "instance1", "10.0.0.1", "proxy1"
    )
    direct_ingress_controller.proxy_state_manager.add_proxy_details(
        "node2", "instance2", "10.0.0.2", "proxy2"
    )

    # Allocate ports for the only existing replica
    http_port = direct_ingress_controller.allocate_replica_port(
        "node1", replica_id1.unique_id, RequestProtocol.HTTP
    )
    grpc_port = direct_ingress_controller.allocate_replica_port(
        "node1", replica_id1.unique_id, RequestProtocol.GRPC
    )

    # Call get_target_groups
    target_groups = direct_ingress_controller.get_target_groups()

    # Create expected target groups - only including app1, nothing for app2
    expected_target_groups = [
        TargetGroup(
            protocol=RequestProtocol.HTTP,
            route_prefix="/app1",
            app_name="app1",
            targets=[
                Target(ip="10.0.0.1", port=http_port, instance_id="", name="replica1"),
            ],
        ),
        TargetGroup(
            protocol=RequestProtocol.GRPC,
            route_prefix="/app1",
            app_name="app1",
            targets=[
                Target(ip="10.0.0.1", port=grpc_port, instance_id="", name="replica1"),
            ],
        ),
        TargetGroup(
            protocol=RequestProtocol.HTTP,
            route_prefix="/app2",
            app_name="app2",
            targets=[
                Target(ip="10.0.0.1", port=8000, instance_id="instance1", name="proxy1"),
                Target(ip="10.0.0.2", port=8000, instance_id="instance2", name="proxy2"),
            ],
        ),
        TargetGroup(
            protocol=RequestProtocol.GRPC,
            route_prefix="/app2",
            app_name="app2",
            targets=[
                Target(ip="10.0.0.1", port=9000, instance_id="instance1", name="proxy1"),
                Target(ip="10.0.0.2", port=9000, instance_id="instance2", name="proxy2"),
            ],
        ),
    ]

    # Sort both lists to ensure consistent comparison
    target_groups.sort(key=lambda g: (g.protocol, g.route_prefix))
    expected_target_groups.sort(key=lambda g: (g.protocol, g.route_prefix))

    # Direct comparison - app2 should have no target groups since it has no replicas
    assert target_groups == expected_target_groups


def test_control_loop_pruning(
    direct_ingress_controller: FakeDirectIngressController,
):
    """Test that the controller loop properly prunes stale node port managers."""
    # Setup replica info for testing
    deployment_id = DeploymentID(name="app1_ingress", app_name="app1")
    replica_id1 = ReplicaID(unique_id="replica1", deployment_id=deployment_id)
    replica_id2 = ReplicaID(unique_id="replica2", deployment_id=deployment_id)
    replica_id3 = ReplicaID(unique_id="replica3", deployment_id=deployment_id)

    replica_info1 = RunningReplicaInfo(
        replica_id=replica_id1,
        node_id="node1",
        node_ip="10.0.0.1",
        availability_zone="az1",
        actor_name="replica1",
        max_ongoing_requests=100,
    )
    replica_info2 = RunningReplicaInfo(
        replica_id=replica_id2,
        node_id="node1",
        node_ip="10.0.0.1",
        availability_zone="az1",
        actor_name="replica2",
        max_ongoing_requests=100,
    )
    replica_info3 = RunningReplicaInfo(
        replica_id=replica_id3,
        node_id="node2",
        node_ip="10.0.0.2",
        availability_zone="az2",
        actor_name="replica3",
        max_ongoing_requests=100,
    )

    running_replica_infos = {
        deployment_id: [replica_info1, replica_info2, replica_info3],
    }

    # Set up controller with the replica info
    direct_ingress_controller.deployment_state_manager = FakeDeploymentStateManager(
        running_replica_infos=running_replica_infos,
    )

    # Allocate ports for testing
    direct_ingress_controller.allocate_replica_port(
        "node1", replica_id1.unique_id, RequestProtocol.HTTP
    )
    direct_ingress_controller.allocate_replica_port(
        "node1", replica_id2.unique_id, RequestProtocol.HTTP
    )
    direct_ingress_controller.allocate_replica_port(
        "node1", replica_id3.unique_id, RequestProtocol.HTTP
    )  # This should be pruned
    direct_ingress_controller.allocate_replica_port(
        "node2", replica_id3.unique_id, RequestProtocol.HTTP
    )
    direct_ingress_controller.allocate_replica_port(
        "node3", "replica4", RequestProtocol.HTTP
    )  # Node should be pruned

    # Verify ports are initially allocated
    assert direct_ingress_controller._is_port_allocated(
        direct_ingress_controller.deployment_state_manager.get_replica_details(
            replica_info1
        ),
        RequestProtocol.HTTP,
    )
    assert direct_ingress_controller._is_port_allocated(
        direct_ingress_controller.deployment_state_manager.get_replica_details(
            replica_info2
        ),
        RequestProtocol.HTTP,
    )
    assert direct_ingress_controller._is_port_allocated(
        direct_ingress_controller.deployment_state_manager.get_replica_details(
            replica_info3
        ),
        RequestProtocol.HTTP,
    )

    # We need to use NodePortManager directly for this check since we don't have a ReplicaInfo for stale_replica
    node1_manager = NodePortManager.get_node_manager("node1")
    node3_manager = NodePortManager.get_node_manager("node3")
    assert node1_manager.is_port_allocated(replica_id3.unique_id, RequestProtocol.HTTP)
    assert node3_manager.is_port_allocated("replica4", RequestProtocol.HTTP)

    # Call the control loop step - this should trigger port pruning
    direct_ingress_controller._maybe_update_ingress_ports()

    # Verify the active replicas still have their ports
    assert direct_ingress_controller._is_port_allocated(
        direct_ingress_controller.deployment_state_manager.get_replica_details(
            replica_info1
        ),
        RequestProtocol.HTTP,
    )
    assert direct_ingress_controller._is_port_allocated(
        direct_ingress_controller.deployment_state_manager.get_replica_details(
            replica_info2
        ),
        RequestProtocol.HTTP,
    )
    assert direct_ingress_controller._is_port_allocated(
        direct_ingress_controller.deployment_state_manager.get_replica_details(
            replica_info3
        ),
        RequestProtocol.HTTP,
    )

    # Verify stale ports were pruned
    assert not node1_manager.is_port_allocated(
        replica_id3.unique_id, RequestProtocol.HTTP
    )
    assert "node3" not in NodePortManager._node_managers  # Entire node should be pruned


if __name__ == "__main__":
    pytest.main()
