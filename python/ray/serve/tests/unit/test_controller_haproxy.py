import pytest

from ray.serve._private.common import (
    DeploymentID,
    ReplicaID,
    RequestProtocol,
    RunningReplicaInfo,
)
from ray.serve.schema import (
    Target,
    TargetGroup,
)
from ray.serve.tests.unit.test_controller_direct_ingress import (
    FakeApplicationStateManager,
    FakeDeploymentStateManager,
    FakeDirectIngressController,
    FakeKVStore,
    FakeLongPollHost,
    FakeProxyStateManager,
)


# Test Controller that overrides methods and dependencies for HAProxy testing
class FakeHAProxyController(FakeDirectIngressController):
    def __init__(
        self,
        kv_store,
        long_poll_host,
        application_state_manager,
        deployment_state_manager,
        proxy_state_manager,
    ):
        super().__init__(
            kv_store=kv_store,
            long_poll_host=long_poll_host,
            application_state_manager=application_state_manager,
            deployment_state_manager=deployment_state_manager,
            proxy_state_manager=proxy_state_manager,
        )

        self._ha_proxy_enabled = True


@pytest.fixture
def haproxy_controller():
    kv_store = FakeKVStore()
    long_poll_host = FakeLongPollHost()
    app_state_manager = FakeApplicationStateManager({}, {}, {})
    deployment_state_manager = FakeDeploymentStateManager({})
    proxy_state_manager = FakeProxyStateManager()
    proxy_state_manager.add_fallback_proxy_details(
        "fallback_node_id", "fallback_instance_id", "10.0.0.1", "fallback_proxy"
    )

    controller = FakeHAProxyController(
        kv_store=kv_store,
        long_poll_host=long_poll_host,
        application_state_manager=app_state_manager,
        deployment_state_manager=deployment_state_manager,
        proxy_state_manager=proxy_state_manager,
    )

    yield controller


@pytest.mark.parametrize("from_proxy_manager", [True, False])
@pytest.mark.parametrize("ha_proxy_enabled", [True, False])
def test_get_target_groups_haproxy(
    haproxy_controller: FakeHAProxyController,
    from_proxy_manager: bool,
    ha_proxy_enabled: bool,
):
    """Tests get_target_groups returns the appropriate target groups based on the
    ha_proxy_enabled and from_proxy_manager parameters."""

    haproxy_controller._ha_proxy_enabled = ha_proxy_enabled

    # Setup test data with running applications
    app_statuses = {"app1": {}}
    route_prefixes = {"app1": "/app1"}
    ingress_deployments = {"app1": "app1_ingress"}

    deployment_id1 = DeploymentID(name="app1_ingress", app_name="app1")

    # Create replica info
    replica_id1 = ReplicaID(unique_id="replica1", deployment_id=deployment_id1)
    replica_info1 = RunningReplicaInfo(
        replica_id=replica_id1,
        node_id="node1",
        node_ip="10.0.0.1",
        availability_zone="az1",
        actor_name="replica1",
        max_ongoing_requests=100,
    )

    running_replica_infos = {deployment_id1: [replica_info1]}

    # Setup test application state manager
    haproxy_controller.application_state_manager = FakeApplicationStateManager(
        app_statuses=app_statuses,
        route_prefixes=route_prefixes,
        ingress_deployments=ingress_deployments,
    )

    # Setup test deployment state manager
    haproxy_controller.deployment_state_manager = FakeDeploymentStateManager(
        running_replica_infos=running_replica_infos,
    )

    # Setup proxy state manager
    haproxy_controller.proxy_state_manager.add_proxy_details(
        "proxy_node1", "instance1", "10.0.1.1", "proxy1"
    )
    haproxy_controller.proxy_state_manager.add_proxy_details(
        "proxy_node2", "instance2", "10.0.1.2", "proxy2"
    )

    # Allocate ports for replicas using controller's methods
    http_port1 = haproxy_controller.allocate_replica_port(
        "node1", replica_id1.unique_id, RequestProtocol.HTTP
    )
    grpc_port1 = haproxy_controller.allocate_replica_port(
        "node1", replica_id1.unique_id, RequestProtocol.GRPC
    )

    target_groups = haproxy_controller.get_target_groups(
        from_proxy_manager=from_proxy_manager
    )

    # Create expected target groups
    if ha_proxy_enabled and not from_proxy_manager:
        expected_target_groups = [
            TargetGroup(
                protocol=RequestProtocol.HTTP,
                route_prefix="/",
                app_name="",
                targets=[
                    Target(
                        ip="10.0.1.1",
                        port=8000,
                        instance_id="instance1",
                        name="proxy1",
                    ),
                    Target(
                        ip="10.0.1.2",
                        port=8000,
                        instance_id="instance2",
                        name="proxy2",
                    ),
                ],
            ),
            TargetGroup(
                protocol=RequestProtocol.GRPC,
                route_prefix="/",
                app_name="",
                targets=[
                    Target(
                        ip="10.0.1.1",
                        port=9000,
                        instance_id="instance1",
                        name="proxy1",
                    ),
                    Target(
                        ip="10.0.1.2",
                        port=9000,
                        instance_id="instance2",
                        name="proxy2",
                    ),
                ],
            ),
        ]
    else:
        expected_target_groups = [
            TargetGroup(
                protocol=RequestProtocol.HTTP,
                route_prefix="/app1",
                app_name="app1",
                targets=[
                    Target(
                        ip="10.0.0.1",
                        port=http_port1,
                        instance_id="",
                        name="replica1",
                    ),
                ],
                fallback_target=Target(
                    ip="10.0.0.1",
                    port=8500,
                    instance_id="fallback_instance_id",
                    name="fallback_proxy",
                ),
            ),
            TargetGroup(
                protocol=RequestProtocol.GRPC,
                route_prefix="/app1",
                app_name="app1",
                targets=[
                    Target(
                        ip="10.0.0.1",
                        port=grpc_port1,
                        instance_id="",
                        name="replica1",
                    ),
                ],
                fallback_target=Target(
                    ip="10.0.0.1",
                    port=9500,
                    instance_id="fallback_instance_id",
                    name="fallback_proxy",
                ),
            ),
        ]

    # Sort both lists to ensure consistent comparison
    target_groups.sort(key=lambda g: (g.protocol, g.route_prefix))
    expected_target_groups.sort(key=lambda g: (g.protocol, g.route_prefix))

    assert target_groups == expected_target_groups


def test_get_target_groups_haproxy_no_apps(
    haproxy_controller: FakeHAProxyController,
):
    """Tests get_target_groups returns the appropriate target groups based on the
    ha_proxy_enabled and from_proxy_manager parameters."""

    haproxy_controller._ha_proxy_enabled = True
    haproxy_controller.proxy_state_manager.add_fallback_proxy_details(
        "fallback_node_id", "fallback_instance_id", "10.0.0.1", "fallback_proxy"
    )

    target_groups = haproxy_controller.get_target_groups(
        from_proxy_manager=True,
    )

    assert target_groups == [
        TargetGroup(
            protocol=RequestProtocol.HTTP,
            route_prefix="/",
            app_name="",
            targets=[],
            fallback_target=Target(
                ip="10.0.0.1",
                port=8500,
                instance_id="fallback_instance_id",
                name="fallback_proxy",
            ),
        ),
        TargetGroup(
            protocol=RequestProtocol.GRPC,
            route_prefix="/",
            app_name="",
            targets=[],
            fallback_target=Target(
                ip="10.0.0.1",
                port=9500,
                instance_id="fallback_instance_id",
                name="fallback_proxy",
            ),
        ),
    ]


def test_get_target_groups_app_with_no_running_replicas(
    haproxy_controller: FakeHAProxyController,
):
    """Tests get_target_groups returns the appropriate target groups when an app
    has no running replicas."""

    haproxy_controller._ha_proxy_enabled = True
    haproxy_controller.proxy_state_manager.add_fallback_proxy_details(
        "fallback_node_id", "fallback_instance_id", "10.0.0.1", "fallback_proxy"
    )

    # Setup test data with running applications
    app_statuses = {"app1": {}}
    route_prefixes = {"app1": "/app1"}
    ingress_deployments = {"app1": "app1_ingress"}

    deployment_id1 = DeploymentID(name="app1_ingress", app_name="app1")

    running_replica_infos = {deployment_id1: []}

    # Setup test application state manager
    haproxy_controller.application_state_manager = FakeApplicationStateManager(
        app_statuses=app_statuses,
        route_prefixes=route_prefixes,
        ingress_deployments=ingress_deployments,
    )

    # Setup test deployment state manager
    haproxy_controller.deployment_state_manager = FakeDeploymentStateManager(
        running_replica_infos=running_replica_infos,
    )

    haproxy_controller.proxy_state_manager.add_proxy_details(
        "proxy_node1", "instance1", "10.0.0.1", "proxy1"
    )

    target_groups = haproxy_controller.get_target_groups(
        from_proxy_manager=True,
    )

    assert target_groups == [
        TargetGroup(
            protocol=RequestProtocol.HTTP,
            route_prefix="/app1",
            app_name="app1",
            targets=[],
            fallback_target=Target(
                ip="10.0.0.1",
                port=8500,
                instance_id="fallback_instance_id",
                name="fallback_proxy",
            ),
        ),
        TargetGroup(
            protocol=RequestProtocol.GRPC,
            route_prefix="/app1",
            app_name="app1",
            targets=[],
            fallback_target=Target(
                ip="10.0.0.1",
                port=9500,
                instance_id="fallback_instance_id",
                name="fallback_proxy",
            ),
        ),
    ]


if __name__ == "__main__":
    pytest.main()
