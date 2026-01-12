"""Temporary tests for direct ingress feature.

These tests verify the direct ingress components work correctly without
requiring full Ray/Serve integration (no ray.shutdown() calls).
"""
import pytest


class TestPortAllocator:
    """Test PortAllocator class."""

    def test_basic_allocation(self):
        from ray.serve._private.common import RequestProtocol
        from ray.serve._private.node_port_manager import PortAllocator

        allocator = PortAllocator(
            min_port=30000,
            max_port=30010,
            protocol=RequestProtocol.HTTP,
            node_id="test-node",
        )

        # Allocate a port
        port1 = allocator.allocate("replica-1")
        assert port1 == 30000  # Should get minimum port first

        port2 = allocator.allocate("replica-2")
        assert port2 == 30001

        # Re-allocating same replica should return same port
        port1_again = allocator.allocate("replica-1")
        assert port1_again == port1

    def test_release_and_reuse(self):
        from ray.serve._private.common import RequestProtocol
        from ray.serve._private.node_port_manager import PortAllocator

        allocator = PortAllocator(
            min_port=30000,
            max_port=30010,
            protocol=RequestProtocol.HTTP,
            node_id="test-node",
        )

        port1 = allocator.allocate("replica-1")
        allocator.release("replica-1", port1)

        # After release, the same port should be available
        port2 = allocator.allocate("replica-2")
        assert port2 == port1  # Min-heap returns smallest available

    def test_block_port(self):
        from ray.serve._private.common import RequestProtocol
        from ray.serve._private.node_port_manager import PortAllocator

        allocator = PortAllocator(
            min_port=30000,
            max_port=30010,
            protocol=RequestProtocol.HTTP,
            node_id="test-node",
        )

        port1 = allocator.allocate("replica-1")
        allocator.release("replica-1", port1, block_port=True)

        # Blocked port should not be reused
        port2 = allocator.allocate("replica-2")
        assert port2 != port1

    def test_prune_stale_replicas(self):
        from ray.serve._private.common import RequestProtocol
        from ray.serve._private.node_port_manager import PortAllocator

        allocator = PortAllocator(
            min_port=30000,
            max_port=30010,
            protocol=RequestProtocol.HTTP,
            node_id="test-node",
        )

        allocator.allocate("replica-1")
        allocator.allocate("replica-2")
        allocator.allocate("replica-3")

        # Prune to only keep replica-2
        allocator.prune({"replica-2"})

        # replica-1 and replica-3 should be cleaned up
        assert allocator.is_port_allocated("replica-2")
        assert not allocator.is_port_allocated("replica-1")
        assert not allocator.is_port_allocated("replica-3")

    def test_no_available_ports(self):
        from ray.serve._private.common import RequestProtocol
        from ray.serve._private.node_port_manager import (
            NoAvailablePortError,
            PortAllocator,
        )

        allocator = PortAllocator(
            min_port=30000,
            max_port=30002,  # Only 2 ports available
            protocol=RequestProtocol.HTTP,
            node_id="test-node",
        )

        allocator.allocate("replica-1")
        allocator.allocate("replica-2")

        with pytest.raises(NoAvailablePortError):
            allocator.allocate("replica-3")


class TestNodePortManager:
    """Test NodePortManager class."""

    def test_singleton_per_node(self):
        from ray.serve._private.node_port_manager import NodePortManager

        # Clear any existing managers
        NodePortManager._node_managers.clear()

        mgr1 = NodePortManager.get_node_manager("node-1")
        mgr2 = NodePortManager.get_node_manager("node-1")
        mgr3 = NodePortManager.get_node_manager("node-2")

        assert mgr1 is mgr2  # Same node should return same manager
        assert mgr1 is not mgr3  # Different nodes should have different managers

        # Cleanup
        NodePortManager._node_managers.clear()

    def test_allocate_http_and_grpc(self):
        from ray.serve._private.common import RequestProtocol
        from ray.serve._private.node_port_manager import NodePortManager

        # Clear any existing managers
        NodePortManager._node_managers.clear()

        mgr = NodePortManager.get_node_manager("test-node")

        http_port = mgr.allocate_port("replica-1", RequestProtocol.HTTP)
        grpc_port = mgr.allocate_port("replica-1", RequestProtocol.GRPC)

        # HTTP ports should be in 30000-31000 range
        assert 30000 <= http_port < 31000
        # gRPC ports should be in 40000-41000 range
        assert 40000 <= grpc_port < 41000

        # Cleanup
        NodePortManager._node_managers.clear()

    def test_prune_nodes(self):
        from ray.serve._private.common import RequestProtocol
        from ray.serve._private.node_port_manager import NodePortManager

        # Clear any existing managers
        NodePortManager._node_managers.clear()

        mgr1 = NodePortManager.get_node_manager("node-1")
        mgr2 = NodePortManager.get_node_manager("node-2")

        mgr1.allocate_port("replica-1", RequestProtocol.HTTP)
        mgr2.allocate_port("replica-2", RequestProtocol.HTTP)

        # Prune to only keep node-1 with replica-1
        NodePortManager.prune({"node-1": {"replica-1"}})

        assert "node-1" in NodePortManager._node_managers
        assert "node-2" not in NodePortManager._node_managers

        # Cleanup
        NodePortManager._node_managers.clear()


class TestTargetGroupSchema:
    """Test TargetGroup schema changes."""

    def test_app_name_field(self):
        from ray.serve._private.common import RequestProtocol
        from ray.serve.schema import Target, TargetGroup

        # Create target group with app_name
        tg = TargetGroup(
            targets=[Target(ip="10.0.0.1", port=30000)],
            route_prefix="/app",
            protocol=RequestProtocol.HTTP,
            app_name="my-app",
        )

        assert tg.app_name == "my-app"
        assert tg.route_prefix == "/app"
        assert tg.protocol == RequestProtocol.HTTP

    def test_app_name_default_empty(self):
        from ray.serve._private.common import RequestProtocol
        from ray.serve.schema import TargetGroup

        # Create target group without app_name (should default to "")
        tg = TargetGroup(
            targets=[],
            route_prefix="/",
            protocol=RequestProtocol.HTTP,
        )

        assert tg.app_name == ""


class TestLongPollNamespace:
    """Test LongPollNamespace changes."""

    def test_target_groups_namespace_exists(self):
        from ray.serve._private.long_poll import LongPollNamespace

        # Verify TARGET_GROUPS namespace exists
        assert hasattr(LongPollNamespace, "TARGET_GROUPS")
        assert LongPollNamespace.TARGET_GROUPS is not None


class TestConstants:
    """Test direct ingress constants."""

    def test_constants_exist(self):
        from ray.serve._private.constants import (
            RAY_SERVE_DIRECT_INGRESS_MAX_GRPC_PORT,
            RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT,
            RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S,
            RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT,
            RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT,
            RAY_SERVE_DIRECT_INGRESS_PORT_RETRY_COUNT,
            RAY_SERVE_ENABLE_DIRECT_INGRESS,
        )

        # Check default values
        assert RAY_SERVE_ENABLE_DIRECT_INGRESS is False  # Disabled by default
        assert RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT == 30000
        assert RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT == 31000
        assert RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT == 40000
        assert RAY_SERVE_DIRECT_INGRESS_MAX_GRPC_PORT == 41000
        assert RAY_SERVE_DIRECT_INGRESS_PORT_RETRY_COUNT == 100
        assert RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S == 30.0


class TestControllerMethods:
    """Test that controller has direct ingress methods."""

    def test_controller_has_direct_ingress_methods(self):
        from ray.serve._private.controller import ServeController

        # Check that methods exist
        assert hasattr(ServeController, "allocate_replica_port")
        assert hasattr(ServeController, "release_replica_port")
        assert hasattr(ServeController, "_get_port")
        assert hasattr(ServeController, "_is_port_allocated")
        assert hasattr(ServeController, "_broadcast_target_groups_if_changed")
        assert hasattr(ServeController, "_get_target_groups_for_app")
        assert hasattr(
            ServeController, "_get_running_replica_details_for_ingress_deployment"
        )


class TestReplicaMethods:
    """Test that replica has direct ingress methods."""

    def test_replica_has_direct_ingress_methods(self):
        from ray.serve._private.replica import Replica

        # Check that methods exist
        assert hasattr(Replica, "_maybe_start_direct_ingress_servers")
        assert hasattr(Replica, "_direct_ingress_asgi")
        assert hasattr(Replica, "_can_accept_request")
        assert hasattr(Replica, "_dataplane_health_check")
        assert hasattr(Replica, "_parse_request_timeout")
        assert hasattr(Replica, "_determine_http_route")
        assert hasattr(Replica, "get_asgi_tracing_context")
        assert hasattr(Replica, "perform_graceful_shutdown")
        assert hasattr(Replica, "max_queued_requests")


class TestReplicaResponseGenerator:
    """Test ReplicaResponseGenerator class."""

    def test_import(self):
        from ray.serve._private.replica_response_generator import (
            ReplicaResponseGenerator,
        )

        assert ReplicaResponseGenerator is not None


class TestASGIDIReceiveProxy:
    """Test ASGIDIReceiveProxy class."""

    def test_import(self):
        from ray.serve._private.direct_ingress_http_util import ASGIDIReceiveProxy

        assert ASGIDIReceiveProxy is not None


class TestNewConstants:
    """Test new HTTP header constants."""

    def test_http_header_constants(self):
        from ray.serve._private.constants import (
            HEALTHY_MESSAGE,
            SERVE_HTTP_REQUEST_DISCONNECT_DISABLED_HEADER,
            SERVE_HTTP_REQUEST_ID_HEADER,
            SERVE_HTTP_REQUEST_TIMEOUT_S_HEADER,
        )

        assert SERVE_HTTP_REQUEST_ID_HEADER == "x-request-id"
        assert SERVE_HTTP_REQUEST_TIMEOUT_S_HEADER == "x-request-timeout-seconds"
        assert (
            SERVE_HTTP_REQUEST_DISCONNECT_DISABLED_HEADER
            == "x-request-disconnect-disabled"
        )
        assert HEALTHY_MESSAGE == "success"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
