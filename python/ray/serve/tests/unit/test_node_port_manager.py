import sys
from typing import Set

import pytest

from ray.serve._private.common import RequestProtocol
from ray.serve._private.node_port_manager import NoAvailablePortError, NodePortManager


@pytest.fixture
def port_range_constants():
    """Fixture to set up port range constants for testing."""
    return {
        "RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT": 8000,
        "RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT": 8005,
        "RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT": 9000,
        "RAY_SERVE_DIRECT_INGRESS_MAX_GRPC_PORT": 9005,
    }


@pytest.fixture(autouse=True)
def setup_port_constants(monkeypatch, port_range_constants):
    monkeypatch.setattr(
        "ray.serve._private.node_port_manager.RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT",
        port_range_constants["RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT"],
    )
    monkeypatch.setattr(
        "ray.serve._private.node_port_manager.RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT",
        port_range_constants["RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT"],
    )
    monkeypatch.setattr(
        "ray.serve._private.node_port_manager.RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT",
        port_range_constants["RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT"],
    )
    monkeypatch.setattr(
        "ray.serve._private.node_port_manager.RAY_SERVE_DIRECT_INGRESS_MAX_GRPC_PORT",
        port_range_constants["RAY_SERVE_DIRECT_INGRESS_MAX_GRPC_PORT"],
    )
    yield


def test_http_port_allocation_and_release(port_range_constants):
    manager = NodePortManager.get_node_manager("node-1")
    port = manager.allocate_port("replica-1", RequestProtocol.HTTP)
    assert (
        port_range_constants["RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT"]
        <= port
        < port_range_constants["RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT"]
    )

    # Port should be reusable after release
    manager.release_port("replica-1", port, RequestProtocol.HTTP)
    port2 = manager.allocate_port("replica-2", RequestProtocol.HTTP)
    assert port2 == port


def test_grpc_port_allocation_and_blocking():
    manager = NodePortManager.get_node_manager("node-2")
    port = manager.allocate_port("replica-a", RequestProtocol.GRPC)
    manager.release_port("replica-a", port, RequestProtocol.GRPC, block_port=True)

    # New allocation should skip blocked port
    allocated_ports = set()
    for i in range(4):
        new_port = manager.allocate_port(f"replica-b{i}", RequestProtocol.GRPC)
        assert new_port != port
        allocated_ports.add(new_port)

    assert len(allocated_ports) == 4


def test_reuse_existing_port_if_already_allocated():
    manager = NodePortManager.get_node_manager("node-3")
    port1 = manager.allocate_port("replica-x", RequestProtocol.HTTP)
    port2 = manager.allocate_port("replica-x", RequestProtocol.HTTP)
    assert port1 == port2


def test_cleanup_releases_ports():
    manager = NodePortManager.get_node_manager("node-4")
    allocated = set()

    # Allocate all HTTP ports
    for i in range(5):
        port = manager.allocate_port(f"replica-{i}", RequestProtocol.HTTP)
        allocated.add(port)

    # Only keep some replicas active
    active_replica_ids: Set[str] = {"replica-0", "replica-1"}
    NodePortManager.prune({"node-4": active_replica_ids})

    # We should be able to reallocate some of the cleaned-up ports
    new_port = manager.allocate_port("replica-new", RequestProtocol.HTTP)
    assert new_port in allocated - {
        manager.get_port("replica-0", RequestProtocol.HTTP),
        manager.get_port("replica-1", RequestProtocol.HTTP),
    }


def test_node_manager_cleanup():
    NodePortManager.get_node_manager("node-5")
    assert "node-5" in NodePortManager._node_managers

    NodePortManager.prune(node_id_to_alive_replica_ids={})
    assert "node-5" not in NodePortManager._node_managers


def test_port_exhaustion():
    """Test behavior when all ports are exhausted."""
    manager = NodePortManager.get_node_manager("node-6")

    # Allocate all HTTP ports
    allocated_ports = set()
    for i in range(5):  # 5 ports available (8000-8004)
        port = manager.allocate_port(f"replica-{i}", RequestProtocol.HTTP)
        allocated_ports.add(port)

    # Try to allocate one more port
    with pytest.raises(NoAvailablePortError, match="No available"):
        manager.allocate_port("replica-extra", RequestProtocol.HTTP)


def test_invalid_port_release():
    """Test error handling when releasing invalid ports."""
    manager = NodePortManager.get_node_manager("node-7")

    # Allocate a port
    port = manager.allocate_port("replica-1", RequestProtocol.HTTP)

    # Try to release with wrong port number
    with pytest.raises(AssertionError, match="port mismatch"):
        manager.release_port("replica-1", port + 1, RequestProtocol.HTTP)


def test_concurrent_port_allocation():
    """Test concurrent port allocation from multiple coroutines."""
    manager = NodePortManager.get_node_manager("node-8")

    def allocate_port(replica_id: str):
        return manager.allocate_port(replica_id, RequestProtocol.HTTP)

    # Allocate ports concurrently
    ports = [allocate_port(f"replica-{i}") for i in range(5)]

    # Verify all ports are unique and in range
    assert len(set(ports)) == len(ports)
    assert all(8000 <= port < 8005 for port in ports)


def test_mixed_protocol_port_allocation():
    """Test allocating both HTTP and gRPC ports for the same replica."""
    manager = NodePortManager.get_node_manager("node-9")

    http_port = manager.allocate_port("replica-1", RequestProtocol.HTTP)
    grpc_port = manager.allocate_port("replica-1", RequestProtocol.GRPC)

    assert 8000 <= http_port < 8005
    assert 9000 <= grpc_port < 9005
    assert http_port != grpc_port


def test_port_blocking_persistence():
    """Test that blocked ports remain blocked across multiple allocations."""
    manager = NodePortManager.get_node_manager("node-10")

    # Block a port
    port = manager.allocate_port("replica-1", RequestProtocol.HTTP)
    manager.release_port("replica-1", port, RequestProtocol.HTTP, block_port=True)

    # Try to allocate ports multiple times
    for _ in range(3):
        new_port = manager.allocate_port("replica-2", RequestProtocol.HTTP)
        assert new_port != port
        manager.release_port("replica-2", new_port, RequestProtocol.HTTP)


def test_check_replica_port_allocated():
    """Test the check_replica_port_allocated method."""
    manager = NodePortManager.get_node_manager("node-11")

    # Test case where port is allocated
    port = manager.allocate_port("replica-1", RequestProtocol.HTTP)
    assert manager.get_port("replica-1", RequestProtocol.HTTP) == port

    # Test case where port is not allocated
    with pytest.raises(
        ValueError,
        match="HTTP port not allocated for replica non-existent on node node-11",
    ):
        manager.get_port("non-existent", RequestProtocol.HTTP)

    # Clean up
    manager.release_port("replica-1", port, RequestProtocol.HTTP)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
