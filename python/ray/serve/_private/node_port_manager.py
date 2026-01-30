import heapq
import logging
from typing import Dict, List, Optional, Set, Tuple

from ray.serve._private.common import RequestProtocol
from ray.serve._private.constants import (
    RAY_SERVE_DIRECT_INGRESS_MAX_GRPC_PORT,
    RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT,
    RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT,
    RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT,
    SERVE_LOGGER_NAME,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class NoAvailablePortError(Exception):
    def __init__(self, protocol: str, node_id: str):
        message = f"No available ports on node {node_id} for {protocol} protocol."
        super().__init__(message)


class PortAllocator:
    """Manages a pool of ports for a specific protocol (e.g., HTTP or gRPC)."""

    def __init__(self, min_port: int, max_port: int, protocol: str, node_id: str):
        self._protocol = protocol
        self._node_id = node_id
        # TODO(abrar): add a validation here to ensure min_port and max_port dont overlap with
        # ray params min_worker_port and max_worker_port.
        self._available_ports = list(range(min_port, max_port))
        heapq.heapify(self._available_ports)

        self._allocated_ports: Dict[str, int] = {}
        self._blocked_ports: Set[int] = set()

    def update_port_if_missing(self, replica_id: str, port: Optional[int]):
        """Update port value for a replica."""
        if replica_id in self._allocated_ports:
            return
        assert (
            port is not None
        ), f"Port is None for {self._protocol} protocol on replica {replica_id} on node {self._node_id}"
        if self._protocol == RequestProtocol.HTTP:
            if not (
                RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT
                <= port
                <= RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT
            ):
                logger.warning(f"HTTP port out of range: {port}")
        elif self._protocol == RequestProtocol.GRPC:
            if not (
                RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT
                <= port
                <= RAY_SERVE_DIRECT_INGRESS_MAX_GRPC_PORT
            ):
                logger.warning(f"GRPC port out of range: {port}")
        self._allocated_ports[replica_id] = port

        logger.info(
            f"Recovered {self._protocol} port {port} for replica {replica_id} on node {self._node_id}"
        )
        return port

    def allocate(self, replica_id: str) -> int:
        if replica_id in self._allocated_ports:
            logger.warning(
                f"{self._protocol} port already allocated for replica {replica_id}"
            )
            return self._allocated_ports[replica_id]

        while self._available_ports:
            port = heapq.heappop(self._available_ports)
            if port not in self._blocked_ports:
                self._allocated_ports[replica_id] = port
                logger.info(
                    f"Allocated {self._protocol} port {port} to replica {replica_id} on node {self._node_id}"
                )
                return port

        raise NoAvailablePortError(self._protocol, self._node_id)

    def release(self, replica_id: str, port: int, block_port: bool = False):
        """
        Releases a port for a replica.

        Args:
            replica_id: The ID of the replica to release the port for.
            port: The port to release.
            block_port: Whether to block the port from being allocated again. Use this in
                situations where the port is being released due some other process is using it.
        """
        if replica_id not in self._allocated_ports:
            raise ValueError(
                f"{self._protocol} port not allocated for replica {replica_id} on node {self._node_id}"
            )
        expected_port = self._allocated_ports[replica_id]
        assert expected_port == port, (
            f"{self._protocol} port mismatch for replica {replica_id} on node {self._node_id}: "
            f"expected {expected_port}, got {port}"
        )

        heapq.heappush(self._available_ports, port)
        del self._allocated_ports[replica_id]
        logger.info(
            f"Released {self._protocol} port {port} for replica {replica_id} on node {self._node_id}"
        )

        if block_port:
            self._blocked_ports.add(port)
            logger.info(f"Blocked {self._protocol} port {port} on node {self._node_id}")

    def prune(self, active_replica_ids: Set[str]):
        for replica_id in list(self._allocated_ports.keys()):
            if replica_id not in active_replica_ids:
                port = self._allocated_ports[replica_id]
                logger.info(
                    f"Cleaning up {self._protocol} port {port} for stale replica {replica_id} on node {self._node_id}"
                )
                self.release(replica_id, port)

    def get_port(self, replica_id: str) -> int:
        if replica_id not in self._allocated_ports:
            raise ValueError(
                f"{self._protocol} port not allocated for replica {replica_id} on node {self._node_id}"
            )
        return self._allocated_ports[replica_id]

    def is_port_allocated(self, replica_id: str) -> bool:
        return replica_id in self._allocated_ports


class NodePortManager:
    """
    This class is responsible for managing replica-specific port allocations on a node,
    and is only used in direct ingress mode, where each Serve replica is exposed individually
    via a Kubernetes or GCP or AWS Ingress.

    The primary goal of this class is to assign ports in a consistent and efficient manner,
    minimizing EndpointSlice fragmentation in Kubernetes. It uses a min-heap strategy to
    allocate ports incrementally, ensuring that all nodes tend to reuse the same port numbers.

    Background:
    Kubernetes groups endpoints into EndpointSlices based on the set of ports exposed by each Pod.
    If Pods expose different port combinations (e.g., due to random port assignment), Kubernetes
    generates separate EndpointSlices per unique port list. This leads to unnecessary fragmentation
    and increased resource consumption.

    By allocating ports deterministically, we ensure:
    - Consistent port usage across all nodes
    - Fewer unique port lists, reducing the number of EndpointSlices created
    - Improved performance and resource utilization

    Although Kubernetes does not allow users to explicitly configure the ports included in
    EndpointSlices, maintaining a uniform port layout across nodes is still beneficial.

    Port lifecycle:
    - Replicas are expected to release their ports when stopped
    - If a replica crashes without releasing its port, the controller loop will detect and
    reclaim leaked ports during reconciliation

    Note:
    Although this strategy is designed with Kubernetes in mind, it is applied uniformly
    across all platforms for consistency.
    """

    _node_managers: Dict[str, "NodePortManager"] = {}

    @classmethod
    def get_node_manager(cls, node_id: str) -> "NodePortManager":
        # this doesn't need to be behind a lock because it will already be called from same thread
        if node_id not in cls._node_managers:
            logger.info(f"Creating node manager for node {node_id}")
            cls._node_managers[node_id] = cls(node_id)
        return cls._node_managers[node_id]

    @classmethod
    def prune(cls, node_id_to_alive_replica_ids: Dict[str, Set[str]]):
        # this doesn't need to be behind a lock because it will already be called from same thread
        for node_id in list(cls._node_managers):
            if node_id not in node_id_to_alive_replica_ids:
                logger.info(f"Removing node manager for node {node_id}")
                del cls._node_managers[node_id]
            else:
                manager = cls._node_managers[node_id]
                manager._prune_replica_ports(node_id_to_alive_replica_ids[node_id])

    @classmethod
    def update_ports(cls, ingress_replicas_info: List[Tuple[str, str, int, int]]):
        """Update port values for ingress replicas."""
        for node_id, replica_id, http_port, grpc_port in ingress_replicas_info:
            if node_id is None:
                continue
            node_port_manager = cls.get_node_manager(node_id)
            if http_port is not None:
                node_port_manager._http_allocator.update_port_if_missing(
                    replica_id,
                    http_port,
                )
            if grpc_port is not None:
                node_port_manager._grpc_allocator.update_port_if_missing(
                    replica_id,
                    grpc_port,
                )

    def __init__(self, node_id: str):
        self._node_id = node_id

        self._http_allocator = PortAllocator(
            RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT,
            RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT,
            protocol=RequestProtocol.HTTP,
            node_id=node_id,
        )
        self._grpc_allocator = PortAllocator(
            RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT,
            RAY_SERVE_DIRECT_INGRESS_MAX_GRPC_PORT,
            protocol=RequestProtocol.GRPC,
            node_id=node_id,
        )

    def _prune_replica_ports(self, active_replica_ids: Set[str]):
        self._http_allocator.prune(active_replica_ids)
        self._grpc_allocator.prune(active_replica_ids)

    def allocate_port(self, replica_id: str, protocol: RequestProtocol) -> int:
        if protocol == RequestProtocol.HTTP:
            return self._http_allocator.allocate(replica_id)
        elif protocol == RequestProtocol.GRPC:
            return self._grpc_allocator.allocate(replica_id)
        else:
            raise ValueError(f"Unsupported protocol: {protocol}")

    def release_port(
        self,
        replica_id: str,
        port: int,
        protocol: RequestProtocol,
        block_port: bool = False,
    ):
        if protocol == RequestProtocol.HTTP:
            self._http_allocator.release(replica_id, port, block_port)
        elif protocol == RequestProtocol.GRPC:
            self._grpc_allocator.release(replica_id, port, block_port)
        else:
            raise ValueError(f"Unsupported protocol: {protocol}")

    def get_port(self, replica_id: str, protocol: RequestProtocol) -> int:
        if protocol == RequestProtocol.HTTP:
            return self._http_allocator.get_port(replica_id)
        elif protocol == RequestProtocol.GRPC:
            return self._grpc_allocator.get_port(replica_id)
        else:
            raise ValueError(f"Unsupported protocol: {protocol}")

    def is_port_allocated(self, replica_id: str, protocol: RequestProtocol) -> bool:
        if protocol == RequestProtocol.HTTP:
            return self._http_allocator.is_port_allocated(replica_id)
        elif protocol == RequestProtocol.GRPC:
            return self._grpc_allocator.is_port_allocated(replica_id)
        else:
            raise ValueError(f"Unsupported protocol: {protocol}")
