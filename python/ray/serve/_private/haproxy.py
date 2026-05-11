import asyncio
import csv
import heapq
import io
import json
import logging
import math
import os
import re
import shutil
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from jinja2 import Environment

import ray
from ray._common.utils import get_or_create_event_loop
from ray.serve._private.common import (
    NodeId,
    ReplicaID,
    RequestMetadata,
    RequestProtocol,
)
from ray.serve._private.constants import (
    DRAINING_MESSAGE,
    HEALTHY_MESSAGE,
    NO_REPLICAS_MESSAGE,
    NO_ROUTES_MESSAGE,
    PROXY_MIN_DRAINING_PERIOD_S,
    RAY_SERVE_ENABLE_HAPROXY_OPTIMIZED_CONFIG,
    RAY_SERVE_EXPERIMENTAL_PIP_HAPROXY,
    RAY_SERVE_HAPROXY_BALANCE_ALGORITHM,
    RAY_SERVE_HAPROXY_BINARY_PATH,
    RAY_SERVE_HAPROXY_BROADCAST_COALESCE_S,
    RAY_SERVE_HAPROXY_CONFIG_FILE_LOC,
    RAY_SERVE_HAPROXY_HARD_STOP_AFTER_S,
    RAY_SERVE_HAPROXY_HEALTH_CHECK_DOWNINTER,
    RAY_SERVE_HAPROXY_HEALTH_CHECK_FALL,
    RAY_SERVE_HAPROXY_HEALTH_CHECK_FASTINTER,
    RAY_SERVE_HAPROXY_HEALTH_CHECK_INTER,
    RAY_SERVE_HAPROXY_HEALTH_CHECK_RISE,
    RAY_SERVE_HAPROXY_MAXCONN,
    RAY_SERVE_HAPROXY_METRICS_PORT,
    RAY_SERVE_HAPROXY_MIN_SLOTS_PER_BACKEND,
    RAY_SERVE_HAPROXY_NBTHREAD,
    RAY_SERVE_HAPROXY_RELOAD_TIMEOUT_S,
    RAY_SERVE_HAPROXY_RETRIES,
    RAY_SERVE_HAPROXY_RUNTIME_CHUNK_SIZE,
    RAY_SERVE_HAPROXY_SERVER_STATE_BASE,
    RAY_SERVE_HAPROXY_SERVER_STATE_FILE,
    RAY_SERVE_HAPROXY_SLOT_HEADROOM_FACTOR,
    RAY_SERVE_HAPROXY_SOCKET_PATH,
    RAY_SERVE_HAPROXY_SOCKET_TIMEOUT_S,
    RAY_SERVE_HAPROXY_STATS_PORT,
    RAY_SERVE_HAPROXY_SYSLOG_PORT,
    RAY_SERVE_HAPROXY_TCP_NODELAY,
    RAY_SERVE_HAPROXY_TIMEOUT_CLIENT_S,
    RAY_SERVE_HAPROXY_TIMEOUT_CONNECT_S,
    RAY_SERVE_HAPROXY_TIMEOUT_SERVER_S,
    RAY_SERVE_HAPROXY_TOTAL_SLOTS,
    SERVE_CONTROLLER_NAME,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.haproxy_templates import (
    HAPROXY_CONFIG_TEMPLATE,
    HAPROXY_HEALTHZ_RULES_TEMPLATE,
)
from ray.serve._private.logging_utils import get_component_logger_file_path
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace
from ray.serve._private.proxy import ProxyActorInterface
from ray.serve._private.utils import get_head_node_id
from ray.serve.config import HTTPOptions, gRPCOptions
from ray.serve.schema import (
    LoggingConfig,
    Target,
    TargetGroup,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


def get_haproxy_binary() -> str:
    """Return the path to the HAProxy binary.

    When RAY_SERVE_EXPERIMENTAL_PIP_HAPROXY is disabled (default), returns
    RAY_SERVE_HAPROXY_BINARY_PATH (defaults to "haproxy", i.e. system PATH).

    When enabled, resolution order:
      1. ``RAY_SERVE_HAPROXY_BINARY_PATH`` if explicitly set to an absolute path.
      2. The binary bundled in the ``ray-haproxy`` package.
      3. ``haproxy`` on the system PATH (fallback).

    Raises ``FileNotFoundError`` if no usable binary is found.
    """
    binary = _resolve_haproxy_binary()
    logger.info(f"Using HAProxy binary: {binary}")
    return binary


def _resolve_haproxy_binary() -> str:
    if not RAY_SERVE_EXPERIMENTAL_PIP_HAPROXY:
        return RAY_SERVE_HAPROXY_BINARY_PATH

    # 1. If RAY_SERVE_HAPROXY_BINARY_PATH was explicitly set (not the default),
    # use it as an override.
    if RAY_SERVE_HAPROXY_BINARY_PATH != "haproxy":
        if os.path.isfile(RAY_SERVE_HAPROXY_BINARY_PATH) and os.access(
            RAY_SERVE_HAPROXY_BINARY_PATH, os.X_OK
        ):
            return RAY_SERVE_HAPROXY_BINARY_PATH
        raise FileNotFoundError(
            f"RAY_SERVE_HAPROXY_BINARY_PATH={RAY_SERVE_HAPROXY_BINARY_PATH!r} "
            "does not point to an executable file."
        )

    # 2. Bundled binary from the ray-haproxy package.
    try:
        from ray_haproxy import get_haproxy_binary as _pip_binary

        return _pip_binary()
    except ImportError:
        pass
    except OSError:
        pass

    # 3. System PATH fallback.
    system_haproxy = shutil.which("haproxy")
    if system_haproxy:
        return system_haproxy

    raise FileNotFoundError(
        "Could not find an HAProxy binary. "
        "Install 'ray[haproxy]' for the bundled binary, "
        "set RAY_SERVE_HAPROXY_BINARY_PATH, "
        "or ensure 'haproxy' is on PATH."
    )


@dataclass
class ServerStats:
    """Server statistics from HAProxy."""

    backend: str  # Which backend this server belongs to
    server: str  # Server name within the backend
    status: str  # Current status: "UP", "DOWN", "DRAIN", etc.
    current_sessions: int  # Active sessions (HAProxy 'scur')
    queued: int  # Queued requests (HAProxy 'qcur')

    @property
    def is_up(self) -> bool:
        return self.status == "UP"

    @property
    def is_draining(self) -> bool:
        return self.status in ["DRAIN", "NOLB"]

    @property
    def can_drain_safely(self) -> bool:
        """
        Return True if the server can be drained safely based on the current load.
        Safe to drain when:
        - No current active sessions (0)
        - No queued requests waiting
        This ensures no active user sessions are disrupted during draining.
        """
        return self.current_sessions == 0 and self.queued == 0


@dataclass
class HAProxyStats:
    """Complete HAProxy statistics with both individual server data and aggregate metrics."""

    # Individual server statistics by backend and server name
    backend_to_servers: Dict[str, Dict[str, ServerStats]] = field(default_factory=dict)

    # Computed aggregate metrics (calculated from server data)
    @property
    def total_backends(self) -> int:
        """Total number of backends."""
        return len(self.backend_to_servers)

    @property
    def total_servers(self) -> int:
        """Total number of servers across all backends."""
        return sum(
            len(backend_servers) for backend_servers in self.backend_to_servers.values()
        )

    @property
    def active_servers(self) -> int:
        """Number of servers currently UP."""
        return sum(
            1
            for backend_servers in self.backend_to_servers.values()
            for server in backend_servers.values()
            if server.is_up
        )

    @property
    def draining_servers(self) -> int:
        """Number of servers currently draining."""
        return sum(
            1
            for backend_servers in self.backend_to_servers.values()
            for server in backend_servers.values()
            if server.is_draining
        )

    @property
    def total_active_sessions(self) -> int:
        """Sum of all active sessions across all servers."""
        return sum(
            server.current_sessions
            for backend_servers in self.backend_to_servers.values()
            for server in backend_servers.values()
        )

    @property
    def total_queued_requests(self) -> int:
        """Sum of all queued requests across all servers."""
        return sum(
            server.queued
            for backend_servers in self.backend_to_servers.values()
            for server in backend_servers.values()
        )

    @property
    def is_system_idle(self) -> bool:
        """Return True if the entire system has no active load."""
        return self.total_active_sessions == 0 and self.total_queued_requests == 0

    @property
    def draining_progress_pct(self) -> float:
        """Return percentage of servers currently draining (0.0 to 100.0)."""
        if self.total_servers == 0:
            return 0.0
        return (self.draining_servers / self.total_servers) * 100.0


@dataclass
class HealthRouteInfo:
    """Information regarding how proxy should respond to health and routes requests."""

    healthy: bool = True
    status: int = 200
    health_message: str = HEALTHY_MESSAGE
    routes_message: str = "{}"
    routes_content_type: str = "application/json"


@dataclass
class ServerConfig:
    """Configuration for a single server."""

    name: str  # Server identifier for HAProxy config
    host: str  # IP/hostname to connect to
    port: int  # Port to connect to

    def __str__(self) -> str:
        return f"ServerConfig(name='{self.name}', host='{self.host}', port={self.port})"

    def __repr__(self) -> str:
        return str(self)


@dataclass
class BackendConfig:
    """Configuration for a single application backend."""

    # Name of the target group.
    name: str

    # Path prefix for the target group. This will be used to route requests to the target group.
    path_prefix: str

    # Maximum time HAProxy will wait for a successful TCP connection to be established with the backend server.
    timeout_connect_s: Optional[int] = None

    # Maximum time that the backend server can be inactive while sending data back to HAProxy.
    # This is also active during the initial response phase.
    timeout_server_s: Optional[int] = None

    # Maximum time that the client can be inactive while sending data to HAProxy.
    # This is active during the initial request phase.
    timeout_client_s: Optional[int] = None
    timeout_http_request_s: Optional[int] = None

    # Maximum time HAProxy will wait for a request in the queue.
    timeout_queue_s: Optional[int] = None

    # Maximum time HAProxy will keep the connection alive.
    # This has to be the same or greater than the client side keep-alive timeout.
    timeout_http_keep_alive_s: Optional[int] = None

    # Control the inactivity timeout for established WebSocket connections.
    # Without this setting, a WebSocket connection could be prematurely terminated by other,
    # more general timeout settings like timeout client or timeout server,
    # which are intended for the initial phases of a connection.
    timeout_tunnel_s: Optional[int] = None

    # The number of consecutive failed health checks that must occur before a service instance is marked as unhealthy
    health_check_fall: Optional[int] = None

    # Number of consecutive successful health checks required to mark an unhealthy service instance as healthy again
    health_check_rise: Optional[int] = None

    # Interval, or the amount of time, between each health check attempt
    health_check_inter: Optional[str] = None

    # The interval between two consecutive health checks when the server is in any of the transition states: UP - transitionally DOWN or DOWN - transitionally UP
    health_check_fastinter: Optional[str] = None

    # The interval between two consecutive health checks when the server is in the DOWN state
    health_check_downinter: Optional[str] = None

    # Endpoint path that the health check mechanism will send a request to. It's typically an HTTP path.
    health_check_path: Optional[str] = "/-/healthz"

    # List of servers in this backend
    servers: List[ServerConfig] = field(default_factory=list)

    # The fallback server for this backend.
    fallback_server: Optional[ServerConfig] = None

    # The app name for this backend.
    app_name: str = field(default_factory=str)

    # Size of this backend's `server-template` slot pool. The rendered
    # config emits `server-template srv 1-{slot_pool_size} ...` so the
    # backend can hold up to `slot_pool_size` replicas before the runtime
    # API has to fall back to a reload to re-split. Set by HAProxyManager
    # via `_compute_slot_split` on every config (re)generation.
    slot_pool_size: int = 0

    def build_health_check_config(self, global_config: "HAProxyConfig") -> dict:
        """Build health check configuration for HAProxy backend.

        Returns a dict with:
        - health_path: path for HTTP health checks (or None)
        - default_server_directive: complete "default-server" line with all params
        """
        # Resolve values: backend-specific overrides global defaults
        fall = (
            self.health_check_fall
            if self.health_check_fall is not None
            else global_config.health_check_fall
        )
        rise = (
            self.health_check_rise
            if self.health_check_rise is not None
            else global_config.health_check_rise
        )
        inter = (
            self.health_check_inter
            if self.health_check_inter is not None
            else global_config.health_check_inter
        )
        fastinter = (
            self.health_check_fastinter
            if self.health_check_fastinter is not None
            else global_config.health_check_fastinter
        )
        downinter = (
            self.health_check_downinter
            if self.health_check_downinter is not None
            else global_config.health_check_downinter
        )
        health_path = (
            self.health_check_path
            if self.health_check_path is not None
            else global_config.health_check_path
        )

        # Build default-server directive
        parts = []

        # Add optional fastinter/downinter only if provided
        if fastinter is not None:
            parts.append(f"fastinter {fastinter}")
        if downinter is not None:
            parts.append(f"downinter {downinter}")

        # Add required fall/rise/inter if any are set
        if fall is not None:
            parts.append(f"fall {fall}")
        if rise is not None:
            parts.append(f"rise {rise}")
        if inter is not None:
            parts.append(f"inter {inter}")

        # Always add check at the end
        parts.append("check")

        default_server_directive = "default-server " + " ".join(parts)

        return {
            "health_path": health_path,
            "default_server_directive": default_server_directive,
        }

    def __str__(self) -> str:
        return f"BackendConfig(app_name='{self.app_name}', name='{self.name}', path_prefix='{self.path_prefix}', servers={self.servers}, fallback_server={self.fallback_server})"

    def __repr__(self) -> str:
        return str(self)


@dataclass
class HAProxyConfig:
    """Configuration for HAProxy."""

    socket_path: str = RAY_SERVE_HAPROXY_SOCKET_PATH
    server_state_base: str = RAY_SERVE_HAPROXY_SERVER_STATE_BASE
    server_state_file: str = RAY_SERVE_HAPROXY_SERVER_STATE_FILE
    # Enable HAProxy optimizations (server state persistence, etc.)
    # Disabled by default to prevent test suite interference
    enable_hap_optimization: bool = RAY_SERVE_ENABLE_HAPROXY_OPTIMIZED_CONFIG
    maxconn: int = RAY_SERVE_HAPROXY_MAXCONN
    nbthread: int = RAY_SERVE_HAPROXY_NBTHREAD
    stats_port: int = RAY_SERVE_HAPROXY_STATS_PORT
    stats_uri: str = "/stats"
    metrics_port: int = RAY_SERVE_HAPROXY_METRICS_PORT
    metrics_uri: str = "/metrics"
    # All timeout values are in seconds
    timeout_queue_s: Optional[int] = None
    timeout_connect_s: Optional[int] = RAY_SERVE_HAPROXY_TIMEOUT_CONNECT_S
    timeout_client_s: Optional[int] = RAY_SERVE_HAPROXY_TIMEOUT_CLIENT_S
    timeout_server_s: Optional[int] = RAY_SERVE_HAPROXY_TIMEOUT_SERVER_S
    timeout_http_request_s: Optional[int] = None
    hard_stop_after_s: Optional[int] = RAY_SERVE_HAPROXY_HARD_STOP_AFTER_S
    # Number of connection-level retries per request. Used in the `defaults`
    # block; combined with `option redispatch` (set per-backend) each retry
    # picks a different healthy server.
    retries: int = RAY_SERVE_HAPROXY_RETRIES
    custom_global: Dict[str, str] = field(default_factory=dict)
    custom_defaults: Dict[str, str] = field(default_factory=dict)
    inject_process_id_header: bool = False
    reload_id: Optional[str] = None  # Unique ID for each reload
    tcp_nodelay: bool = RAY_SERVE_HAPROXY_TCP_NODELAY
    enable_so_reuseport: bool = (
        os.environ.get("SERVE_SOCKET_REUSE_PORT_ENABLED", "0") == "1"
    )
    has_received_routes: bool = False
    has_received_servers: bool = False
    pass_health_checks: bool = True
    health_check_endpoint: str = "/-/healthz"
    # Global health check parameters (used as defaults for backends)
    # Number of consecutive failed health checks that must occur before a service instance is marked as unhealthy
    health_check_fall: Optional[int] = RAY_SERVE_HAPROXY_HEALTH_CHECK_FALL

    # Number of consecutive successful health checks required to mark an unhealthy service instance as healthy again
    health_check_rise: Optional[int] = RAY_SERVE_HAPROXY_HEALTH_CHECK_RISE

    # Interval, or the amount of time, between each health check attempt
    health_check_inter: Optional[str] = RAY_SERVE_HAPROXY_HEALTH_CHECK_INTER

    # The interval between two consecutive health checks when the server is in any of the transition states: UP - transitionally DOWN or DOWN - transitionally UP
    health_check_fastinter: Optional[str] = RAY_SERVE_HAPROXY_HEALTH_CHECK_FASTINTER

    # The interval between two consecutive health checks when the server is in the DOWN state
    health_check_downinter: Optional[str] = RAY_SERVE_HAPROXY_HEALTH_CHECK_DOWNINTER

    health_check_path: Optional[str] = "/-/healthz"  # For HTTP health checks

    http_options: HTTPOptions = field(default_factory=HTTPOptions)

    syslog_port: int = RAY_SERVE_HAPROXY_SYSLOG_PORT

    balance_algorithm: str = RAY_SERVE_HAPROXY_BALANCE_ALGORITHM

    is_head: bool = False

    @property
    def transfer_socket_path(self) -> str:
        """Path of the dedicated FD-transfer admin socket.

        Separate from `socket_path` so that the `-x` socket transfer during
        reload (which goes through HAProxy's CLI mux) doesn't have to
        compete with our runtime-API command stream (`add server`,
        `del server`, etc.). Derived deterministically from `socket_path`
        so existing per-node path scoping carries over automatically.
        """
        return self.socket_path + ".fd"

    @property
    def frontend_host(self) -> str:
        if self.http_options.host is None or self.http_options.host == "0.0.0.0":
            return "*"

        return self.http_options.host

    @property
    def frontend_port(self) -> int:
        return self.http_options.port

    @property
    def timeout_http_keep_alive_s(self) -> int:
        return self.http_options.keep_alive_timeout_s

    def build_health_route_info(self, backends: List[BackendConfig]) -> HealthRouteInfo:
        if not self.has_received_routes:
            router_ready_for_traffic = False
            router_message = NO_ROUTES_MESSAGE
        elif self.is_head or self.has_received_servers:
            router_ready_for_traffic = True
            router_message = ""
        else:
            router_ready_for_traffic = False
            router_message = NO_REPLICAS_MESSAGE

        if not self.pass_health_checks:
            healthy = False
            message = DRAINING_MESSAGE
        elif not router_ready_for_traffic:
            healthy = False
            message = router_message
        else:
            healthy = True
            message = HEALTHY_MESSAGE

        if healthy:
            # Build routes JSON mapping: {"<path_prefix>": "<app_name>", ...}
            routes = {
                be.path_prefix: be.app_name
                for be in backends
                if be.app_name and be.path_prefix
            }
            routes_json = json.dumps(routes, separators=(",", ":"), ensure_ascii=False)

            # Escape for haproxy double-quoted string literal
            routes_message = routes_json.replace("\\", "\\\\").replace('"', '\\"')
        else:
            routes_message = message

        return HealthRouteInfo(
            healthy=healthy,
            status=200 if healthy else 503,
            health_message=message,
            routes_message=routes_message,
            routes_content_type="application/json" if healthy else "text/plain",
        )

    # TODO: support custom root_path and https


class ProxyApi(ABC):
    """Generic interface for load balancer management operations."""

    @abstractmethod
    async def start(self) -> None:
        """Initializes proxy configuration files."""
        pass

    @abstractmethod
    async def get_all_stats(self) -> Dict[str, Dict[str, ServerStats]]:
        """Get statistics for all servers in all backends."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop the proxy."""
        pass

    @abstractmethod
    async def disable(self) -> None:
        """Disables the proxy from receiving any HTTP requests"""
        pass

    @abstractmethod
    async def enable(self) -> None:
        """Enables the proxy from receiving any HTTP requests"""
        pass

    @abstractmethod
    async def reload(self) -> None:
        """Gracefully reload the service."""
        pass


@dataclass
class _BackendDiff:
    """Diff between old and new BackendConfigs for incremental update path.

    When `is_structural` is True, a full reload is required (backends added/
    removed, ACLs/timeouts/health-check parameters changed, fallback changed).

    When `is_structural` is False, only the `servers` list within one or more
    backends differs, and the change can be applied via HAProxy's runtime API
    (`add server` / `del server` over the admin socket) without a reload.
    """

    is_structural: bool
    servers_to_add: Dict[str, List[ServerConfig]] = field(default_factory=dict)
    servers_to_remove: Dict[str, List[str]] = field(default_factory=dict)


def _compute_backend_diff(
    old_configs: Dict[str, BackendConfig],
    new_configs: Dict[str, BackendConfig],
) -> _BackendDiff:
    """Determine whether a config change can be applied incrementally.

    Anything other than pure server list differences within an existing
    backend is treated as a structural change requiring a reload. This keeps
    the incremental path conservative: when in doubt, reload.
    """
    # Different set of backends (added/removed) → reload
    if set(old_configs.keys()) != set(new_configs.keys()):
        return _BackendDiff(is_structural=True)

    servers_to_add: Dict[str, List[ServerConfig]] = {}
    servers_to_remove: Dict[str, List[str]] = {}

    for name in new_configs:
        old_be = old_configs[name]
        new_be = new_configs[name]

        # Any backend-level config change → reload. Compare the fields that
        # affect the rendered backend block (excluding the servers list).
        # `name` is included defensively: callers in this file invariably
        # use the dict key as the BackendConfig.name, but the helper itself
        # shouldn't assume that invariant — a name change has to reload to
        # update the frontend ACL labels and `use_backend` directives.
        backend_level_fields = (
            "name",
            "path_prefix",
            "fallback_server",
            "app_name",
            # slot_pool_size is part of the rendered `server-template` line,
            # so any change requires a reload to take effect.
            "slot_pool_size",
            "timeout_connect_s",
            "timeout_server_s",
            "timeout_client_s",
            "timeout_http_request_s",
            "timeout_queue_s",
            "timeout_http_keep_alive_s",
            "timeout_tunnel_s",
            "health_check_fall",
            "health_check_rise",
            "health_check_inter",
            "health_check_fastinter",
            "health_check_downinter",
            "health_check_path",
        )
        for f in backend_level_fields:
            if getattr(old_be, f) != getattr(new_be, f):
                return _BackendDiff(is_structural=True)

        # Compare server lists by name. A name present in both with different
        # host/port is treated as remove + add (HAProxy's runtime API can't
        # update a server's address in-place across all versions).
        old_servers = {s.name: s for s in old_be.servers}
        new_servers = {s.name: s for s in new_be.servers}

        added: List[ServerConfig] = []
        removed: List[str] = []

        for sname, server in new_servers.items():
            if sname not in old_servers:
                added.append(server)
            elif old_servers[sname] != server:
                added.append(server)
                removed.append(sname)

        for sname in old_servers:
            if sname not in new_servers:
                removed.append(sname)

        if added:
            servers_to_add[name] = added
        if removed:
            servers_to_remove[name] = removed

    return _BackendDiff(
        is_structural=False,
        servers_to_add=servers_to_add,
        servers_to_remove=servers_to_remove,
    )


class BackendSlotPool:
    """Tracks slot ↔ replica mapping for one backend's `server-template` block.

    HAProxy's `server-template` directive pre-allocates a fixed pool of
    server slots at config-generation time. The slots are named
    `<prefix><N>` (e.g. `srv1`, `srv2`, ...) and start disabled with a
    placeholder address. Once HAProxy is running we repurpose slots
    dynamically via the runtime API:

        # Assign slot N to a new replica:
        set server <backend>/srvN addr <ip> port <port>
        set server <backend>/srvN state ready

        # Release slot N (replica removed):
        set server <backend>/srvN state maint

    The pool tracks which slots are in use and which are free; allocation
    is "first-free" via a min-heap so slot numbers stay compact under
    churn. `assign` is idempotent — re-assigning an already-present
    replica returns its existing slot, which lets the apply loop handle
    same-name-different-address transitions without bookkeeping.
    """

    def __init__(self, backend_name: str, pool_size: int):
        if pool_size <= 0:
            raise ValueError(f"BackendSlotPool requires pool_size > 0, got {pool_size}")
        self.backend_name = backend_name
        self.pool_size = pool_size
        self._replica_to_slot: Dict[str, int] = {}
        # min-heap of free slot numbers (1-indexed to match HAProxy CLI)
        self._free_slots: List[int] = list(range(1, pool_size + 1))
        heapq.heapify(self._free_slots)

    def assign(self, replica_name: str) -> Optional[int]:
        """Allocate a slot for `replica_name`. Returns the slot number,
        or None if the pool is full. Idempotent: re-assigning an
        already-present replica returns its existing slot."""
        existing = self._replica_to_slot.get(replica_name)
        if existing is not None:
            return existing
        if not self._free_slots:
            return None
        slot = heapq.heappop(self._free_slots)
        self._replica_to_slot[replica_name] = slot
        return slot

    def release(self, replica_name: str) -> Optional[int]:
        """Free the slot held by `replica_name`. Returns the slot number,
        or None if the replica wasn't holding a slot."""
        slot = self._replica_to_slot.pop(replica_name, None)
        if slot is not None:
            heapq.heappush(self._free_slots, slot)
        return slot

    def slot_for(self, replica_name: str) -> Optional[int]:
        """Return the slot held by `replica_name`, or None if absent."""
        return self._replica_to_slot.get(replica_name)

    def in_use(self) -> int:
        return len(self._replica_to_slot)

    def free(self) -> int:
        return len(self._free_slots)

    def is_exhausted(self) -> bool:
        return not self._free_slots

    def mapping(self) -> Dict[str, int]:
        """Return a copy of the replica → slot mapping (for diagnostics)."""
        return dict(self._replica_to_slot)


def _compute_slot_split(
    backend_to_replica_count: Dict[str, int],
    total: int = RAY_SERVE_HAPROXY_TOTAL_SLOTS,
    min_per_backend: int = RAY_SERVE_HAPROXY_MIN_SLOTS_PER_BACKEND,
    headroom_factor: float = RAY_SERVE_HAPROXY_SLOT_HEADROOM_FACTOR,
) -> Dict[str, int]:
    """Partition `total` slots across backends.

    Allocation strategy:
      1. Reserve `min_per_backend` for every backend (floor).
      2. Compute each backend's "demand" as `replicas * headroom_factor`.
      3. Distribute the remaining slots proportionally to demand. Backends
         with zero replicas only get their minimum (no extra share).

    Invariants when N := len(backend_to_replica_count):
      - sum(result.values()) <= total
      - For each b: result[b] >= min_per_backend  (when N * min <= total)
      - For each b: result[b] >= 1                (always)

    Degraded path: if `N * min_per_backend > total`, we can't honor the
    minimum and fall back to equal split (floor(total / N) each, with the
    remainder distributed to the first few backends alphabetically). This
    is a pathological case that should not occur with default settings
    (4096 / 16 = 256 backends).
    """
    if not backend_to_replica_count:
        return {}

    n = len(backend_to_replica_count)
    names = sorted(backend_to_replica_count.keys())

    if n * min_per_backend > total:
        # Pathological: too many backends to give each the minimum. Equal split.
        base = max(1, total // n)
        remainder = max(0, total - base * n)
        return {
            name: base + (1 if i < remainder else 0) for i, name in enumerate(names)
        }

    reserved = n * min_per_backend
    remaining = total - reserved

    demand = {
        name: max(0, int(math.ceil(backend_to_replica_count[name] * headroom_factor)))
        for name in names
    }
    total_demand = sum(demand.values())

    extra: Dict[str, int] = {name: 0 for name in names}
    if remaining > 0:
        if total_demand == 0:
            # No replicas anywhere — split the remainder equally.
            per_backend = remaining // n
            leftover = remaining - per_backend * n
            for i, name in enumerate(names):
                extra[name] = per_backend + (1 if i < leftover else 0)
        else:
            # Proportional to demand. Float-then-floor, then distribute the
            # rounding remainder to backends with the largest fractional parts
            # so we always allocate exactly `remaining` slots.
            shares_float = {
                name: remaining * demand[name] / total_demand for name in names
            }
            shares = {name: int(shares_float[name]) for name in names}
            allocated = sum(shares.values())
            leftover = remaining - allocated
            # Sort by fractional part descending; ties broken by name asc.
            order = sorted(
                names,
                key=lambda name: (
                    -(shares_float[name] - shares[name]),
                    name,
                ),
            )
            for name in order[:leftover]:
                shares[name] += 1
            extra = shares

    return {name: min_per_backend + extra[name] for name in names}


class HAProxyApi(ProxyApi):
    """ProxyApi implementation for HAProxy."""

    def __init__(
        self,
        cfg: HAProxyConfig,
        backend_configs: Dict[str, BackendConfig] = None,
        config_file_path: str = RAY_SERVE_HAPROXY_CONFIG_FILE_LOC,
    ):
        self.cfg = cfg
        self.backend_configs = backend_configs or {}
        self.config_file_path = config_file_path
        # Lock to prevent concurrent config modifications
        self._config_lock = asyncio.Lock()
        self._proc = None
        # Track old processes from graceful reloads that may still be draining
        self._old_procs: List[asyncio.subprocess.Process] = []
        # Slot pools, one per backend, sized by the most recent rendered
        # config's `slot_pool_size`. Populated/reset by `_rebind_slot_pools`
        # which is called after every successful (re)load.
        self._slot_pools: Dict[str, BackendSlotPool] = {}

        # Ensure required directories exist during initialization
        self._initialize_directories_and_error_files()

    def _initialize_directories_and_error_files(self) -> None:
        """
        Ensures all required directories exist, creates a unified 500 error file,
        and assigns its path to self.cfg.error_file_path. Called once during initialization.
        """
        # Create a config file directory
        config_dir = os.path.dirname(self.config_file_path)
        os.makedirs(config_dir, exist_ok=True)

        # Create a socket directory
        socket_dir = os.path.dirname(self.cfg.socket_path)
        os.makedirs(socket_dir, exist_ok=True)

        # Create a server state directory only if optimization is enabled
        if self.cfg.enable_hap_optimization:
            server_state_dir = os.path.dirname(self.cfg.server_state_file)
            os.makedirs(server_state_dir, exist_ok=True)

        # Create a single error file for both 502 and 504 errors
        # Both will be normalized to 500 Internal Server Error
        error_file_path = os.path.join(config_dir, "500.http")
        with open(error_file_path, "w") as ef:
            ef.write("HTTP/1.1 500 Internal Server Error\r\n")
            ef.write("Content-Type: text/plain\r\n")
            ef.write("Content-Length: 21\r\n")
            ef.write("\r\n")
            ef.write("Internal Server Error")

        self.cfg.error_file_path = error_file_path

    async def _rebind_slot_pools(self) -> None:
        """Rebuild slot pools from backend_configs and populate slots.

        Called after every successful (re)load. The new HAProxy process
        starts with all template slots disabled at the placeholder
        address, so we:
          1. Recreate the in-memory pools (sized per BackendConfig's
             `slot_pool_size`, set by `_compute_slot_split` at
             config-generation time).
          2. Assign current `backend_configs[*].servers` to slots.
          3. Send a single batched runtime-API call to point those slots
             at the right addresses and bring them up.

        This handles two cases under one path:
          - Production: a reload was triggered because the incremental
            apply gave up (e.g. structural change, pool exhausted). The
            new desired state was already written to `backend_configs`
            by `try_apply_servers_dynamically` before it returned False;
            this method makes HAProxy actually serve from it.
          - Tests: HAProxyApi is constructed with explicit
            `backend_configs={...}` and `start()` is called; this
            method binds those servers to slots without requiring the
            caller to make an extra runtime-API call.

        Step 3 is best-effort: on failure HAProxy is up but slots are
        unpopulated; the next broadcast will diff against
        `backend_configs` (which already reflects desired state) and
        find no work to do, leaving slots empty. To recover, callers
        should observe a failed apply and retry. In practice failures
        here are rare since we just restarted HAProxy.
        """
        new_pools: Dict[str, BackendSlotPool] = {}
        for name, bc in self.backend_configs.items():
            if bc.slot_pool_size <= 0:
                continue
            new_pools[name] = BackendSlotPool(name, bc.slot_pool_size)
        self._slot_pools = new_pools

        if new_pools:
            sizes = {n: p.pool_size for n, p in new_pools.items()}
            logger.info(
                "Slot pools reset: total=%d, split=%s",
                sum(sizes.values()),
                sizes,
                extra={"log_to_stderr": True},
            )

        if not self.backend_configs:
            return
        commands: List[str] = []
        for backend_name, bc in self.backend_configs.items():
            pool = self._slot_pools.get(backend_name)
            if pool is None or not bc.servers:
                continue
            for server in bc.servers:
                slot = pool.assign(server.name)
                if slot is None:
                    logger.warning(
                        "Slot population: backend %s pool exhausted "
                        "(pool_size=%d). Some servers will be unreachable "
                        "until the next reload re-splits.",
                        backend_name,
                        pool.pool_size,
                    )
                    break
                commands.append(
                    f"set server {backend_name}/srv{slot} "
                    f"addr {server.host} port {server.port}"
                )
                commands.append(f"set server {backend_name}/srv{slot} state ready")
        if not commands:
            return
        try:
            await self._send_runtime_commands(commands)
            logger.info(
                "Slot population OK: %d servers placed across %d backends",
                sum(len(bc.servers) for bc in self.backend_configs.values()),
                len(self._slot_pools),
                extra={"log_to_stderr": True},
            )
        except Exception as e:
            logger.warning(
                f"Slot population failed after reload: {e}. HAProxy is up "
                f"but no replicas are routable until the next broadcast retry."
            )
        # Best-effort snapshot for live debugging.
        self._write_slot_mapping_for_debug()

    def _write_slot_mapping_for_debug(self) -> None:
        """Best-effort dump of current slot mapping to a JSON file.

        Writes `<socket_dir>/slot_mapping.json` so operators can `cat` it
        to translate `srv1`/`srv2`/... back to replica IDs without
        grepping logs. Best-effort: any failure is swallowed so a write
        problem can't break an otherwise-successful apply.
        """
        try:
            socket_dir = os.path.dirname(self.cfg.socket_path)
            mapping_path = os.path.join(socket_dir, "slot_mapping.json")
            payload = {
                name: {
                    "pool_size": pool.pool_size,
                    "in_use": pool.in_use(),
                    "free": pool.free(),
                    "slots": {
                        f"srv{slot}": replica
                        for replica, slot in pool.mapping().items()
                    },
                }
                for name, pool in self._slot_pools.items()
            }
            tmp = mapping_path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(payload, f, indent=2, sort_keys=True)
            os.replace(tmp, mapping_path)
        except Exception as e:
            logger.debug(f"Failed to write slot_mapping.json: {e}")

    def _is_running(self) -> bool:
        """Check if the HAProxy process is still running."""
        return self._proc is not None and self._proc.returncode is None

    async def _start_and_wait_for_haproxy(
        self,
        *extra_args: str,
        timeout_s: int = RAY_SERVE_HAPROXY_RELOAD_TIMEOUT_S,
    ) -> asyncio.subprocess.Process:
        # Build command args
        haproxy_bin = get_haproxy_binary()
        args = [haproxy_bin, "-db", "-f", self.config_file_path]

        if not self.cfg.enable_so_reuseport:
            args.append("-dR")

        # Add any extra args (like -sf for graceful reload)
        args.extend(extra_args)

        logger.debug(f"Starting HAProxy with args: {args}")

        spawn_start = time.monotonic()
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        spawn_elapsed = time.monotonic() - spawn_start

        wait_start = time.monotonic()
        try:
            await self._wait_for_hap_availability(proc, timeout_s=timeout_s)
        except Exception:
            wait_elapsed = time.monotonic() - wait_start
            logger.warning(
                "HAProxy start failed (PID=%s): spawn=%.2fs wait=%.2fs args=%s",
                proc.pid,
                spawn_elapsed,
                wait_elapsed,
                extra_args,
            )
            # If startup fails, ensure the process is killed to avoid orphaned processes
            if proc.returncode is None:
                proc.kill()
                await proc.wait()
            raise

        wait_elapsed = time.monotonic() - wait_start
        logger.info(
            "HAProxy started (PID=%s): spawn=%.2fs wait=%.2fs args=%s",
            proc.pid,
            spawn_elapsed,
            wait_elapsed,
            extra_args,
        )
        return proc

    async def _save_server_state(self) -> None:
        """Save the server state to the file."""
        server_state = await self._send_socket_command("show servers state")
        with open(self.cfg.server_state_file, "w") as f:
            f.write(server_state)

    async def _terminate_old_procs(self) -> None:
        """SIGKILL any old HAProxy procs we're still tracking.

        Used during fresh-start recovery (when -sf/-x graceful reload isn't
        possible) to free listener ports the orphans may still be holding.
        Idempotent — already-dead procs are a no-op.
        """
        for old_proc in self._old_procs:
            if old_proc.returncode is not None:
                continue
            try:
                old_proc.kill()
                await old_proc.wait()
                logger.info(
                    f"Killed orphaned HAProxy process (PID: {old_proc.pid}) "
                    "to free listener ports for fresh start."
                )
            except Exception as e:
                logger.warning(
                    f"Failed to kill orphaned HAProxy process "
                    f"(PID: {old_proc.pid}): {e}"
                )
        self._old_procs.clear()

    async def _graceful_reload(self) -> None:
        """Perform a graceful reload of HAProxy by starting a new process with -sf."""
        reload_start = time.monotonic()
        phases: List[str] = []
        try:
            old_proc = self._proc
            # If the previous HAProxy process has already exited (e.g., a prior
            # reload's `-x admin.sock` socket transfer failed and the new
            # process exited 1), there's nothing to graceful-reload from: no
            # listening FDs to inherit and no live PID to signal. Without this
            # branch every subsequent reload would re-raise "HAProxy crashed
            # during startup: exit code 1" from `_wait_for_hap_availability`
            # below and the actor would be stuck with no listener on 8000
            # until the controller kills it. Start a fresh process instead so
            # we recover automatically; this drops in-flight connections
            # briefly but is strictly better than staying down.
            if old_proc is None or old_proc.returncode is not None:
                logger.warning(
                    "Previous HAProxy process is not running "
                    "(returncode=%s); starting a fresh HAProxy instead of "
                    "attempting a graceful reload.",
                    old_proc.returncode if old_proc is not None else None,
                )
                # Kill any tracked old procs that may still be holding the
                # listener ports. Under -x failures the prior reload never
                # delivered SIGUSR1 to its predecessor, so processes appended
                # to `_old_procs` can stay fully bound to ports 8000 / 9101 /
                # 8404 indefinitely. A fresh-start without -sf/-x doesn't
                # inherit anything, so it would otherwise hit EADDRINUSE on
                # those ports until the orphans finish their natural drain
                # (tens of seconds at minimum). SIGKILL them up front so the
                # fresh process can bind immediately.
                t0 = time.monotonic()
                await self._terminate_old_procs()
                phases.append(f"terminate_old={time.monotonic() - t0:.2f}s")

                t0 = time.monotonic()
                self._proc = await self._start_and_wait_for_haproxy()
                phases.append(f"fresh_start={time.monotonic() - t0:.2f}s")

                # New process started with empty server-template slots;
                # rebind pools from backend_configs and repopulate.
                t0 = time.monotonic()
                await self._rebind_slot_pools()
                phases.append(f"rebind_slots={time.monotonic() - t0:.2f}s")

                logger.info(
                    "HAProxy fresh-start reload OK in %.2fs (%s)",
                    time.monotonic() - reload_start,
                    " ".join(phases),
                )
                return

            t0 = time.monotonic()
            await self._wait_for_hap_availability(old_proc)
            phases.append(f"old_proc_check={time.monotonic() - t0:.2f}s")

            # Save server state if optimization is enabled. Treat failures
            # as non-fatal: the state file is purely an optimization (lets
            # the new HAProxy preserve per-server connection counts across
            # reload), and a stale or missing file just causes HAProxy to
            # start counters from zero. A `show servers state` admin-socket
            # timeout here under load used to abort the entire reload --
            # which then cascaded since the next broadcast retried with
            # the same overloaded socket.
            if self.cfg.enable_hap_optimization:
                t0 = time.monotonic()
                try:
                    await self._save_server_state()
                    phases.append(f"save_state={time.monotonic() - t0:.2f}s")
                except Exception as e:
                    phases.append(f"save_state_skipped={time.monotonic() - t0:.2f}s")
                    logger.warning(
                        f"Skipping HAProxy server state save before reload: {e}"
                    )

            # Start new HAProxy process with -sf flag to gracefully take over from old process
            # Use -x socket transfer for seamless reloads if optimization is enabled
            reload_args = ["-sf", str(old_proc.pid)]
            if self.cfg.enable_hap_optimization:
                # Use the dedicated FD-transfer socket so `_getsocks` doesn't
                # queue behind in-flight runtime-API commands on the main
                # admin socket.
                reload_args.extend(["-x", self.cfg.transfer_socket_path])

            t0 = time.monotonic()
            self._proc = await self._start_and_wait_for_haproxy(*reload_args)
            phases.append(f"new_proc_start={time.monotonic() - t0:.2f}s")

            # Track old process so we can ensure it's cleaned up during shutdown
            if old_proc is not None:
                self._old_procs.append(old_proc)

            # New process has empty server-template slots; rebind pools
            # from backend_configs and repopulate.
            t0 = time.monotonic()
            await self._rebind_slot_pools()
            phases.append(f"rebind_slots={time.monotonic() - t0:.2f}s")

            logger.info(
                "HAProxy graceful reload OK in %.2fs (%s)",
                time.monotonic() - reload_start,
                " ".join(phases),
            )
        except Exception as e:
            logger.error(
                "HAProxy graceful reload failed after %.2fs (%s): %s",
                time.monotonic() - reload_start,
                " ".join(phases) if phases else "no_phases_completed",
                e,
            )
            raise

    async def _is_listener_bound(self) -> bool:
        """Quick TCP-only check: is HAProxy accepting connections on its port?

        Used for readiness detection inside `_wait_for_hap_availability`.
        We deliberately don't send an HTTP request here -- under startup
        load (large config, many backends, server-state file replay)
        HAProxy can be bound but momentarily slow to dispatch the first
        HTTP requests to its worker threads, which would burn our entire
        5s startup budget on a single read timeout. TCP-bound is the
        right signal for "the listener is up"; HTTP-level responsiveness
        follows within milliseconds and is verified separately by the
        controller's health check via `probe_http_listener`.
        """
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", self.cfg.frontend_port),
                timeout=0.5,
            )
        except Exception:
            return False
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        return True

    async def probe_http_listener(self) -> bool:
        """Probe the HAProxy HTTP listener via /-/healthz.

        Returns True if HAProxy is parsing HTTP on its frontend port,
        False otherwise. Used by the controller's health check, which
        runs on a stable proxy and needs to differentiate "alive and
        serving" from "bound but stuck". Does NOT depend on the admin
        socket -- the admin socket is shared with our runtime-API
        commands and the `-x` reload FD transfer, and saturates under
        autoscaling churn. The HTTP listener is served by HAProxy worker
        threads independent of the CLI mux, so it stays responsive when
        the admin socket is busy.

        During graceful reload (where the old process is still bound to
        the same port) this probe may be answered by either the old or
        the new process; that's acceptable for readiness purposes because
        either way the port is serving traffic. Crash detection is
        handled separately by polling `proc.returncode`.
        """
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", self.cfg.frontend_port),
                timeout=2.0,
            )
        except Exception:
            return False
        try:
            writer.write(
                b"GET /-/healthz HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n"
            )
            await writer.drain()
            response = await asyncio.wait_for(reader.read(256), timeout=2.0)
            # Accept any HTTP response, not just 200. The signal we want is
            # "HAProxy is alive and parsing HTTP", not "HAProxy says traffic
            # is healthy". /-/healthz legitimately returns 503 in two cases:
            #   - drain mode (disable() sets pass_health_checks=False to
            #     signal upstream LB to stop routing).
            #   - initial startup before has_received_routes is True.
            # In both states HAProxy is functioning and admin-socket-based
            # `is_running()` would return True; we match that behavior.
            return response.startswith(b"HTTP/")
        except Exception:
            return False
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _wait_for_hap_availability(
        self,
        proc: asyncio.subprocess.Process,
        timeout_s: int = RAY_SERVE_HAPROXY_RELOAD_TIMEOUT_S,
    ) -> None:
        start_time = time.monotonic()
        iterations = 0

        while time.monotonic() - start_time < timeout_s:
            iterations += 1
            if proc.returncode is not None:
                stdout = await proc.stdout.read() if proc.stdout else b""
                stderr = await proc.stderr.read() if proc.stderr else b""
                output = (
                    stderr.decode("utf-8", errors="ignore").strip()
                    or stdout.decode("utf-8", errors="ignore").strip()
                )

                raise RuntimeError(
                    f"HAProxy crashed during startup after "
                    f"{time.monotonic() - start_time:.2f}s, "
                    f"{iterations} probe attempts: "
                    f"{output or f'exit code {proc.returncode}'}"
                )

            # TCP-only check: is the listener accepting connections? We
            # don't send an HTTP request here because the per-iteration
            # cost would burn our startup budget. TCP-bound is the right
            # signal for "HAProxy is up"; the controller's check_health
            # uses the HTTP probe to verify ongoing serving capability.
            if await self._is_listener_bound():
                logger.debug(
                    "HAProxy listener bound after %.2fs (%d probe attempts)",
                    time.monotonic() - start_time,
                    iterations,
                )
                return

            await asyncio.sleep(0.5)

        raise RuntimeError(
            f"HAProxy did not enter running state within {timeout_s} seconds "
            f"({iterations} probe attempts)."
        )

    def _generate_config_file_internal(self) -> None:
        """Internal config generation without locking (for use within locked sections)."""
        try:
            env = Environment()

            # Backends are sorted in decreasing order of length of path prefix
            # to ensure that the longest path prefix match is taken first.
            # Equal lengthed prefixes are then sorted alphabetically.
            backends = sorted(
                self.backend_configs.values(),
                key=lambda be: (-len(be.path_prefix), be.path_prefix),
            )

            # Enrich backends with precomputed health check configuration strings
            backends_with_health_config = [
                {
                    "backend": backend,
                    "health_config": backend.build_health_check_config(self.cfg),
                }
                for backend in backends
            ]

            health_route_info = self.cfg.build_health_route_info(backends)

            # Render healthz rules separately for readability/reuse
            healthz_template = env.from_string(HAPROXY_HEALTHZ_RULES_TEMPLATE)
            healthz_rules = healthz_template.render(
                {
                    "config": self.cfg,
                    "backends": backends,
                    "health_info": health_route_info,
                }
            )

            config_template = env.from_string(HAPROXY_CONFIG_TEMPLATE)
            config_content = config_template.render(
                {
                    "config": self.cfg,
                    "backends": backends,
                    "backends_with_health_config": backends_with_health_config,
                    "healthz_rules": healthz_rules,
                    "route_info": health_route_info,
                }
            )

            # Ensure the config ends with a newline
            if not config_content.endswith("\n"):
                config_content += "\n"

            # Use file locking to prevent concurrent writes from multiple processes
            # This is important in test environments where multiple nodes may run
            # on the same machine
            import fcntl

            lock_file_path = self.config_file_path + ".lock"
            with open(lock_file_path, "w") as lock_f:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
                try:
                    with open(self.config_file_path, "w") as f:
                        f.write(config_content)
                finally:
                    fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)

            logger.debug(
                f"Succesfully generated HAProxy configuration: {self.config_file_path}."
            )
        except Exception as e:
            logger.error(f"Failed to create HAProxy configuration files: {e}")
            raise

    async def start(self) -> None:
        """
        Generate HAProxy configuration files and start the HAProxy server process.

        This method creates the necessary configuration files and launches the HAProxy
        process in foreground mode, ensuring that the proxy is running with the latest
        configuration and that the parent retains control of the subprocess handle.
        """
        try:
            async with self._config_lock:
                # Set initial reload ID if header injection is enabled and ID is not set
                if self.cfg.inject_process_id_header and self.cfg.reload_id is None:
                    self.cfg.reload_id = f"initial-{int(time.time() * 1000)}"

                self._generate_config_file_internal()
                logger.info("Successfully generated HAProxy config file.")

            self._proc = await self._start_and_wait_for_haproxy()
            # Rebuild in-memory pools and populate slots from any
            # already-set backend_configs (test fixtures or production
            # reloads that wrote backend_configs before calling start).
            await self._rebind_slot_pools()
            logger.info("HAProxy started successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize and start HAProxy configuration: {e}")
            raise

    async def get_all_stats(self) -> Dict[str, Dict[str, ServerStats]]:
        """Get statistics for all servers in all backends (implements abstract method).

        Returns only application backends configured in self.backend_configs,
        excluding HAProxy internal components (frontends, default_backend, stats).
        Also excludes BACKEND aggregate entries, returning only individual servers.
        """
        try:
            stats_output = await self._send_socket_command("show stat")
            all_stats = self._parse_haproxy_csv_stats(stats_output)

            # Filter to only return application backends (ones in backend_configs)
            # Exclude HAProxy internal components like frontends, default_backend, stats
            # Also exclude BACKEND aggregate entries, keep only individual servers
            return {
                backend_name: {
                    server_name: stats
                    for server_name, stats in servers.items()
                    if server_name != "BACKEND"
                }
                for backend_name, servers in all_stats.items()
                if backend_name in self.backend_configs
            }
        except Exception as e:
            logger.error(f"Failed to get HAProxy stats: {e}")
            return {}

    async def get_haproxy_stats(self) -> HAProxyStats:
        """Get complete HAProxy statistics including both individual and aggregate data."""
        server_stats = await self.get_all_stats()
        return HAProxyStats(backend_to_servers=server_stats)

    async def _send_socket_command(self, command: str) -> str:
        """Send a command to the HAProxy stats socket via Unix domain socket."""
        try:
            if not os.path.exists(self.cfg.socket_path):
                raise RuntimeError(
                    f"HAProxy socket file does not exist: {self.cfg.socket_path}."
                )

            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_unix_connection(self.cfg.socket_path),
                    timeout=RAY_SERVE_HAPROXY_SOCKET_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                raise RuntimeError(
                    f"Timeout connecting to HAProxy socket: {self.cfg.socket_path}"
                )

            try:
                writer.write(f"{command}\n".encode("utf-8"))
                await writer.drain()

                # Read until EOF (HAProxy closes connection after response)
                try:
                    result_bytes = await asyncio.wait_for(
                        reader.read(), timeout=RAY_SERVE_HAPROXY_SOCKET_TIMEOUT_S
                    )
                except asyncio.TimeoutError:
                    raise RuntimeError(
                        f"Timeout while sending command '{command}' to HAProxy socket"
                    )
            finally:
                writer.close()
                await writer.wait_closed()

            result = result_bytes.decode("utf-8", errors="ignore")
            logger.debug(f"Socket command '{command}' returned {len(result)} chars.")
            return result
        except Exception as e:
            raise RuntimeError(f"Failed to send socket command '{command}': {e}")

    async def _send_runtime_command(self, command: str) -> str:
        """Send an admin-socket command and treat error responses as failures.

        HAProxy's admin socket conveys command failures by returning an
        error string in the response body — not by closing the connection
        or raising at the transport level. `_send_socket_command` returns
        the body verbatim, so callers that only catch transport-level
        exceptions (e.g. mutating commands like `add server` / `del server`)
        can mistake a rejection for a success.

        This wrapper inspects the response for the well-known error markers
        HAProxy emits and raises RuntimeError when present, so the caller's
        `try/except` reliably catches both transport failures and
        application-level rejections.
        """
        result = await self._send_socket_command(command)
        if not result:
            return result
        # HAProxy prefixes alert/warning/notice levels into the admin-socket
        # response when a command is rejected (e.g.,
        #   "[ALERT] (XYZ) : 'add server' : Backend X not found.")
        # Plain string error messages without a prefix also occur — check
        # for a small set of common error tokens. Any false-positive here
        # is fine: the worst case is we trigger a fallback reload.
        normalized = result.lower()
        error_markers = (
            "[alert]",
            "[warning]",
            "[notice]",
            "no such server",
            "no such backend",
            "doesn't exist",
            "does not exist",
            "not found",
            "not allowed",
            "syntax error",
            "invalid argument",
            "unknown command",
            "must be disabled",
        )
        for marker in error_markers:
            if marker in normalized:
                raise RuntimeError(
                    f"HAProxy rejected runtime command '{command}': "
                    f"{result.strip()}"
                )
        return result

    async def _send_runtime_commands(
        self,
        commands: List[str],
        chunk_size: int = RAY_SERVE_HAPROXY_RUNTIME_CHUNK_SIZE,
    ) -> None:
        """Send multiple admin-socket commands batched onto single connections.

        HAProxy's CLI accepts multiple commands per connection separated by
        `;`. This lets us collapse what would otherwise be N separate Unix
        socket round-trips (each contending with HTTP worker threads for
        accept/dispatch) into a small number of connections, reducing the
        wallclock cost of a large apply.

        chunk_size controls how many commands ride through each socket
        connection. The socket-level timeout (RAY_SERVE_HAPROXY_SOCKET_TIMEOUT_S)
        applies per chunk, so smaller chunks give each chunk its own budget
        rather than forcing the whole batch to fit in one timeout window.
        Under load HAProxy's CLI mux serializes admin operations behind HTTP
        worker dispatch, so the dominant cost is HAProxy's processing time
        per command, not per-chunk socket setup -- keeping chunks small
        trades a small amount of extra wallclock for a much higher chance of
        completing the runtime-API path instead of timing out into a full
        reload.

        Error detection: substring-match the concatenated response for known
        HAProxy error markers and raise on the first hit. Loses per-command
        attribution (we don't know which command in the batch failed), but
        the caller's response to any failure is the same fallback reload
        regardless.

        On failure, logs per-chunk wallclock so we can distinguish "just over
        the timeout budget" from "HAProxy genuinely wedged" -- the right fix
        for the two is different (smaller chunks vs. investigating HAProxy
        state).

        chunk_size stays comfortably under HAProxy's default 16 KB CLI
        buffer (`tune.bufsize`) for typical command lengths (~80 bytes).
        """
        if not commands:
            return
        error_markers = (
            "[alert]",
            "[warning]",
            "[notice]",
            "no such server",
            "no such backend",
            "doesn't exist",
            "does not exist",
            "not found",
            "not allowed",
            "syntax error",
            "invalid argument",
            "unknown command",
            "must be disabled",
        )
        num_chunks = (len(commands) + chunk_size - 1) // chunk_size
        chunk_times: List[float] = []
        batch_start = time.monotonic()
        for i in range(0, len(commands), chunk_size):
            chunk = commands[i : i + chunk_size]
            batched = ";".join(chunk)
            chunk_start = time.monotonic()
            try:
                result = await self._send_socket_command(batched)
            except Exception:
                # Per-chunk wallclock tells us whether we're at "just over
                # budget" (e.g. 15.0s = hit the timeout exactly, smaller chunks
                # or larger timeout would help) or "HAProxy is wedged" (much
                # longer than that, something else is wrong).
                logger.warning(
                    "Runtime API chunk %d/%d (size=%d) failed after %.2fs; "
                    "total_commands=%d, prior_chunk_times=%s, "
                    "batch_elapsed=%.2fs",
                    i // chunk_size + 1,
                    num_chunks,
                    len(chunk),
                    time.monotonic() - chunk_start,
                    len(commands),
                    [f"{t:.2f}s" for t in chunk_times],
                    time.monotonic() - batch_start,
                    extra={"log_to_stderr": False},
                )
                raise
            chunk_times.append(time.monotonic() - chunk_start)
            if not result:
                continue
            normalized = result.lower()
            for marker in error_markers:
                if marker in normalized:
                    raise RuntimeError(
                        f"HAProxy rejected one or more runtime commands in "
                        f"batch of {len(chunk)} (chunk starts at index {i}): "
                        f"{result.strip()[:500]}"
                    )
        logger.info(
            "Runtime API batch OK: %d commands, %d chunks, chunk_times=%s, "
            "total=%.2fs",
            len(commands),
            num_chunks,
            [f"{t:.2f}s" for t in chunk_times],
            time.monotonic() - batch_start,
            extra={"log_to_stderr": True},
        )

    @staticmethod
    def _parse_haproxy_csv_stats(
        stats_output: str,
    ) -> Dict[str, Dict[str, ServerStats]]:
        """Parse HAProxy stats CSV output into structured data."""
        if not stats_output or not stats_output.strip():
            return {}

        # HAProxy stats start with '#' comment - replace with nothing for CSV parsing
        csv_data = stats_output.replace("# ", "", 1)
        backend_stats: Dict[str, Dict[str, ServerStats]] = {}

        def safe_int(v):
            try:
                return int(v)
            except (TypeError, ValueError):
                return 0

        for row in csv.DictReader(io.StringIO(csv_data)):
            backend = row.get("pxname", "").strip()
            server = row.get("svname", "").strip()
            status = row.get("status", "").strip() or "UNKNOWN"

            if not backend or not server:
                continue

            backend_stats.setdefault(backend, {})
            backend_stats[backend][server] = ServerStats(
                backend=backend,
                server=server,
                status=status,
                current_sessions=safe_int(row.get("scur")),
                queued=safe_int(row.get("qcur")),
            )

        return backend_stats

    async def stop(self) -> None:
        proc = self._proc
        if proc is None:
            logger.info("HAProxy process not running, skipping shutdown.")
            return

        try:
            # Kill the current process
            if proc.returncode is None:
                proc.kill()
                await proc.wait()
                self._proc = None

            # Also kill any old processes from graceful reloads that might still be running
            for old_proc in self._old_procs:
                try:
                    if old_proc.returncode is None:
                        old_proc.kill()
                        await old_proc.wait()
                        logger.info(f"Killed old HAProxy process (PID: {old_proc.pid})")
                except Exception as e:
                    logger.warning(f"Error killing old HAProxy process: {e}")

            self._old_procs.clear()

            logger.info("Stopped HAProxy process.")
        except RuntimeError as e:
            logger.error(f"Error during HAProxy shutdown: {e}")

    async def reload(self) -> None:
        try:
            self._generate_config_file_internal()
            await self._graceful_reload()
        except Exception as e:
            raise RuntimeError(f"Failed to update and reload HAProxy: {e}")

    async def disable(self) -> None:
        """Force haproxy health checks to fail."""
        try:
            # Disable health checks (set to fail)
            self.cfg.pass_health_checks = False

            # Regenerate the config file with the deny rule
            self._generate_config_file_internal()

            # Perform a graceful reload to apply changes
            await self._graceful_reload()
            logger.info("Successfully disabled health checks.")
        except Exception as e:
            logger.error(f"Failed to disable health checks: {e}")
            raise

    async def enable(self) -> None:
        """Force haproxy health checks to pass."""
        try:
            self.cfg.pass_health_checks = True

            self._generate_config_file_internal()
            # Perform a graceful reload to apply changes
            await self._graceful_reload()
            logger.info("Successfully enabled health checks.")
        except Exception as e:
            logger.error(f"Failed to disable health checks: {e}")
            raise

    def set_backend_configs(
        self,
        backend_configs: Dict[str, BackendConfig],
    ) -> None:
        if backend_configs:
            self.cfg.has_received_routes = True

        self.backend_configs = backend_configs

        self.cfg.has_received_servers = self.cfg.has_received_servers or any(
            len(bc.servers) > 0 for bc in backend_configs.values()
        )

    async def try_apply_servers_dynamically(
        self,
        new_backend_configs: Dict[str, BackendConfig],
    ) -> bool:
        """Try to apply server-list changes via HAProxy's runtime API.

        Compares ``new_backend_configs`` to the current ``self.backend_configs``.
        If the change is purely server adds/removes within existing backends
        with unchanged slot-pool sizes, applies the diff over the admin socket
        using `set server <backend>/srvN addr ... port ...` plus
        `set server ... state ready/maint` against the pre-allocated
        `server-template` slots. Otherwise falls back to a full reload.

        The slot pool model trades up-front memory (one templated server per
        slot, regardless of whether it's in use) for runtime-API efficiency:
        we never need to `add server`/`del server` -- which mutate HAProxy's
        per-backend server list under lock -- only repurpose pre-existing
        slots. The result is fewer admin-socket commands per broadcast and
        no server-list mutation contention with HTTP workers.

        Slot accounting (which slot holds which replica) lives in
        ``self._slot_pools`` and is reset to "all free" after every
        successful (re)load, since the new HAProxy process starts with all
        template slots disabled at the placeholder address.

        Returns True if applied incrementally (no reload needed). Returns
        False if a reload is required (caller is responsible for calling
        ``reload()``).
        """
        # First-time setup or no current state to diff against → reload.
        if not self.backend_configs:
            self.set_backend_configs(new_backend_configs)
            return False

        # If HAProxy isn't running yet (initial startup), the runtime API
        # isn't available. Fall back to reload, which will start HAProxy.
        if not await self.is_running():
            self.set_backend_configs(new_backend_configs)
            return False

        diff = _compute_backend_diff(self.backend_configs, new_backend_configs)
        if diff.is_structural:
            # Structural change (which includes slot_pool_size shifts) → reload.
            self.set_backend_configs(new_backend_configs)
            return False

        if not diff.servers_to_add and not diff.servers_to_remove:
            # No-op: server lists are identical.
            self.set_backend_configs(new_backend_configs)
            return True

        # Build the command list. Process removals before adds so freed
        # slots are available for new replicas in the same broadcast --
        # otherwise a 1:1 churn (5 in, 5 out) would temporarily need 2x
        # the slots.
        commands: List[str] = []
        added_count = 0
        removed_count = 0

        for backend_name, server_names in diff.servers_to_remove.items():
            pool = self._slot_pools.get(backend_name)
            if pool is None:
                # No pool means the manager has produced a config we
                # haven't loaded into HAProxy yet. Falling back to reload
                # is the safe choice: it'll regenerate the config and
                # rebuild pools.
                logger.warning(
                    "No slot pool for backend %s; falling back to reload.",
                    backend_name,
                )
                self.set_backend_configs(new_backend_configs)
                return False
            for sname in server_names:
                slot = pool.release(sname)
                if slot is None:
                    # Replica wasn't holding a slot. This can happen if a
                    # prior apply failed mid-way; benign, skip.
                    continue
                commands.append(f"set server {backend_name}/srv{slot} state maint")
                removed_count += 1

        for backend_name, servers in diff.servers_to_add.items():
            pool = self._slot_pools.get(backend_name)
            if pool is None:
                logger.warning(
                    "No slot pool for backend %s; falling back to reload.",
                    backend_name,
                )
                self.set_backend_configs(new_backend_configs)
                return False
            for server in servers:
                slot = pool.assign(server.name)
                if slot is None:
                    logger.warning(
                        "Backend %s slot pool exhausted "
                        "(pool_size=%d, in_use=%d). Falling back to reload "
                        "to re-split slots with bigger share for this "
                        "backend.",
                        backend_name,
                        pool.pool_size,
                        pool.in_use(),
                    )
                    self.set_backend_configs(new_backend_configs)
                    return False
                commands.append(
                    f"set server {backend_name}/srv{slot} "
                    f"addr {server.host} port {server.port}"
                )
                commands.append(f"set server {backend_name}/srv{slot} state ready")
                added_count += 1

        if not commands:
            # All adds/removes were no-ops (e.g. removes for replicas that
            # never got slot-assigned). Update state and we're done.
            self.set_backend_configs(new_backend_configs)
            return True

        try:
            await self._send_runtime_commands(commands)
        except Exception as e:
            logger.warning(
                f"Failed to apply backend update incrementally via runtime "
                f"API: {e}. Falling back to a full reload.",
                extra={"log_to_stderr": False},
            )
            self.set_backend_configs(new_backend_configs)
            return False

        # Update internal state and regenerate the config file so any
        # subsequent reload starts from the current set of servers.
        self.set_backend_configs(new_backend_configs)
        try:
            self._generate_config_file_internal()
        except Exception as e:
            # Non-fatal: runtime change already applied; next reload rebuilds.
            logger.warning(
                f"Applied backend update via runtime API but failed to "
                f"refresh the config file: {e}",
                extra={"log_to_stderr": False},
            )

        logger.info(
            f"Applied backend update via runtime API: "
            f"{added_count} server(s) added, {removed_count} server(s) removed.",
            extra={"log_to_stderr": False},
        )
        # Best-effort slot-mapping snapshot for live debugging.
        self._write_slot_mapping_for_debug()
        return True

    async def is_running(self) -> bool:
        try:
            await self._send_socket_command("show info")
            return True
        except Exception:
            # During reload or shutdown, socket can be temporarily unavailable.
            # Treat as unhealthy instead of raising.
            return False


@ray.remote(num_cpus=0)
class HAProxyManager(ProxyActorInterface):
    def __init__(
        self,
        http_options: HTTPOptions,
        grpc_options: gRPCOptions,
        *,
        node_id: NodeId,
        node_ip_address: str,
        logging_config: LoggingConfig,
        long_poll_client: Optional[LongPollClient] = None,
    ):  # noqa: F821
        super().__init__(
            node_id=node_id,
            node_ip_address=node_ip_address,
            logging_config=logging_config,
            # HAProxyManager is not on the request path, so we can disable
            # the buffer to ensure logs are immediately flushed.
            log_buffer_size=1,
        )

        self._grpc_options = grpc_options
        self._http_options = http_options

        # The time when the node starts to drain.
        # The node is not draining if it's None.
        self._draining_start_time: Optional[float] = None

        self.event_loop = get_or_create_event_loop()

        self._target_groups: List[TargetGroup] = []

        # Fallback targets.
        self._http_fallback_target: Optional[Target] = None
        self._grpc_fallback_target: Optional[Target] = None

        # Lock to serialize HAProxy reloads and prevent concurrent reload operations
        # which can cause race conditions with SO_REUSEPORT
        self._reload_lock = asyncio.Lock()

        # Coalesce controller broadcasts: when a broadcast arrives we schedule
        # a single sleeping flush task. Subsequent broadcasts during the sleep
        # are absorbed implicitly because they overwrite `self._target_groups`
        # / fallback fields in place; the flush picks up the latest snapshot
        # when it wakes. This collapses bursts of replica-add/remove events
        # (common during autoscaling churn) into one runtime-API command pile,
        # which keeps the HAProxy admin socket from saturating.
        # `_coalesce_pending` lets the flush task notice broadcasts that
        # arrive during a long-running apply and trigger another iteration
        # so the trailing broadcast in a flurry isn't dropped.
        self._coalesce_window_s = RAY_SERVE_HAPROXY_BROADCAST_COALESCE_S
        self._coalesce_task: Optional[asyncio.Task] = None
        self._coalesce_pending = False

        self.long_poll_client = long_poll_client or LongPollClient(
            ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE),
            {
                LongPollNamespace.GLOBAL_LOGGING_CONFIG: self._update_logging_config,
                LongPollNamespace.TARGET_GROUPS: self.update_target_groups,
                LongPollNamespace.FALLBACK_TARGETS: self.update_fallback_targets,
            },
            call_in_event_loop=self.event_loop,
            client_id=f"{type(self).__name__}:{ray.get_runtime_context().get_actor_id()}",
        )

        is_head = self._node_id == get_head_node_id()

        startup_msg = f"HAProxy starting on node {self._node_id} (HTTP port: {self._http_options.port})."
        logger.info(startup_msg)
        logger.debug(
            f"Configure HAProxyManager actor {ray.get_runtime_context().get_actor_id()} "
            f"logger with logging config: {logging_config}"
        )

        # Scope paths under node_id so co-located managers (multi-raylet
        # test clusters on one host) don't overwrite each other's files.
        def _per_node(path: str) -> str:
            d, f = os.path.split(path)
            return os.path.join(d, self._node_id, f)

        self._haproxy = HAProxyApi(
            cfg=HAProxyConfig(
                http_options=http_options,
                is_head=is_head,
                socket_path=_per_node(RAY_SERVE_HAPROXY_SOCKET_PATH),
                server_state_base=os.path.join(
                    RAY_SERVE_HAPROXY_SERVER_STATE_BASE, self._node_id
                ),
                server_state_file=_per_node(RAY_SERVE_HAPROXY_SERVER_STATE_FILE),
            ),
            config_file_path=_per_node(RAY_SERVE_HAPROXY_CONFIG_FILE_LOC),
        )
        self._haproxy_start_task = self.event_loop.create_task(self._haproxy.start())

    async def shutdown(self) -> None:
        """Shutdown the HAProxyManager and clean up the HAProxy process.

        This method should be called before the actor is killed to ensure
        the HAProxy subprocess is properly terminated.
        """
        try:
            logger.info(
                f"Shutting down HAProxyManager on node {self._node_id}.",
                extra={"log_to_stderr": False},
            )

            await self._haproxy.stop()

            logger.info(
                f"Successfully stopped HAProxy process on node {self._node_id}.",
                extra={"log_to_stderr": False},
            )
        except Exception as e:
            raise RuntimeError(f"Error stopping HAProxy during shutdown: {e}")

    async def ready(self) -> str:
        try:
            # Wait for haproxy to start. Internally, this starts the process and
            # waits for it to be running by querying the stats socket.
            await self._haproxy_start_task
        except Exception as e:
            logger.exception("Failed to start HAProxy.")
            raise e from None

        # Return proxy metadata used by the controller.
        # NOTE(zcin): We need to convert the metadata to a json string because
        # of cross-language scenarios. Java can't deserialize a Python tuple.
        return json.dumps(
            [
                ray.get_runtime_context().get_worker_id(),
                get_component_logger_file_path(),
            ]
        )

    async def serving(self, wait_for_applications_running: bool = True) -> None:
        """Wait for the HAProxy process to be ready to serve requests."""
        if not wait_for_applications_running:
            return

        ready_to_serve = False
        while not ready_to_serve:
            if self._is_draining():
                return

            try:
                all_backends = set()
                ready_backends = set()
                stats = await self._haproxy.get_all_stats()
                for backend, servers in stats.items():
                    # The backend name is suffixed with the protocol. We omit
                    # grpc backends for now since they aren't supported yet.
                    if backend.lower().startswith("grpc"):
                        continue
                    all_backends.add(backend)
                    for server in servers.values():
                        if server.is_up:
                            ready_backends.add(backend)
                ready_to_serve = all_backends == ready_backends
            except Exception:
                pass
            if not ready_to_serve:
                await asyncio.sleep(0.2)

    def _is_draining(self) -> bool:
        """Whether is haproxy is in the draining status or not."""
        return self._draining_start_time is not None

    async def update_draining(
        self, draining: bool, _after: Optional[Any] = None
    ) -> None:
        """Update the draining status of the proxy.

        This is called by the proxy state manager
        to drain or un-drain the haproxy.
        """

        if draining and (not self._is_draining()):
            logger.info(
                f"Start to drain the HAProxy on node {self._node_id}.",
                extra={"log_to_stderr": False},
            )
            # Use the reload lock to serialize with other HAProxy reload operations
            async with self._reload_lock:
                await self._haproxy.disable()
            self._draining_start_time = time.time()
        if (not draining) and self._is_draining():
            logger.info(
                f"Stop draining the HAProxy on node {self._node_id}.",
                extra={"log_to_stderr": False},
            )
            # Use the reload lock to serialize with other HAProxy reload operations
            async with self._reload_lock:
                await self._haproxy.enable()
            self._draining_start_time = None

    async def is_drained(self, _after: Optional[Any] = None) -> bool:
        """Check whether the haproxy is drained or not.

        An haproxy is drained if it has no ongoing requests
        AND it has been draining for more than
        `PROXY_MIN_DRAINING_PERIOD_S` seconds.
        """
        if not self._is_draining():
            return False

        haproxy_stats = await self._haproxy.get_haproxy_stats()
        return haproxy_stats.is_system_idle and (
            (time.time() - self._draining_start_time) > PROXY_MIN_DRAINING_PERIOD_S
        )

    async def check_health(self) -> bool:
        # If haproxy is already shutdown, return False.
        if not self._haproxy or not self._haproxy._proc:
            return False
        if self._haproxy._proc.returncode is not None:
            return False

        logger.debug("Received health check.", extra={"log_to_stderr": False})
        # Probe the HTTP listener instead of the admin socket. See
        # HAProxyApi.probe_http_listener for the rationale -- briefly: the
        # admin socket competes with runtime-API load, and a saturated
        # admin socket would otherwise cause the controller to kill the
        # actor and trigger a reload-orphan-port cascade.
        return await self._haproxy.probe_http_listener()

    def pong(self) -> str:
        pass

    async def receive_asgi_messages(self, request_metadata: RequestMetadata) -> bytes:
        raise NotImplementedError("Receive is handled by the ingress replicas.")

    async def receive_grpc_messages(
        self, session_id: str
    ) -> Tuple[bool, Optional[Any], bool]:
        raise NotImplementedError("Receive is handled by the ingress replicas.")

    def _get_http_options(self) -> HTTPOptions:
        return self._http_options

    def _get_logging_config(self) -> Optional[str]:
        """Get the logging configuration (for testing purposes)."""
        log_file_path = None
        for handler in logger.handlers:
            if isinstance(handler, logging.handlers.MemoryHandler):
                log_file_path = handler.target.baseFilename

        return log_file_path

    def _target_to_server(self, target: Target) -> ServerConfig:
        """Convert a target to a server."""
        return ServerConfig(
            # The server name is derived from the replica's actor name, with the
            # format `SERVE_REPLICA::<app>#<deployment>#<replica_id>`, or the
            # proxy's actor name, with the format `SERVE_PROXY_ACTOR-<node_id>`.
            # Special characters in the names are converted to comply with haproxy
            # config's allowed characters, e.g. `#` -> `-`.
            name=self.get_safe_name(target.name),
            # Use localhost if target is on the same node as HAProxy
            host="127.0.0.1" if target.ip == self._node_ip_address else target.ip,
            port=target.port,
        )

    def _create_backend_config(
        self,
        target_group: TargetGroup,
        fallback_target: Optional[Target],
    ) -> BackendConfig:
        """Create a backend configuration from a target group and fallback target."""
        servers = [self._target_to_server(target) for target in target_group.targets]

        fallback_server = None
        if fallback_target is not None:
            fallback_server = self._target_to_server(fallback_target)

        return BackendConfig(
            # The name is lowercased and formatted as <protocol>-<app_name>. Special
            # characters in the name are converted to comply with haproxy config's
            # allowed characters, e.g. `#` -> `-`.
            name=self.get_safe_name(
                f"{target_group.protocol.value.lower()}-{target_group.app_name}"
            ),
            path_prefix=target_group.route_prefix,
            servers=servers,
            app_name=target_group.app_name,
            fallback_server=fallback_server,
        )

    async def _reload_haproxy(self) -> None:
        # To avoid dropping updates from a long poll, we wait until HAProxy
        # is up and running before attempting to generate config and reload.
        # Use lock to serialize reloads and prevent race conditions with SO_REUSEPORT
        async with self._reload_lock:
            await self._haproxy_start_task
            await self._haproxy.reload()

    async def _apply_backend_update(
        self, name_to_backend_configs: Dict[str, BackendConfig]
    ) -> None:
        """Apply a backend config update incrementally if possible, else reload.

        At scale, each controller broadcast triggers a config update. Doing a
        full HAProxy reload for every update blocks the actor's event loop
        long enough to miss the controller's health-check window, which causes
        the controller to kill and recreate the proxy actor — leading to a
        cascading failure where every proxy in the cluster cycles continuously.

        The runtime API path applies pure server adds/removes in milliseconds
        without a reload. For structural changes (new backend, ACL change,
        timeout change, etc.) we still fall back to a full reload.
        """
        async with self._reload_lock:
            await self._haproxy_start_task
            applied_incrementally = await self._haproxy.try_apply_servers_dynamically(
                name_to_backend_configs
            )
            if not applied_incrementally:
                await self._haproxy.reload()

    def _build_name_to_backend_configs(self) -> Dict[str, BackendConfig]:
        """Snapshot the current target groups + fallbacks into backend configs.

        Also computes the per-backend slot pool size via `_compute_slot_split`
        and stamps each BackendConfig with its share. Slot pool sizes are
        part of the rendered config (they show up in `server-template
        srv 1-N`), so a change in the split is treated as a structural
        change by `_compute_backend_diff` -- which is what we want: when
        the split shifts, we need a reload to update the template.
        """
        backend_configs = []
        for target_group in self._target_groups:
            fallback_target = None
            if target_group.protocol == RequestProtocol.HTTP:
                fallback_target = self._http_fallback_target
            elif target_group.protocol == RequestProtocol.GRPC:
                fallback_target = self._grpc_fallback_target

            backend_config = self._create_backend_config(target_group, fallback_target)
            backend_configs.append(backend_config)

        # Stamp each backend with its slot-pool size from the split.
        replica_counts = {bc.name: len(bc.servers) for bc in backend_configs}
        split = _compute_slot_split(replica_counts)
        for bc in backend_configs:
            bc.slot_pool_size = split.get(bc.name, 0)

        return {bc.name: bc for bc in backend_configs}

    def _update_haproxy_backends(self) -> None:
        # Schedule a coalesced flush. If a flush is already pending (sleeping
        # or applying), set `_coalesce_pending` so the running task picks up
        # our broadcast on the next loop iteration. With coalescing disabled
        # (window=0), apply synchronously to preserve legacy behaviour.
        if self._coalesce_window_s <= 0:
            name_to_backend_configs = self._build_name_to_backend_configs()
            logger.info(
                f"Got updated backend configs: {list(name_to_backend_configs.values())}.",
                extra={"log_to_stderr": True},
            )
            self.event_loop.create_task(
                self._apply_backend_update(name_to_backend_configs)
            )
            return

        self._coalesce_pending = True
        if self._coalesce_task is None or self._coalesce_task.done():
            self._coalesce_task = self.event_loop.create_task(
                self._coalesce_and_apply()
            )

    async def _coalesce_and_apply(self) -> None:
        """Drain pending broadcasts, applying each batch after a short sleep.

        Broadcasts arriving during the sleep are absorbed because the
        long-poll callbacks overwrite `self._target_groups` / fallback fields
        in place — by the time we wake up, the snapshot already reflects them.
        Broadcasts arriving during the apply are handled by the outer loop:
        they re-set `_coalesce_pending`, and we iterate again.
        """
        try:
            while self._coalesce_pending:
                # Clear before sleep so any broadcast during sleep+apply
                # re-arms the flag and triggers another iteration.
                self._coalesce_pending = False
                await asyncio.sleep(self._coalesce_window_s)
                name_to_backend_configs = self._build_name_to_backend_configs()
                logger.info(
                    f"Got updated backend configs: {list(name_to_backend_configs.values())}.",
                    extra={"log_to_stderr": True},
                )
                await self._apply_backend_update(name_to_backend_configs)
        except Exception as e:
            # Don't let an apply failure kill the coalescer. The next broadcast
            # will schedule a fresh flush task; the underlying error is already
            # logged by `_apply_backend_update` / `try_apply_servers_dynamically`.
            logger.error(f"Coalesced backend apply failed: {e}")

    def update_target_groups(self, target_groups: List[TargetGroup]) -> None:
        self._target_groups = target_groups
        self._update_haproxy_backends()

    def update_fallback_targets(
        self,
        fallback_targets: Dict[RequestProtocol, Target],
    ) -> None:
        # Reset so that fallbacks are repopulated from LongPoll.
        self._http_fallback_target = None
        self._grpc_fallback_target = None

        for protocol, target in fallback_targets.items():
            if protocol == RequestProtocol.HTTP:
                self._http_fallback_target = target
            elif protocol == RequestProtocol.GRPC:
                self._grpc_fallback_target = target

        self._update_haproxy_backends()

    def get_target_groups(self) -> List[TargetGroup]:
        """Get current target groups."""
        return self._target_groups

    @staticmethod
    def get_safe_name(name: str) -> str:
        """Get a safe label name for the haproxy config."""
        name = name.replace("#", "-").replace("/", ".")
        # replace all remaining non-alphanumeric and non-{".", "_", "-"} with "_"
        return re.sub(r"[^A-Za-z0-9._-]+", "_", name)

    def _dump_ingress_replicas_for_testing(self, route: str) -> Set[ReplicaID]:
        """Return the set of replica IDs for targets matching the given route.

        Args:
            route: The route prefix to match against target groups.

        Returns:
            Set of ReplicaID objects for targets in the matching target group.
        """
        replica_ids = set()

        if self._target_groups is None:
            return replica_ids

        for target_group in self._target_groups:
            if target_group.route_prefix == route:
                for target in target_group.targets:
                    # Target names are in the format "SERVE_REPLICA::<app>#<deployment>#<replica_id>"
                    if ReplicaID.is_full_id_str(target.name):
                        replica_id = ReplicaID.from_full_id_str(target.name)
                        replica_ids.add(replica_id)

        return replica_ids

    def _dump_ingress_cache_for_testing(self, route: str) -> Set[ReplicaID]:
        """Return replica IDs that are cached/ready for the given route (for testing).

        For HAProxy, all registered replicas are immediately ready for routing
        (no warm-up cache like the internal router), so this returns the same
        set as _dump_ingress_replicas_for_testing.

        Args:
            route: The route prefix to match against target groups.

        Returns:
            Set of ReplicaID objects for targets in the matching target group.
        """
        return self._dump_ingress_replicas_for_testing(route)
