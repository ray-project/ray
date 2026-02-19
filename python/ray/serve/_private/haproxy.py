import asyncio
import csv
import io
import json
import logging
import os
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from jinja2 import Environment

import ray
from ray._common.utils import get_or_create_event_loop
from ray.serve._private.common import (
    NodeId,
    ReplicaID,
    RequestMetadata,
)
from ray.serve._private.constants import (
    DRAINING_MESSAGE,
    HEALTHY_MESSAGE,
    NO_REPLICAS_MESSAGE,
    NO_ROUTES_MESSAGE,
    PROXY_MIN_DRAINING_PERIOD_S,
    RAY_SERVE_ENABLE_HAPROXY_OPTIMIZED_CONFIG,
    RAY_SERVE_HAPROXY_CONFIG_FILE_LOC,
    RAY_SERVE_HAPROXY_HARD_STOP_AFTER_S,
    RAY_SERVE_HAPROXY_HEALTH_CHECK_DOWNINTER,
    RAY_SERVE_HAPROXY_HEALTH_CHECK_FALL,
    RAY_SERVE_HAPROXY_HEALTH_CHECK_FASTINTER,
    RAY_SERVE_HAPROXY_HEALTH_CHECK_INTER,
    RAY_SERVE_HAPROXY_HEALTH_CHECK_RISE,
    RAY_SERVE_HAPROXY_MAXCONN,
    RAY_SERVE_HAPROXY_METRICS_PORT,
    RAY_SERVE_HAPROXY_NBTHREAD,
    RAY_SERVE_HAPROXY_SERVER_STATE_BASE,
    RAY_SERVE_HAPROXY_SERVER_STATE_FILE,
    RAY_SERVE_HAPROXY_SOCKET_PATH,
    RAY_SERVE_HAPROXY_SYSLOG_PORT,
    RAY_SERVE_HAPROXY_TIMEOUT_CLIENT_S,
    RAY_SERVE_HAPROXY_TIMEOUT_CONNECT_S,
    RAY_SERVE_HAPROXY_TIMEOUT_SERVER_S,
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
from ray.serve.config import HTTPOptions, gRPCOptions
from ray.serve.schema import (
    LoggingConfig,
    Target,
    TargetGroup,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


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

    # The app name for this backend.
    app_name: str = field(default_factory=str)

    # The fallback target for this backend.
    fallback_server: ServerConfig = field(default_factory=ServerConfig)

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
    stats_port: int = 8404
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
    custom_global: Dict[str, str] = field(default_factory=dict)
    custom_defaults: Dict[str, str] = field(default_factory=dict)
    inject_process_id_header: bool = False
    reload_id: Optional[str] = None  # Unique ID for each reload
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
        elif not self.has_received_servers:
            router_ready_for_traffic = False
            router_message = NO_REPLICAS_MESSAGE
        else:
            router_ready_for_traffic = True
            router_message = ""

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

    def _is_running(self) -> bool:
        """Check if the HAProxy process is still running."""
        return self._proc is not None and self._proc.returncode is None

    async def _start_and_wait_for_haproxy(
        self, *extra_args: str, timeout_s: int = 5
    ) -> asyncio.subprocess.Process:
        # Build command args
        args = ["haproxy", "-db", "-f", self.config_file_path]

        if not self.cfg.enable_so_reuseport:
            args.append("-dR")

        # Add any extra args (like -sf for graceful reload)
        args.extend(extra_args)

        logger.debug(f"Starting HAProxy with args: {args}")

        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            await self._wait_for_hap_availability(proc)
        except Exception:
            # If startup fails, ensure the process is killed to avoid orphaned processes
            if proc.returncode is None:
                proc.kill()
                await proc.wait()
            raise

        return proc

    async def _save_server_state(self) -> None:
        """Save the server state to the file."""
        server_state = await self._send_socket_command("show servers state")
        with open(self.cfg.server_state_file, "w") as f:
            f.write(server_state)

    async def _graceful_reload(self) -> None:
        """Perform a graceful reload of HAProxy by starting a new process with -sf."""
        try:
            old_proc = self._proc
            await self._wait_for_hap_availability(old_proc)

            # Save server state if optimization is enabled
            if self.cfg.enable_hap_optimization:
                await self._save_server_state()

            # Start new HAProxy process with -sf flag to gracefully take over from old process
            # Use -x socket transfer for seamless reloads if optimization is enabled
            reload_args = ["-sf", str(old_proc.pid)]
            if self.cfg.enable_hap_optimization:
                reload_args.extend(["-x", self.cfg.socket_path])

            self._proc = await self._start_and_wait_for_haproxy(*reload_args)

            # Track old process so we can ensure it's cleaned up during shutdown
            if old_proc is not None:
                self._old_procs.append(old_proc)

            logger.info(
                "Successfully performed graceful HAProxy reload with process restart."
            )
        except Exception as e:
            logger.error(f"HAProxy graceful reload failed: {e}")
            raise

    async def _wait_for_hap_availability(
        self, proc: asyncio.subprocess.Process, timeout_s: int = 5
    ) -> None:
        start_time = time.time()

        # TODO: update this to use health checks
        while time.time() - start_time < timeout_s:
            if proc.returncode is not None:
                stdout = await proc.stdout.read() if proc.stdout else b""
                stderr = await proc.stderr.read() if proc.stderr else b""
                output = (
                    stderr.decode("utf-8", errors="ignore").strip()
                    or stdout.decode("utf-8", errors="ignore").strip()
                )

                raise RuntimeError(
                    f"HAProxy crashed during startup: {output or f'exit code {proc.returncode}'}"
                )

            if await self.is_running():
                return

            await asyncio.sleep(0.5)

        raise RuntimeError(
            f"HAProxy did not enter running state within {timeout_s} seconds."
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

    # TODO: use socket library instead of subprocess
    async def _send_socket_command(self, command: str) -> str:
        """Send a command to the HAProxy stats socket via subprocess."""
        try:
            # Check if a socket file exists
            if not os.path.exists(self.cfg.socket_path):
                raise RuntimeError(
                    f"HAProxy socket file does not exist: {self.cfg.socket_path}."
                )

            proc = await asyncio.create_subprocess_exec(
                "socat",
                "-",
                f"UNIX-CONNECT:{self.cfg.socket_path}",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(f"{command}\n".encode("utf-8")), timeout=5.0
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                raise RuntimeError(
                    f"Timeout while sending command '{command}' to HAProxy socket"
                )

            if proc.returncode != 0:
                err = stderr.decode("utf-8", errors="ignore").strip()
                raise RuntimeError(
                    f"Command '{command}' failed with code {proc.returncode}: {err}"
                )

            result = stdout.decode("utf-8", errors="ignore")
            logger.debug(f"Socket command '{command}' returned {len(result)} chars.")
            return result
        except Exception as e:
            raise RuntimeError(f"Failed to send socket command '{command}': {e}")

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

        # Lock to serialize HAProxy reloads and prevent concurrent reload operations
        # which can cause race conditions with SO_REUSEPORT
        self._reload_lock = asyncio.Lock()

        self.long_poll_client = long_poll_client or LongPollClient(
            ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE),
            {
                LongPollNamespace.GLOBAL_LOGGING_CONFIG: self._update_logging_config,
                LongPollNamespace.TARGET_GROUPS: self.update_target_groups,
            },
            call_in_event_loop=self.event_loop,
        )

        startup_msg = f"HAProxy starting on node {self._node_id} (HTTP port: {self._http_options.port})."
        logger.info(startup_msg)
        logger.debug(
            f"Configure HAProxyManager actor {ray.get_runtime_context().get_actor_id()} "
            f"logger with logging config: {logging_config}"
        )

        self._haproxy = HAProxyApi(cfg=HAProxyConfig(http_options=http_options))
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

        logger.debug("Received health check.", extra={"log_to_stderr": False})
        return await self._haproxy.is_running()

    def pong(self) -> str:
        pass

    async def receive_asgi_messages(self, request_metadata: RequestMetadata) -> bytes:
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

    def _target_group_to_backend(self, target_group: TargetGroup) -> BackendConfig:
        """Convert a target group to a backend name."""
        servers = [
            self._target_to_server(target)
            for target in target_group.targets
        ]
        # The name is lowercased and formatted as <protocol>-<app_name>. Special
        # characters in the name are converted to comply with haproxy config's
        # allowed characters, e.g. `#` -> `-`.
        return BackendConfig(
            name=self.get_safe_name(
                f"{target_group.protocol.value.lower()}-{target_group.app_name}"
            ),
            path_prefix=target_group.route_prefix,
            servers=servers,
            app_name=target_group.app_name,
            fallback_server=self._target_to_server(target_group.fallback_target),
        )

    async def _reload_haproxy(self) -> None:
        # To avoid dropping updates from a long poll, we wait until HAProxy
        # is up and running before attempting to generate config and reload.
        # Use lock to serialize reloads and prevent race conditions with SO_REUSEPORT
        async with self._reload_lock:
            await self._haproxy_start_task
            await self._haproxy.reload()

    def update_target_groups(self, target_groups: List[TargetGroup]) -> None:
        self._target_groups = target_groups

        backend_configs = [
            self._target_group_to_backend(target_group)
            for target_group in target_groups
        ]

        logger.info(
            f"Got updated backend configs: {backend_configs}.",
            extra={"log_to_stderr": True},
        )

        name_to_backend_configs = {
            backend_config.name: backend_config for backend_config in backend_configs
        }

        self._haproxy.set_backend_configs(name_to_backend_configs)
        self.event_loop.create_task(self._reload_haproxy())

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
