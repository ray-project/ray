import asyncio
import logging
import os
import subprocess
import sys
import tempfile
import threading
import time
from typing import Optional
from unittest import mock

import pytest
import pytest_asyncio
import requests
import uvicorn
from fastapi import FastAPI, Request, Response

from ray._common.test_utils import async_wait_for_condition, wait_for_condition
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_HA_PROXY,
)
from ray.serve._private.haproxy import (
    BackendConfig,
    HAProxyApi,
    HAProxyConfig,
    ServerConfig,
)
from ray.serve.config import HTTPOptions

logger = logging.getLogger(__name__)

# Skip all tests in this module if the HAProxy feature flag is not enabled
pytestmark = pytest.mark.skipif(
    not RAY_SERVE_ENABLE_HA_PROXY,
    reason="RAY_SERVE_ENABLE_HA_PROXY not set.",
)

EXCLUDED_ACL_NAMES = ("healthcheck", "routes")


def check_haproxy_ready(stats_port: int, timeout: int = 2) -> bool:
    """Check if HAProxy is ready by verifying the stats endpoint is accessible."""
    try:
        response = requests.get(f"http://127.0.0.1:{stats_port}/stats", timeout=timeout)
        return response.status_code == 200
    except Exception:
        return False


def create_test_backend_server(port: int):
    """Create a test backend server with slow and fast endpoints using uvicorn."""
    app = FastAPI()

    @app.get("/-/healthz")
    async def health_endpoint():
        return {"status": "OK"}

    @app.get("/slow")
    async def slow_endpoint():
        await asyncio.sleep(3)  # 3-second delay
        return "Slow response completed"

    @app.get("/fast")
    async def fast_endpoint(req: Request, res: Response):
        res.headers["x-haproxy-reload-id"] = req.headers.get("x-haproxy-reload-id", "")

        return "Fast response"

    # Configure uvicorn server with 60s keep-alive timeout
    config = uvicorn.Config(
        app=app,
        host="127.0.0.1",
        port=port,
        log_level="error",  # Reduce log noise
        access_log=False,
        timeout_keep_alive=60,  # 60 seconds keep-alive timeout
    )
    server = uvicorn.Server(config)

    # Run server in a separate thread
    def run_server():
        asyncio.run(server.serve())

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()

    # Wait for the server to start
    def wait_for_server():
        r = requests.get(f"http://127.0.0.1:{port}/-/healthz")
        assert r.status_code == 200
        return True

    wait_for_condition(wait_for_server)
    return server, thread


def process_exists(pid: int) -> bool:
    """Check if a process with the given PID exists."""
    try:
        # Send signal 0 to check if process exists without actually sending a signal
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def make_test_request(
    url: str,
    track_results: list = None,
    signal_started: threading.Event = None,
    timeout: int = 10,
):
    """Unified function to make test requests with optional result tracking."""
    try:
        if signal_started:
            signal_started.set()  # Signal that request has started

        start_time = time.time()
        response = requests.get(url, timeout=timeout)
        end_time = time.time()

        if track_results is not None:
            track_results.append(
                {
                    "status": response.status_code,
                    "duration": end_time - start_time,
                    "content": response.content,
                }
            )
    except Exception as ex:
        if track_results is not None:
            track_results.append({"error": str(ex)})


@pytest.fixture(autouse=True)
def clean_up_haproxy_processes():
    """Clean up haproxy processes before and after each test."""

    subprocess.run(
        ["pkill", "-x", "haproxy"], capture_output=True, text=True, check=False
    )
    yield
    # After test: verify no haproxy processes are running
    result = subprocess.run(
        ["pgrep", "-x", "haproxy"], capture_output=True, text=True, check=False
    )
    assert (
        result.returncode != 0 or not result.stdout.strip()
    ), f"HAProxy processes still running after test: {result.stdout.strip()}"


@pytest_asyncio.fixture
async def haproxy_api_cleanup():
    registered_apis = []

    def register(api: Optional[HAProxyApi]) -> None:
        if api is not None:
            registered_apis.append(api)

    yield register

    for api in registered_apis:
        proc = getattr(api, "_proc", None)
        if proc and proc.returncode is None:
            try:
                await api.stop()
            except Exception as exc:  # pragma: no cover - best effort cleanup
                logger.warning(f"Failed to stop HAProxy API cleanly: {exc}")
                try:
                    proc.kill()
                    await proc.wait()
                except Exception as kill_exc:
                    logger.error(
                        f"Failed to kill HAProxy process {proc.pid}: {kill_exc}"
                    )
        elif proc and proc.returncode is not None:
            continue


def test_generate_config_file_internal(haproxy_api_cleanup):
    """Test that initialize writes the correct config_stub file content using the actual template."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")
        socket_path = os.path.join(temp_dir, "admin.sock")

        config_stub = HAProxyConfig(
            socket_path=socket_path,
            maxconn=1000,
            nbthread=2,
            timeout_connect_s=5,
            timeout_client_s=30,
            timeout_server_s=30,
            timeout_http_request_s=10,
            timeout_queue_s=1,
            stats_port=8080,
            stats_uri="/mystats",
            health_check_fall=3,
            health_check_rise=2,
            health_check_inter="2s",
            health_check_path="/health",
            http_options=HTTPOptions(
                host="0.0.0.0",
                port=8000,
                keep_alive_timeout_s=55,
            ),
            has_received_routes=True,
            has_received_servers=True,
            enable_hap_optimization=True,
        )
        backend_config_stub = {
            "api_backend": BackendConfig(
                name="api_backend",
                path_prefix="/api",
                app_name="api_backend",
                timeout_http_keep_alive_s=60,
                timeout_tunnel_s=60,
                health_check_path="/api/health",
                health_check_fall=2,
                health_check_rise=3,
                health_check_inter="5s",
                servers=[
                    ServerConfig(name="api_server1", host="127.0.0.1", port=8001),
                    ServerConfig(name="api_server2", host="127.0.0.1", port=8002),
                ],
            ),
            "web_backend": BackendConfig(
                name="web_backend",
                path_prefix="/web",
                app_name="web_backend",
                timeout_connect_s=3,
                timeout_server_s=25,
                timeout_http_keep_alive_s=45,
                timeout_tunnel_s=45,
                servers=[
                    ServerConfig(name="web_server1", host="127.0.0.1", port=8003),
                ]
                # No health check overrides - should use global defaults
            ),
        }

        with mock.patch(
            "ray.serve._private.constants.RAY_SERVE_HAPROXY_CONFIG_FILE_LOC",
            config_file_path,
        ):

            api = HAProxyApi(
                cfg=config_stub,
                backend_configs=backend_config_stub,
                config_file_path=config_file_path,
            )

            try:
                api._generate_config_file_internal()

                # Read and verify the generated file
                with open(config_file_path, "r") as f:
                    actual_content = f.read()

                routes = '{\\"/api\\":\\"api_backend\\",\\"/web\\":\\"web_backend\\"}'
                # Expected configuration stub (matching the actual template output)
                expected_config = f"""
global
    # Log to the standard system log socket with debug level.
    log /dev/log local0 debug
    log 127.0.0.1:514 local0 debug
    stats socket {socket_path} mode 666 level admin expose-fd listeners
    stats timeout 30s
    maxconn 1000
    nbthread 2
    server-state-base /tmp/haproxy-serve
    server-state-file /tmp/haproxy-serve/server-state
    hard-stop-after 120s
defaults
    mode http
    option log-health-checks
    timeout connect 5s
    timeout client 30s
    timeout server 30s
    timeout http-request 10s
    timeout http-keep-alive 55s
    timeout queue 1s
    log global
    option httplog
    option abortonclose
    option idle-close-on-response
    # Normalize 502 and 504 errors to 500 per Serve's default behavior
    errorfile 502 {temp_dir}/500.http
    errorfile 504 {temp_dir}/500.http
    load-server-state-from-file global
frontend prometheus
    bind :9101
    mode http
    http-request use-service prometheus-exporter if {{ path /metrics }}
    no log
frontend http_frontend
    bind *:8000
    # Health check endpoint
    acl healthcheck path -i /-/healthz
    # Suppress logging for health checks
    http-request set-log-level silent if healthcheck
    # 200 if any backend has at least one server UP
    acl backend_api_backend_server_up nbsrv(api_backend) ge 1
    acl backend_web_backend_server_up nbsrv(web_backend) ge 1
    # Any backend with a server UP passes the health check (OR logic)
    http-request return status 200 content-type text/plain string "success" if healthcheck backend_api_backend_server_up
    http-request return status 200 content-type text/plain string "success" if healthcheck backend_web_backend_server_up
    http-request return status 503 content-type text/plain string "Service Unavailable" if healthcheck
    # Routes endpoint
    acl routes path -i /-/routes
    http-request return status 200 content-type application/json string "{routes}" if routes
    # Static routing based on path prefixes in decreasing length then alphabetical order
    acl is_api_backend path_beg /api/
    acl is_api_backend path /api
    use_backend api_backend if is_api_backend
    acl is_web_backend path_beg /web/
    acl is_web_backend path /web
    use_backend web_backend if is_web_backend
    default_backend default_backend
backend default_backend
    http-request return status 404 content-type text/plain lf-string "Path \'%[path]\' not found. Ping http://.../-/routes for available routes."
backend api_backend
    log global
    balance leastconn
    # Enable HTTP connection reuse for better performance
    http-reuse always
    # Set backend-specific timeouts, overriding defaults if specified
    # Set timeouts to support keep-alive connections
    timeout http-keep-alive 60s
    timeout tunnel 60s
    # Health check configuration - use backend-specific or global defaults
    # HTTP health check with custom path
    option httpchk GET /api/health
    http-check expect status 200
    default-server fastinter 250ms downinter 250ms fall 2 rise 3 inter 5s check
    # Servers in this backend
    server api_server1 127.0.0.1:8001 check
    server api_server2 127.0.0.1:8002 check
backend web_backend
    log global
    balance leastconn
    # Enable HTTP connection reuse for better performance
    http-reuse always
    # Set backend-specific timeouts, overriding defaults if specified
    timeout connect 3s
    timeout server 25s
    # Set timeouts to support keep-alive connections
    timeout http-keep-alive 45s
    timeout tunnel 45s
    # Health check configuration - use backend-specific or global defaults
    # HTTP health check with custom path
    option httpchk GET /-/healthz
    http-check expect status 200
    default-server fastinter 250ms downinter 250ms fall 3 rise 2 inter 2s check
    # Servers in this backend
    server web_server1 127.0.0.1:8003 check
listen stats
  bind *:8080
  stats enable
  stats uri /mystats
  stats refresh 1s
"""

                # Compare the entire configuration
                assert actual_content.strip() == expected_config.strip()
            finally:
                # Clean up any temporary files created by initialize()
                temp_files = ["haproxy.cfg", "routes.map"]
                for temp_file in temp_files:
                    try:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                    except (FileNotFoundError, OSError):
                        pass  # File already removed or doesn't exist


def test_generate_backends_in_order(haproxy_api_cleanup):
    """Test that the backends are generated in the correct order."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")
        backend_config_stub = {
            "foo": BackendConfig(
                name="foo",
                path_prefix="/foo",
                app_name="foo",
            ),
            "foobar": BackendConfig(
                name="foobar",
                path_prefix="/foo/bar",
                app_name="foobar",
            ),
            "bar": BackendConfig(
                name="bar",
                path_prefix="/bar",
                app_name="bar",
            ),
            "default": BackendConfig(
                name="default",
                path_prefix="/",
                app_name="default",
            ),
        }

        with mock.patch(
            "ray.serve._private.constants.RAY_SERVE_HAPROXY_CONFIG_FILE_LOC",
            config_file_path,
        ):
            api = HAProxyApi(
                cfg=HAProxyConfig(),
                config_file_path=config_file_path,
                backend_configs=backend_config_stub,
            )

            api._generate_config_file_internal()

        # Read and verify the generated file
        lines = []
        with open(config_file_path, "r") as f:
            lines = f.readlines()

        acl_names = []
        path_begs = []
        paths = []
        backend_lines = []
        for line in lines:
            line = line.strip()
            if line.startswith("acl"):
                acl_name = line.split(" ")[1]
                if acl_name in EXCLUDED_ACL_NAMES:
                    continue

                acl_names.append(acl_name)

                # strip prefix/suffix added for acl checks
                backend_name = (
                    acl_name.lstrip("is_")
                    .replace("backend_", "")
                    .replace("_server_up", "")
                )
                assert backend_name in backend_config_stub

                condition = line.split(" ")[-2]
                if condition == "path_beg":
                    path_prefix = line.split(" ")[-1].rstrip("/") or "/"
                    assert backend_config_stub[backend_name].path_prefix == path_prefix
                    path_begs.append(path_prefix)
                elif condition == "path":
                    path_prefix = line.split(" ")[-1]
                    assert backend_config_stub[backend_name].path_prefix == path_prefix
                    paths.append(path_prefix)
                else:
                    # gt condition is used for health check, no need to check.
                    continue
            if line.startswith("use_backend"):
                acl_name = line.split(" ")[-1]
                assert acl_name in acl_names
                backend_lines.append(acl_name)

        expected_order = ["is_foobar", "is_bar", "is_foo", "is_default"]
        assert backend_lines == expected_order


@pytest.mark.asyncio
async def test_graceful_reload(haproxy_api_cleanup):
    """Test that graceful reload preserves long-running connections."""

    with tempfile.TemporaryDirectory() as temp_dir:
        # Setup ports
        haproxy_port = 8000
        backend_port = 8404
        stats_port = 8405

        # Create and start a backend server
        backend_server, backend_thread = create_test_backend_server(backend_port)

        # Configure HAProxy

        config = HAProxyConfig(
            http_options=HTTPOptions(
                host="127.0.0.1",
                port=haproxy_port,
                keep_alive_timeout_s=58,
            ),
            stats_port=stats_port,
            inject_process_id_header=True,  # Enable for testing graceful reload
            reload_id=f"initial-{int(time.time() * 1000)}",  # Set initial reload ID
            socket_path=os.path.join(temp_dir, "admin.sock"),
        )

        backend_config = BackendConfig(
            name="test_backend",
            path_prefix="/",
            app_name="test_app",
            servers=[ServerConfig(name="backend", host="127.0.0.1", port=backend_port)],
            timeout_http_keep_alive_s=58,
        )

        config_file_path = os.path.join(temp_dir, "haproxy.cfg")

        api = HAProxyApi(
            cfg=config,
            backend_configs={"test_backend": backend_config},
            config_file_path=config_file_path,
        )

        haproxy_api_cleanup(api)

        try:
            await api.start()

            # Wait for HAProxy to be ready (check stat endpoint)
            def check_stats_ready():
                try:
                    response = requests.get(
                        f"http://127.0.0.1:{config.stats_port}/stats", timeout=2
                    )
                    return response.status_code == 200
                except Exception:
                    return False

            wait_for_condition(check_stats_ready, timeout=10, retry_interval_ms=100)

            # Track slow request results
            slow_results = []
            request_started = threading.Event()

            slow_thread = threading.Thread(
                target=make_test_request,
                args=[f"http://127.0.0.1:{haproxy_port}/slow"],
                kwargs={
                    "track_results": slow_results,
                    "signal_started": request_started,
                },
            )

            slow_thread.start()
            wait_for_condition(
                lambda: request_started.is_set(), timeout=5, retry_interval_ms=10
            )

            assert api._proc is not None
            original_pid = api._proc.pid

            await api._graceful_reload()

            assert api._proc is not None
            new_pid = api._proc.pid

            def check_for_new_reload_id():
                fast_response = requests.get(
                    f"http://127.0.0.1:{haproxy_port}/fast", timeout=5
                )

                # Reload ID should always match what exists in the config.
                return (
                    fast_response.headers.get("x-haproxy-reload-id")
                    == api.cfg.reload_id
                    and fast_response.status_code == 200
                )

            wait_for_condition(
                check_for_new_reload_id, timeout=5, retry_interval_ms=100
            )

            slow_thread.join(timeout=10)

            assert (
                original_pid != new_pid
            ), "Process should have been reloaded with new PID"

            wait_for_condition(
                lambda: not process_exists(original_pid),
                timeout=15,
                retry_interval_ms=100,
            )

            assert len(slow_results) == 1, "Slow request should have completed"

            result = slow_results[0]
            assert "error" not in result, f"Slow request failed: {result.get('error')}"
            assert result["status"] == 200, "Slow request should have succeeded"
            assert result["duration"] >= 3.0, "Slow request should have taken full time"
            assert (
                b"Slow response completed" in result["content"]
            ), "Slow request should have completed"

        finally:
            # Backend server cleanup
            try:
                backend_server.should_exit = True
                backend_thread.join(timeout=5)  # Wait for thread to finish
            except Exception as e:
                print(f"Error occurred while shutting down server stub. Error: {e}")


@pytest.mark.asyncio
async def test_start(haproxy_api_cleanup):
    """Test HAProxy start functionality."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")
        socket_path = os.path.join(temp_dir, "admin.sock")

        # Create HAProxy config
        config = HAProxyConfig(
            http_options=HTTPOptions(
                host="127.0.0.1",
                port=8000,
                keep_alive_timeout_s=58,
            ),
            stats_port=8404,
            pass_health_checks=True,
            socket_path=socket_path,
            has_received_routes=True,
            has_received_servers=True,
        )

        # Add a backend so routes are populated
        backend = BackendConfig(
            name="test_backend",
            path_prefix="/",
            app_name="test_app",
            servers=[ServerConfig(name="server", host="127.0.0.1", port=9999)],
        )

        api = HAProxyApi(
            cfg=config,
            backend_configs={"test_backend": backend},
            config_file_path=config_file_path,
        )

        haproxy_api_cleanup(api)

        await api.start()

        assert api._proc is not None, "HAProxy process should exist"
        assert api._is_running(), "HAProxy should be running"

        # Verify config file contains expected content
        with open(config_file_path, "r") as f:
            config_content = f.read()
            assert "frontend http_frontend" in config_content
            assert f"bind 127.0.0.1:{config.frontend_port}" in config_content
            assert "acl healthcheck path -i /-/healthz" in config_content

        health_response = requests.get(
            f"http://127.0.0.1:{config.frontend_port}/-/healthz", timeout=5
        )
        assert (
            health_response.status_code == 503
        ), "Health check with no servers up should return 503"

        await api.stop()
        assert api._proc is None
        assert not api._is_running()


@pytest.mark.asyncio
async def test_stop(haproxy_api_cleanup):
    """Test HAProxy stop functionality."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")

        config = HAProxyConfig(
            http_options=HTTPOptions(
                host="127.0.0.1",
                port=8000,
            ),
            stats_port=8404,
            socket_path=os.path.join(temp_dir, "admin.sock"),
        )

        api = HAProxyApi(cfg=config, config_file_path=config_file_path)

        haproxy_api_cleanup(api)

        # Start HAProxy
        await api.start()

        haproxy_api_cleanup(api)

        await api.stop()

        # Verify it's stopped
        assert not api._is_running(), "HAProxy should be stopped after shutdown"


@pytest.mark.asyncio
async def test_stop_kills_haproxy_process(haproxy_api_cleanup):
    """Test that stop() properly kills the HAProxy subprocess."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")

        config = HAProxyConfig(
            http_options=HTTPOptions(
                host="127.0.0.1",
                port=8000,
            ),
            stats_port=8404,
            socket_path=os.path.join(temp_dir, "admin.sock"),
        )

        api = HAProxyApi(cfg=config, config_file_path=config_file_path)
        haproxy_api_cleanup(api)

        # Start HAProxy
        await api.start()
        assert api._proc is not None, "HAProxy process should exist after start"

        haproxy_pid = api._proc.pid
        assert process_exists(haproxy_pid), "HAProxy process should be running"

        # Stop HAProxy
        await api.stop()

        # Verify the process is killed
        assert api._proc is None, "HAProxy proc should be None after stop"

        # Wait a bit for process cleanup
        def haproxy_process_killed():
            return not process_exists(haproxy_pid)

        wait_for_condition(
            haproxy_process_killed,
            timeout=1,
            retry_interval_ms=100,
        )


@pytest.mark.asyncio
async def test_get_stats_integration(haproxy_api_cleanup):
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")
        socket_path = os.path.join(temp_dir, "admin.sock")

        # Create test backend servers
        backend_port1 = 9900
        backend_port2 = 9901
        backend_server1, backend_thread1 = create_test_backend_server(backend_port1)
        backend_server2, backend_thread2 = create_test_backend_server(backend_port2)

        # Configure HAProxy with multiple backends
        config = HAProxyConfig(
            http_options=HTTPOptions(
                port=8000,
                keep_alive_timeout_s=58,
            ),
            socket_path=socket_path,
            stats_port=8404,
        )

        backend_configs = {
            "test_backend1": BackendConfig(
                name="test_backend1",
                path_prefix="/api",
                app_name="test_app1",
                servers=[
                    ServerConfig(name="server1", host="127.0.0.1", port=backend_port1)
                ],
                timeout_http_keep_alive_s=58,
            ),
            "test_backend2": BackendConfig(
                name="test_backend2",
                path_prefix="/web",
                app_name="test_app2",
                servers=[
                    ServerConfig(name="server2", host="127.0.0.1", port=backend_port2)
                ],
                timeout_http_keep_alive_s=58,
            ),
        }

        api = HAProxyApi(
            cfg=config,
            backend_configs=backend_configs,
            config_file_path=config_file_path,
        )

        haproxy_api_cleanup(api)

        try:
            # Start HAProxy
            await api.start()

            # Wait for HAProxy to be ready
            wait_for_condition(
                lambda: check_haproxy_ready(config.stats_port),
                timeout=10,
                retry_interval_ms=500,
            )

            # Make some API calls to generate sessions and traffic
            request_threads = []

            for i in range(3):
                thread = threading.Thread(
                    target=make_test_request,
                    args=[f"http://127.0.0.1:{config.frontend_port}/api/slow"],
                )
                thread.start()
                request_threads.append(thread)

            for i in range(3):
                thread = threading.Thread(
                    target=make_test_request,
                    args=[f"http://127.0.0.1:{config.frontend_port}/web/slow"],
                )
                thread.start()
                request_threads.append(thread)

            # Get actual stats
            async def two_servers_up():
                stats = await api.get_haproxy_stats()
                return stats.active_servers == 2

            await async_wait_for_condition(
                two_servers_up, timeout=10, retry_interval_ms=200
            )

            async def wait_for_running():
                return await api.is_running()

            await async_wait_for_condition(
                wait_for_running, timeout=10, retry_interval_ms=200
            )

            all_stats = await api.get_all_stats()
            haproxy_stats = await api.get_haproxy_stats()

            # Assert against the expected stub with exact values
            assert (
                len(all_stats) == 2
            ), f"Should have exactly 2 backends, got {len(all_stats)}"
            assert (
                haproxy_stats.total_backends == 2
            ), f"Should have exactly 2 backends, got {haproxy_stats.total_backends}"
            assert (
                haproxy_stats.total_servers == 2
            ), f"Should have exactly 2 servers, got {haproxy_stats.total_servers}"
            assert (
                haproxy_stats.active_servers == 2
            ), f"Should have exactly 2 active servers, got {haproxy_stats.active_servers}"

            # Wait for request threads to complete
            for thread in request_threads:
                thread.join(timeout=1)
        finally:
            # Stop HAProxy
            await api.stop()

            # Cleanup backend servers
            try:
                backend_server1.should_exit = True
                backend_server2.should_exit = True
                backend_thread1.join(timeout=5)  # Wait for the thread to finish
                backend_thread2.join(timeout=5)  # Wait for the thread to finish
            except Exception as e:
                print(f"Error cleaning up backend servers: {e}")


@pytest.mark.asyncio
async def test_update_and_reload(haproxy_api_cleanup):
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")
        socket_path = os.path.join(temp_dir, "admin.sock")

        backend = BackendConfig(
            name="backend",
            path_prefix="/",
            app_name="backend_app",
            servers=[ServerConfig(name="server", host="127.0.0.1", port=9999)],
        )

        config = HAProxyConfig(
            http_options=HTTPOptions(
                host="127.0.0.1",
                port=8000,
            ),
            stats_port=8404,
            socket_path=socket_path,
        )

        api = HAProxyApi(
            cfg=config,
            backend_configs={backend.name: backend},
            config_file_path=config_file_path,
        )

        await api.start()
        haproxy_api_cleanup(api)

        with open(config_file_path, "r") as f:
            actual_content = f.read()
            assert "backend_2" not in actual_content

        original_proc = api._proc
        original_pid = original_proc.pid

        # Add another backend
        backend2 = BackendConfig(
            name="backend_2",
            path_prefix="/",
            app_name="backend_app_2",
            servers=[ServerConfig(name="server", host="127.0.0.1", port=9999)],
        )

        api.set_backend_configs({backend.name: backend, backend2.name: backend2})
        await api.reload()

        assert api._proc is not None
        assert api._proc.pid != original_pid

        with open(config_file_path, "r") as f:
            actual_content = f.read()
            assert "backend_2" in actual_content

        wait_for_condition(
            lambda: not process_exists(original_pid),
            timeout=5,
            retry_interval_ms=100,
        )


@pytest.mark.asyncio
async def test_haproxy_start_should_throw_error_when_already_running(
    haproxy_api_cleanup,
):
    """Test that HAProxy throws an error when trying to start on an already-used port (SO_REUSEPORT disabled)."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")
        socket_path = os.path.join(temp_dir, "admin.sock")

        config = HAProxyConfig(
            http_options=HTTPOptions(
                host="127.0.0.1",
                port=8000,
            ),
            stats_port=8404,
            socket_path=socket_path,
            enable_so_reuseport=False,  # Disable SO_REUSEPORT
        )

        api = HAProxyApi(cfg=config, config_file_path=config_file_path)
        haproxy_api_cleanup(api)

        # Start HAProxy with SO_REUSEPORT disabled
        await api.start()

        assert api._proc is not None, "HAProxy process should be running"
        first_pid = api._proc.pid

        # Verify we can't start another instance on the same port (SO_REUSEPORT disabled)
        config2 = HAProxyConfig(
            http_options=HTTPOptions(
                host="127.0.0.1",
                port=config.frontend_port,  # Same port
            ),
            stats_port=8404,
            socket_path=os.path.join(temp_dir, "admin2.sock"),
            enable_so_reuseport=False,  # Disable SO_REUSEPORT
        )

        api2 = HAProxyApi(
            cfg=config2, config_file_path=os.path.join(temp_dir, "haproxy2.cfg")
        )

        # This should fail because SO_REUSEPORT is disabled
        with pytest.raises(RuntimeError, match="(Address already in use)"):
            await api2.start()

        # Cleanup first instance
        await api.stop()
        assert not process_exists(first_pid), "HAProxy process should be stopped"


@pytest.mark.asyncio
async def test_toggle_health_checks(haproxy_api_cleanup):
    """Test that disable()/enable() toggle HAProxy health checks end-to-end."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")
        socket_path = os.path.join(temp_dir, "admin.sock")

        backend = BackendConfig(
            name="backend",
            path_prefix="/",
            app_name="backend_app",
            servers=[ServerConfig(name="server", host="127.0.0.1", port=9999)],
        )

        config = HAProxyConfig(
            http_options=HTTPOptions(
                host="127.0.0.1",
                port=8000,
            ),
            stats_port=8404,
            socket_path=socket_path,
            inject_process_id_header=True,
            has_received_routes=True,
            has_received_servers=True,
        )

        # Start a real backend server so HAProxy can mark the server UP
        backend_server, backend_thread = create_test_backend_server(9999)
        try:
            api = HAProxyApi(
                cfg=config,
                backend_configs={backend.name: backend},
                config_file_path=config_file_path,
            )

            await api.start()
            haproxy_api_cleanup(api)

            # Verify HAProxy is running
            assert api._is_running(), "HAProxy should be running"

            # Health requires servers; wait until health passes
            def health_ok():
                resp = requests.get(
                    f"http://127.0.0.1:{config.frontend_port}{config.health_check_endpoint}",
                    timeout=5,
                )
                return resp.status_code == 200

            wait_for_condition(health_ok, timeout=10)

            # Verify a config file contains health check enabled
            with open(api.config_file_path, "r") as f:
                config_content = f.read()
                assert (
                    "http-request return status 200" in config_content
                ), "Health checks should be enabled in config"

            # Disable health checks
            await api.disable()

            # Verify HAProxy is still running after calling disable()
            assert api._is_running(), "HAProxy should still be running after disable"

            # Config should now deny the health endpoint
            with open(api.config_file_path, "r") as f:
                config_content = f.read()
                assert (
                    "http-request return status 503" in config_content
                ), "Health checks should be disabled in config"

            def health_check_condition(status_code: int):
                # Test health check endpoint now fails
                health_response = requests.get(
                    f"http://127.0.0.1:{config.frontend_port}{config.health_check_endpoint}",
                    timeout=5,
                )

                return health_response.status_code == status_code

            wait_for_condition(health_check_condition, timeout=2, status_code=503)

            # Re-enable health checks
            await api.enable()

            # Config should contain the 200 response again
            with open(api.config_file_path, "r") as f:
                config_content = f.read()
                assert (
                    "http-request return status 200" in config_content
                ), "Health checks should be re-enabled in config"

            wait_for_condition(health_check_condition, timeout=5, status_code=200)

        finally:
            backend_server.should_exit = True
            backend_thread.join(timeout=5)


@pytest.mark.asyncio
async def test_health_endpoint_or_logic_multiple_backends(haproxy_api_cleanup):
    """Test that the health endpoint returns 200 if ANY backend has at least one server UP (OR logic)."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")
        socket_path = os.path.join(temp_dir, "admin.sock")
        backend1_port = 9996
        backend2_port = 9997

        config = HAProxyConfig(
            http_options=HTTPOptions(
                host="127.0.0.1",
                port=8000,
            ),
            stats_port=8404,
            socket_path=socket_path,
            has_received_routes=True,
            has_received_servers=True,
        )

        backend1 = BackendConfig(
            name="backend1",
            path_prefix="/api1",
            servers=[
                ServerConfig(name="server1", host="127.0.0.1", port=backend1_port)
            ],
            health_check_fall=1,
            health_check_rise=1,
            health_check_inter="1s",
        )

        backend2 = BackendConfig(
            name="backend2",
            path_prefix="/api2",
            servers=[
                ServerConfig(name="server2", host="127.0.0.1", port=backend2_port)
            ],
            health_check_fall=1,
            health_check_rise=1,
            health_check_inter="1s",
        )

        backend1_server, backend1_thread = create_test_backend_server(backend1_port)
        backend2_server, backend2_thread = create_test_backend_server(backend2_port)

        try:
            api = HAProxyApi(
                cfg=config,
                backend_configs={backend1.name: backend1, backend2.name: backend2},
                config_file_path=config_file_path,
            )

            await api.start()
            haproxy_api_cleanup(api)

            # Wait for health check to pass (both servers are UP)
            def health_ok():
                resp = requests.get(
                    f"http://127.0.0.1:{config.frontend_port}{config.health_check_endpoint}",
                    timeout=5,
                )
                return resp.status_code == 200

            wait_for_condition(health_ok, timeout=10, retry_interval_ms=200)

            # Verify health check returns 200 when both servers are UP
            health_response = requests.get(
                f"http://127.0.0.1:{config.frontend_port}{config.health_check_endpoint}",
                timeout=5,
            )
            assert (
                health_response.status_code == 200
            ), "Health check should return 200 when both servers are UP"
            assert b"success" in health_response.content

            # Stop backend1 server
            backend1_server.should_exit = True
            backend1_thread.join(timeout=5)

            # Wait a bit for HAProxy to detect backend1 is down
            await asyncio.sleep(2)

            # Verify health check STILL returns 200 (backend2 is still UP - OR logic)
            health_response = requests.get(
                f"http://127.0.0.1:{config.frontend_port}{config.health_check_endpoint}",
                timeout=5,
            )
            assert (
                health_response.status_code == 200
            ), "Health check should return 200 when at least one backend (backend2) is UP (OR logic)"
            assert b"success" in health_response.content

            # Stop backend2 server as well
            backend2_server.should_exit = True
            backend2_thread.join(timeout=5)

            # Wait for health check to fail (both servers are DOWN)
            def health_fails():
                resp = requests.get(
                    f"http://127.0.0.1:{config.frontend_port}{config.health_check_endpoint}",
                    timeout=5,
                )
                return resp.status_code == 503

            wait_for_condition(health_fails, timeout=10, retry_interval_ms=200)

            # Verify health check returns 503 when ALL servers are DOWN
            health_response = requests.get(
                f"http://127.0.0.1:{config.frontend_port}{config.health_check_endpoint}",
                timeout=5,
            )
            assert (
                health_response.status_code == 503
            ), "Health check should return 503 when all servers are DOWN"
            assert b"Service Unavailable" in health_response.content

            await api.stop()
        finally:
            # Cleanup
            try:
                if not backend1_server.should_exit:
                    backend1_server.should_exit = True
                    backend1_thread.join(timeout=5)
            except Exception:
                pass
            try:
                if not backend2_server.should_exit:
                    backend2_server.should_exit = True
                    backend2_thread.join(timeout=5)
            except Exception:
                pass


@pytest.mark.asyncio
async def test_errorfile_creation_and_config(haproxy_api_cleanup):
    """Test that the errorfile is created and configured correctly for both 502 and 504."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")
        socket_path = os.path.join(temp_dir, "admin.sock")

        # Launch a simple backend server with /fast endpoint
        backend_port = 9107
        backend_server, backend_thread = create_test_backend_server(backend_port)

        # Configure HAProxy with one backend under root ('/') so upstream sees '/fast'
        config = HAProxyConfig(
            http_options=HTTPOptions(
                host="127.0.0.1",
                port=8000,
                keep_alive_timeout_s=58,
            ),
            stats_port=8404,
            socket_path=socket_path,
        )

        api = HAProxyApi(cfg=config, config_file_path=config_file_path)
        haproxy_api_cleanup(api)

        # Verify the error file was created during initialization
        expected_error_file_path = os.path.join(temp_dir, "500.http")
        assert os.path.exists(
            expected_error_file_path
        ), "Error file 500.http should be created"
        assert (
            api.cfg.error_file_path == expected_error_file_path
        ), "Error file path should be set in config"

        # Verify the error file content
        with open(expected_error_file_path, "r") as ef:
            error_content = ef.read()
            assert (
                "HTTP/1.1 500 Internal Server Error" in error_content
            ), "Error file should contain 500 status"
            assert (
                "Content-Type: text/plain" in error_content
            ), "Error file should contain content-type header"
            assert (
                "Internal Server Error" in error_content
            ), "Error file should contain error message"

        # Start HAProxy and verify config contains errorfile directives
        await api.start()

        # Verify config file contains errorfile directives for both 502 and 504 pointing to the same file
        with open(config_file_path, "r") as f:
            config_content = f.read()
            assert (
                f"errorfile 502 {expected_error_file_path}" in config_content
            ), "HAProxy config should contain 502 errorfile directive"
            assert (
                f"errorfile 504 {expected_error_file_path}" in config_content
            ), "HAProxy config should contain 504 errorfile directive"

        await api.stop()
        backend = BackendConfig(
            name="app_backend",
            path_prefix="/",
            app_name="app",
            servers=[ServerConfig(name="server1", host="127.0.0.1", port=backend_port)],
            timeout_http_keep_alive_s=58,
        )

        api = HAProxyApi(
            cfg=config,
            backend_configs={backend.name: backend},
            config_file_path=config_file_path,
        )

        haproxy_api_cleanup(api)

        try:
            await api.start()

            # Ensure HAProxy is up (stats endpoint reachable)
            wait_for_condition(
                lambda: check_haproxy_ready(config.stats_port),
                timeout=10,
                retry_interval_ms=100,
            )

            # Route exists -> expect 200
            r = requests.get("http://127.0.0.1:8000/fast", timeout=5)
            assert r.status_code == 200

            # Remove backend (no targets for /app) and reload
            api.set_backend_configs({})
            await api.reload()

            # After removal, route should fall back to default backend -> 404
            def get_status():
                resp = requests.get("http://127.0.0.1:8000/fast", timeout=5)
                return resp.status_code

            # Allow a brief window for reload to take effect
            wait_for_condition(
                lambda: get_status() == 404, timeout=5, retry_interval_ms=100
            )

        finally:
            try:
                await api.stop()
            except Exception:
                pass

            try:
                backend_server.should_exit = True
                backend_thread.join(timeout=5)
            except Exception:
                pass


@pytest.mark.asyncio
async def test_routes_endpoint_returns_backends_and_respects_health(
    haproxy_api_cleanup,
):
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")
        socket_path = os.path.join(temp_dir, "admin.sock")

        # Start two backend servers; health endpoint exists at '/-/healthz'.
        backend_port1 = 9910
        backend_port2 = 9911
        backend_server1, backend_thread1 = create_test_backend_server(backend_port1)
        backend_server2, backend_thread2 = create_test_backend_server(backend_port2)

        # Configure HAProxy with two prefixed backends
        config = HAProxyConfig(
            http_options=HTTPOptions(
                host="127.0.0.1",
                port=8013,
                keep_alive_timeout_s=58,
            ),
            stats_port=8413,
            socket_path=socket_path,
        )

        backend_api = BackendConfig(
            name="api_backend",
            path_prefix="/api",
            app_name="api_app",
            servers=[
                ServerConfig(name="server1", host="127.0.0.1", port=backend_port1)
            ],
            timeout_http_keep_alive_s=58,
        )
        backend_web = BackendConfig(
            name="web_backend",
            path_prefix="/web",
            app_name="web_app",
            servers=[
                ServerConfig(name="server2", host="127.0.0.1", port=backend_port2)
            ],
            timeout_http_keep_alive_s=58,
        )

        api = HAProxyApi(
            cfg=config,
            backend_configs={
                backend_api.name: backend_api,
                backend_web.name: backend_web,
            },
            config_file_path=config_file_path,
        )

        haproxy_api_cleanup(api)

        try:
            await api.start()

            # Wait for HAProxy to be ready
            wait_for_condition(
                lambda: check_haproxy_ready(config.stats_port),
                timeout=10,
                retry_interval_ms=100,
            )

            # Helper to get fresh routes response (avoids connection reuse)
            def get_routes():
                with requests.Session() as session:
                    return session.get("http://127.0.0.1:8013/-/routes", timeout=1)

            # Initial state: no routes
            r = requests.get("http://127.0.0.1:8013/-/routes", timeout=5)
            assert r.status_code == 503
            assert r.headers.get("content-type", "").startswith("text/plain")
            assert r.text == "Route table is not populated yet."

            # Set has_received_routes but not has_received_servers -> should show "No replicas available"
            api.cfg.has_received_routes = True
            api.cfg.has_received_servers = False
            await api.reload()
            get_routes().text == "No replicas are available yet.",
            r = get_routes()
            assert r.status_code == 503
            assert r.headers.get("content-type", "").startswith("text/plain")

            # Set both flags -> should show routes JSON
            api.cfg.has_received_routes = True
            api.cfg.has_received_servers = True
            await api.reload()

            # Reload is not synchronous, so we need to wait for the config to be applied
            def check_json_routes():
                r = get_routes()
                return r.status_code == 200 and r.headers.get(
                    "content-type", ""
                ).startswith("application/json")

            wait_for_condition(check_json_routes, timeout=5, retry_interval_ms=50)
            r = get_routes()
            data = r.json()
            assert data == {"/api": "api_app", "/web": "web_app"}

            # Disable (simulate draining/unhealthy) -> wait for healthz to flip, then routes 503
            await api.disable()

            def health_is(code: int):
                resp = requests.get("http://127.0.0.1:8013/-/healthz", timeout=5)
                return resp.status_code == code

            wait_for_condition(health_is, timeout=5, retry_interval_ms=100, code=503)
            r = requests.get("http://127.0.0.1:8013/-/routes", timeout=5)
            assert r.status_code == 503
            assert r.headers.get("content-type", "").startswith("text/plain")
            assert r.text == "This node is being drained."

            # Re-enable -> wait for healthz to flip back, then routes 200
            await api.enable()
            wait_for_condition(health_is, timeout=5, retry_interval_ms=100, code=200)
            r = requests.get("http://127.0.0.1:8013/-/routes", timeout=5)
            assert r.status_code == 200

        finally:
            try:
                await api.stop()
            except Exception:
                pass


@pytest.mark.asyncio
async def test_routes_endpoint_no_routes(haproxy_api_cleanup):
    """When no backends are configured, /-/routes should return {} and respect health gating."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")
        socket_path = os.path.join(temp_dir, "admin.sock")

        config = HAProxyConfig(
            http_options=HTTPOptions(
                host="127.0.0.1",
                port=8014,
                keep_alive_timeout_s=58,
            ),
            stats_port=8414,
            socket_path=socket_path,
        )

        api = HAProxyApi(
            cfg=config,
            backend_configs={},
            config_file_path=config_file_path,
        )

        haproxy_api_cleanup(api)

        try:
            await api.start()

            # Wait for HAProxy to be ready
            wait_for_condition(
                lambda: check_haproxy_ready(config.stats_port),
                timeout=10,
                retry_interval_ms=100,
            )

            # Healthy -> expect 200 and empty JSON
            r = requests.get(
                f"http://127.0.0.1:{config.frontend_port}/-/routes", timeout=5
            )
            assert r.status_code == 503
            assert r.headers.get("content-type", "").startswith("text/plain")
            assert r.text == "Route table is not populated yet."

            # Disable -> wait for healthz to flip, then expect 503 with draining message
            await api.disable()

            def health_is(code: int):
                resp = requests.get(
                    f"http://127.0.0.1:{config.frontend_port}/-/healthz", timeout=5
                )
                return resp.status_code == code

            wait_for_condition(health_is, timeout=5, retry_interval_ms=100, code=503)

            # Wait for routes endpoint to also return draining message (graceful reload might take a moment)
            def routes_is_draining():
                try:
                    resp = requests.get(
                        f"http://127.0.0.1:{config.frontend_port}/-/routes", timeout=5
                    )
                    return (
                        resp.status_code == 503
                        and resp.text == "This node is being drained."
                    )
                except Exception:
                    return False

            wait_for_condition(routes_is_draining, timeout=5, retry_interval_ms=100)

            r = requests.get(
                f"http://127.0.0.1:{config.frontend_port}/-/routes", timeout=5
            )
            assert r.status_code == 503
            assert r.headers.get("content-type", "").startswith("text/plain")
            assert r.text == "This node is being drained."

            # Re-enable -> wait for healthz back to 200, then routes 200
            await api.enable()
            wait_for_condition(health_is, timeout=5, retry_interval_ms=100, code=503)

            def routes_is_healthy():
                try:
                    r = requests.get(
                        f"http://127.0.0.1:{config.frontend_port}/-/routes", timeout=5
                    )
                    return (
                        r.status_code == 503
                        and r.text == "Route table is not populated yet."
                    )
                except Exception:
                    return False

            wait_for_condition(routes_is_healthy, timeout=5, retry_interval_ms=100)
        finally:
            try:
                await api.stop()
            except Exception:
                pass


@pytest.mark.asyncio
async def test_404_error_message(haproxy_api_cleanup):
    """Test that HAProxy returns the correct 404 error message for non-existent paths."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_path = os.path.join(temp_dir, "haproxy.cfg")
        socket_path = os.path.join(temp_dir, "admin.sock")

        # Create a backend that serves /api
        backend = BackendConfig(
            name="api_backend",
            path_prefix="/api",
            servers=[],  # No servers, but we're testing the 404 path anyway
        )

        config = HAProxyConfig(
            http_options=HTTPOptions(
                host="127.0.0.1",
                port=8000,
            ),
            stats_port=8404,
            socket_path=socket_path,
        )

        api = HAProxyApi(
            cfg=config,
            backend_configs={"api_backend": backend},
            config_file_path=config_file_path,
        )

        await api.start()
        haproxy_api_cleanup(api)

        # Verify HAProxy is running
        assert api._is_running(), "HAProxy should be running"

        # Wait for HAProxy to be ready
        wait_for_condition(
            lambda: check_haproxy_ready(config.stats_port),
            timeout=10,
            retry_interval_ms=500,
        )

        # Request a non-existent path and verify the error message
        response = requests.get(
            f"http://127.0.0.1:{config.frontend_port}/nonexistent",
            timeout=5,
        )

        assert response.status_code == 404, "Should return 404 for non-existent path"
        assert (
            "Path '/nonexistent' not found" in response.text
        ), f"Error message should contain path. Got: {response.text}"
        assert (
            "Ping http://.../-/routes for available routes" in response.text
        ), f"Error message should contain routes hint. Got: {response.text}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
