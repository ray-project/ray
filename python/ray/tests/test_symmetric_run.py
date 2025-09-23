import atexit
import os
import shutil
import socket
import subprocess
import tempfile
import time
import sys
import threading
from contextlib import contextmanager
from typing import Callable, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

import ray
import ray.scripts.scripts as scripts
import ray._private.services as services
from ray._private.test_utils import get_current_unused_port


# ---------------------------------------------------------------------------
# Thread-aware network & subprocess mocking.

_ORIGINAL_GETADDRINFO = socket.getaddrinfo
_ORIGINAL_SUBPROCESS_RUN = subprocess.run

try:  # psutil may not be installed in some environments.
    import psutil

    _ORIGINAL_NET_IF_ADDRS = psutil.net_if_addrs
except ImportError:  # pragma: no cover
    psutil = None
    _ORIGINAL_NET_IF_ADDRS = None

_NETWORK_PATCH_LOCK = threading.Lock()
_NETWORK_PATCH_REFCOUNT = 0
_THREAD_NETWORK_CONFIGS: Dict[int, Dict[str, str]] = {}
_THREAD_PORT_CONFIGS: Dict[int, Dict[str, object]] = {}
_PATCHERS = []
_SITECUSTOMIZE_DIR: Optional[str] = None


def _ensure_sitecustomize_dir() -> str:
    global _SITECUSTOMIZE_DIR
    if _SITECUSTOMIZE_DIR is None:
        tmp_dir = tempfile.mkdtemp(prefix="ray_symmetric_run_sitecustomize_")
        path = os.path.join(tmp_dir, "sitecustomize.py")
        with open(path, "w", encoding="utf-8") as f:
            f.write(
                """
import os
import socket

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None

_original_getaddrinfo = socket.getaddrinfo
_original_net_if_addrs = psutil.net_if_addrs if psutil else None


def _get_thread_key():
    return os.environ.get("RAY_SIM_THREAD_KEY")


def _load_config():
    key = _get_thread_key()
    if not key:
        return None
    prefix = f"RAY_SIM_CONFIG_{key}_"
    cfg = {}
    for k, v in os.environ.items():
        if k.startswith(prefix):
            cfg[k[len(prefix):]] = v
    return cfg if cfg else None


def _patched_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    cfg = _load_config()
    if cfg is None:
        return _original_getaddrinfo(host, port, family, type, proto, flags)

    resolved_host = cfg.get("GCS_IP", host)
    resolved_port = int(cfg.get("GCS_PORT", port))
    return [
        (
            socket.AF_INET,
            socket.SOCK_STREAM,
            proto or 0,
            "",
            (resolved_host, resolved_port),
        )
    ]


def _patched_net_if_addrs():
    if psutil is None:
        raise RuntimeError("psutil is required for symmetric_run tests")

    cfg = _load_config()
    if cfg is None:
        return _original_net_if_addrs()

    ip = cfg.get("NODE_IP", "127.0.0.1")
    family = socket.AF_INET6 if ":" in ip else socket.AF_INET
    addr = type("addr", (), {"family": family, "address": ip})()
    return {"lo": [addr]}


socket.getaddrinfo = _patched_getaddrinfo
if psutil is not None:
    psutil.net_if_addrs = _patched_net_if_addrs

"""
            )
        _SITECUSTOMIZE_DIR = tmp_dir
        atexit.register(shutil.rmtree, tmp_dir, True)
    return _SITECUSTOMIZE_DIR


def _patched_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    cfg = _THREAD_NETWORK_CONFIGS.get(threading.get_ident())
    if cfg is None:
        return _ORIGINAL_GETADDRINFO(host, port, family, type, proto, flags)

    resolved_host = cfg.get("gcs_ip", host)
    resolved_port = cfg.get("gcs_port", port)
    return [
        (
            socket.AF_INET,
            socket.SOCK_STREAM,
            proto or 0,
            "",
            (resolved_host, int(resolved_port)),
        )
    ]


def _patched_net_if_addrs():
    if psutil is None:
        raise RuntimeError("psutil is required for symmetric_run tests")

    cfg = _THREAD_NETWORK_CONFIGS.get(threading.get_ident())
    if cfg is None:
        return _ORIGINAL_NET_IF_ADDRS()

    ip = cfg.get("node_ip", "127.0.0.1")
    family = socket.AF_INET6 if ":" in ip else socket.AF_INET
    addr = type("addr", (), {"family": family, "address": ip})()
    return {"lo": [addr]}


def _patched_subprocess_run(cmd, *args, **kwargs):
    cfg = _THREAD_PORT_CONFIGS.get(threading.get_ident())
    if cfg and isinstance(cmd, (list, tuple)) and len(cmd) >= 2 and cmd[0] == "ray" and cmd[1] == "start":
        cmd = list(cmd)
        extra_args = cfg.get("extra_args", [])
        # Append extra args while keeping existing order stable.
        cmd.extend(extra_args)

        env = kwargs.get("env") or os.environ.copy()
        env.update(cfg.get("env", {}))
        kwargs["env"] = env

    return _ORIGINAL_SUBPROCESS_RUN(cmd, *args, **kwargs)


@contextmanager
def _setup_mock_network_utils(curr_ip, node_ip, port_config=None, gcs_port=6379):
    global _NETWORK_PATCH_REFCOUNT, _PATCHERS

    thread_id = threading.get_ident()

    with _NETWORK_PATCH_LOCK:
        if _NETWORK_PATCH_REFCOUNT == 0:
            socket_patcher = patch("socket.getaddrinfo", new=_patched_getaddrinfo)
            _PATCHERS.append(socket_patcher)
            socket_patcher.start()

            if psutil is not None:
                net_if_patcher = patch("psutil.net_if_addrs", new=_patched_net_if_addrs)
                _PATCHERS.append(net_if_patcher)
                net_if_patcher.start()

            # Only patch subprocess.run when we need to augment Ray start commands.
            if port_config is not None:
                subprocess_patcher = patch(
                    "subprocess.run", new=_patched_subprocess_run
                )
                _PATCHERS.append(subprocess_patcher)
                subprocess_patcher.start()

        _NETWORK_PATCH_REFCOUNT += 1

    thread_key = str(thread_id)

    _THREAD_NETWORK_CONFIGS[thread_id] = {
        "gcs_ip": curr_ip,
        "gcs_port": str(gcs_port),
        "node_ip": node_ip,
    }

    if port_config is not None:
        config_copy = dict(port_config)
        env = dict(config_copy.get("env", {}))

        site_dir = _ensure_sitecustomize_dir()
        existing_pythonpath = env.get("PYTHONPATH", os.environ.get("PYTHONPATH", ""))
        new_pythonpath = site_dir
        if existing_pythonpath:
            new_pythonpath = site_dir + os.pathsep + existing_pythonpath
        env["PYTHONPATH"] = new_pythonpath
        env["RAY_SIM_THREAD_KEY"] = thread_key
        env[f"RAY_SIM_CONFIG_{thread_key}_GCS_IP"] = curr_ip
        env[f"RAY_SIM_CONFIG_{thread_key}_GCS_PORT"] = str(gcs_port)
        env[f"RAY_SIM_CONFIG_{thread_key}_NODE_IP"] = node_ip
        config_copy["env"] = env

        _THREAD_PORT_CONFIGS[thread_id] = config_copy

    try:
        yield
    finally:
        _THREAD_NETWORK_CONFIGS.pop(thread_id, None)
        _THREAD_PORT_CONFIGS.pop(thread_id, None)

        with _NETWORK_PATCH_LOCK:
            _NETWORK_PATCH_REFCOUNT -= 1
            if _NETWORK_PATCH_REFCOUNT == 0:
                while _PATCHERS:
                    patcher = _PATCHERS.pop()
                    patcher.stop()


def _run_head_and_workers(
    symmetric_run_cmd, args: List[str], head_ip: str, worker_ips: List[str]
):
    """Run symmetric_run concurrently on head and worker nodes.
    
    Worker nodes use `ray start --block` to wait for the cluster to be ready, which means
    that we can't call `runner.invoke()` for the workers because it will block forever.
    Instead, we need to call the `runner.invoke()` for each 'node' in a separate thread.

    We use thread-safe shared state using Event and Lock. In order to get different behavior
    for each call, we do mock isolation for each call.

    Finally, we end with a cleanup step to kill any failed threads.
    """
    expected_workers = len(worker_ips)
    head_ready = threading.Event()
    workers_ready = threading.Event()
    worker_counter = {"count": 0}
    counter_lock = threading.Lock()

    def mark_worker_ready():
        with counter_lock:
            worker_counter["count"] += 1
            if worker_counter["count"] == expected_workers:
                workers_ready.set()

    def fake_check_head_node_ready(address, timeout=None):
        # Wait until head reports readiness and confirm the GCS port is reachable.
        if not head_ready.wait(timeout):
            return False

        host, _, port_str = address.partition(":")
        port = int(port_str)
        deadline = time.time() + (timeout if timeout is not None else 15)

        while time.time() < deadline:
            try:
                with socket.create_connection((host, port), timeout=1):
                    mark_worker_ready()
                    return True
            except OSError:
                time.sleep(0.5)

        return False

    def fake_check_cluster_ready(nnodes, timeout=None):
        head_ready.set()
        if expected_workers == 0:
            return True
        ready = workers_ready.wait(timeout)
        return ready

    used_ports = set()

    def _alloc_unique_port() -> int:
        while True:
            candidate = get_current_unused_port()
            if candidate not in used_ports:
                used_ports.add(candidate)
                return candidate

    def _reserve_port_range(start: int, end: int) -> None:
        used_ports.update(range(start, end + 1))

    def _build_port_config(base_name: str) -> Dict[str, object]:
        temp_dir = tempfile.mkdtemp(prefix=f"ray_symrun_{base_name}_")

        node_manager_port = _alloc_unique_port()
        object_manager_port = _alloc_unique_port()
        dashboard_port = _alloc_unique_port()
        dashboard_agent_listen_port = _alloc_unique_port()
        dashboard_agent_grpc_port = _alloc_unique_port()
        ray_client_server_port = _alloc_unique_port()
        metrics_export_port = _alloc_unique_port()
        runtime_env_agent_port = _alloc_unique_port()
        min_worker_port = _alloc_unique_port()
        max_worker_port = min_worker_port + 9
        _reserve_port_range(min_worker_port, max_worker_port)

        extra_args = [
            f"--node-manager-port={node_manager_port}",
            f"--object-manager-port={object_manager_port}",
            f"--min-worker-port={min_worker_port}",
            f"--max-worker-port={max_worker_port}",
            f"--ray-client-server-port={ray_client_server_port}",
            f"--dashboard-port={dashboard_port}",
            f"--dashboard-agent-listen-port={dashboard_agent_listen_port}",
            f"--dashboard-agent-grpc-port={dashboard_agent_grpc_port}",
            f"--metrics-export-port={metrics_export_port}",
            f"--runtime-env-agent-port={runtime_env_agent_port}",
        ]

        env = {
            "RAY_FAKE_CLUSTER": "1",
            "RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER": "1",
        }

        return {"extra_args": extra_args, "env": env, "temp_dir": temp_dir}

    head_port_config = _build_port_config("head")
    worker_port_configs = [
        _build_port_config(f"worker_{idx}") for idx, _ in enumerate(worker_ips)
    ]

    head_result = {}
    worker_results = [{} for _ in worker_ips]

    def run_head():
        runner = CliRunner()
        with _setup_mock_network_utils(head_ip, head_ip, port_config=head_port_config):
            head_result["result"] = runner.invoke(symmetric_run_cmd, args)

    def run_worker(idx, worker_ip):
        runner = CliRunner()
        with _setup_mock_network_utils(
            head_ip, worker_ip, port_config=worker_port_configs[idx]
        ):
            worker_results[idx]["result"] = runner.invoke(symmetric_run_cmd, args)

    threads = []
    try:
        with patch(
            "ray.scripts.symmetric_run.check_ray_already_started", return_value=False
        ), patch(
            "ray.scripts.symmetric_run.check_cluster_ready",
            side_effect=fake_check_cluster_ready,
        ), patch(
            "ray.scripts.symmetric_run.check_head_node_ready",
            side_effect=fake_check_head_node_ready,
        ):
            head_thread = threading.Thread(target=run_head, name="head-thread")
            threads.append(head_thread)
            for idx, worker_ip in enumerate(worker_ips):
                t = threading.Thread(
                    target=run_worker,
                    args=(idx, worker_ip),
                    name=f"worker-thread-{idx}",
                )
                threads.append(t)

            for t in threads:
                t.start()

            deadline = time.time() + 15
            for t in threads:
                remaining = max(0, deadline - time.time())
                t.join(timeout=remaining)
    finally:
        head_ready.set()
        workers_ready.set()

    for t in threads:
        t.join(timeout=5)
        assert not t.is_alive(), f"Thread {t.name} did not finish in time"

    temp_dirs = [head_port_config["temp_dir"]]
    temp_dirs.extend(cfg["temp_dir"] for cfg in worker_port_configs)
    for td in temp_dirs:
        shutil.rmtree(td, ignore_errors=True)

    return head_result.get("result"), [r.get("result") for r in worker_results]


@pytest.fixture
def cleanup_ray():
    """Shutdown all ray instances"""
    yield
    runner = CliRunner()
    runner.invoke(scripts.stop, ["--force"])
    ray.shutdown()


def test_symmetric_run_basic_interface(monkeypatch, cleanup_ray):
    """Test basic symmetric_run interface with minimal arguments."""
    from ray.scripts.symmetric_run import symmetric_run

    runner = CliRunner()

    # Mock subprocess.run to avoid actually starting Ray
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        with _setup_mock_network_utils("127.0.0.1", "127.0.0.1"):
            args = ["--address", "127.0.0.1:6379", "--", "echo", "test"]

            with patch("sys.argv", ["/bin/ray", "symmetric-run", *args]):
                # Test basic symmetric_run call using CliRunner
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 0

            # Verify that subprocess.run was called for ray start
            assert mock_run.called
            calls = mock_run.call_args_list

            # Should have called ray start with --head
            ray_start_calls = [
                call for call in calls if "ray" in str(call) and "start" in str(call)
            ]
            assert len(ray_start_calls) > 0

            # Should have called ray stop
            ray_stop_calls = [
                call for call in calls if "ray" in str(call) and "stop" in str(call)
            ]
            assert len(ray_stop_calls) > 0


def test_symmetric_run_worker_node_behavior(monkeypatch, cleanup_ray):
    """Test symmetric_run behavior when not on the head node."""
    from ray.scripts.symmetric_run import symmetric_run

    runner = CliRunner()

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0

        with _setup_mock_network_utils("192.168.1.100", "192.168.1.101"):
            # Mock socket connection check to simulate head node ready
            with patch("socket.socket") as mock_socket:
                mock_socket_instance = MagicMock()
                mock_socket_instance.connect_ex.return_value = 0
                mock_socket.return_value.__enter__.return_value = mock_socket_instance

                # Test worker node behavior
                args = ["--address", "192.168.1.100:6379", "--", "echo", "test"]
                with patch("sys.argv", ["/bin/ray", "symmetric-run", *args]):
                    with patch(
                        "ray.scripts.symmetric_run.check_head_node_ready"
                    ) as mock_check_head_node_ready:
                        mock_check_head_node_ready.return_value = True
                        result = runner.invoke(symmetric_run, args)
                        assert result.exit_code == 0

                # Verify that subprocess.run was called
                assert mock_run.called
                calls = mock_run.call_args_list

                # Should have called ray start with --address (worker mode)
                ray_start_calls = [
                    call
                    for call in calls
                    if "ray" in str(call) and "start" in str(call)
                ]
                assert len(ray_start_calls) > 0

                # Check that it's in worker mode (--address instead of --head)
                start_call = ray_start_calls[0]
                start_args = start_call[0][0]
                assert "--address" in start_args
                assert "192.168.1.100:6379" in start_args
                assert "--head" not in start_args
                assert "--block" in start_args  # Worker nodes should block


def test_symmetric_run_arg_validation(monkeypatch, cleanup_ray):
    """Test that symmetric_run validates arguments."""
    from ray.scripts.symmetric_run import symmetric_run

    runner = CliRunner()

    # Mock subprocess.run to avoid actually starting Ray
    with _setup_mock_network_utils("127.0.0.1", "127.0.0.1"):

        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            args = ["--address", "127.0.0.1:6379", "--", "echo", "test"]

            with patch("sys.argv", ["/bin/ray", "symmetric-run", *args]):
                # Test basic symmetric_run call using CliRunner
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 0

        # Test that invalid arguments are rejected
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            args = ["--address", "127.0.0.1:6379", "echo", "test"]
            with patch("sys.argv", ["/bin/ray", "symmetric-run", *args]):
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 1
                assert "No separator" in result.output

        # Test that invalid arguments are rejected
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            args = ["--address", "127.0.0.1:6379", "--head", "--", "echo", "test"]
            with patch("sys.argv", ["/bin/ray", "symmetric-run", *args]):
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 1
                assert "Cannot use --head option in symmetric_run." in result.output

        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0

            # Test args with "=" are passed to ray start
            args = ["--address", "127.0.0.1:6379", "--num-cpus=4", "--", "echo", "test"]
            with patch("sys.argv", ["/bin/ray", "symmetric-run", *args]):
                result = runner.invoke(symmetric_run, args)
                assert result.exit_code == 0

                ray_start_calls = [
                    call
                    for call in mock_run.call_args_list
                    if "ray" in str(call) and "start" in str(call)
                ]
                assert len(ray_start_calls) > 0
                assert "--num-cpus=4" in ray_start_calls[0][0][0]


def test_symmetric_run_multi_node(monkeypatch, cleanup_ray):
    """
    Test symmetric_run with a simulated 3-node (1 head + 2 workers) cluster.
    """
    from ray.scripts.symmetric_run import symmetric_run

    head_ip = services.get_node_ip_address()
    address = f"{head_ip}:6379"
    worker_ips = ["10.0.0.2", "10.0.0.3"]

    common_args = ["--address", address, "--min-nodes", "3", "--", "echo", "ok"]

    head_result, worker_results = _run_head_and_workers(
        symmetric_run, common_args, head_ip, worker_ips
    )

    assert head_result is not None
    assert all(result is not None for result in worker_results)
    assert head_result.exception is None
    worker_exceptions = [result.exception for result in worker_results]
    if not all(exc is None for exc in worker_exceptions):
        debug_outputs = [result.output for result in worker_results]
        raise RuntimeError(
            f"Worker exceptions encountered: {worker_exceptions}. Outputs: {debug_outputs}"
        )
    assert head_result.exit_code == 0
    assert all(result.exit_code == 0 for result in worker_results)
    assert "On head node. Starting Ray cluster head..." in head_result.output
    assert "Running command on head node: ['echo', 'ok']" in head_result.output
    for result in worker_results:
        assert "On worker node. Connecting to Ray cluster" in result.output
        assert address in result.output


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
