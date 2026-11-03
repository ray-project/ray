"""End-to-end coverage for the six raygo-launched Go components.

Each test turns one Go switch on, starts a local cluster, and asserts the
raygo-launched component actually runs: right binary, right subcommand, alive
and serving. Every ray.init pins address="local" so the matrix always starts a
fresh cluster of its own — on a bare dev machine and equally inside a pod that
already hosts a live cluster (where the default address resolution would
connect to the existing cluster instead). Gated twice so default dev/CI runs
skip instantly: RAYGO_E2E=1
must be set, and the raygo binary must be resolvable (a machine with the Go
runtime artifacts built, e.g. via //go:gen_ray_go_pkg).

Clusters run under the ambient auth posture: token-auth clusters (the
ray.init default) are supported — the head loads the auth token like any Go
client, and the agent's token is attached by its C++ bridge from the
inherited environment.

The module adapts three environment knobs so a live matrix can run on a dev
machine as well as on a Go-runtime image:

- RAY_GO_RUNTIME_PATH is pinned to the go_runtime.so shipped beside the raygo
  binary, so the plugin loader cannot fall back to a stale copy left under an
  unrelated site-packages tree.
- PATH gains a shim directory whose python3/python point at this test
  process's interpreter: the Go setup worker resolves the Python worker
  interpreter via PATH lookup, and it must land on the interpreter that owns
  the _raylet.so being imported.
- PYTHONUSERBASE is pinned to the user site captured at import time, undoing
  the session-scoped HOME redirection in conftest for the cluster's Python
  subprocesses so they keep importing user-site packages.
"""

import os
import site
import sys
import time
from typing import List, Optional

import pytest

import ray
from ray._private import ray_constants
from ray._private.services import RAYGO_EXECUTABLE

import psutil

# How long a long-lived Go component must stay up before it counts as
# "running" rather than "crashed shortly after launch".
_SURVIVAL_WINDOW_S = 5.0

# Proxy-bypass settings for the HTTP probes against local cluster endpoints:
# an explicit None per scheme keeps requests off any ambient proxy regardless
# of the machine's proxy environment.
_NO_PROXY = {"http": None, "https": None}

# Captured at import time, before conftest's session-scoped HOME redirection:
# the cluster's Python subprocesses must keep resolving packages from the real
# user site (msgpack and friends live there), so the module fixture pins
# PYTHONUSERBASE to it.
_USER_SITE_BASE = site.getuserbase()

pytestmark = [
    pytest.mark.skipif(
        os.environ.get("RAYGO_E2E") != "1",
        reason="set RAYGO_E2E=1 on a machine with Go runtime artifacts",
    ),
    pytest.mark.skipif(
        not (RAYGO_EXECUTABLE and os.path.exists(RAYGO_EXECUTABLE)),
        reason="raygo binary not resolvable",
    ),
]


def _go_runtime_plugin_path() -> Optional[str]:
    """Return the go_runtime.so shipped next to the raygo binary, if any."""
    if not RAYGO_EXECUTABLE:
        return None
    candidate = os.path.join(
        os.path.dirname(RAYGO_EXECUTABLE), os.pardir, "lib", "go_runtime.so"
    )
    if os.path.exists(candidate):
        return os.path.abspath(candidate)
    return None


@pytest.fixture(autouse=True, scope="module")
def _go_e2e_process_env(tmp_path_factory):
    """Pin the Go runtime plugin and the Python interpreter for raygo children."""
    plugin_path = _go_runtime_plugin_path()
    shim_dir = tmp_path_factory.mktemp("raygo_e2e_path_shim")
    for name in ("python3", "python"):
        link = shim_dir / name
        if not link.exists():
            link.symlink_to(sys.executable)
    with pytest.MonkeyPatch.context() as mp:
        if plugin_path:
            mp.setenv("RAY_GO_RUNTIME_PATH", plugin_path)
        if _USER_SITE_BASE:
            mp.setenv("PYTHONUSERBASE", _USER_SITE_BASE)
        mp.setenv("PATH", f"{shim_dir}{os.pathsep}{os.environ.get('PATH', '')}")
        yield


def _raygo_processes(session_dir: str) -> List[psutil.Process]:
    """Return this session's raygo processes.

    Every component embeds the session directory in its argv (as
    --session-dir, or as --logs-dir for the autoscaler monitor), so matching
    on it keeps the scan from tripping over raygo processes of unrelated
    clusters on the same machine.
    """
    procs = []
    for proc in psutil.process_iter(["cmdline"]):
        cmdline = proc.info["cmdline"]
        if cmdline and cmdline[0].endswith("raygo"):
            if any(session_dir in part for part in cmdline[1:]):
                procs.append(proc)
    return procs


def _find_raygo_process(subcommand: str, session_dir: str) -> Optional[psutil.Process]:
    """Return the session's raygo process running the given subcommand."""
    for proc in _raygo_processes(session_dir):
        cmdline = proc.info["cmdline"]
        if len(cmdline) > 1 and cmdline[1] == subcommand:
            return proc
    return None


def _await_raygo_process(
    subcommand: str, session_dir: str, timeout_s: float = 15.0
) -> psutil.Process:
    """Wait for a `raygo <subcommand>` process of this session to appear."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        proc = _find_raygo_process(subcommand, session_dir)
        if proc is not None:
            return proc
        time.sleep(0.2)
    raise AssertionError(
        f"no 'raygo {subcommand}' process appeared within {timeout_s}s"
    )


def _assert_raygo_identity(proc: psutil.Process, subcommand: str) -> List[str]:
    """Assert the process runs the raygo binary with the given subcommand."""
    cmdline = proc.cmdline()
    assert cmdline, f"raygo {subcommand} process lost its cmdline"
    assert (
        cmdline[0] == RAYGO_EXECUTABLE
    ), f"expected {subcommand} to run {RAYGO_EXECUTABLE}, got {cmdline[0]}"
    assert (
        len(cmdline) > 1 and cmdline[1] == subcommand
    ), f"expected subcommand {subcommand}, got {cmdline[1:]}"
    return cmdline


def _assert_raygo_process_alive(proc: psutil.Process) -> None:
    """Assert the process is still alive and not a zombie."""
    assert proc.is_running(), "raygo process exited early"
    assert (
        proc.status() != psutil.STATUS_ZOMBIE
    ), "raygo process is a zombie (exited, not yet reaped)"


def _assert_no_raygo_processes(session_dir: str) -> None:
    """Assert every raygo process of this session died with the cluster."""
    deadline = time.time() + 15
    while time.time() < deadline:
        if not _raygo_processes(session_dir):
            return
        time.sleep(0.5)
    leftover = [proc.cmdline() for proc in _raygo_processes(session_dir)]
    raise AssertionError(f"raygo processes survived shutdown: {leftover}")


def _cmdline_flag_value(cmdline: List[str], flag: str) -> Optional[str]:
    """Return the value of an `--flag=value` style command line flag."""
    prefix = f"{flag}="
    for part in cmdline:
        if part.startswith(prefix):
            return part[len(prefix) :]
    return None


def _read_port_file(path: str, timeout_s: float = 15.0) -> int:
    """Wait for a persisted port file and return its integer content."""
    deadline = time.time() + timeout_s
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            with open(path) as fh:
                return int(fh.read().strip())
        except (OSError, ValueError) as error:
            last_error = error
            time.sleep(0.2)
    raise AssertionError(f"port file {path} never became readable: {last_error}")


def test_go_dashboard_agent_runs_and_persists_ports(monkeypatch):
    """The raylet-launched Go dashboard agent runs and persists its ports.

    The agent owns the port-discovery file protocol of
    go/internal/dashboard/agent/port_persistence.go: three
    `{port_name}_{node_id_hex}` files in the session directory, with
    metrics_export_port persisted as -1 because the Go agent does not export
    metrics.
    """
    monkeypatch.setattr(ray_constants, "ENABLE_GO_DASHBOARD_AGENT", True)
    ctx = ray.init(address="local", num_cpus=1)
    try:
        session_dir = ctx.address_info["session_dir"]
        node_id = ctx.address_info["node_id"]

        proc = _await_raygo_process("dashboard-agent", session_dir)
        _assert_raygo_identity(proc, "dashboard-agent")
        ports = {
            port_name: _read_port_file(
                os.path.join(session_dir, f"{port_name}_{node_id}")
            )
            for port_name in (
                "metrics_agent_port",
                "metrics_export_port",
                "dashboard_agent_listen_port",
            )
        }
        assert ports["metrics_export_port"] == -1
        assert ports["metrics_agent_port"] > 0
        assert ports["dashboard_agent_listen_port"] > 0
    finally:
        ray.shutdown()
    _assert_no_raygo_processes(session_dir)


def test_go_dashboard_head_serves(monkeypatch):
    """The Go dashboard head runs and serves the web UI."""
    # requests is imported lazily so the module stays collectable (and
    # skippable) on minimal installs without the dependency.
    import requests

    monkeypatch.setattr(ray_constants, "ENABLE_GO_DASHBOARD_HEAD", True)
    ctx = ray.init(address="local", num_cpus=1, include_dashboard=True)
    try:
        session_dir = ctx.address_info["session_dir"]

        proc = _await_raygo_process("dashboard", session_dir)
        _assert_raygo_identity(proc, "dashboard")

        webui_url = ctx.address_info["webui_url"]
        if not webui_url.startswith("http"):
            webui_url = f"http://{webui_url}"
        response = requests.get(webui_url, timeout=10, proxies=_NO_PROXY)
        assert response.status_code == 200
    finally:
        ray.shutdown()
    _assert_no_raygo_processes(session_dir)


def test_go_log_monitor_stays_running(monkeypatch):
    """The Go log monitor runs against the live cluster and stays up."""
    monkeypatch.setattr(ray_constants, "ENABLE_GO_LOG_MONITOR", True)
    ctx = ray.init(address="local", num_cpus=1)
    try:
        session_dir = ctx.address_info["session_dir"]

        proc = _await_raygo_process("log-monitor", session_dir)
        _assert_raygo_identity(proc, "log-monitor")
        time.sleep(_SURVIVAL_WINDOW_S)
        _assert_raygo_process_alive(proc)
    finally:
        ray.shutdown()
    _assert_no_raygo_processes(session_dir)


def test_go_setup_worker_executes_tasks(monkeypatch):
    """Workers start through the Go setup worker and execute real tasks.

    On Unix the setup worker exec's into the worker process, so the
    `raygo setup_worker` argv is transient by design; the durable local
    evidence is (a) the live raylet carrying the raygo setup_worker command
    for its workers and (b) a task completing end-to-end through that exec
    chain.
    """
    monkeypatch.setattr(ray_constants, "ENABLE_GO_SETUP_WORKER", True)
    ctx = ray.init(address="local", num_cpus=2)
    try:
        session_dir = ctx.address_info["session_dir"]

        # Match this session's raylet only: its argv carries the session dir,
        # so a concurrent foreign cluster's raylet cannot be picked.
        session_flag = f"--session_dir={session_dir}"
        raylet_cmdline: Optional[List[str]] = None
        for proc in psutil.process_iter(["cmdline"]):
            cmdline = proc.info["cmdline"] or []
            if cmdline and cmdline[0].endswith("/raylet") and session_flag in cmdline:
                raylet_cmdline = cmdline
                break
        assert raylet_cmdline is not None, "no raylet process found for this session"
        worker_command = _cmdline_flag_value(raylet_cmdline, "--python_worker_command")
        assert worker_command is not None, "raylet has no python worker command"
        expected_prefix = (
            f"{RAYGO_EXECUTABLE} "
            f"{ray_constants.RAYGO_AVAILABLE_COMMAND_SETUP_WORKER} "
        )
        assert worker_command.startswith(
            expected_prefix
        ), f"raylet worker command does not use the Go setup worker: {worker_command}"

        @ray.remote
        def add(x, y):
            return x + y

        assert ray.get([add.remote(i, i) for i in range(4)]) == [0, 2, 4, 6]
    finally:
        ray.shutdown()
    _assert_no_raygo_processes(session_dir)


def test_go_runtime_env_agent_serves_http(monkeypatch):
    """The Go runtime env agent runs and serves its HTTP service.

    The agent binds `--node-ip-address` with an OS-assigned port when
    `--runtime-env-agent-port=0`, and reports the bound port through the
    `runtime_env_agent_port_{node_id}` file in the session directory; a
    protobuf POST to /get_runtime_envs_info (empty request is a valid empty
    message) proves the service is live.
    """
    import requests

    monkeypatch.setattr(ray_constants, "ENABLE_GO_RUNTIME_ENV_AGENT", True)
    ctx = ray.init(address="local", num_cpus=1)
    try:
        session_dir = ctx.address_info["session_dir"]
        node_id = ctx.address_info["node_id"]

        proc = _await_raygo_process("runtime-env-agent", session_dir)
        cmdline = _assert_raygo_identity(proc, "runtime-env-agent")
        host = _cmdline_flag_value(cmdline, "--node-ip-address")
        assert host, "runtime env agent cmdline lacks --node-ip-address"
        assert (
            _cmdline_flag_value(cmdline, "--runtime-env-agent-port") is not None
        ), "runtime env agent cmdline lacks --runtime-env-agent-port"

        port = _read_port_file(
            os.path.join(session_dir, f"runtime_env_agent_port_{node_id}")
        )

        response = requests.post(
            f"http://{host}:{port}/get_runtime_envs_info",
            data=b"",
            timeout=10,
            proxies=_NO_PROXY,
        )
        assert response.status_code == 200
    finally:
        ray.shutdown()
    _assert_no_raygo_processes(session_dir)


def test_go_autoscaler_monitor_survives_locally(monkeypatch):
    """The Go autoscaler monitor runs against the local GCS and stays up.

    The local form starts the real monitor against the local cluster's GCS;
    it is asserted to survive the launch window. The full autoscaling
    deployment shape is covered by the real-machine matrix.
    """
    monkeypatch.setattr(ray_constants, "ENABLE_GO_AUTOSCALER_MONITOR", True)
    ctx = ray.init(address="local", num_cpus=1)
    try:
        session_dir = ctx.address_info["session_dir"]

        proc = _await_raygo_process("monitor", session_dir)
        _assert_raygo_identity(proc, "monitor")
        time.sleep(_SURVIVAL_WINDOW_S)
        _assert_raygo_process_alive(proc)
    finally:
        ray.shutdown()
    _assert_no_raygo_processes(session_dir)
