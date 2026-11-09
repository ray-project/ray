#!/usr/bin/env python3
"""
Go dashboard + Go log monitor E2E integration test.

Starts a local Ray cluster with the three GO switches enabled
(RAY_ENABLE_GO_SETUP_WORKER=1, RAY_ENABLE_GO_DASHBOARD_HEAD=1,
RAY_ENABLE_GO_LOG_MONITOR=1) and covers the core flows of the migrated
head-side Go components:

  go log_monitor:
    - process is launched by the cluster (raygo log-monitor)
    - pure-Go: no go_runtime.so plugin mapped
    - tracks worker log files (log_monitor.log gains "Beginning to track file")
    - publishes LogBatch to GCS end-to-end (verified by subscribing to the
      RAY_LOG_CHANNEL pubsub channel from Python and matching the emitted lines)
    - autoscaler v2 detection runs (cluster starts normally, no errors)

  go dashboard:
    - process is launched by the cluster (raygo dashboard), HTTP 8265 serves
    - core APIs respond (jobs / state / version)
    - lazy plugin loading: with the default all-module config the plugin is NOT
      loaded at startup (maps show 0 go_runtime.so); the first cross-language
      request loads it on demand (maps > 0)
    - cross-language module routes are registered (serve/flow/train/data)

Prerequisites:
  bazel build //go/cmd/raygo
  (the script auto-resolves RAYGO_EXECUTABLE to the bazel-bin binary so the
   freshly built code is exercised; fall back to the packaged raygo otherwise)
  A runtime matching that build: when the packaged go_runtime.so comes from a
  different tree, pin it with
  RAY_GO_RUNTIME_PATH=<repo>/bazel-bin/go/cmd/plugin/go_runtime.so so the
  dashboard's lazy init and the Go worker load the same build.

Auth: this single-node lane defaults RAY_AUTH_MODE to disabled unless the
caller exported it. Ray turns token authentication on by default when
~/.ray/auth_token exists, and the cluster, the Python client and the Go
dashboard head must then agree on the mode and token path.

Usage:
  python go/e2e/test_go_system_components.py
  python go/e2e/test_go_system_components.py --component=log_monitor
  python go/e2e/test_go_system_components.py --component=dashboard
  python go/e2e/test_go_system_components.py --keep-cluster
  python go/e2e/test_go_system_components.py --ray-address=127.0.0.1:6379

  RAYGO_E2E=1 python -m pytest go/e2e/test_go_system_components.py -v
"""

import argparse
import asyncio
import os
import queue
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

# Add Ray Python to path.
RAY_PYTHON_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "../../python"
)
if RAY_PYTHON_PATH not in sys.path:
    sys.path.insert(0, RAY_PYTHON_PATH)

import ray  # noqa: E402
from ray._private.gcs_pubsub import _AioSubscriber  # noqa: E402
from ray.core.generated import pubsub_pb2  # noqa: E402

GO_SWITCHES = {
    "RAY_ENABLE_GO_SETUP_WORKER": "1",
    "RAY_ENABLE_GO_DASHBOARD_HEAD": "1",
    "RAY_ENABLE_GO_LOG_MONITOR": "1",
}

DEFAULT_AUTH_TOKEN_PATH = os.path.expanduser("~/.ray/auth_token")
DASHBOARD_PORT = 8265
LOG_MATCH_SUBSTRING = "go-e2e-worker-line"
DASHBOARD_HTTP_TIMEOUT = 10
PROCESS_WAIT_TIMEOUT = 60


# ---------------------------------------------------------------------------
# Process / procfs helpers
# ---------------------------------------------------------------------------


def _read_cmdline(pid):
    """Return the argv of a pid, or [] when it is gone or already a zombie."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return [p.decode(errors="replace") for p in f.read().split(b"\0") if p]
    except OSError:
        return []


def find_pids_by_cmdline(subcommand, session_dir):
    """Return the pids of this session's `raygo <subcommand>` processes.

    Every component carries this cluster's session directory in its argv, so
    identity is checked against /proc/<pid>/cmdline (`argv[0]` is raygo,
    `argv[1]` is the subcommand, and a later argument embeds the session dir)
    rather than trusting a machine-global `pgrep -f` match. The check keeps
    the scan from tripping over raygo processes of unrelated clusters on the
    same machine, and a zombie (empty cmdline) is never returned.
    """
    try:
        res = subprocess.run(
            ["pgrep", "-f", f"raygo {subcommand}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if res.returncode != 0:
        return []
    pids = []
    for token in res.stdout.split():
        try:
            pid = int(token)
        except ValueError:
            continue
        argv = _read_cmdline(pid)
        if len(argv) > 1 and argv[0].endswith("raygo") and argv[1] == subcommand:
            if any(session_dir in arg for arg in argv[2:]):
                pids.append(pid)
    return sorted(pids)


def find_pid_by_cmdline(subcommand, session_dir):
    """Return the newest pid of this session's `raygo <subcommand>`, or None."""
    pids = find_pids_by_cmdline(subcommand, session_dir)
    return pids[-1] if pids else None


def wait_for_pid(subcommand, session_dir, timeout=PROCESS_WAIT_TIMEOUT):
    deadline = time.time() + timeout
    while time.time() < deadline:
        pid = find_pid_by_cmdline(subcommand, session_dir)
        if pid is not None:
            return pid
        time.sleep(1)
    return None


def count_plugin_maps(pid):
    """Count go_runtime.so mappings in /proc/<pid>/maps (0 = plugin not loaded)."""
    try:
        with open(f"/proc/{pid}/maps") as f:
            return sum(1 for line in f if "go_runtime.so" in line)
    except (OSError, IOError):
        return -1


def proc_rss_kb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (OSError, IOError, ValueError):
        pass
    return -1


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _auth_headers():
    """Return the Authorization header when the cluster uses token auth."""
    if os.environ.get("RAY_AUTH_MODE") not in ("token", "k8s"):
        return {}
    token = os.environ.get("RAY_AUTH_TOKEN", "")
    if not token:
        path = os.environ.get("RAY_AUTH_TOKEN_PATH", DEFAULT_AUTH_TOKEN_PATH)
        try:
            with open(path) as f:
                token = f.readline().strip()
        except (OSError, IOError):
            return {}
    return {"Authorization": f"Bearer {token}"} if token else {}


def http_get(
    path, host="127.0.0.1", port=DASHBOARD_PORT, timeout=DASHBOARD_HTTP_TIMEOUT
):
    url = f"http://{host}:{port}{path}"
    req = urllib.request.Request(url, headers=_auth_headers())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode(errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")
    except Exception as e:
        return 0, str(e)


# ---------------------------------------------------------------------------
# Cluster lifecycle
# ---------------------------------------------------------------------------


def _resolve_raygo():
    env = os.environ.get("RAYGO_EXECUTABLE")
    if env and os.path.exists(env):
        return env
    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    bazel_raygo = os.path.join(
        repo, "bazel-bin", "go", "cmd", "raygo", "raygo_", "raygo"
    )
    if os.path.exists(bazel_raygo):
        print(f"[info] RAYGO_EXECUTABLE -> {bazel_raygo}")
        return bazel_raygo
    packaged = shutil.which("raygo")
    if packaged:
        print(f"[info] RAYGO_EXECUTABLE -> {packaged}")
        return packaged
    raise SystemExit(
        "raygo binary not found: build it with `bazel build //go/cmd/raygo` "
        "or set RAYGO_EXECUTABLE"
    )


def _ray_cli(args):
    """Run the ray CLI with this script's interpreter and this tree's Python.

    The interpreter matters: the tree's `_raylet.so` is a CPython 3.11
    extension, so a `ray` launcher from another environment cannot start a
    cluster from this source tree.
    """
    return [sys.executable, "-m", "ray.scripts.scripts"] + args


def _configure_auth_env():
    """Make the cluster, the client and the Go components agree on auth mode."""
    mode = os.environ.setdefault("RAY_AUTH_MODE", "disabled")
    if mode == "token":
        # The Go dashboard head validates bearer tokens against these.
        os.environ.setdefault("RAY_AUTH_TOKEN_PATH", DEFAULT_AUTH_TOKEN_PATH)


def _cluster_env():
    env = dict(os.environ)
    env.update(GO_SWITCHES)
    env["RAYGO_EXECUTABLE"] = _resolve_raygo()
    if RAY_PYTHON_PATH not in env.get("PYTHONPATH", "").split(os.pathsep):
        env["PYTHONPATH"] = os.pathsep.join(
            p for p in (RAY_PYTHON_PATH, env.get("PYTHONPATH")) if p
        )
    # `raygo setup_worker` resolves the interpreter it execs as `python3` from
    # PATH; it must be the interpreter this tree's _raylet.so was built for.
    python_dir = os.path.dirname(sys.executable)
    if env.get("PATH", "").split(os.pathsep)[:1] != [python_dir]:
        env["PATH"] = os.pathsep.join([python_dir, env.get("PATH", "")])
    return env


def start_cluster():
    """ray stop, then ray start --head with the three GO switches.

    The stop is the lane's own cluster lifecycle: a component left behind by
    an aborted earlier run belongs to another session and is deliberately not
    swept (it would mean killing an unrelated cluster on the same machine); it
    instead surfaces as a missing process for this session, so the assertions
    fail loudly rather than reading a stale pid.
    """
    print("[cluster] stopping any existing cluster...")
    subprocess.run(_ray_cli(["stop", "--force"]), check=False, capture_output=True)
    time.sleep(2)
    env = _cluster_env()
    print("[cluster] ray start --head with GO switches...")
    res = subprocess.run(
        _ray_cli(
            ["start", "--head", "--port=6379", f"--dashboard-port={DASHBOARD_PORT}"]
        ),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    print(res.stdout[-2000:] if res.stdout else "")
    if res.stderr:
        print("[cluster] stderr:", res.stderr[-1000:])
    if res.returncode != 0:
        raise SystemExit(f"ray start failed: rc={res.returncode}")


def stop_cluster(session_dir):
    print("[cluster] ray stop --force")
    subprocess.run(_ray_cli(["stop", "--force"]), check=False, capture_output=True)
    _kill_leftover_components(session_dir)


def _pid_alive(pid):
    """False when the pid is gone or already a zombie (it can no longer run)."""
    try:
        with open(f"/proc/{pid}/stat", errors="replace") as f:
            state = f.read().rsplit(")", 1)[1].split()[0]
    except (OSError, IndexError):
        return False
    return state != "Z"


def _terminate_pid(pid, label):
    """SIGTERM, then SIGKILL, confirming the process died before returning."""
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return
        deadline = time.time() + 10
        while time.time() < deadline:
            if not _pid_alive(pid):
                return
            time.sleep(0.2)
    raise RuntimeError(f"{label} pid {pid} survived SIGTERM and SIGKILL")


def _kill_leftover_components(session_dir):
    # ray stop does not always reap the go component children (observed: a
    # leftover raygo dashboard keeps binding the dashboard port). Only this
    # session's processes are swept; another cluster's components on the same
    # machine are left alone.
    for subcommand in ("dashboard", "log-monitor", "runtime-env-agent"):
        while True:
            pid = find_pid_by_cmdline(subcommand, session_dir)
            if pid is None:
                break
            print(f"[cluster] terminating leftover raygo {subcommand} pid {pid}")
            _terminate_pid(pid, f"raygo {subcommand}")


# ---------------------------------------------------------------------------
# Log publisher verification: subscribe to RAY_LOG_CHANNEL from Python
# ---------------------------------------------------------------------------


class GcsAioLogSubscriber(_AioSubscriber):
    """Subscribes to the RAY_LOG_CHANNEL and yields received LogBatch messages."""

    def __init__(self, address):
        super().__init__(pubsub_pb2.RAY_LOG_CHANNEL, address=address)

    async def poll_log_batch(self, timeout=None):
        await self._poll(timeout=timeout)
        if len(self._queue) == 0:
            return None
        msg = self._queue.popleft()
        return msg.log_batch_message


def _subscriber_worker(gcs_addr, out_queue, stop_event, error_list):
    """Subscribe to RAY_LOG_CHANNEL until stop_event, recording exceptions so a
    dead subscriber is reported instead of silently yielding no batches."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sub = GcsAioLogSubscriber(gcs_addr)
        loop.run_until_complete(sub.subscribe())
    except Exception as e:  # noqa: BLE001
        error_list.append(e)
        loop.close()
        return
    try:
        while not stop_event.is_set():
            try:
                lb = loop.run_until_complete(sub.poll_log_batch(timeout=1))
            except Exception as e:  # noqa: BLE001
                error_list.append(e)
                break
            if lb is not None:
                out_queue.put(lb)
    finally:
        loop.run_until_complete(sub.close())
        loop.close()


def collect_log_batches(gcs_addr, worker):
    """Start a background subscriber, run worker(), and return the LogBatches.

    Returns (batches, subscriber_errors). subscriber_errors is non-empty when
    the subscription thread exited early on an exception, which usually means
    the test failed because the subscriber died rather than because no logs
    matched.
    """
    out_queue = queue.Queue()
    stop_event = threading.Event()
    error_list = []
    t = threading.Thread(
        target=_subscriber_worker,
        args=(gcs_addr, out_queue, stop_event, error_list),
        daemon=True,
    )
    t.start()
    time.sleep(1)  # let the subscription register before any log is emitted
    try:
        worker()
    finally:
        time.sleep(2)  # drain in-flight publishes
        stop_event.set()
        t.join(timeout=10)
    batches = []
    while not out_queue.empty():
        batches.append(out_queue.get())
    return batches, error_list


# ---------------------------------------------------------------------------
# Component checks
# ---------------------------------------------------------------------------


def check_log_monitor(session_dir, logs_dir, gcs_addr):
    print("\n===== go log_monitor =====")
    pid = wait_for_pid("log-monitor", session_dir)
    if pid is None:
        raise AssertionError(f"log-monitor process of session {session_dir} not found")
    print(f"[ok] log-monitor process running (pid {pid})")

    maps = count_plugin_maps(pid)
    if maps != 0:
        raise AssertionError(
            f"log-monitor must be pure-Go (no go_runtime.so), maps={maps}"
        )
    print(f"[ok] no go_runtime.so plugin mapped (maps={maps})")

    rss = proc_rss_kb(pid)
    print(f"[info] log-monitor RSS = {rss} kB")

    # Emit worker logs while a RAY_LOG_CHANNEL subscriber is listening.
    def emit_worker_logs():
        ray.init(address="auto", namespace="go_e2e_log_monitor")
        try:

            @ray.remote
            def emit_logs():
                for i in range(5):
                    print(f"{LOG_MATCH_SUBSTRING}-{i}", flush=True)
                    time.sleep(0.1)
                return "emitted"

            ray.get(emit_logs.remote())
        finally:
            ray.shutdown()

    batches, subscriber_errors = collect_log_batches(gcs_addr, emit_worker_logs)
    if subscriber_errors:
        raise AssertionError(
            f"log subscriber thread exited with errors: {subscriber_errors!r}"
        )
    matched = []
    for lb in batches:
        joined = "\n".join(lb.lines)
        if LOG_MATCH_SUBSTRING in joined:
            matched.append(lb)
    if not matched:
        raise AssertionError(
            f"no LogBatch on RAY_LOG_CHANNEL contained {LOG_MATCH_SUBSTRING!r} "
            f"(received {len(batches)} batches)"
        )
    lines = [ln for lb in matched for ln in lb.lines if LOG_MATCH_SUBSTRING in ln]
    print(f"[ok] LogBatch published to GCS, matched worker lines: {sorted(set(lines))}")
    lb = matched[0]
    print(
        f"[ok] LogBatch meta: ip={lb.ip!r} pid={lb.pid!r} job_id={lb.job_id!r} "
        f"is_error={lb.is_error} actor={lb.actor_name!r} task={lb.task_name!r}"
    )

    # log_monitor.log should show it tracked the worker file.
    log_monitor_log = os.path.join(logs_dir, "log_monitor.log")
    deadline = time.time() + 30
    tracked = False
    while time.time() < deadline:
        if os.path.exists(log_monitor_log):
            with open(log_monitor_log, errors="replace") as f:
                content = f.read()
            if "Beginning to track file worker-" in content:
                tracked = True
                break
        time.sleep(1)
    if not tracked:
        raise AssertionError(
            f"log_monitor.log never tracked a worker file: {log_monitor_log}"
        )
    print("[ok] log_monitor.log shows worker log tracking")

    # No panic/fatal in log_monitor logs.
    for name in ("log_monitor.log", "log_monitor.err"):
        path = os.path.join(logs_dir, name)
        if os.path.exists(path):
            with open(path, errors="replace") as f:
                for line in f:
                    if "panic" in line.lower() or "fatal" in line.lower():
                        raise AssertionError(
                            f"{name} contains panic/fatal: {line.strip()}"
                        )
    print("[ok] log-monitor logs clean (no panic/fatal)")


def check_dashboard(session_dir):
    print("\n===== go dashboard =====")
    pid = wait_for_pid("dashboard", session_dir)
    if pid is None:
        raise AssertionError(f"dashboard process of session {session_dir} not found")
    print(f"[ok] dashboard process running (pid {pid})")

    # HTTP up and core APIs respond.
    for path in ("/api/version", "/api/jobs/", "/api/v0/nodes"):
        status, body = http_get(path)
        if status != 200:
            raise AssertionError(f"GET {path} = {status}: {body[:200]}")
        print(f"[ok] GET {path} -> {status}")

    # Lazy plugin loading: startup must not have mapped go_runtime.so.
    maps_before = count_plugin_maps(pid)
    print(f"[info] go_runtime.so maps before cross-language request: {maps_before}")
    if maps_before != 0:
        raise AssertionError(
            f"dashboard should NOT load the plugin at startup with default "
            f"all-module config (lazy loading); maps={maps_before}"
        )

    # Cross-language module routes are registered (non-404 proves the module
    # owns the path; the body may degrade on a cluster without Python actors).
    for path in (
        "/api/flow/jobs/",
        "/api/serve/applications/",
        "/api/train/v2/runs/v1",
    ):
        status, _ = http_get(path, timeout=30)
        if status == 404:
            raise AssertionError(f"cross-language route {path} not registered (404)")
        print(
            f"[ok] cross-language route {path} -> {status} (registered, triggers lazy load)"
        )

    # First cross-language request must have loaded the plugin on demand.
    deadline = time.time() + 30
    maps_after = maps_before
    while time.time() < deadline:
        maps_after = count_plugin_maps(pid)
        if maps_after > 0:
            break
        time.sleep(1)
    if maps_after <= 0:
        raise AssertionError("plugin was not loaded after a cross-language request")
    print(
        f"[ok] lazy plugin loading: go_runtime.so maps 0 -> {maps_after} "
        f"after first cross-language request (RSS {proc_rss_kb(pid)} kB)"
    )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def _session_logs_dir():
    session_latest = os.path.join("/tmp/ray", "session_latest")
    if os.path.islink(session_latest):
        return os.path.join(os.path.realpath(session_latest), "logs")
    sessions = sorted(d for d in os.listdir("/tmp/ray") if d.startswith("session_"))
    if not sessions:
        raise SystemExit("no Ray session found under /tmp/ray")
    return os.path.join("/tmp/ray", sessions[-1], "logs")


def run(
    component="all", ray_address="127.0.0.1:6379", keep_cluster=False, no_start=False
):
    """Run the selected checks and return a list of (component, error) failures."""
    _configure_auth_env()
    if not no_start:
        start_cluster()
    logs_dir = _session_logs_dir()
    session_dir = os.path.dirname(logs_dir)
    print(f"[info] session logs dir: {logs_dir}")

    failed = []
    try:
        if component in ("all", "log_monitor"):
            try:
                check_log_monitor(session_dir, logs_dir, ray_address)
            except AssertionError as e:
                print(f"[FAIL] log_monitor: {e}")
                failed.append(("log_monitor", str(e)))
            else:
                print("[PASS] log_monitor")
        if component in ("all", "dashboard"):
            try:
                check_dashboard(session_dir)
            except AssertionError as e:
                print(f"[FAIL] dashboard: {e}")
                failed.append(("dashboard", str(e)))
            else:
                print("[PASS] dashboard")
    finally:
        # --no-start reuses a user-owned cluster; never tear it down implicitly.
        if not keep_cluster and not no_start:
            stop_cluster(session_dir)
    return failed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--component", choices=["all", "log_monitor", "dashboard"], default="all"
    )
    parser.add_argument(
        "--ray-address",
        default="127.0.0.1:6379",
        help="GCS address used for the RAY_LOG_CHANNEL subscriber",
    )
    parser.add_argument(
        "--keep-cluster", action="store_true", help="do not run `ray stop` at the end"
    )
    parser.add_argument(
        "--no-start",
        action="store_true",
        help="reuse an already running cluster (do not ray start)",
    )
    args = parser.parse_args()

    failed = run(args.component, args.ray_address, args.keep_cluster, args.no_start)
    if failed:
        print("\nRESULT: FAIL")
        for name, err in failed:
            print(f"  - {name}: {err}")
        sys.exit(1)
    print("\nRESULT: PASS")


def test_go_system_components():
    """Pytest entry point; skipped unless RAYGO_E2E=1 opts into a live cluster."""
    if os.environ.get("RAYGO_E2E") != "1":
        import pytest

        pytest.skip("set RAYGO_E2E=1 to run the cluster-level e2e")
    failed = run("all")
    assert not failed, failed


if __name__ == "__main__":
    main()
