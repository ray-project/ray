"""Kernel enforcement tests requiring delegated cgroup v2 and real gVisor."""

import json
import os
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from ray.experimental.sandbox.runtime import SandboxRuntime

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or not os.environ.get("RAY_SANDBOX_CGROUP_PATH"),
    reason="Set RAY_SANDBOX_CGROUP_PATH to a delegated cgroup v2 subtree",
)

_BURN = """
import json, multiprocessing as mp, sys, time
def burn(_):
    value = 0
    for index in range(20_000_000):
        value += index
if __name__ == '__main__':
    start = time.monotonic()
    with mp.Pool(int(sys.argv[1])) as pool:
        pool.map(burn, range(int(sys.argv[1])))
    print(json.dumps({'seconds': time.monotonic() - start}))
"""


def _create(network, **kwargs):
    runtime = SandboxRuntime()
    sid = runtime.create(
        os.environ.get("RAY_SANDBOX_TEST_IMAGE", "python:3.12-slim"),
        network=network,
        readonly=False,
        timeout_seconds=300,
        env={"PATH": "/usr/local/bin:/usr/bin:/bin"},
        **kwargs,
    )
    return runtime, sid


@pytest.mark.parametrize("network", ["none", "public"])
def test_busy_workers_cannot_exceed_cpu_bandwidth(network, request):
    if network == "public":
        request.getfixturevalue("ensure_slirp4netns")
    runtime, sid = _create(network, cpu=1, memory="512Mi")
    try:
        runtime.write_file(sid, "/tmp/burn.py", _BURN)
        cgroup = runtime.backend._sandbox_metadata[sid]["cgroup"].path
        before = dict(
            line.split() for line in (cgroup / "cpu.stat").read_text().splitlines()
        )
        start = time.monotonic()
        one = runtime.exec(sid, ["python3", "/tmp/burn.py", "1"], timeout=120)
        four = runtime.exec(sid, ["python3", "/tmp/burn.py", "4"], timeout=120)
        elapsed = time.monotonic() - start
        after = dict(
            line.split() for line in (cgroup / "cpu.stat").read_text().splitlines()
        )
        assert one.exit_code == four.exit_code == 0
        assert int(after["usage_usec"]) - int(before["usage_usec"]) <= elapsed * 1.15e6
        assert (
            json.loads(four.stdout)["seconds"] > json.loads(one.stdout)["seconds"] * 2.5
        )
    finally:
        runtime.delete(sid)


@pytest.mark.parametrize("network", ["none", "public"])
def test_shared_sandbox_reports_its_cpu_budget(network, request):
    """nproc reports max(2, ceil(cpu)) instead of every CPU on the node."""
    if network == "public":
        request.getfixturevalue("ensure_slirp4netns")
    if len(os.sched_getaffinity(0)) <= 3:
        pytest.skip("Needs more than three CPUs")
    for cpu, expected in ((1, "2"), (2.5, "3")):
        runtime, sid = _create(network, cpu=cpu)
        try:
            assert runtime.exec(sid, "nproc").stdout.strip() == expected
        finally:
            runtime.delete(sid)


@pytest.mark.parametrize("network", ["none", "public"])
def test_memory_limit_contains_oom_and_preserves_worker(network, request):
    if network == "public":
        request.getfixturevalue("ensure_slirp4netns")
    runtime, sid = _create(network, cpu=1, memory="512Mi")
    try:
        result = runtime.exec(
            sid,
            ["python3", "-c", "x=bytearray(1536*1024*1024); print('exceeded limit')"],
            timeout=120,
        )
        assert result.exit_code != 0
        assert "exceeded limit" not in result.stdout
        assert (
            runtime.get_termination_reason(sid)
            == "Sandbox exceeded its enforced memory limit"
        )
    finally:
        runtime.delete(sid)
    # A new sandbox proves the hosting worker and its runtime remain usable.
    next_sid = runtime.create(
        os.environ.get("RAY_SANDBOX_TEST_IMAGE", "python:3.12-slim"),
        cpu=1,
        memory="512Mi",
        timeout_seconds=300,
        env={"PATH": "/usr/local/bin:/usr/bin:/bin"},
    )
    try:
        assert (
            runtime.exec(next_sid, ["python3", "-c", "print('alive')"]).stdout.strip()
            == "alive"
        )
    finally:
        runtime.delete(next_sid)


@pytest.mark.skipif(
    not os.environ.get("RAY_SANDBOX_EXCLUSIVE_CGROUP_PATH"),
    reason="Set RAY_SANDBOX_EXCLUSIVE_CGROUP_PATH to a provisioned exclusive partition",
)
def test_exclusive_cpu_is_visible_and_cannot_escape(ensure_slirp4netns):
    runtime, sid = _create("public", cpu=1, cpu_isolation="exclusive", memory="512Mi")
    try:
        assert runtime.exec(sid, ["nproc"]).stdout.strip() == "1"
        # Asking for all CPUs cannot expand the host cgroup's allowed set.
        result = runtime.exec(
            sid,
            [
                "python3",
                "-c",
                "import os; os.sched_setaffinity(0, range(4096)); print(len(os.sched_getaffinity(0)))",
            ],
        )
        assert result.exit_code == 0
        assert result.stdout.strip() == "1"
    finally:
        runtime.delete(sid)


def test_fractional_cpu_quota_is_exact():
    runtime, sid = _create("none", cpu=0.25, memory="512Mi")
    try:
        runtime.write_file(sid, "/tmp/burn.py", _BURN)
        cgroup = runtime.backend._sandbox_metadata[sid]["cgroup"].path
        before = dict(
            line.split() for line in (cgroup / "cpu.stat").read_text().splitlines()
        )
        started = time.monotonic()
        result = runtime.exec(sid, ["python3", "/tmp/burn.py", "1"], timeout=120)
        elapsed = time.monotonic() - started
        after = dict(
            line.split() for line in (cgroup / "cpu.stat").read_text().splitlines()
        )
        assert result.exit_code == 0
        assert (cgroup / "cpu.max").read_text().strip() == "25000 100000"
        assert int(after["usage_usec"]) - int(before["usage_usec"]) <= elapsed * 0.35e6
    finally:
        runtime.delete(sid)


def test_worker_crash_kills_sandbox_and_releases_cgroup():
    # Abrupt exit bypasses every Python destructor and runtime cleanup path.
    code = """
import os
from ray.experimental.sandbox.runtime import SandboxRuntime
runtime = SandboxRuntime()
sid = runtime.create(os.environ.get('RAY_SANDBOX_TEST_IMAGE', 'python:3.12-slim'),
                     cpu=1, memory='512Mi', timeout_seconds=300,
                     env={'PATH':'/usr/local/bin:/usr/bin:/bin'})
print(sid, flush=True)
os._exit(0)
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=300
    )
    assert result.returncode == 0, result.stderr
    sid = result.stdout.strip()
    path = Path(os.environ["RAY_SANDBOX_CGROUP_PATH"]) / sid
    deadline = time.monotonic() + 10
    while path.exists() and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not path.exists(), f"Orphaned running sandbox cgroup: {path}"


def _stop_reapers() -> None:
    for entry in Path("/proc").iterdir():
        try:
            argv = (entry / "cmdline").read_bytes().split(b"\0")
        except OSError:
            continue
        if b"--reap" in argv and entry.name.isdigit():
            os.kill(int(entry.name), signal.SIGKILL)


def test_worker_exit_that_kills_its_children_still_reaps_the_sandbox():
    """Ray kills a worker's child processes when it exits. The reaper must
    survive that, kill the sandbox, and delete its runsc state and bundle."""
    # Only a reaper that this worker starts can then do the cleanup.
    _stop_reapers()
    code = """
import os, signal
from pathlib import Path
from ray.experimental.sandbox.runtime import SandboxRuntime
runtime = SandboxRuntime()
sid = runtime.create(os.environ.get('RAY_SANDBOX_TEST_IMAGE', 'python:3.12-slim'),
                     cpu=1, memory='512Mi', timeout_seconds=300,
                     env={'PATH':'/usr/local/bin:/usr/bin:/bin'})
print(sid, flush=True)
# What Ray's CoreWorker::KillChildProcs does as the worker exits.
for entry in Path('/proc').iterdir():
    try:
        stat = (entry / 'stat').read_text()
    except OSError:
        continue
    if entry.name.isdigit() and int(stat.rsplit(')', 1)[1].split()[1]) == os.getpid():
        os.kill(int(entry.name), signal.SIGKILL)
os._exit(0)
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=300
    )
    assert result.returncode == 0, result.stderr
    sid = result.stdout.strip()
    cgroup_path = Path(os.environ["RAY_SANDBOX_CGROUP_PATH"]) / sid
    bundle = Path("/tmp/ray/sandbox") / sid
    deadline = time.monotonic() + 15
    while (cgroup_path.exists() or bundle.exists()) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not cgroup_path.exists(), f"Orphaned running sandbox cgroup: {cgroup_path}"
    assert not bundle.exists(), f"Orphaned sandbox bundle: {bundle}"
    state = subprocess.run(
        ["runsc", "--root", "/tmp/runsc", "state", sid], capture_output=True
    )
    assert state.returncode != 0, "runsc still has the orphan's container state"


@pytest.mark.skipif(
    not os.environ.get("RAY_SANDBOX_EXCLUSIVE_CGROUP_PATH"),
    reason="An exclusive cgroup partition is required",
)
def test_concurrent_allocators_cannot_reserve_the_same_cores():
    from ray.experimental.sandbox._internal.cgroup import SandboxCgroupManager

    manager = SandboxCgroupManager()
    count = len(manager._cores(manager.exclusive))

    def allocate(index):
        independent = SandboxCgroupManager()
        try:
            return independent.allocate(
                f"ray-sandbox-{index:012x}", count, 0, "exclusive"
            )
        except RuntimeError as exc:
            assert "Not enough exclusive" in str(exc)
            return None

    leases = []
    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            leases = [
                lease for lease in pool.map(allocate, [1, 2, 3]) if lease is not None
            ]
        assert len(leases) == 1
    finally:
        for lease in leases:
            lease.release()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
