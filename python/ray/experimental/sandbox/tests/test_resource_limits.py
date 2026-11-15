"""Kernel enforcement tests requiring delegated cgroup v2 and real gVisor."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
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


def _cpu_seconds(cgroup):
    stat = dict(line.split() for line in (cgroup / "cpu.stat").read_text().splitlines())
    return int(stat["usage_usec"]) / 1e6


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
        # A new sandbox's first run is slow enough to hide the slowdown on a
        # fast CPU, so time a second one.
        burn = ["python3", "/tmp/burn.py"]
        assert runtime.exec(sid, [*burn, "1"], timeout=120).exit_code == 0
        one = runtime.exec(sid, [*burn, "1"], timeout=120)
        before = _cpu_seconds(cgroup)
        start = time.monotonic()
        four = runtime.exec(sid, [*burn, "4"], timeout=120)
        elapsed = time.monotonic() - start
        assert one.exit_code == four.exit_code == 0
        # The sandbox runs on two CPUs, but four busy workers share one.
        assert _cpu_seconds(cgroup) - before <= elapsed * 1.15
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


def test_memory_limit_in_si_units_starts():
    """The kernel rounds memory.max down to whole pages, and 1GB isn't a
    whole number of pages."""
    runtime, sid = _create("none", memory="1GB")
    try:
        assert runtime.exec(sid, ["true"]).exit_code == 0
    finally:
        runtime.delete(sid)


def test_fractional_cpu_quota_is_exact():
    runtime, sid = _create("none", cpu=0.25, memory="512Mi")
    try:
        runtime.write_file(sid, "/tmp/burn.py", _BURN)
        cgroup = runtime.backend._sandbox_metadata[sid]["cgroup"].path
        before = _cpu_seconds(cgroup)
        started = time.monotonic()
        result = runtime.exec(sid, ["python3", "/tmp/burn.py", "1"], timeout=120)
        elapsed = time.monotonic() - started
        assert result.exit_code == 0
        assert (cgroup / "cpu.max").read_text().strip() == "25000 100000"
        assert _cpu_seconds(cgroup) - before <= elapsed * 0.35
    finally:
        runtime.delete(sid)


# Creates a sandbox with limits, prints its ID, and exits without deleting
# it. With "kill-children", it first kills its child processes, as Ray's
# CoreWorker::KillChildProcs does when a worker exits.
_OWNER = """
import os, signal, sys
from pathlib import Path
from ray.experimental.sandbox.runtime import SandboxRuntime
runtime = SandboxRuntime()
sid = runtime.create(os.environ.get('RAY_SANDBOX_TEST_IMAGE', 'python:3.12-slim'),
                     cpu=1, memory='512Mi', timeout_seconds=300,
                     env={'PATH':'/usr/local/bin:/usr/bin:/bin'})
print(sid, flush=True)
if sys.argv[1] == 'kill-children':
    for entry in Path('/proc').iterdir():
        try:
            stat = (entry / 'stat').read_text()
        except OSError:
            continue
        if entry.name.isdigit() and int(stat.rsplit(')', 1)[1].split()[1]) == os.getpid():
            os.kill(int(entry.name), signal.SIGKILL)
os._exit(0)
"""


def _populated(cgroup: Path) -> bool:
    return "populated 1" in (cgroup / "cgroup.events").read_text()


@pytest.mark.parametrize("exit_mode", ["kill-children", "exit"])
def test_reclaim_removes_a_dead_owners_cgroup(exit_mode):
    """A Ray worker's sandbox stops when the worker's children are killed, so
    its cgroup empties at once. A driver's sandbox keeps running in its cgroup
    after the driver exits. Either way the next reclaim pass removes the
    cgroup with the sandbox's runsc state and bundle."""
    result = subprocess.run(
        [sys.executable, "-c", _OWNER, exit_mode],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, result.stderr
    sid = result.stdout.strip()
    cgroup_path = Path(os.environ["RAY_SANDBOX_CGROUP_PATH"]) / sid
    if exit_mode == "kill-children":
        deadline = time.monotonic() + 10
        while _populated(cgroup_path) and time.monotonic() < deadline:
            time.sleep(0.1)
        assert not _populated(cgroup_path), "The sandbox outlived its worker"
    else:
        assert _populated(cgroup_path)

    assert sid in GVisorSandboxBackend()._reclaim_unowned_bundles()
    assert not cgroup_path.exists(), f"Orphaned sandbox cgroup: {cgroup_path}"
    assert not (Path("/tmp/ray/sandbox") / sid).exists()
    state = subprocess.run(
        ["runsc", "--root", "/tmp/runsc", "state", sid], capture_output=True
    )
    assert state.returncode != 0, "runsc still has the orphan's container state"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
