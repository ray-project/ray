"""A sandbox that runs out of memory dies alone.

Each test drives a sandbox past its memory limit while observers stand by: a
neighbor sandbox that keeps a heartbeat, a process outside every sandbox, and
the runtime that owns them all. It then checks where the kernel's OOM kills
landed. A hog stops at a few times its limit, so a limit that doesn't hold
fails the test instead of taking the node's memory.

Needs Linux, a delegated cgroup v2 subtree in RAY_SANDBOX_CGROUP_PATH, and
real gVisor (TEST_SANDBOX=1).
"""

import concurrent.futures as cf
import json
import os
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, TypeVar

import pytest

from ray.experimental.sandbox.exceptions import SandboxError
from ray.experimental.sandbox.runtime import SandboxRuntime

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or not os.environ.get("RAY_SANDBOX_CGROUP_PATH"),
    reason="Set RAY_SANDBOX_CGROUP_PATH to a delegated cgroup v2 subtree",
)

_IMAGE = os.environ.get("RAY_SANDBOX_TEST_IMAGE", "python:3.12-slim")
_LIMIT_MIB = 256
# How far past its limit a hog goes before it gives up and reports success.
_CAP_MIB = 4 * _LIMIT_MIB
_REASON = "Sandbox exceeded its enforced memory limit"

# bytes * n fills every byte, so every MiB is charged, not just reserved.
_ANONYMOUS_HOG = textwrap.dedent(
    f"""
    held = []
    for mib in range(16, {_CAP_MIB} + 1, 16):
        held.append(b"x" * (16 << 20))
        print(mib, flush=True)
    print("ALLOCATED_ALL", flush=True)
    """
)

# A tmpfs file lives in the Sentry's memory, not on any disk.
_TMPFS_HOG = textwrap.dedent(
    f"""
    with open("/dev/shm/fill", "wb") as f:
        for mib in range(16, {_CAP_MIB} + 1, 16):
            f.write(b"x" * (16 << 20))
            f.flush()
            print(mib, flush=True)
    print("ALLOCATED_ALL", flush=True)
    """
)

# Eight processes that each take an eighth of the cap: none of them is large,
# so only a kill of the whole group stops all of them.
_FORKING_HOG = textwrap.dedent(
    f"""
    import os, time
    for child in range(8):
        if os.fork() == 0:
            held = [b"x" * (1 << 20) for _ in range({_CAP_MIB} // 8)]
            open(f"/tmp/child-{{child}}", "w").close()
            time.sleep(600)
            os._exit(0)
    deadline = time.time() + 60
    while time.time() < deadline:
        if all(os.path.exists(f"/tmp/child-{{c}}") for c in range(8)):
            print("ALLOCATED_ALL", flush=True)
            break
        time.sleep(0.1)
    """
)

# Holds some memory and records, every 100 ms, the time and the longest gap
# between two of its beats.
_KEEPER = textwrap.dedent(
    """
    import json, os, time
    held = b"k" * (64 << 20)
    last, worst = time.time(), 0.0
    while True:
        now = time.time()
        worst, last = max(worst, now - last), now
        with open("/tmp/beat.tmp", "w") as f:
            json.dump({"t": now, "worst_gap": worst}, f)
        os.replace("/tmp/beat.tmp", "/tmp/beat.json")
        time.sleep(0.1)
    """
)

T = TypeVar("T")


def _wait(probe: Callable[[], Optional[T]], timeout: float, what: str) -> T:
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"Timed out after {timeout:g} s waiting for {what}")
        time.sleep(0.2)


def _events(cgroup: Path, name: str = "memory.events") -> Dict[str, int]:
    lines = (cgroup / name).read_text().splitlines()
    return {key: int(value) for key, value in (line.split() for line in lines)}


def _populated(cgroup: Path) -> bool:
    try:
        lines = (cgroup / "cgroup.events").read_text().splitlines()
    except FileNotFoundError:
        return False
    return dict(line.split() for line in lines).get("populated") == "1"


def _need_headroom(mib: int) -> None:
    """Skip unless the node could absorb every hog even if no limit held."""
    meminfo = dict(
        line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines()
    )
    available_mib = int(meminfo["MemAvailable"].split()[0]) // 1024
    if available_mib < mib + 2048:
        pytest.skip(f"Needs {mib + 2048} MiB available, the node has {available_mib}")


def _create(runtime: SandboxRuntime, network: str = "none") -> str:
    return runtime.create(
        _IMAGE,
        network=network,
        readonly=False,
        timeout_seconds=300,
        env={"PATH": "/usr/local/bin:/usr/bin:/bin"},
        cpu=1,
        memory=f"{_LIMIT_MIB}Mi",
    )


def _cgroup(runtime: SandboxRuntime, sandbox_id: str) -> Path:
    return Path(runtime.backend._sandbox_metadata[sandbox_id]["cgroup"].path)


class _Observers:
    """A neighbor sandbox with a heartbeat, and a process outside every sandbox."""

    def __init__(self, runtime: SandboxRuntime, network: str = "none") -> None:
        self.runtime = runtime
        self.neighbor = _create(runtime, network)
        self.neighbor_cgroup = _cgroup(runtime, self.neighbor)
        runtime.write_file(self.neighbor, "/tmp/keeper.py", _KEEPER)
        started = runtime.exec(
            self.neighbor,
            ["sh", "-c", "nohup python3 /tmp/keeper.py >/dev/null 2>&1 &"],
            timeout=60,
        )
        assert started.exit_code == 0, started.stderr
        _wait(self._beat, 60, "the neighbor's first heartbeat")
        self.canary = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "x = b'c' * (32 << 20)\nimport time\ntime.sleep(3600)",
            ]
        )
        self.parent = Path(os.environ["RAY_SANDBOX_CGROUP_PATH"])
        self.parent_oom = self._parent_oom()

    def _beat(self) -> Optional[dict]:
        result = self.runtime.exec(self.neighbor, ["cat", "/tmp/beat.json"], timeout=30)
        if result.exit_code != 0 or not result.stdout.strip():
            return None
        return json.loads(result.stdout)

    def _parent_oom(self) -> Optional[int]:
        # memory.events.local counts only the parent's own limit, not its
        # children's.
        if not (self.parent / "memory.events.local").exists():
            return None
        return _events(self.parent, "memory.events.local").get("oom", 0)

    def assert_untouched(self) -> None:
        events = _events(self.neighbor_cgroup)
        assert events.get("oom", 0) == 0 and events.get("oom_kill", 0) == 0, events
        beat = self._beat()
        assert beat is not None, "The neighbor's heartbeat is gone"
        assert time.time() - beat["t"] < 5, beat
        assert beat["worst_gap"] < 5, beat
        assert self.runtime.exec(self.neighbor, ["true"], timeout=30).exit_code == 0
        assert self.canary.poll() is None, "A process outside every sandbox died"
        assert self._parent_oom() == self.parent_oom

    def close(self) -> None:
        self.canary.kill()
        self.canary.wait()
        self.runtime.delete(self.neighbor)


def _drive_past_limit(runtime: SandboxRuntime, sandbox_id: str, hog: str) -> Path:
    """Run ``hog`` in the sandbox and check that its own limit killed it whole."""
    cgroup = _cgroup(runtime, sandbox_id)
    assert (cgroup / "memory.oom.group").read_text().strip() == "1"
    assert (cgroup / "memory.swap.max").read_text().strip() == "0"
    limit = int((cgroup / "memory.max").read_text())
    runtime.write_file(sandbox_id, "/tmp/hog.py", hog)
    try:
        result = runtime.exec(sandbox_id, ["python3", "/tmp/hog.py"], timeout=120)
        stdout, exit_code = result.stdout, result.exit_code
    except SandboxError:
        # The sandbox died under the command.
        stdout, exit_code = "", None
    assert (
        "ALLOCATED_ALL" not in stdout
    ), f"Allocated {_CAP_MIB} MiB in a sandbox limited to {_LIMIT_MIB} MiB"
    assert exit_code != 0

    def killed() -> Optional[Dict[str, int]]:
        events = _events(cgroup)
        return events if events.get("oom_kill", 0) else None

    # The kill came from this sandbox's own limit...
    events = _wait(killed, 30, "the sandbox's OOM kill")
    assert events["max"] >= 1 and events["oom"] >= 1, events
    if "oom_group_kill" in events:
        assert events["oom_group_kill"] >= 1, events
    if (cgroup / "memory.peak").exists():
        # The kernel lets an OOM victim charge a few pages past the limit so
        # that it can exit, so allow a little slack. A limit that didn't hold
        # would show the hog's whole cap.
        assert int((cgroup / "memory.peak").read_text()) <= limit * 1.05
    # ...and every process of the sandbox went with it: the Sentry, the
    # gofer, the systrap stubs and, for a public sandbox, slirp4netns.
    _wait(lambda: not _populated(cgroup), 30, "every process in the sandbox to exit")
    reason = _wait(
        lambda: runtime.get_termination_reason(sandbox_id), 30, "the termination reason"
    )
    assert reason == _REASON
    with pytest.raises(SandboxError, match="memory limit"):
        runtime.exec(sandbox_id, ["true"], timeout=30)
    return cgroup


@pytest.mark.parametrize("network", ["none", "public"])
def test_out_of_memory_kills_the_sandbox_and_nothing_else(network, request):
    if network == "public":
        request.getfixturevalue("ensure_slirp4netns")
    _need_headroom(2 * _CAP_MIB)
    runtime = SandboxRuntime()
    observers = _Observers(runtime, network)
    try:
        hog = _create(runtime, network)
        cgroup = _drive_past_limit(runtime, hog, _ANONYMOUS_HOG)
        observers.assert_untouched()
        runtime.delete(hog)
        assert not cgroup.exists()
        # The runtime that owned it still creates and runs sandboxes.
        fresh = _create(runtime)
        try:
            alive = runtime.exec(fresh, ["python3", "-c", "print('alive')"], timeout=60)
            assert alive.stdout.strip() == "alive"
        finally:
            runtime.delete(fresh)
    finally:
        observers.close()


def test_tmpfs_files_count_against_the_memory_limit():
    _need_headroom(2 * _CAP_MIB)
    runtime = SandboxRuntime()
    observers = _Observers(runtime)
    try:
        hog = _create(runtime)
        shm = runtime.exec(hog, ["sh", "-c", "stat -f -c %T /dev/shm"], timeout=30)
        if shm.exit_code != 0 or shm.stdout.strip() != "tmpfs":
            runtime.delete(hog)
            pytest.skip("The sandbox has no tmpfs at /dev/shm")
        cgroup = _drive_past_limit(runtime, hog, _TMPFS_HOG)
        observers.assert_untouched()
        runtime.delete(hog)
        assert not cgroup.exists()
    finally:
        observers.close()


def test_out_of_memory_kills_every_process_in_the_sandbox():
    _need_headroom(2 * _CAP_MIB)
    runtime = SandboxRuntime()
    observers = _Observers(runtime)
    try:
        hog = _create(runtime)
        cgroup = _drive_past_limit(runtime, hog, _FORKING_HOG)
        observers.assert_untouched()
        runtime.delete(hog)
        assert not cgroup.exists()
    finally:
        observers.close()


def test_concurrent_out_of_memory_kills_stay_contained():
    hogs = max(2, min(6, len(os.sched_getaffinity(0)) // 4))
    _need_headroom((hogs + 1) * _CAP_MIB)
    runtime = SandboxRuntime()
    observers = _Observers(runtime)
    probes: List[Tuple[float, Optional[int]]] = []
    stop = threading.Event()

    def probe_neighbor() -> None:
        while not stop.is_set():
            start = time.monotonic()
            try:
                exit_code = runtime.exec(
                    observers.neighbor, ["true"], timeout=30
                ).exit_code
            except SandboxError:
                exit_code = None
            probes.append((time.monotonic() - start, exit_code))
            stop.wait(0.2)

    ids: List[str] = []
    try:
        with cf.ThreadPoolExecutor(hogs) as pool:
            ids = list(pool.map(lambda _: _create(runtime), range(hogs)))
        prober = threading.Thread(target=probe_neighbor, daemon=True)
        prober.start()
        try:
            with cf.ThreadPoolExecutor(hogs) as pool:
                cgroups = list(
                    pool.map(
                        lambda sid: _drive_past_limit(runtime, sid, _ANONYMOUS_HOG), ids
                    )
                )
        finally:
            stop.set()
            prober.join(timeout=60)
        assert probes, "The neighbor was never probed during the OOM kills"
        assert all(code == 0 for _, code in probes), probes
        assert max(latency for latency, _ in probes) < 10, probes
        observers.assert_untouched()
        for sid, cgroup in zip(ids, cgroups):
            runtime.delete(sid)
            assert not cgroup.exists()
        ids = []
    finally:
        for sid in ids:
            runtime.delete(sid)
        observers.close()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
