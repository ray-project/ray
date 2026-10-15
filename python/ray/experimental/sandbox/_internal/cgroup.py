"""Node-local sandbox cgroups and their leases.

Node provisioning delegates a cgroup v2 subtree. The workers on a node share a
file lock and durable leases. A reaper runs outside the sandbox cgroups,
independently of the workers that own them. This module's CLI uses only the
standard library so the reaper doesn't need to initialize Ray.
"""

import argparse
import fcntl
import hashlib
import json
import logging
import math
import os
import re
import shutil
import stat
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)

_SANDBOX_ID = re.compile(r"ray-sandbox-[0-9a-f]{12}\Z")
_PERIOD_US = 100000


def _read(path: Path) -> str:
    return path.read_text().strip()


def _write(path: Path, value: str) -> None:
    # Never create a missing controller file on a regular filesystem.
    # cgroup.kill is write-only, including for the delegated owner.
    fd = os.open(path, os.O_WRONLY)
    try:
        os.write(fd, value.encode())
    finally:
        os.close(fd)


def _cpus(value: str) -> Set[int]:
    result = set()
    for part in value.split(","):
        if not part:
            continue
        first, _, last = part.partition("-")
        result.update(range(int(first), int(last or first) + 1))
    return result


def _cpu_list(cpus: Set[int]) -> str:
    return ",".join(str(cpu) for cpu in sorted(cpus))


def _is_cgroup2(path: Path) -> bool:
    # Use the longest matching mount, including mounts hidden by another
    # filesystem. Merely finding cgroup.controllers isn't sufficient.
    matches = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        before, after = line.split(" - ", 1)
        mount = before.split()[4]
        mount = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), mount)
        if path == Path(mount) or Path(mount) in path.parents:
            matches.append((len(mount), after.split()[0]))
    return bool(matches) and max(matches)[1] == "cgroup2"


def _process_identity(pid: int) -> Optional[str]:
    try:
        # starttime distinguishes a live owner from a reused PID. Zombies
        # cannot own sandboxes even though their /proc entries still exist.
        fields = _read(Path(f"/proc/{pid}/stat")).rsplit(")", 1)[1].split()
        return None if fields[0] == "Z" else fields[19]
    except (FileNotFoundError, ProcessLookupError):
        # ESRCH: the process was reaped between open() and read(). Other
        # errors don't prove that the owner is dead, and _reconcile kills
        # the sandboxes of dead owners.
        return None


def _boot_id() -> str:
    return _read(Path("/proc/sys/kernel/random/boot_id"))


def _owner_alive(record: Dict) -> bool:
    # Start times count from boot, so after a reboot another process can
    # have the owner's PID and start time.
    return (
        record["boot_id"] == _boot_id()
        and _process_identity(record["owner_pid"]) == record["owner_starttime"]
    )


@contextmanager
def _locked(state_dir: Path):
    with (state_dir / "lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def _records(state_dir: Path):
    for path in sorted(state_dir.glob("ray-sandbox-*.json")):
        record = json.loads(path.read_text())
        if not _SANDBOX_ID.fullmatch(path.stem):
            raise RuntimeError(f"Invalid sandbox lease: {path}")
        if Path(record["path"]).name != path.stem:
            raise RuntimeError(f"Sandbox lease path doesn't match its ID: {path}")
        yield path, record


def _write_record(record_path: Path, record: Dict) -> None:
    temporary = record_path.with_suffix(".tmp")
    temporary.write_text(json.dumps(record))
    temporary.replace(record_path)


def _remove(record_path: Path, record: Dict) -> None:
    cgroup = Path(record["path"])
    if cgroup.exists():
        # cgroup.kill handles concurrent forks and descendants. Don't free
        # the lease if killing or removing the cgroup fails.
        if (cgroup / "cgroup.kill").exists():
            try:
                _write(cgroup / "cgroup.kill", "1")
            except OSError:
                # A failed allocation may have no kill permission, but it
                # hasn't launched a workload. rmdir still refuses a cgroup
                # populated by a concurrent process, preserving its lease.
                if "populated 1" in _read(cgroup / "cgroup.events"):
                    raise
        elif "populated 1" in _read(cgroup / "cgroup.events"):
            raise RuntimeError(f"Cannot kill populated sandbox cgroup: {cgroup}")
        deadline = time.monotonic() + 5
        while "populated 1" in _read(cgroup / "cgroup.events"):
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Sandbox cgroup is still populated: {cgroup}")
            time.sleep(0.02)
        cgroup.rmdir()
    record_path.unlink(missing_ok=True)


def _reconcile(state_dir: Path) -> None:
    for record_path, record in _records(state_dir):
        try:
            _reconcile_lease(record_path, record)
        except (OSError, RuntimeError) as err:
            # Keep the lease for a later pass. One lease that can't be
            # checked or removed mustn't fail every allocation on the node.
            logger.warning("Failed to reclaim sandbox lease %s: %s", record_path, err)


def _private_dir(path: str) -> bool:
    try:
        st = os.lstat(path)
    except OSError:
        return False
    return (
        stat.S_ISDIR(st.st_mode)
        and st.st_uid == os.geteuid()
        and not st.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
    )


def _clean_up_orphan(record: Dict) -> None:
    """Delete what a dead owner's sandbox leaves after its cgroup is killed.

    The kill stops the workload, but runsc's container state, the bundle
    (with the writable overlay's backing file) and the image's in-use marker
    would otherwise stay on the node.

    The reaper may run as root, and leases sit under /tmp/ray, which Ray
    makes world-writable. So a lease never supplies a command: the reaper
    runs runsc with known flags, only on a state directory that it owns,
    and deletes only paths named after the sandbox.
    """
    sandbox_id = Path(record["path"]).name
    cleanup = record.get("cleanup") or {}
    runsc_root = cleanup.get("runsc_root")
    if isinstance(runsc_root, str) and _private_dir(runsc_root):
        rootless = ["--rootless"] if cleanup.get("rootless") is True else []
        try:
            subprocess.run(
                ["runsc", *rootless, "--ignore-cgroups", "--root", runsc_root]
                + ["delete", "-force", sandbox_id],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=10,
            )
        except (OSError, subprocess.TimeoutExpired):
            pass
    for path in cleanup.get("remove", []):
        if not isinstance(path, str) or os.path.basename(path) != sandbox_id:
            continue
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
        else:
            try:
                os.remove(path)
            except OSError:
                pass


def _reconcile_lease(record_path: Path, record: Dict) -> None:
    if not _owner_alive(record):
        # Nobody else will delete the sandbox's files, whether or not its
        # cgroup still exists.
        _remove(record_path, record)
        _clean_up_orphan(record)
    elif record["affinity"] and not Path(record["path"]).exists():
        # A cgroup that's gone holds no workload, so its CPUs stop counting
        # when sandboxes are spread. Keep the lease: the owner deletes the
        # sandbox's files, and if it dies first, the lease is how the reaper
        # finds them.
        _write_record(record_path, {**record, "affinity": []})


def _reap(state_dir: Path, detach: bool = False) -> None:
    with (state_dir / "reaper.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        # Ray kills a worker's child processes when the worker exits, which
        # is exactly when its sandboxes need reaping, so the reaper detaches
        # from the worker that starts it. It forks only once it holds the
        # lock: a duplicate exits above as the worker's child, which reaps
        # it, rather than as an orphan that the pod's init may never reap.
        # The forked reaper keeps the lock through the inherited descriptor.
        if detach and os.fork() > 0:
            os._exit(0)
        # Stay alive until node shutdown. Allocation can race an idle
        # reaper's exit, leaving a new sandbox without a crash monitor.
        while True:
            try:
                with _locked(state_dir):
                    _reconcile(state_dir)
            except Exception:
                logger.exception("Failed to reap sandbox cgroups")
            time.sleep(1)


# gVisor gives a sandbox at least this many vCPUs, because some applications
# drop locking on a single CPU. runsc applies the same floor when it sizes a
# sandbox from a cgroup quota.
_MIN_SANDBOX_CPUS = 2


def _pool_cpus(root: Path) -> Set[int]:
    """CPUs that sandboxes in the shared pool may run on."""
    effective = root / "cpuset.cpus.effective"
    if effective.exists():
        return _cpus(_read(effective))
    return set(os.sched_getaffinity(0))


def _shared_affinity(cpu: float, pool: Set[int], leases: List[Dict]) -> List[int]:
    """CPUs for a shared sandbox's affinity, or [] to leave it unrestricted.

    gVisor sizes a sandbox's vCPUs from the CPUs the sandbox may run on, not
    from its cgroup quota, so a shared sandbox sees every CPU in ``nproc``
    and tools size their worker pools for the whole node. Restricting it to
    ``max(2, ceil(cpu))`` CPUs makes ``nproc`` match its budget. The CPUs
    that the fewest other shared sandboxes use are chosen, so sandboxes
    spread over the pool.
    """
    count = max(_MIN_SANDBOX_CPUS, math.ceil(cpu))
    if count >= len(pool):
        return []
    load = {c: 0 for c in pool}
    for record in leases:
        for c in record.get("affinity", ()):
            if c in load:
                load[c] += 1
    return sorted(sorted(load, key=lambda c: (load[c], c))[:count])


class SandboxCgroup:
    """An enforced sandbox budget."""

    def __init__(
        self,
        path: Path,
        state_dir: Path,
        limits: Dict[str, str],
        affinity: Optional[List[int]] = None,
    ):
        self.path = path
        self.state_dir = state_dir
        self.limits = limits
        self.affinity = affinity or []

    def wrap(self, command: List[str]) -> List[str]:
        launcher = Path(__file__).with_name("cgroup_launcher.py")
        cpus = ["--cpus", _cpu_list(set(self.affinity))] if self.affinity else []
        return [sys.executable, "-S", str(launcher), str(self.path), *cpus, *command]

    def verify(self, pid: int) -> None:
        for name, value in self.limits.items():
            actual = _read(self.path / name)
            if actual != value:
                raise RuntimeError(f"Sandbox {name} is {actual!r}, expected {value!r}")
        if str(pid) not in _read(self.path / "cgroup.procs").splitlines():
            raise RuntimeError(f"Sandbox launcher {pid} isn't in {self.path}")

    def termination_reason(self) -> Optional[str]:
        events = self.path / "memory.events"
        if events.exists():
            values = dict(line.split() for line in _read(events).splitlines())
            if int(values.get("oom_kill", "0")):
                return "Sandbox exceeded its enforced memory limit"
        return None

    def release(self) -> None:
        with _locked(self.state_dir):
            record_path = self.state_dir / f"{self.path.name}.json"
            if record_path.exists():
                _remove(record_path, json.loads(record_path.read_text()))


class SandboxCgroupManager:
    """Coordinate resource ownership across sandbox workers on one node.

    The delegated path must be mounted at the same location in workers
    sharing a node, and the state directory must be shared by those workers.
    """

    def __init__(
        self,
        shared_path: Optional[str] = None,
        state_path: Optional[str] = None,
    ):
        shared_path = shared_path or os.environ.get("RAY_SANDBOX_CGROUP_PATH")
        self.shared = Path(shared_path).resolve() if shared_path else None
        digest = hashlib.sha256(str(self.shared).encode()).hexdigest()[:16]
        state_root = state_path or os.environ.get(
            "RAY_SANDBOX_CGROUP_STATE_DIR", "/tmp/ray/sandbox/cgroup-state"
        )
        self.state_dir = Path(state_root) / digest

    def check(self, cpu: float, memory: int) -> None:
        root = self.shared
        if sys.platform != "linux" or root is None:
            raise RuntimeError(
                "Sandbox resource enforcement requires Linux and a delegated "
                "cgroup v2 subtree. Set RAY_SANDBOX_CGROUP_PATH on the worker node."
            )
        if not _is_cgroup2(root):
            raise RuntimeError(
                f"RAY_SANDBOX_CGROUP_PATH={root} isn't on a cgroup v2 filesystem"
            )
        if not (root / "cgroup.kill").exists():
            raise RuntimeError("Sandbox enforcement requires cgroup.kill support")
        if not os.access(root, os.W_OK | os.X_OK):
            raise RuntimeError(f"Sandbox cgroup subtree {root} isn't writable")
        required = {"cpu"} if cpu > 0 else set()
        if memory > 0:
            required.add("memory")
        enabled = set(_read(root / "cgroup.subtree_control").split())
        if not required <= enabled:
            raise RuntimeError(
                f"Enable {sorted(required - enabled)} controllers in {root}'s "
                "cgroup.subtree_control before starting sandbox workers"
            )
        if _read(root / "cgroup.procs"):
            raise RuntimeError(f"Delegated sandbox parent {root} contains processes")

    def allocate(
        self,
        sandbox_id: str,
        cpu: float,
        memory: int,
        cleanup: Optional[Dict] = None,
    ) -> SandboxCgroup:
        if not _SANDBOX_ID.fullmatch(sandbox_id):
            raise ValueError(f"Invalid sandbox ID: {sandbox_id}")
        self.check(cpu, memory)
        root = self.shared
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        with _locked(self.state_dir):
            _reconcile(self.state_dir)
            leases = [record for _, record in _records(self.state_dir)]
            limits = {}
            if cpu > 0:
                limits["cpu.max"] = f"{int(cpu * _PERIOD_US)} {_PERIOD_US}"
            if memory > 0:
                limits.update(
                    {
                        "memory.max": str(memory),
                        "memory.swap.max": "0",
                        "memory.oom.group": "1",
                    }
                )
            affinity = (
                _shared_affinity(cpu, _pool_cpus(root), leases) if cpu > 0 else []
            )
            path = root / sandbox_id
            record_path = self.state_dir / f"{sandbox_id}.json"
            if path.exists() or record_path.exists():
                raise RuntimeError(f"Sandbox allocation already exists: {sandbox_id}")
            record = {
                "path": str(path),
                "owner_pid": os.getpid(),
                "owner_starttime": _process_identity(os.getpid()),
                "boot_id": _boot_id(),
                "affinity": affinity,
                # What the reaper deletes after killing an orphan: the
                # sandbox's runsc state, and paths named after the sandbox.
                "cleanup": cleanup or {},
            }
            if record["owner_starttime"] is None:
                raise RuntimeError("Cannot identify sandbox owner process")
            # Record ownership before mkdir so an interrupted allocation can
            # always be reconciled, including a worker killed during startup.
            _write_record(record_path, record)
            cgroup = SandboxCgroup(path, self.state_dir, limits, affinity)
            try:
                path.mkdir()
                for name, value in limits.items():
                    _write(path / name, value)
                if not (path / "cgroup.kill").exists():
                    raise RuntimeError(
                        "Sandbox enforcement requires cgroup.kill support"
                    )
                if not os.access(path / "cgroup.kill", os.W_OK):
                    raise RuntimeError(
                        "Sandbox enforcement requires permission to write cgroup.kill"
                    )
                subprocess.Popen(
                    [
                        sys.executable,
                        "-S",
                        __file__,
                        "--reap",
                        str(self.state_dir),
                        "--detach",
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
            except Exception:
                _remove(record_path, record)
                raise
        return cgroup


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reap", type=Path, required=True)
    parser.add_argument("--detach", action="store_true")
    args = parser.parse_args()
    _reap(args.reap, detach=args.detach)
