"""Node-local sandbox cgroups and physical-core leases.

Node provisioning delegates the shared subtree and, optionally, an exclusive
cpuset partition. All workers using an exclusive partition share a file lock
and durable leases. A reaper runs outside the sandbox cgroups, independently
of the workers that own them. This module's CLI uses only the standard library
so the reaper doesn't need to initialize Ray.
"""

import argparse
import fcntl
import hashlib
import json
import logging
import os
import re
import shutil
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
            # Keep the lease, and any cores it reserves, for a later pass.
            # One lease that can't be checked or removed mustn't fail every
            # allocation on the node.
            logger.warning("Failed to reclaim sandbox lease %s: %s", record_path, err)


def _clean_up_orphan(record: Dict) -> None:
    """Delete what a dead owner's sandbox leaves after its cgroup is killed.

    The kill stops the workload, but runsc's container state, the bundle
    (with the writable overlay's backing file) and the image's in-use marker
    would otherwise stay on the node.
    """
    cleanup = record.get("cleanup") or {}
    runsc_args = cleanup.get("runsc_args")
    if runsc_args:
        try:
            subprocess.run(
                [*runsc_args, "delete", "-force", Path(record["path"]).name],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=10,
            )
        except (OSError, subprocess.TimeoutExpired):
            pass
    for path in cleanup.get("remove", []):
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
        else:
            try:
                os.remove(path)
            except OSError:
                pass


def _reconcile_lease(record_path: Path, record: Dict) -> None:
    if not Path(record["path"]).exists():
        # Cgroups don't survive a node reboot. A reused owner identity
        # mustn't retain a lease for a cgroup that no longer exists.
        record_path.unlink(missing_ok=True)
        return
    invalid_partition = False
    if record["reserved_cpus"]:
        pool = Path(record["path"]).parent
        invalid_partition = _read(pool / "cpuset.cpus.partition") not in (
            "root",
            "isolated",
        ) or not set(record["reserved_cpus"]) <= _cpus(
            _read(pool / "cpuset.cpus.effective")
        )
        try:
            # CPU hotplug can bring a sibling online outside the pool
            # without invalidating the kernel's partition flag.
            SandboxCgroupManager._cores(pool)
        except (OSError, RuntimeError):
            invalid_partition = True
    owner_gone = _process_identity(record["owner_pid"]) != record["owner_starttime"]
    if invalid_partition or owner_gone:
        _remove(record_path, record)
        if owner_gone:
            _clean_up_orphan(record)


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


class SandboxCgroup:
    """An enforced sandbox budget and optional exclusive-core allocation."""

    def __init__(self, path: Path, state_dir: Path, limits: Dict[str, str]):
        self.path = path
        self.state_dir = state_dir
        self.limits = limits

    def wrap(self, command: List[str]) -> List[str]:
        launcher = Path(__file__).with_name("cgroup_launcher.py")
        return [sys.executable, "-S", str(launcher), str(self.path), *command]

    def verify(self, pid: int) -> None:
        if "cpuset.cpus" in self.limits and _read(
            self.path.parent / "cpuset.cpus.partition"
        ) not in ("root", "isolated"):
            raise RuntimeError("Sandbox exclusive cpuset partition is no longer valid")
        if "cpuset.cpus" in self.limits:
            SandboxCgroupManager._cores(self.path.parent)
        for name, value in self.limits.items():
            actual = _read(self.path / name)
            if name == "cpuset.cpus":
                actual = _cpu_list(_cpus(_read(self.path / "cpuset.cpus.effective")))
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

    The delegated paths must be mounted at the same locations in workers
    sharing a node, and the state directory must be shared by those workers.
    The exclusive subtree must already be a valid cpuset partition whose
    CPUs contain every online SMT sibling of each physical core.
    """

    def __init__(
        self,
        shared_path: Optional[str] = None,
        exclusive_path: Optional[str] = None,
        state_path: Optional[str] = None,
    ):
        shared_path = shared_path or os.environ.get("RAY_SANDBOX_CGROUP_PATH")
        exclusive_path = exclusive_path or os.environ.get(
            "RAY_SANDBOX_EXCLUSIVE_CGROUP_PATH"
        )
        self.shared = Path(shared_path).resolve() if shared_path else None
        self.exclusive = Path(exclusive_path).resolve() if exclusive_path else None
        identity = str(self.exclusive or self.shared)
        digest = hashlib.sha256(identity.encode()).hexdigest()[:16]
        state_root = state_path or os.environ.get(
            "RAY_SANDBOX_CGROUP_STATE_DIR", "/tmp/ray/sandbox/cgroup-state"
        )
        self.state_dir = Path(state_root) / digest

    def check(self, cpu: float, memory: int, isolation: str) -> None:
        root = self.exclusive if isolation == "exclusive" else self.shared
        setting = (
            "RAY_SANDBOX_EXCLUSIVE_CGROUP_PATH"
            if isolation == "exclusive"
            else "RAY_SANDBOX_CGROUP_PATH"
        )
        if sys.platform != "linux" or root is None:
            raise RuntimeError(
                f"Sandbox resource enforcement requires Linux and a delegated "
                f"cgroup v2 subtree. Set {setting} on the worker node."
            )
        if not _is_cgroup2(root):
            raise RuntimeError(f"{setting}={root} isn't on a cgroup v2 filesystem")
        if not (root / "cgroup.kill").exists():
            raise RuntimeError("Sandbox enforcement requires cgroup.kill support")
        if not os.access(root, os.W_OK | os.X_OK):
            raise RuntimeError(f"Sandbox cgroup subtree {root} isn't writable")
        if (
            self.shared is not None
            and self.exclusive is not None
            and (
                self.shared == self.exclusive
                or self.exclusive in self.shared.parents
                or self.shared in self.exclusive.parents
            )
        ):
            raise RuntimeError(
                "Shared and exclusive sandbox pools must be disjoint subtrees"
            )
        required = {"cpu"} if cpu > 0 else set()
        if memory > 0:
            required.add("memory")
        if isolation == "exclusive":
            required.add("cpuset")
        enabled = set(_read(root / "cgroup.subtree_control").split())
        if not required <= enabled:
            raise RuntimeError(
                f"Enable {sorted(required - enabled)} controllers in {root}'s "
                "cgroup.subtree_control before starting sandbox workers"
            )
        if _read(root / "cgroup.procs"):
            raise RuntimeError(f"Delegated sandbox parent {root} contains processes")
        if isolation == "exclusive":
            if _read(root / "cpuset.cpus.partition") not in ("root", "isolated"):
                raise RuntimeError(f"{root} isn't a valid exclusive cpuset partition")
            self._cores(root)

    @staticmethod
    def _cores(root: Path) -> List[Set[int]]:
        available = _cpus(_read(root / "cpuset.cpus.effective"))
        online = _cpus(_read(Path("/sys/devices/system/cpu/online")))
        cores = set()
        for cpu in sorted(available):
            siblings = (
                _cpus(
                    _read(
                        Path(
                            f"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"
                        )
                    )
                )
                & online
            )
            if not siblings or not siblings <= available:
                raise RuntimeError(
                    f"Exclusive partition {root} omits online SMT siblings of CPU {cpu}"
                )
            cores.add(tuple(sorted(siblings)))
        return [set(core) for core in sorted(cores)]

    def allocate(
        self,
        sandbox_id: str,
        cpu: float,
        memory: int,
        isolation: str,
        cleanup: Optional[Dict] = None,
    ) -> SandboxCgroup:
        if not _SANDBOX_ID.fullmatch(sandbox_id):
            raise ValueError(f"Invalid sandbox ID: {sandbox_id}")
        self.check(cpu, memory, isolation)
        root = self.exclusive if isolation == "exclusive" else self.shared
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        with _locked(self.state_dir):
            _reconcile(self.state_dir)
            leases = list(_records(self.state_dir))
            reserved = set()
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
            if isolation == "exclusive":
                owned = {Path(record["path"]).name for _, record in leases}
                foreign = {
                    child.name for child in root.iterdir() if child.is_dir()
                } - owned
                if foreign:
                    raise RuntimeError(
                        f"Unmanaged cgroups in exclusive pool: {foreign}"
                    )
                used = {core for _, r in leases for core in r["reserved_cpus"]}
                cores = [core for core in self._cores(root) if not core & used]
                if len(cores) < int(cpu):
                    raise RuntimeError(
                        f"Not enough exclusive physical cores for {cpu} CPUs"
                    )
                selected = cores[: int(cpu)]
                reserved = set().union(*selected)
                # Reserve every sibling but expose only one hardware thread
                # per physical core. nproc agrees with the integer CPU budget.
                limits["cpuset.mems"] = _read(root / "cpuset.mems.effective")
                limits["cpuset.cpus"] = _cpu_list({min(core) for core in selected})
            path = root / sandbox_id
            record_path = self.state_dir / f"{sandbox_id}.json"
            if path.exists() or record_path.exists():
                raise RuntimeError(f"Sandbox allocation already exists: {sandbox_id}")
            record = {
                "path": str(path),
                "owner_pid": os.getpid(),
                "owner_starttime": _process_identity(os.getpid()),
                "reserved_cpus": sorted(reserved),
                # What the reaper deletes after killing an orphan: the
                # runsc command prefix for `delete -force`, and paths.
                "cleanup": cleanup or {},
            }
            if record["owner_starttime"] is None:
                raise RuntimeError("Cannot identify sandbox owner process")
            # Record ownership before mkdir so an interrupted allocation can
            # always be reconciled, including a worker killed during startup.
            temporary = record_path.with_suffix(".tmp")
            temporary.write_text(json.dumps(record))
            temporary.replace(record_path)
            cgroup = SandboxCgroup(path, self.state_dir, limits)
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
