"""Host cgroups that enforce sandbox CPU and memory limits.

Node provisioning delegates a cgroup v2 subtree, which
RAY_SANDBOX_CGROUP_PATH names. Each sandbox with limits runs in a child
cgroup of that subtree, named after the sandbox. The gVisor backend tracks
who owns each sandbox with its bundle's owner file, and removes the cgroup
when it deletes the sandbox or reclaims the bundle of an owner that died.
"""

import math
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

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


def _remove(cgroup: Path) -> None:
    """Kill every process in a sandbox's cgroup, then remove the cgroup.

    Args:
        cgroup: The cgroup. Nothing happens if it doesn't exist.

    Raises:
        OSError: If the cgroup can't be killed or removed.
        RuntimeError: If the cgroup still holds processes after 5 seconds.
    """
    if not cgroup.exists():
        return
    # cgroup.kill handles concurrent forks and descendants.
    if (cgroup / "cgroup.kill").exists():
        try:
            _write(cgroup / "cgroup.kill", "1")
        except OSError:
            # A failed allocation may have no kill permission, but it
            # hasn't launched a workload. rmdir still refuses a cgroup
            # populated by a concurrent process.
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


def _shared_affinity(
    cpu: float, pool: Set[int], used: Iterable[Iterable[int]]
) -> List[int]:
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
    for cpus in used:
        for c in cpus:
            if c in load:
                load[c] += 1
    return sorted(sorted(load, key=lambda c: (load[c], c))[:count])


class SandboxCgroup:
    """An enforced sandbox budget."""

    def __init__(
        self,
        path: Path,
        limits: Dict[str, str],
        affinity: Optional[List[int]] = None,
    ):
        self.path = path
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
        """Kill whatever still runs in the cgroup, and remove it."""
        _remove(self.path)


class SandboxCgroupManager:
    """Create and remove sandbox cgroups in the subtree that the node delegates.

    The delegated path must be mounted at the same location in every worker
    that shares a node.
    """

    def __init__(self, shared_path: Optional[str] = None):
        shared_path = shared_path or os.environ.get("RAY_SANDBOX_CGROUP_PATH")
        self.shared = Path(shared_path).resolve() if shared_path else None

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

    def choose_cpus(self, cpu: float, used: Iterable[Iterable[int]]) -> List[int]:
        """Choose the CPUs that a sandbox with a CPU limit runs on.

        Args:
            cpu: The sandbox's CPU limit. Zero means no limit.
            used: The CPUs that each other live sandbox on the node runs on.

        Returns:
            The CPUs, or [] to let the sandbox run on any CPU.
        """
        if cpu <= 0:
            return []
        return _shared_affinity(cpu, _pool_cpus(self.shared), used)

    def allocate(
        self, sandbox_id: str, cpu: float, memory: int, cpus: List[int]
    ) -> SandboxCgroup:
        """Create a sandbox's cgroup and write its limits.

        Call ``check`` first.

        Args:
            sandbox_id: The sandbox, which names the cgroup.
            cpu: The CPU limit. Zero means no limit.
            memory: The memory limit in bytes. Zero means no limit.
            cpus: The CPUs from ``choose_cpus``.

        Returns:
            The cgroup, which the sandbox's launch command joins through
            ``SandboxCgroup.wrap``.
        """
        if not _SANDBOX_ID.fullmatch(sandbox_id):
            raise ValueError(f"Invalid sandbox ID: {sandbox_id}")
        limits = {}
        if cpu > 0:
            limits["cpu.max"] = f"{int(cpu * _PERIOD_US)} {_PERIOD_US}"
        if memory > 0:
            # The kernel keeps memory.max in whole pages, rounded down, and
            # verify reads back what was written.
            page = os.sysconf("SC_PAGE_SIZE")
            limits.update(
                {
                    "memory.max": str(memory // page * page),
                    "memory.swap.max": "0",
                    "memory.oom.group": "1",
                }
            )
        path = self.shared / sandbox_id
        path.mkdir()
        try:
            for name, value in limits.items():
                _write(path / name, value)
            if not (path / "cgroup.kill").exists():
                raise RuntimeError("Sandbox enforcement requires cgroup.kill support")
            if not os.access(path / "cgroup.kill", os.W_OK):
                raise RuntimeError(
                    "Sandbox enforcement requires permission to write cgroup.kill"
                )
        except Exception:
            _remove(path)
            raise
        return SandboxCgroup(path, limits, cpus)

    def remove(self, sandbox_id: str) -> None:
        """Remove a sandbox's cgroup, if this node has one for it.

        The path comes from this process's own setting and the sandbox ID,
        never from a file that others may be able to write.

        Args:
            sandbox_id: The sandbox, which names the cgroup.
        """
        if self.shared is not None and _SANDBOX_ID.fullmatch(sandbox_id):
            _remove(self.shared / sandbox_id)
