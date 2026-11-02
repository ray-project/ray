import collections
import fcntl
import glob
import ipaddress
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from typing import IO, Any, Callable, Dict, List, NamedTuple, Optional, Tuple, Union

from ray.experimental.sandbox._internal import egress_guard, overlayfs
from ray.experimental.sandbox._internal.cgroup import (
    SandboxCgroup,
    SandboxCgroupManager,
)
from ray.experimental.sandbox._internal.image_utils import (
    ROOTFS_IMAGE,
    erofs_superblock_fingerprint,
)
from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.backend.checkpoint_utils import (
    CheckpointPath,
    StagedCheckpoint,
    build_restored_config,
    copy_fs_tree,
    create_manifest,
    get_current_node_id,
    load_checkpoint_manifest,
)
from ray.experimental.sandbox.config import (
    DEFAULT_PUBLIC_ALLOWLIST,
    DEFAULT_PUBLIC_DNS,
    INTERNET,
    INTERNET_EXCLUDED_IPV4,
    INTERNET_IPV6,
    SandboxConfig,
    normalize_cidr_allowlist,
    parse_cidr_allowlist_entry,
    parse_memory_bytes,
)
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxExecError,
    SandboxNotFoundError,
    SandboxTimeoutError,
)
from ray.experimental.sandbox.image_manager import BaseImageManager

logger = logging.getLogger(__name__)

# Directory where runsc keeps container state. Every runsc invocation for a
# sandbox must agree on this, otherwise the container cannot be looked up.
_RUNSC_ROOT = "/tmp/runsc"

# How long teardown waits for a sandbox's launch process to exit on its own
# after the container stops, before it kills that process.
_TREE_EXIT_GRACE_SECONDS = 2.0

# Each bundle holds a file that its owner keeps open and locked while the
# sandbox lives. The kernel drops the lock when the owner exits or crashes, so
# a backend in any process can find the bundles that nobody owns anymore.
_OWNER_FILE = ".owner"
_BUNDLE_NAME = re.compile(r"ray-sandbox-[0-9a-f]{12}")
# How often a process looks for bundles nobody owns.
_RECLAIM_INTERVAL_SECONDS = 60.0
_reclaim_lock = threading.Lock()
_last_reclaim = float("-inf")
# A backend holds this lock in _RAY_SANDBOX_DIR while it picks the CPUs of a
# sandbox with a CPU limit and records them in the sandbox's owner file, so
# that sandboxes created at the same time spread over the node's CPUs too.
_CPUS_LOCK_FILE = "cpus.lock"

# Directory for sandbox bundles, cached container images, and per-sandbox
# overlay state.
_RAY_SANDBOX_DIR = "/tmp/ray/sandbox"

# network="public" gives each sandbox a private user+network namespace pair
# bridged by slirp4netns user-mode networking, the rootless-container shape:
# a holder process (`unshare --user --map-root-user --net`) pins the
# namespaces; slirp4netns attaches to them from the pod side, so its uplink
# is the pod's network, and runs in the foreground under the sandbox's
# launch shell; runsc runs inside via nsenter as mapped root. runsc still
# gets --network=host, but "host" is now private to the sandbox: binds cannot
# collide with or be reached by the pod or other sandboxes, while egress
# leaves through slirp4netns's tap. Mount and pid namespaces stay shared, so
# the bundle and runsc's control sockets under _RUNSC_ROOT keep working for
# pod-side state/exec/kill/delete.
# An overlayfs sandbox runs all of this inside its kernel overlay's
# namespaces (see RootfsOverlay.wrap). If those include a user namespace of
# its own, the holder only creates the network namespace. Otherwise the holder
# still creates both, but runsc joins only the network namespace, so it keeps
# running in the host's user namespace, where the overlay is mounted. The
# overlay's mount namespace starts as a copy of the pod's and gains only the
# kernel overlay, so those paths still resolve.
#
# slirp4netns NATs every flow through a fresh, kernel-assigned host port, so
# flows from different sandboxes can never share a host socket (pasta, which
# preserves UDP source ports with SO_REUSEADDR, delivered one sandbox's
# replies to another when their source ports collided). It relays through
# the pod's own sockets, so on its own the sandbox could reach any address
# the pod can reach: other Ray nodes (including the head node's GCS and
# dashboard), other pods, internal services, the Kubernetes API, and cloud
# instance metadata. A pod-level NetworkPolicy can't tell those flows from
# the worker's own.
#
# An nftables ruleset in the sandbox's network namespace (see
# _render_egress_ruleset) narrows that, for every network="public" sandbox.
# It drops each locally generated packet bound for the denylist
# (egress_guard: instance metadata, loopback, the Kubernetes API, and the
# operator's RAY_SANDBOX_EGRESS_DENY) or outside cidr_allowlist, which
# defaults to the public internet, before it reaches the tap. It is
# installed from the pod side, between slirp4netns coming up and runsc
# starting, and a failed install aborts the start.
#
# Why nftables inside the sandbox netns is the right enforcement point:
# Inside the sandbox's private user + net namespace, the host-side launch
# script has CAP_NET_ADMIN over that child network namespace (without
# needing CAP_NET_ADMIN on the Kubernetes worker pod itself).
#
# runsc runs inside that child netns with --network=host. That means
# gVisor's hostinet creates real Linux kernel AF_INET/AF_INET6 sockets
# inside the child netns, so every packet from the sandbox passes through
# the Linux kernel's nf_tables output chain before reaching slirp4netns's
# tap0.
#
# Why sandbox code cannot tamper with the rules (even with CAP_NET_ADMIN):
# gVisor's Sentry intercepts all syscalls inside the sandbox. Even if the
# container process has CAP_NET_ADMIN or CAP_NET_RAW, gVisor's hostinet
# (without --net-raw) never forwards AF_NETLINK (NETLINK_NETFILTER /
# NETLINK_ROUTE) or AF_PACKET sockets to the host kernel. Running nft flush
# ruleset or iptables -F inside the sandbox talks only to gVisor's internal
# Sentry stub, never touching the host kernel netns where the rules live.
#
# The output hook is interface-independent, so added routes or addresses do
# not bypass it either.
#
# These flags are the isolation property; tests pin the exact list:
#   --configure              bring the tap up: network + 100, gateway + 2.
#   --cidr=198.18.0.0/24     the RFC 2544 benchmarking range: it is never
#                            routed on the internet and, unlike the
#                            slirp4netns default 10.0.2.0/24, does not
#                            overlap pod or service CIDRs, which would be
#                            on-link in the sandbox instead of NAT'd.
#   --mtu=65520              the largest MTU slirp4netns supports.
#   --disable-host-loopback  no path from the sandbox to the pod's loopback.
#   --disable-dns            no built-in resolver: the sandbox sees only the
#                            generated resolv.conf, nothing of the host's.
#   --enable-seccomp         a syscall filter on the slirp4netns process.
#                            (--enable-sandbox is not used: its setegid(0)
#                            fails inside a --map-root-user namespace.)
_SLIRP4NETNS_FLAGS = [
    "--configure",
    "--cidr=198.18.0.0/24",
    "--mtu=65520",
    "--disable-host-loopback",
    "--disable-dns",
    "--enable-seccomp",
]

# The launch process stays in its owner's process group (see create_sandbox),
# so a Ctrl-C in a driver's terminal, or a Jupyter kernel interrupt, signals
# it too. Neither should stop a sandbox, so every launch command starts with
# this: the shell ignores SIGINT and SIGQUIT, and so do the commands it runs,
# across exec. runsc keeps SIGINT ignored, but Go installs its own SIGQUIT
# handler, so a SIGQUIT (Ctrl-\) still ends runsc and its sandbox, cleanly.
_IGNORE_INTERRUPTS = "trap '' INT QUIT; "


# The image compressions `runsc checkpoint --compression` takes.
_CHECKPOINT_COMPRESSIONS = ("none", "flate-best-speed")


class _RestoreImage(NamedTuple):
    """A checkpoint image that a sandbox's launch command restores from."""

    # The checkpoint's state directory (``runsc restore --image-path``).
    image_path: str
    # Load guest memory in the background after the sandbox starts running.
    # runsc supports this only for uncompressed images.
    background: bool = False
    # Read the image with O_DIRECT.
    direct: bool = False


def _runsc_args(rootless: bool, ignore_cgroups: bool) -> List[str]:
    """runsc and the global flags that its run/exec/kill/delete share."""
    args = ["runsc"]
    if rootless:
        args.append("--rootless")
    if ignore_cgroups:
        args.append("--ignore-cgroups")
    args.extend(["--root", _RUNSC_ROOT])
    return args


# The egress allowlist ruleset lives in the sandbox's root_dir and is loaded
# with `nft -f` inside the sandbox's network namespace.
_EGRESS_RULES_FILE = "egress.nft"
_EGRESS_TABLE = "ray_sandbox_egress"

# The namespaces set_egress_allowlist enters to reload the ruleset.
_EGRESS_NAMESPACES = ("user", "net")

# Run inside a sandbox's namespaces with resolver addresses as arguments. It
# sends each one a single byte on UDP port 53, which fails at once if the
# egress filter drops the packet or the sandbox has no route to it, and
# prints the first resolver that failed and why.
_RESOLVER_PROBE = """\
import socket, sys
for server in sys.argv[1:]:
    family = socket.AF_INET6 if ":" in server else socket.AF_INET
    with socket.socket(family, socket.SOCK_DGRAM) as sock:
        try:
            sock.connect((server, 53))
            sock.send(b"\\0")
        except OSError as err:
            print(server, err.strerror, sep="\\t")
            sys.exit(3)
"""
_RESOLVER_UNREACHABLE = 3


def _namespace_ids(pid: int) -> Optional[List[Tuple[int, int]]]:
    """Identify the user and network namespaces of a process.

    A namespace's identity is the ``(st_dev, st_ino)`` of its nsfs inode,
    which stays the same for the namespace's whole life.

    Args:
        pid: The process.

    Returns:
        One identity per ``_EGRESS_NAMESPACES`` entry, or None if ``pid`` is
        gone or its namespaces can't be read.
    """
    try:
        stats = [os.stat(f"/proc/{pid}/ns/{ns}") for ns in _EGRESS_NAMESPACES]
    except OSError:
        return None
    return [(st.st_dev, st.st_ino) for st in stats]


def _holder_namespace_ids(root_dir: str) -> Optional[List[Tuple[int, int]]]:
    """Identify the namespaces of the holder named in ``root_dir``'s netns.pid.

    Called right after the sandbox starts, while that holder runs its
    network. A holder never shares this process's network namespace, so a
    pid that does isn't the holder. It does share this process's user
    namespace when it runs an overlayfs sandbox on a worker with mount
    privileges.

    Args:
        root_dir: The sandbox's per-instance directory.

    Returns:
        The holder's namespaces, as ``_namespace_ids`` gives them, or None.
    """
    try:
        with open(os.path.join(root_dir, "netns.pid"), encoding="utf-8") as f:
            pid = f.read().strip()
    except OSError:
        return None
    ids = _namespace_ids(int(pid)) if pid.isdigit() else None
    own = _namespace_ids(os.getpid())
    if ids is None or own is None or ids[1] == own[1]:
        return None
    return ids


def _open_holder_namespaces(
    root_dir: str, expected: Optional[List[Tuple[int, int]]]
) -> Optional[List[int]]:
    """Open the namespaces of the holder named in ``root_dir``'s netns.pid.

    Once the holder exits, its pid can be reused by any process, such as
    another sandbox's holder. So the namespaces found there must be the
    ones in ``expected``, and the caller enters them through the returned
    fds rather than the pid, which could be reused again after this check.

    Args:
        root_dir: The sandbox's per-instance directory.
        expected: The sandbox's namespaces, from ``_namespace_ids``.

    Returns:
        Open fds, one per ``_EGRESS_NAMESPACES`` entry, that the caller
        closes; or None if the holder is gone or not in those namespaces.
    """
    fds: List[int] = []
    try:
        with open(os.path.join(root_dir, "netns.pid"), encoding="utf-8") as f:
            pid = f.read().strip()
        if pid.isdigit() and expected is not None:
            for ns in _EGRESS_NAMESPACES:
                fds.append(os.open(f"/proc/{pid}/ns/{ns}", os.O_RDONLY))
            if [(st.st_dev, st.st_ino) for st in map(os.fstat, fds)] == expected:
                return fds
    except OSError:
        pass
    for fd in fds:
        os.close(fd)
    return None


def _render_egress_ruleset(
    cidrs: List[str], dns_servers: List[str], *, denied: List[str]
) -> str:
    """Render the nftables ruleset that restricts a sandbox's egress to ``cidrs``.

    The ruleset is one transaction: ``flush ruleset`` (the namespace is
    private to the sandbox, so this only ever clears this sandbox's own
    rules) followed by a single ``inet`` table whose output chain drops by
    default. Its rules run in this order:

    1. Loopback (the sandbox's own) and DNS, UDP and TCP port 53, to the
       sandbox's resolvers: accepted.
    2. ``denied``: dropped, so no allowlist entry opens it.
    3. The entries: accepted. An entry with ports accepts only those TCP and
       UDP ports, and ``internet`` accepts every address outside the
       non-public IPv4 networks plus IPv6 global unicast.

    Plain prefix matches, anonymous sets, and a numeric hook priority keep it
    valid on every nft from 0.9 on, and nothing depends on conntrack, so only
    nf_tables itself is needed in the kernel.

    Args:
        cidrs: Allowed destination networks (IPv4 or IPv6, any form
            ``normalize_cidr_allowlist`` accepts).
        dns_servers: Resolver addresses the generated resolv.conf lists.
        denied: Networks no entry opens (``egress_guard.always_denied``).

    Returns:
        The ruleset text, ready for ``nft -f``.

    Raises:
        ValueError: If an entry is not an IP address or network.
    """
    lines = [
        "flush ruleset",
        f"table inet {_EGRESS_TABLE} {{",
        "    chain output {",
        "        type filter hook output priority 0; policy drop;",
        '        oif "lo" accept',
    ]
    for server in dns_servers:
        try:
            address = ipaddress.ip_address(str(server).strip())
        except ValueError:
            raise ValueError(
                f"dns entry {server!r} is not an IP address; network='public' "
                "needs resolver addresses to keep DNS reachable"
            ) from None
        family = "ip6" if address.version == 6 else "ip"
        for proto in ("udp", "tcp"):
            lines.append(f"        {family} daddr {address} {proto} dport 53 accept")
    for family, members in zip(("ip", "ip6"), egress_guard.collapse(denied)):
        if members:
            lines.append(f"        {family} daddr {{ {', '.join(members)} }} drop")
    for entry in normalize_cidr_allowlist(cidrs):
        destination, ports = parse_cidr_allowlist_entry(entry)
        if destination == INTERNET:
            matches = [
                " ".join(f"ip daddr != {net}" for net in INTERNET_EXCLUDED_IPV4),
                f"ip6 daddr {INTERNET_IPV6}",
            ]
        else:
            family = "ip6" if ipaddress.ip_network(destination).version == 6 else "ip"
            matches = [f"{family} daddr {destination}"]
        for match in matches:
            if ports is None:
                lines.append(f"        {match} accept")
            else:
                for proto in ("tcp", "udp"):
                    lines.append(f"        {match} {proto} dport {ports} accept")
    lines.extend(["    }", "}", ""])
    return "\n".join(lines)


def _write_egress_ruleset(
    root_dir: str, cidrs: List[str], dns: Optional[List[str]]
) -> str:
    """Write the egress ruleset for ``cidrs`` into ``root_dir`` atomically.

    The denylist is read afresh, so each write also picks up changes to
    ``RAY_SANDBOX_EGRESS_DENY``.

    Args:
        root_dir: The sandbox's per-instance directory.
        cidrs: Allowed destination networks.
        dns: The sandbox's ``dns`` setting; None means the public defaults,
            the same rule the generated resolv.conf follows.

    Returns:
        The path of the written ruleset.

    Raises:
        Exception: If the denylist can't be read (for example, an entry in
            ``RAY_SANDBOX_EGRESS_DENY`` isn't an IP address or network) or
            the file can't be written. Nothing is written then.
    """
    resolvers = list(dns) if dns else list(DEFAULT_PUBLIC_DNS)
    text = _render_egress_ruleset(cidrs, resolvers, denied=egress_guard.always_denied())
    path = os.path.join(root_dir, _EGRESS_RULES_FILE)
    fd, tmp_path = tempfile.mkstemp(dir=root_dir, prefix=_EGRESS_RULES_FILE + ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    return path


# The bundle directory where gVisor's --overlay2 layer keeps writes made inside
# a writable overlayfs sandbox. runsc keeps them under root.path by default,
# but an overlayfs sandbox's root.path is its kernel overlay, whose upper layer
# is a small tmpfs.
_OVERLAY2_DIRNAME = "runsc-overlay2"


def _caps_workdir(config: SandboxConfig) -> bool:
    """Whether ``config``'s disk cap applies to its workdir, a host directory
    bind-mounted into a readonly sandbox."""
    disk_bytes = parse_memory_bytes(config.disk) or 0
    return bool(config.readonly and config.workdir and disk_bytes > 0)


def _lookup_db_entry(text: str, name: str) -> Optional[List[str]]:
    """Return the fields of the ``name`` entry in passwd- or group-style text."""
    for line in text.splitlines():
        fields = line.split(":")
        if len(fields) >= 3 and fields[0] == name:
            return fields
    return None


# How long a timed-out command gets to release its output pipes after it and
# the processes it started are killed.
_EXEC_KILL_GRACE_SECONDS = 5.0

# The most times a timed-out command's processes are listed and killed.
_EXEC_KILL_ROUNDS = 5


def _exec_processes(procfs_dump: str, root_pid: int) -> List[int]:
    """Live PIDs of an exec'd command and of every process it started.

    runsc exec makes the command a session leader. The processes it starts
    stay in its session when they move to a process group of their own, or
    are reparented because their parent exited. A process that left the
    session with setsid() is still found through its parent while the
    parent lives.

    Args:
        procfs_dump: What ``runsc trace procfs <sandbox-id>`` prints: a
            header, then a JSON object per process. Zombies aren't listed.
        root_pid: The command's PID inside the sandbox.

    Returns:
        The PIDs in ascending order, which usually puts parents first.
    """
    children: Dict[int, List[int]] = {}
    pending = []
    for line in procfs_dump.splitlines():
        try:
            entry = json.loads(line)
            pid, ppid = entry["status"]["pid"], entry["status"]["ppid"]
            sid = entry["stat"]["sid"]
        except (ValueError, KeyError, TypeError):
            # The header and the blank lines between objects.
            continue
        children.setdefault(ppid, []).append(pid)
        if sid == root_pid:
            pending.append(pid)
    found = set()
    while pending:
        pid = pending.pop()
        if pid not in found:
            found.add(pid)
            pending.extend(children.get(pid, []))
    return sorted(found)


def _release_cgroup(sandbox_id: str, cgroup: Optional[SandboxCgroup]) -> bool:
    """Remove a sandbox's cgroup, if it has one, and return whether that worked.

    A cgroup that can't be removed may still hold processes. Then the caller
    leaves the bundle, owner file and image pin included, to the reclaim
    pass, which removes the cgroup before it deletes the rest.
    """
    if cgroup is None:
        return True
    try:
        cgroup.release()
    except Exception as err:
        logger.warning(
            "Couldn't remove the cgroup of sandbox %s, so a reclaim pass will: %s",
            sandbox_id,
            err,
        )
        return False
    return True


def _safe_join_under(base_dir: str, rel_path: str, param_name: str = "workdir") -> str:
    """Resolve rel_path under base_dir, rejecting any directory traversal."""
    resolved = os.path.abspath(os.path.join(base_dir, rel_path.lstrip("/")))
    base_abs = os.path.abspath(base_dir)
    if not (resolved == base_abs or resolved.startswith(base_abs + os.sep)):
        raise SandboxCreationError(
            f"Invalid {param_name} '{rel_path}': Path traversal detected."
        )
    return resolved


class GVisorSandboxBackend(BaseSandboxBackend):
    """gVisor sandbox backend running a single persistent container instance per sandbox locally via runsc."""

    def __init__(self, image_manager: Optional[BaseImageManager] = None):
        super().__init__(image_manager=image_manager)
        self._sandbox_metadata: Dict[str, Dict] = {}

    def create_sandbox(self, config: SandboxConfig) -> str:
        """Create a local directory structure and initialize a gVisor sandbox instance."""
        self._check_host_requirements(config)
        cgroups = self._cgroup_manager_for(config)
        sandbox_id = self._provision(config, cgroups, self._start_sandbox)
        self._check_dns_reachable(config, sandbox_id)
        return sandbox_id

    def _check_host_requirements(self, config: SandboxConfig) -> None:
        """Fail early when the node lacks a binary that the sandbox needs."""
        if not shutil.which("runsc"):
            raise SandboxCreationError(
                "gVisor executable 'runsc' not found in PATH. "
                "Please install gVisor (runsc) on the node."
            )
        if config.network == "public":
            missing = [
                b
                for b in ("slirp4netns", "nsenter", "setpriv", "nft")
                if not shutil.which(b)
            ]
            if missing:
                raise SandboxCreationError(
                    "network='public' isolates each sandbox in its own network "
                    "namespace via slirp4netns and filters its egress with an "
                    "nftables ruleset there, but "
                    f"{', '.join(repr(b) for b in missing)} was not found in "
                    "PATH. Install slirp4netns (distro package, or a static "
                    "build from github.com/rootless-containers/slirp4netns), "
                    "util-linux, and nftables on the node image."
                )

    def _cgroup_manager_for(
        self, config: SandboxConfig
    ) -> Optional[SandboxCgroupManager]:
        """Return the cgroup manager for a sandbox with limits, or None."""
        memory = parse_memory_bytes(config.memory) or 0
        if config.cpu <= 0 and memory <= 0:
            return None
        cgroups = SandboxCgroupManager()
        try:
            cgroups.check(config.cpu, memory)
        except Exception as err:
            raise SandboxCreationError(
                f"Cannot enforce sandbox resources: {err}"
            ) from err
        return cgroups

    def _provision(
        self,
        config: SandboxConfig,
        cgroups: Optional[SandboxCgroupManager],
        start: Callable[
            [SandboxConfig, str, str, IO[str], Optional[SandboxCgroup]], None
        ],
        action: str = "create",
    ) -> str:
        """Make a new sandbox's bundle, owner file and cgroup, then start it.

        Shared by create and restore so that a restored sandbox is owned,
        limited and reclaimed the same way as a created one.

        Args:
            config: The sandbox's configuration.
            cgroups: The cgroup manager for a sandbox with limits, or None.
            start: Prepares the bundle and starts the sandbox in it, with the
                signature of ``_start_sandbox``.
            action: What the caller does, for error messages.

        Returns:
            The new sandbox's ID.
        """
        memory = parse_memory_bytes(config.memory) or 0
        self._maybe_reclaim_unowned_bundles()
        sandbox_uuid = uuid.uuid4().hex[:12]
        sandbox_id = f"ray-sandbox-{sandbox_uuid}"
        root_dir = os.path.join(_RAY_SANDBOX_DIR, sandbox_id)

        # Until the sandbox is registered, nothing else deletes what this
        # create makes, so the handler below does: the cgroup, the image pin,
        # and the bundle with its owner file. _start_sandbox stops the
        # processes it starts.
        owner_file = None
        cgroup = None
        try:
            os.makedirs(root_dir, mode=0o777, exist_ok=True)
            if cgroups is None:
                owner_file = self._own_bundle(config, root_dir)
            else:
                lock_path = os.path.join(_RAY_SANDBOX_DIR, _CPUS_LOCK_FILE)
                with open(lock_path, "a", encoding="utf-8") as lock:
                    fcntl.flock(lock, fcntl.LOCK_EX)
                    cpus = cgroups.choose_cpus(config.cpu, self._cpus_in_use())
                    owner_file = self._own_bundle(config, root_dir, cpus)
                # After the owner file, so that a reclaim removes the cgroup
                # of an owner that dies before it starts the sandbox.
                cgroup = cgroups.allocate(sandbox_id, config.cpu, memory, cpus)
            start(config, sandbox_id, root_dir, owner_file, cgroup)
        except Exception as err:
            if _release_cgroup(sandbox_id, cgroup):
                self._image_manager.release_image(config.image, sandbox_id)
                shutil.rmtree(root_dir, ignore_errors=True)
            if owner_file is not None:
                owner_file.close()
            if isinstance(err, SandboxError):
                raise
            raise SandboxCreationError(f"Failed to {action} sandbox: {err}") from err
        return sandbox_id

    def _check_dns_reachable(self, config: SandboxConfig, sandbox_id: str) -> None:
        """Delete a network="public" sandbox whose resolvers it can't reach."""
        if config.network == "public" and config.dns:
            # The filter drops a resolver it can't match, such as a
            # Service IP that the cluster's network translates when a
            # connection opens, and the sandbox has no IPv6 route. Either
            # way every lookup would fail, so fail the create instead.
            unreachable = self._unreachable_resolver(sandbox_id, config.dns)
            if unreachable is not None:
                self.delete_sandbox(sandbox_id)
                server, reason = unreachable
                raise SandboxCreationError(
                    f"dns resolver {server} isn't reachable from a "
                    f"network='public' sandbox: {reason}. The sandbox "
                    "has no IPv6 route, and on Kubernetes clusters that "
                    "translate Service IPs when a connection opens, such "
                    "as Cilium clusters, its egress filter drops a "
                    "Service IP such as the cluster DNS Service's. Use an "
                    "IPv4 resolver that isn't a Service IP."
                )

    def _start_sandbox(
        self,
        config: SandboxConfig,
        sandbox_id: str,
        root_dir: str,
        owner_file: IO[str],
        cgroup: Optional[SandboxCgroup],
    ) -> None:
        """Start a sandbox in its new bundle and register it.

        If the sandbox doesn't come up, this stops the processes it started
        and deletes runsc's state for them. ``create_sandbox`` deletes the
        rest.

        Args:
            config: The sandbox's configuration.
            sandbox_id: The sandbox's ID, which also names its bundle.
            root_dir: The sandbox's bundle directory.
            owner_file: The bundle's owner file, which this backend holds.
            cgroup: The sandbox's cgroup, or None if it has no limits.
        """
        try:
            # The instance id pins the image in the cache while this sandbox
            # lives (its EROFS image is the sandbox's root filesystem).
            image_dir = self._image_manager.pull_image(
                config.image,
                timeout_seconds=config.timeout_seconds,
                instance_id=sandbox_id,
            )
            # The process cwd: an explicit workdir, else the image's WORKDIR.
            container_cwd = (
                config.workdir or self._image_manager.get_workdir(config.image) or "/"
            )

            # A host-backed scratch directory exists only for an *explicitly*
            # requested workdir on a readonly rootfs — the sandbox's single
            # writable path there. A writable rootfs needs none (the overlay
            # covers writes), and an inherited image WORKDIR is never
            # silently made writable.
            workdir_path = None
            if config.workdir and config.readonly:
                workdir_path = _safe_join_under(root_dir, config.workdir)
                os.makedirs(workdir_path, mode=0o777, exist_ok=True)
        except Exception as err:
            raise SandboxCreationError(
                f"Failed to initialize local sandbox directory '{root_dir}': {err}"
            ) from err

        # Prepare OCI bundle config for long-running container process
        rootfs_overlay = self._prepare_bundle(
            config, root_dir, workdir_path, container_cwd
        )
        self._launch_sandbox(
            config,
            sandbox_id,
            root_dir,
            workdir_path,
            container_cwd,
            owner_file,
            cgroup,
            rootfs_overlay=rootfs_overlay,
        )
        # The cached image this sandbox pins, for checkpoints to fingerprint.
        self._sandbox_metadata[sandbox_id]["image_dir"] = image_dir

    def _prepare_bundle(
        self,
        config: SandboxConfig,
        root_dir: str,
        workdir_path: Optional[str],
        container_cwd: str,
    ) -> Optional[overlayfs.RootfsOverlay]:
        """Write the bundle's config.json, resolv.conf and hosts for ``config``.

        Args:
            config: The sandbox's configuration.
            root_dir: The sandbox's bundle directory.
            workdir_path: The host directory mounted at the workdir, if any.
            container_cwd: The sandbox's working directory.

        Returns:
            The kernel overlay to boot the sandbox from, if its final spec
            needs one, or None to boot it straight from its EROFS image.
        """
        config_json_path = self._image_manager.prepare_oci_bundle(
            root_dir=root_dir,
            workdir_path=workdir_path,
            container_cwd=container_cwd,
            image=config.image,
            env_dict=config.env,
            cpu=config.cpu,
            memory=config.memory,
            readonly=config.readonly,
            capabilities=config.capabilities,
            network=config.network,
            dns=config.dns,
            _oci_spec_transform_fn=config._oci_spec_transform_fn,
            disk=config.disk,
        )
        # Read back the final spec, as written to config.json.
        with open(config_json_path, encoding="utf-8") as f:
            spec = json.load(f)

        # Boot the sandbox from a kernel overlay over its cached EROFS image
        # if its final spec needs one.
        if _caps_workdir(config):
            # gVisor's --overlay2 layer keeps the capped workdir's writes here.
            os.makedirs(os.path.join(root_dir, _OVERLAY2_DIRNAME), exist_ok=True)
        if not overlayfs.needs_rootfs_overlay(spec):
            return None
        return self._prepare_rootfs_overlay(config, spec, root_dir)

    def _launch_sandbox(
        self,
        config: SandboxConfig,
        sandbox_id: str,
        root_dir: str,
        workdir_path: Optional[str],
        container_cwd: str,
        owner_file: IO[str],
        cgroup: Optional[SandboxCgroup],
        restore: Optional["_RestoreImage"] = None,
        rootfs_overlay: Optional[overlayfs.RootfsOverlay] = None,
    ) -> None:
        """Run (or restore) the sandbox in its prepared bundle and register it.

        Args:
            config: The sandbox's configuration.
            sandbox_id: The sandbox's ID, which also names its bundle.
            root_dir: The sandbox's prepared bundle directory.
            workdir_path: The host directory mounted at the workdir, if any.
            container_cwd: The sandbox's working directory.
            owner_file: The bundle's owner file, which this backend holds.
            cgroup: The sandbox's cgroup, or None if it has no limits.
            restore: The checkpoint image to restore from, or None to boot
                the sandbox from its image.
            rootfs_overlay: The kernel overlay to boot the sandbox from, or
                None to boot it straight from its EROFS image.
        """
        egress_allowlist = None
        if config.network == "public":
            egress_allowlist = list(
                config.cidr_allowlist
                if config.cidr_allowlist is not None
                else DEFAULT_PUBLIC_ALLOWLIST
            )
            # Loaded by the run command inside the sandbox's network
            # namespace once slirp4netns has brought the tap up.
            try:
                _write_egress_ruleset(root_dir, egress_allowlist, config.dns)
            except Exception as err:
                raise SandboxCreationError(
                    f"Failed to write the egress ruleset for '{root_dir}': {err}"
                ) from err
        proc, stderr_file = self._run_until_running(
            config,
            sandbox_id,
            root_dir,
            cgroup,
            restore=restore,
            rootfs_overlay=rootfs_overlay,
        )

        # The namespaces set_egress_allowlist must find behind netns.pid,
        # read while the holder is known to run the sandbox's network.
        netns_ids = (
            _holder_namespace_ids(root_dir) if config.network == "public" else None
        )
        self._sandbox_metadata[sandbox_id] = {
            "root_dir": root_dir,
            "workdir": workdir_path,
            "cwd": container_cwd,
            "config": config,
            # The egress allowlist in force (None: not network="public");
            # updated by set_egress_allowlist.
            "egress_allowlist": egress_allowlist,
            # Serializes set_egress_allowlist: the ruleset file, the nft
            # load, and egress_allowlist must change together.
            "egress_lock": threading.Lock(),
            # The sandbox's user and network namespaces (None if unknown,
            # which makes set_egress_allowlist refuse).
            "netns_ids": netns_ids,
            # Whether entering the sandbox's network namespace means entering
            # its user namespace first. Not when the sandbox runs in this
            # process's user namespace, which nsenter can't enter again.
            "enter_userns": netns_ids is None
            or netns_ids[0] != _namespace_ids(os.getpid())[0],
            "cgroup": cgroup,
            # The kernel overlay the sandbox boots from, or None for an EROFS
            # sandbox.
            "rootfs_overlay": rootfs_overlay,
            # The launch process whose tree holds the sandbox and, for
            # network="public", the namespace holder and slirp4netns.
            "proc": proc,
            "stderr_file": stderr_file,
            "owner_file": owner_file,
            "status": SandboxStatus.RUNNING,
        }

    def _run_until_running(
        self,
        config: SandboxConfig,
        sandbox_id: str,
        root_dir: str,
        cgroup: Optional[SandboxCgroup],
        restore: Optional["_RestoreImage"] = None,
        rootfs_overlay: Optional[overlayfs.RootfsOverlay] = None,
    ) -> Tuple[subprocess.Popen, IO[str]]:
        """Start the launch process and wait for the container to run.

        If the container doesn't reach 'running', this deletes runsc's state
        for it, stops the launch process and everything it started, and
        raises.

        Args:
            config: The sandbox's configuration.
            sandbox_id: The sandbox's ID.
            root_dir: The sandbox's prepared bundle directory.
            cgroup: The sandbox's cgroup, or None if it has no limits.
            restore: The checkpoint image to restore from, or None to boot
                the sandbox from its image.
            rootfs_overlay: The kernel overlay to boot the sandbox from, or
                None to boot it straight from its EROFS image.

        Returns:
            The launch process and its open stderr log.
        """
        run_args = self._build_run_command(
            config,
            root_dir,
            sandbox_id,
            restore=restore,
            rootfs_overlay=rootfs_overlay,
        )
        if cgroup is not None:
            run_args = cgroup.wrap(run_args)

        stderr_log_path = os.path.join(root_dir, "runsc.stderr.log")
        stderr_file = open(stderr_log_path, "w+", encoding="utf-8")
        # The launch process stays in this process's group, so the sandbox
        # can't outlive a Ray worker that owns it: when a worker exits or
        # crashes, the raylet kills the worker's process group. Everything
        # the launch process started dies with it, runsc's sandbox through
        # runsc's parent-death signal and a network="public" sandbox's
        # helpers through setpriv. It ignores SIGINT, so a driver's Ctrl-C
        # doesn't stop it (see _IGNORE_INTERRUPTS). The tree shares the stderr
        # log so startup failures (missing /dev/net/tun, no uplink) surface
        # through the SandboxCreationError path below.
        try:
            proc = subprocess.Popen(
                run_args,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=stderr_file,
            )
        except Exception:
            stderr_file.close()
            raise
        start_time = time.time()
        timeout = config.timeout_seconds
        action = "restore" if restore is not None else "start"
        # A restore is expected to come up in milliseconds, so poll it more
        # often than a cold boot.
        poll_interval = 0.01 if restore is not None else 0.1

        def stderr_text() -> str:
            stderr_file.seek(0)
            return stderr_file.read()

        try:
            while True:
                if proc.poll() is not None:
                    raise SandboxCreationError(
                        f"gVisor container failed to {action}: {stderr_text()}"
                    )

                state_args = self._runsc_base_args(config) + ["state", sandbox_id]
                res = subprocess.run(state_args, capture_output=True, text=True)
                if res.returncode == 0:
                    try:
                        state_data = json.loads(res.stdout)
                    except ValueError:
                        state_data = {}
                    status = state_data.get("status")
                    if status == "running":
                        if cgroup is not None:
                            try:
                                cgroup.verify(proc.pid)
                            except Exception as err:
                                raise SandboxCreationError(
                                    f"Sandbox resource verification failed: {err}"
                                ) from err
                        break
                    elif status in ("stopped", "error"):
                        raise SandboxCreationError(
                            f"gVisor container stopped unexpectedly during "
                            f"{action} (status: {status}). Stderr: {stderr_text()}"
                        )

                # Check the deadline only after polling state, so a sandbox
                # that reached 'running' just as the deadline passed is
                # observed and kept rather than torn down unpolled.
                if time.time() - start_time > timeout:
                    raise SandboxTimeoutError(
                        f"gVisor container '{sandbox_id}' failed to reach 'running' state within {timeout} seconds."
                    )

                time.sleep(poll_interval)
        except Exception:
            # Delete runsc's container state, then stop the launch process
            # and everything it started.
            self._delete_container_state(config, sandbox_id)
            self._terminate_tree(proc)
            stderr_file.close()
            raise
        return proc, stderr_file

    def delete_sandbox(self, sandbox_id: str) -> None:
        """Terminate the sandbox and remove its local directory structure."""
        meta = self._sandbox_metadata.pop(sandbox_id, None)
        if meta:
            root_dir = meta["root_dir"]
            config: SandboxConfig = meta["config"]
            proc = meta.get("proc")
            stderr_file = meta.get("stderr_file")

            kill_args = self._runsc_base_args(config)
            kill_args.extend(["kill", sandbox_id, "SIGKILL"])
            try:
                subprocess.run(kill_args, capture_output=True, timeout=5)
            except subprocess.TimeoutExpired:
                pass

            self._delete_container_state(config, sandbox_id)

            # After `runsc run` exits, a network="public" sandbox's launch
            # shell still stops and reaps the namespace holder and slirp4netns.
            if proc:
                self._terminate_tree(proc)

            if stderr_file:
                try:
                    stderr_file.close()
                except Exception:
                    pass

            # The cgroup holds every process the sandbox ran, including any
            # that outlived the launch process, and removing it kills them.
            if _release_cgroup(sandbox_id, meta.get("cgroup")):
                shutil.rmtree(root_dir, ignore_errors=True)
                # Only now is the cached image unused.
                self._image_manager.release_image(config.image, sandbox_id)
            # Last, so no other backend reclaims the bundle while it's
            # being deleted.
            if meta.get("owner_file"):
                meta["owner_file"].close()

    def set_egress_allowlist(self, sandbox_id: str, cidrs: List[str]) -> None:
        """Replace the egress allowlist of a running network="public" sandbox.

        The sandbox's ruleset is already in place, so the update is one
        atomic ``nft -f`` (flush and reinstall in a single transaction) run
        inside the sandbox's own namespaces. They're entered through the
        holder in netns.pid, but only once its namespaces match the ones
        recorded at creation, and through fds rather than the reusable pid.
        The denylist is read again and reinstalled with the list, so no
        list opens it. Flows the new list no longer permits are not
        reset; they stall from their next packet on.

        Args:
            sandbox_id: Unique string identifier of the sandbox.
            cidrs: The new allowlist; ``ALLOW_ALL_CIDRS`` opens every
                destination outside the denylist.

        Raises:
            SandboxNotFoundError: If the sandbox is unknown.
            SandboxError: If the sandbox isn't network="public", or the
                ruleset could not be installed.
            ValueError: If an entry is not an IP address or network.
        """
        meta = self._get_metadata_or_raise(sandbox_id)
        if meta.get("egress_allowlist") is None:
            raise SandboxError(
                f"Sandbox '{sandbox_id}' has no egress allowlist: only "
                "network='public' sandboxes have one."
            )
        normalized = normalize_cidr_allowlist(cidrs)
        config: SandboxConfig = meta["config"]
        root_dir = meta["root_dir"]

        # One update at a time per sandbox, so two overlapping updates can
        # never leave one list loaded while the other is recorded.
        with meta.setdefault("egress_lock", threading.Lock()):
            rules_path = os.path.join(root_dir, _EGRESS_RULES_FILE)
            try:
                with open(rules_path, encoding="utf-8") as f:
                    previous_rules = f.read()
            except OSError:
                previous_rules = None
            # `flush ruleset` must only ever run inside the sandbox's own
            # namespaces, never wherever a reused holder pid now points.
            ns_fds = _open_holder_namespaces(root_dir, meta.get("netns_ids"))
            if ns_fds is None:
                raise SandboxError(
                    f"Sandbox '{sandbox_id}' has lost its network namespace "
                    "holder; cannot update its egress allowlist."
                )
            user_fd, net_fd = ns_fds
            enter = [f"--net=/proc/self/fd/{net_fd}"]
            if meta.get("enter_userns", True):
                enter.insert(0, f"--user=/proc/self/fd/{user_fd}")
            try:
                try:
                    _write_egress_ruleset(root_dir, normalized, config.dns)
                except Exception as err:
                    raise SandboxError(
                        f"Failed to write the egress ruleset of sandbox "
                        f"'{sandbox_id}': {err}"
                    ) from err
                proc = subprocess.run(
                    [
                        "nsenter",
                        "--preserve-credentials",
                        *enter,
                        "--",
                        "nft",
                        "-f",
                        rules_path,
                    ],
                    pass_fds=ns_fds,
                    capture_output=True,
                    timeout=10,
                )
                failure = (
                    proc.stderr.decode("utf-8", errors="replace").strip()
                    if proc.returncode != 0
                    else None
                )
            except subprocess.TimeoutExpired:
                failure = "nft did not finish within 10 seconds"
            finally:
                for fd in ns_fds:
                    os.close(fd)
            if failure is not None:
                # Keep the file describing the ruleset that is actually in force.
                if previous_rules is not None:
                    with open(rules_path, "w", encoding="utf-8") as f:
                        f.write(previous_rules)
                raise SandboxError(
                    f"Failed to update the egress allowlist of sandbox "
                    f"'{sandbox_id}': {failure}"
                )
            meta["egress_allowlist"] = normalized

    def _unreachable_resolver(
        self, sandbox_id: str, resolvers: List[str]
    ) -> Optional[Tuple[str, str]]:
        """The first of ``resolvers`` that a public sandbox can't reach.

        Runs ``_RESOLVER_PROBE`` in the sandbox's network namespace, opened
        the way ``set_egress_allowlist`` opens it. A probe that can't run
        proves nothing, so it counts as reachable.

        Args:
            sandbox_id: Unique string identifier of the sandbox.
            resolvers: Resolver addresses from the sandbox's ``dns``.

        Returns:
            The resolver and the reason the sandbox can't reach it, or None.
        """
        meta = self._sandbox_metadata[sandbox_id]
        ns_fds = _open_holder_namespaces(meta["root_dir"], meta.get("netns_ids"))
        if ns_fds is None:
            return None
        user_fd, net_fd = ns_fds
        # The sandbox's user namespace maps only this process's uid. Root
        # doesn't need it to enter the network namespace, and inside it would
        # lose access to files that other users own, such as an interpreter
        # under /home/ray. Any other user has to enter it first, and its own
        # files stay reachable there.
        enter = [f"--net=/proc/self/fd/{net_fd}"]
        if os.geteuid() != 0 and meta.get("enter_userns", True):
            enter.insert(0, f"--user=/proc/self/fd/{user_fd}")
        try:
            proc = subprocess.run(
                [
                    "nsenter",
                    "--preserve-credentials",
                    *enter,
                    "--",
                    sys.executable,
                    "-I",
                    "-c",
                    _RESOLVER_PROBE,
                    *resolvers,
                ],
                pass_fds=ns_fds,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except (OSError, subprocess.TimeoutExpired) as err:
            logger.warning(
                "Couldn't check the dns resolvers of sandbox %s: %s", sandbox_id, err
            )
            return None
        finally:
            for fd in ns_fds:
                os.close(fd)
        if proc.returncode == _RESOLVER_UNREACHABLE:
            server, _, reason = proc.stdout.strip().partition("\t")
            return server, reason
        if proc.returncode != 0:
            logger.warning(
                "Couldn't check the dns resolvers of sandbox %s: %s",
                sandbox_id,
                proc.stderr.strip(),
            )
        return None

    def _read_account_file(self, sandbox_id: str, path: str) -> str:
        """``/etc/passwd`` or ``/etc/group`` as the running sandbox sees it."""
        try:
            return self.read_file(sandbox_id, path).decode("utf-8", errors="replace")
        except SandboxError as err:
            raise SandboxExecError(
                f"cannot read {path} inside the sandbox to resolve a user name "
                f"(pass a numeric uid instead): {err}"
            ) from err

    def _resolve_exec_user(self, sandbox_id: str, user: str) -> str:
        """Turn ``user`` into the numeric ``uid[:gid]`` form runsc exec accepts.

        Names resolve against the ``/etc/passwd`` and ``/etc/group`` inside
        the running sandbox, read through a first exec: that covers users the
        image ships and users added since, and needs no host copy of the root
        filesystem. A named user with no explicit group gets its login group.

        Args:
            sandbox_id: The running sandbox.
            user: ``uid``, ``uid:gid``, or ``name[:group]``.

        Returns:
            A ``uid`` or ``uid:gid`` string runsc accepts.

        Raises:
            SandboxExecError: When a named user or group is unknown to the
                sandbox, or its account files cannot be read.
        """
        name, _, group = user.partition(":")
        if name.isdigit() and (not group or group.isdigit()):
            return user
        uid, login_gid = name, None
        if not name.isdigit():
            passwd = self._read_account_file(sandbox_id, "/etc/passwd")
            entry = _lookup_db_entry(passwd, name)
            if entry is None:
                raise SandboxExecError(
                    f"user {name!r} not found in the sandbox's /etc/passwd; "
                    "pass a numeric uid instead"
                )
            uid = entry[2]
            login_gid = entry[3] if len(entry) > 3 else None
        if group and not group.isdigit():
            groups = self._read_account_file(sandbox_id, "/etc/group")
            entry = _lookup_db_entry(groups, group)
            if entry is None:
                raise SandboxExecError(
                    f"group {group!r} not found in the sandbox's /etc/group; "
                    "pass a numeric gid instead"
                )
            group = entry[2]
        gid = group or login_gid
        return uid if gid is None else f"{uid}:{gid}"

    def exec_command(
        self,
        sandbox_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: Optional[str] = None,
        user: Optional[str] = None,
    ) -> ExecResult:
        """Execute a process inside the running gVisor sandbox instance via runsc exec."""
        meta = self._get_metadata_or_raise(sandbox_id)
        config: SandboxConfig = meta["config"]

        exec_env = {}
        if env:
            exec_env.update(env)

        exec_cwd = cwd or meta["cwd"]
        # The command's PID inside the sandbox, to kill it if it times out.
        pid_file = os.path.join(meta["root_dir"], f".exec-{uuid.uuid4().hex}.pid")

        # Production execution against running container via `runsc exec`
        runsc_args = self._runsc_base_args(config)
        runsc_args.extend(["exec", "-cwd", exec_cwd, "-internal-pid-file", pid_file])
        if user is not None:
            runsc_args.extend(["-user", self._resolve_exec_user(sandbox_id, user)])
        if env:
            for k, v in env.items():
                runsc_args.extend(["-env", f"{k}={v}"])
        if isinstance(command, list):
            runsc_args.extend([sandbox_id] + command)
        else:
            exec_shell = shell or config.shell
            runsc_args.extend([sandbox_id, exec_shell, "-c", command])

        start_time = time.time()
        runsc_args = self._wrap_workload(meta, runsc_args)

        try:
            proc = subprocess.Popen(
                runsc_args,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                stdout, stderr = proc.communicate(timeout=timeout)
            except subprocess.TimeoutExpired as err:
                # Killing `runsc exec` alone leaves the command running in the
                # sandbox, with its output pipes open until it exits.
                self._kill_exec(config, sandbox_id, pid_file)
                proc.kill()
                try:
                    proc.communicate(timeout=_EXEC_KILL_GRACE_SECONDS)
                except subprocess.TimeoutExpired:
                    # A process that escaped the kill still holds the pipes.
                    proc.stdout.close()
                    proc.stderr.close()
                    proc.wait()
                raise SandboxTimeoutError(
                    f"gVisor exec command timed out after {timeout} seconds."
                ) from err
        except SandboxTimeoutError:
            raise
        except Exception as err:
            raise SandboxExecError(f"gVisor exec failed: {err}") from err
        finally:
            try:
                os.remove(pid_file)
            except OSError:
                pass

        # Commands may print bytes that aren't UTF-8, and decoding them
        # strictly would fail the whole exec.
        return ExecResult(
            exit_code=proc.returncode,
            stdout=stdout.decode("utf-8", errors="replace"),
            stderr=stderr.decode("utf-8", errors="replace"),
            duration_seconds=time.time() - start_time,
        )

    def _kill_exec(self, config: SandboxConfig, sandbox_id: str, pid_file: str) -> None:
        """SIGKILL an exec'd command and every process it started.

        The processes come from ``runsc trace procfs``, the only runsc
        command that reports sessions. ``runsc ps`` has no session column,
        and its PGID column and ``runsc kill --pgid`` arrived in April 2026
        and cover one process group, so each process is killed by PID.
        """
        try:
            with open(pid_file, encoding="utf-8") as f:
                root_pid = int(f.read())
        except (OSError, ValueError):
            # The command hadn't started yet.
            return
        base_args = self._runsc_base_args(config)

        def kill(pid: int) -> None:
            try:
                subprocess.run(
                    base_args + ["kill", "--pid", str(pid), sandbox_id, "SIGKILL"],
                    capture_output=True,
                    timeout=10,
                )
            except (OSError, subprocess.TimeoutExpired):
                pass

        # List before killing anything: a child that left the command's
        # session with setsid() is found only through its parent, and killing
        # the parent first would reparent it out of reach. A process can start
        # another between a listing and its own kill, so list again until no
        # process of the command is left.
        for _ in range(_EXEC_KILL_ROUNDS):
            pids = None
            try:
                dump = subprocess.run(
                    base_args + ["trace", "procfs", sandbox_id],
                    capture_output=True,
                    text=True,
                    errors="replace",
                    timeout=10,
                )
                if dump.returncode == 0:
                    pids = _exec_processes(dump.stdout, root_pid)
            except (OSError, subprocess.TimeoutExpired):
                pass
            if pids is None:
                # Without a listing, stop at least the command itself.
                kill(root_pid)
                return
            if not pids:
                return
            for pid in pids:
                kill(pid)

    def write_file(
        self,
        sandbox_id: str,
        path: str,
        content: Union[str, bytes],
        append: bool = False,
    ) -> None:
        """Write (or append) content to a file inside the sandbox."""
        meta = self._get_metadata_or_raise(sandbox_id)
        config: SandboxConfig = meta["config"]

        runsc_args = self._runsc_base_args(config)
        exec_cwd = meta["cwd"]
        runsc_args.extend(
            [
                "exec",
                "-cwd",
                exec_cwd,
                sandbox_id,
                "/bin/sh",
                "-c",
                (
                    'mkdir -p -- "$(dirname -- "$1")" && cat >> "$1"'
                    if append
                    else 'mkdir -p -- "$(dirname -- "$1")" && cat > "$1"'
                ),
                "--",
                path,
            ]
        )

        content_bytes = content.encode("utf-8") if isinstance(content, str) else content
        runsc_args = self._wrap_workload(meta, runsc_args)

        proc = subprocess.Popen(
            runsc_args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, stderr_str = proc.communicate(input=content_bytes)
        if proc.returncode != 0:
            raise SandboxError(
                f"Failed to write file '{path}': {stderr_str.decode('utf-8', errors='replace')}"
            )

    def read_file(self, sandbox_id: str, path: str) -> bytes:
        """Read binary content from a file inside the local gVisor sandbox directory."""
        meta = self._get_metadata_or_raise(sandbox_id)
        config: SandboxConfig = meta["config"]

        runsc_args = self._runsc_base_args(config)
        exec_cwd = meta["cwd"]
        runsc_args.extend(["exec", "-cwd", exec_cwd, sandbox_id, "cat", "--", path])
        runsc_args = self._wrap_workload(meta, runsc_args)

        proc = subprocess.Popen(
            runsc_args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            err = stderr.decode("utf-8", errors="replace")
            raise SandboxError(
                f"File not found or read error '{path}' inside sandbox '{sandbox_id}': {err}"
            )
        return stdout

    def get_status(self, sandbox_id: str) -> SandboxStatus:
        """Get operational status of the gVisor sandbox."""
        meta = self._sandbox_metadata.get(sandbox_id)
        if meta and os.path.exists(meta["root_dir"]):
            proc = meta.get("proc")
            if proc is not None and proc.poll() is not None:
                return SandboxStatus.TERMINATED
            return meta.get("status", SandboxStatus.RUNNING)
        return SandboxStatus.TERMINATED

    def get_termination_reason(self, sandbox_id: str) -> Optional[str]:
        meta = self._sandbox_metadata.get(sandbox_id)
        if meta is not None and self.get_status(sandbox_id) == SandboxStatus.TERMINATED:
            cgroup = meta.get("cgroup")
            return (
                cgroup.termination_reason() if cgroup is not None else None
            ) or "Sandbox runtime stopped unexpectedly"
        return None

    def _run_runsc_command(
        self,
        config: SandboxConfig,
        subcommand: str,
        sandbox_id: str,
        *extra_args: str,
        timeout_seconds: Optional[float] = None,
    ) -> subprocess.CompletedProcess:
        """Execute a runsc lifecycle control command (e.g. pause, resume, state)."""
        args = self._runsc_base_args(config) + [subcommand, *extra_args, sandbox_id]
        res = subprocess.run(
            args, capture_output=True, text=True, timeout=timeout_seconds
        )
        if res.returncode != 0:
            raise SandboxError(
                f"Failed to {subcommand} sandbox '{sandbox_id}': {res.stderr.strip()}"
            )
        return res

    def pause_sandbox(
        self, sandbox_id: str, timeout_seconds: Optional[float] = None
    ) -> None:
        """Pause all processes inside the sandbox without disk serialization."""
        meta = self._get_metadata_or_raise(sandbox_id)
        if meta.get("status") == SandboxStatus.PAUSED:
            return

        config: SandboxConfig = meta["config"]
        self._run_runsc_command(
            config, "pause", sandbox_id, timeout_seconds=timeout_seconds
        )
        meta["status"] = SandboxStatus.PAUSED

    def resume_sandbox(
        self, sandbox_id: str, timeout_seconds: Optional[float] = None
    ) -> None:
        """Resume execution of a paused sandbox."""
        meta = self._get_metadata_or_raise(sandbox_id)
        if meta.get("status") == SandboxStatus.RUNNING:
            return

        config: SandboxConfig = meta["config"]
        self._run_runsc_command(
            config, "resume", sandbox_id, timeout_seconds=timeout_seconds
        )
        meta["status"] = SandboxStatus.RUNNING

    def checkpoint_sandbox(
        self,
        sandbox_id: str,
        checkpoint_path: Optional[str] = None,
        leave_running: bool = True,
        compression: str = "none",
        exclude_committed_zero_pages: bool = True,
        direct: bool = False,
        remote: bool = False,
        timeout_seconds: float = 30.0,
        **kwargs,
    ) -> Dict[str, Any]:
        """Save the sandbox state to a checkpoint bundle directory.

        ``runsc checkpoint`` stops the sandbox once its image is written. Its
        launch process (``runsc run`` or ``runsc restore``) then exits and
        deletes runsc's container state, so ``--leave-running``, which
        restores the container in place, can't keep it: the launch process
        deletes the restored container as it exits. Instead, this stops the
        sandbox with the checkpoint and, for ``leave_running=True``, restores
        it in place from the new image under a new launch process, with the
        same ID, bundle, owner file and cgroup. The sandbox is stopped while
        its filesystem is copied, so the copy matches the memory image.
        """
        meta = self._get_metadata_or_raise(sandbox_id)
        config: SandboxConfig = meta["config"]
        root_dir = meta["root_dir"]
        node_id = get_current_node_id()

        # Step 1: Validate pre-conditions and determine target checkpoint directory path.
        if config.rootless:
            raise SandboxError(
                "gVisor (runsc) does not support checkpoint/restore in rootless mode. "
                "Please configure the sandbox with rootless=False."
            )
        if compression not in _CHECKPOINT_COMPRESSIONS:
            raise ValueError(
                f"Unsupported checkpoint compression {compression!r}; use one of "
                f"{', '.join(repr(c) for c in _CHECKPOINT_COMPRESSIONS)}."
            )

        if not checkpoint_path:
            chk_uuid = uuid.uuid4().hex[:8]
            checkpoint_path = os.path.join(
                _RAY_SANDBOX_DIR, "checkpoints", f"{sandbox_id}-{chk_uuid}"
            )

        checkpoint_path = os.path.abspath(checkpoint_path)
        if os.path.isfile(checkpoint_path):
            raise SandboxError(
                f"Checkpoint path '{checkpoint_path}' exists and is a file, "
                "not a directory."
            )

        start_time = time.time()
        was_paused = meta.get("status") == SandboxStatus.PAUSED

        # Step 2: Begin atomic staging using StagedCheckpoint context manager.
        # All state dumps and filesystem copies occur inside a staging directory first.
        stage = StagedCheckpoint(checkpoint_path)
        # Whether runsc stopped the sandbox, and whether a failure after that
        # already restored it.
        stopped = restored = False

        def restore_after_failure() -> None:
            # The checkpoint failed after it stopped the sandbox: bring the
            # sandbox back from the staged image, whatever leave_running
            # asked for.
            nonlocal restored
            restored = True
            try:
                self._restore_in_place(
                    meta,
                    sandbox_id,
                    _RestoreImage(stage.state_dir, direct=direct),
                    pause=was_paused,
                )
            except Exception as restore_err:
                logger.warning(
                    "Sandbox %s stopped for a checkpoint that failed, and "
                    "couldn't be restored: %s",
                    sandbox_id,
                    restore_err,
                )

        try:
            with stage:
                # Step 3: Dump kernel and process memory state via `runsc checkpoint`,
                # which stops the sandbox once the image is written.
                checkpoint_args = self._runsc_base_args(config)
                checkpoint_args.extend(
                    [
                        "checkpoint",
                        f"--image-path={stage.state_dir}",
                        f"--compression={compression}",
                    ]
                )
                if exclude_committed_zero_pages:
                    checkpoint_args.append("--exclude-committed-zero-pages")
                if direct:
                    checkpoint_args.append("--direct")
                checkpoint_args.append(sandbox_id)

                try:
                    res = subprocess.run(
                        checkpoint_args,
                        capture_output=True,
                        text=True,
                        timeout=timeout_seconds,
                    )
                except subprocess.TimeoutExpired:
                    # The timeout kills only the runsc client: the save goes
                    # on inside the sandbox, which stops once the image is
                    # written. Give it as long again to finish.
                    proc = meta.get("proc")
                    try:
                        if proc is not None:
                            proc.wait(timeout=timeout_seconds)
                    except subprocess.TimeoutExpired:
                        # Still saving (or never started): deleting the
                        # staging directory under the save would lose both
                        # the checkpoint and, once it stops, the sandbox.
                        stage.keep_staging = True
                        raise SandboxTimeoutError(
                            f"Checkpoint of sandbox '{sandbox_id}' didn't finish "
                            f"within {2 * timeout_seconds:g} seconds. A save in "
                            "progress can't be cancelled: it goes on writing to "
                            f"'{stage.staging_path}', which is left in place, and "
                            "stops the sandbox when it's done. Retry with a larger "
                            "timeout_seconds."
                        ) from None
                    res = None
                if res is not None and res.returncode != 0:
                    # A failed checkpoint normally leaves the sandbox running;
                    # if it didn't, get_status reports it terminated.
                    raise SandboxError(
                        f"Failed to checkpoint sandbox '{sandbox_id}': {res.stderr}"
                    )
                stopped = True
                try:
                    self._reap_launch_process(meta, sandbox_id, timeout_seconds)

                    # Step 4: Snapshot the copy-on-write rootfs overlay and writable workdir into staging.
                    stage.copy_bundle_filesystems(
                        root_dir=root_dir,
                        workdir=meta.get("workdir"),
                        sandbox_id=sandbox_id,
                    )

                    # Step 5: Generate checkpoint manifest and apply strict file permission hardening.
                    manifest = create_manifest(
                        sandbox_id=sandbox_id,
                        config=config,
                        cwd=meta["cwd"],
                        workdir=meta.get("workdir"),
                        leave_running=leave_running,
                        remote=remote,
                        node_id=node_id,
                        compression=compression,
                        # set_egress_allowlist may have changed it since.
                        cidr_allowlist=meta.get("egress_allowlist"),
                        image_fingerprint=self._image_fingerprint(meta),
                    )
                    stage.write_manifest(manifest)
                    stage.harden_permissions(rootless=config.rootless)
                except BaseException:
                    # Before the staging directory, and the image in it, is
                    # deleted on the way out.
                    restore_after_failure()
                    raise
        except BaseException:
            # A failed swap into checkpoint_path leaves the staging directory.
            if stopped and not restored and os.path.isdir(stage.state_dir):
                restore_after_failure()
            raise

        # Step 6: Atomic swap completed upon exiting context manager.
        duration = time.time() - start_time
        if leave_running:
            try:
                self._restore_in_place(
                    meta,
                    sandbox_id,
                    _RestoreImage(
                        os.path.join(checkpoint_path, "state"),
                        background=compression == "none",
                        direct=direct,
                    ),
                    pause=was_paused,
                )
            except Exception as err:
                raise SandboxError(
                    f"Checkpointed sandbox '{sandbox_id}' to '{checkpoint_path}', "
                    f"but couldn't resume it afterwards, so it has stopped: {err}"
                ) from err
        else:
            # Fully clean up the stopped source sandbox.
            self.delete_sandbox(sandbox_id)

        ckpt_path_obj = CheckpointPath(
            checkpoint_path,
            node_id=node_id,
            remote=remote,
            cpu=config.cpu,
            memory=config.memory,
        )

        return {
            "checkpoint_path": ckpt_path_obj,
            "sandbox_id": sandbox_id,
            "duration_seconds": duration,
            "leave_running": leave_running,
            "image": config.image,
            "remote": remote,
            "node_id": node_id,
        }

    @staticmethod
    def _image_fingerprint(meta: Dict) -> Optional[str]:
        """The superblock fingerprint of the image a sandbox runs on, if known."""
        image_dir = meta.get("image_dir")
        if not isinstance(image_dir, str):
            return None
        return erofs_superblock_fingerprint(os.path.join(image_dir, ROOTFS_IMAGE))

    def _reap_launch_process(
        self, meta: Dict, sandbox_id: str, timeout_seconds: float
    ) -> None:
        """Wait for a stopped sandbox's launch process to exit, and clean up.

        Called after ``runsc checkpoint`` stops the sandbox: the launch
        process exits on its own, a network="public" one after stopping its
        namespace holder and slirp4netns.
        """
        proc = meta.get("proc")
        if proc is not None:
            try:
                proc.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                pass
            self._terminate_tree(proc)
        self._delete_container_state(meta["config"], sandbox_id)
        stderr_file = meta.get("stderr_file")
        if stderr_file is not None:
            try:
                stderr_file.close()
            except Exception:
                pass
        meta["stderr_file"] = None
        meta["status"] = SandboxStatus.TERMINATED

    def _restore_in_place(
        self,
        meta: Dict,
        sandbox_id: str,
        restore: _RestoreImage,
        pause: bool = False,
    ) -> None:
        """Restore a stopped sandbox in its own bundle from a checkpoint image.

        Keeps the sandbox's ID, bundle, owner file, cgroup and egress
        allowlist, and replaces its launch process.

        Args:
            meta: The sandbox's metadata.
            sandbox_id: The sandbox's ID.
            restore: The checkpoint image to restore from.
            pause: Pause the sandbox again once it runs, for a sandbox that
                was paused when it was checkpointed.
        """
        config: SandboxConfig = meta["config"]
        root_dir = meta["root_dir"]
        # The launch script waits for these to be written; stale copies from
        # the previous launch would let it go on before the new holder and
        # slirp4netns are up.
        for name in ("netns.pid", "slirp4netns.ready"):
            try:
                os.remove(os.path.join(root_dir, name))
            except FileNotFoundError:
                pass
        if config.network == "public" and meta.get("egress_allowlist") is not None:
            _write_egress_ruleset(root_dir, meta["egress_allowlist"], config.dns)
        proc, stderr_file = self._run_until_running(
            config,
            sandbox_id,
            root_dir,
            meta.get("cgroup"),
            restore=restore,
            rootfs_overlay=meta.get("rootfs_overlay"),
        )
        meta["proc"] = proc
        meta["stderr_file"] = stderr_file
        meta["status"] = SandboxStatus.RUNNING
        if config.network == "public":
            meta["netns_ids"] = _holder_namespace_ids(root_dir)
        if pause:
            self.pause_sandbox(sandbox_id)

    def restore_sandbox(
        self,
        checkpoint_path: str,
        cpu: Optional[float] = None,
        memory: Optional[Union[str, int, float]] = None,
        background: Optional[bool] = None,
        direct: bool = False,
        remote: Optional[bool] = None,
        timeout_seconds: float = 30.0,
        **kwargs,
    ) -> str:
        """Restore a new sandbox instance from a checkpoint bundle.

        The restored sandbox gets a bundle generated from the checkpoint's
        configuration with any overrides applied, and is owned, limited and
        cleaned up like a created one.
        """
        # Step 1: Validate checkpoint bundle directory and load stored manifest configuration.
        checkpoint_path = os.path.abspath(checkpoint_path)
        if not os.path.exists(checkpoint_path):
            raise SandboxError(f"Checkpoint path does not exist: '{checkpoint_path}'")

        manifest, state_dir = load_checkpoint_manifest(checkpoint_path)
        config = build_restored_config(
            manifest,
            cpu=cpu,
            memory=memory,
            timeout_seconds=timeout_seconds,
            **kwargs,
        )

        is_remote = (
            remote if remote is not None else bool(manifest.get("remote", False))
        )
        if not is_remote:
            checkpoint_node_id = manifest.get("node_id")
            current_node_id = get_current_node_id()
            if (
                checkpoint_node_id
                and current_node_id
                and checkpoint_node_id != current_node_id
            ):
                raise SandboxError(
                    f"Checkpoint at '{checkpoint_path}' was created as a node-local "
                    f"checkpoint on node '{checkpoint_node_id}', but restore is running "
                    f"on a different node '{current_node_id}'. To restore checkpoints "
                    f"across different VM nodes, create or restore the checkpoint with "
                    f"remote=True on a shared mount path."
                )

        if config.rootless:
            raise SandboxError(
                "gVisor (runsc) does not support restore in rootless mode."
            )

        # runsc restores guest memory in the background only from an
        # uncompressed image. Checkpoints from before the manifest recorded
        # compression were taken with the default, "none".
        compression = manifest.get("compression", "none")
        if background is None:
            background = compression == "none"
        elif background and compression != "none":
            raise SandboxError(
                f"Checkpoint at '{checkpoint_path}' is compressed "
                f"({compression!r}), and runsc restores memory in the background "
                "only from uncompressed checkpoints. Restore it with "
                "background=False, or checkpoint with compression='none'."
            )

        # Step 2: Verify host runtime environment dependencies (runsc, network helpers).
        self._check_host_requirements(config)
        cgroups = self._cgroup_manager_for(config)
        restore = _RestoreImage(state_dir, background=background, direct=direct)

        def start(config, sandbox_id, root_dir, owner_file, cgroup):
            (
                image_dir,
                workdir_path,
                container_cwd,
                rootfs_overlay,
            ) = self._prepare_restored_bundle(
                config,
                checkpoint_path,
                sandbox_id,
                root_dir,
                manifest.get("image_fingerprint"),
            )
            self._launch_sandbox(
                config,
                sandbox_id,
                root_dir,
                workdir_path,
                container_cwd,
                owner_file,
                cgroup,
                restore=restore,
                rootfs_overlay=rootfs_overlay,
            )
            self._sandbox_metadata[sandbox_id]["image_dir"] = image_dir

        # Step 3: Allocate a new ID, bundle, owner file and cgroup, and restore into them.
        sandbox_id = self._provision(config, cgroups, start, action="restore")
        self._check_dns_reachable(config, sandbox_id)
        return sandbox_id

    def _prepare_restored_bundle(
        self,
        config: SandboxConfig,
        checkpoint_path: str,
        sandbox_id: str,
        root_dir: str,
        image_fingerprint: Optional[str] = None,
    ) -> Tuple[str, Optional[str], str, Optional[overlayfs.RootfsOverlay]]:
        """Fill a new bundle from a checkpoint bundle.

        Copies the checkpoint's filesystem state into the bundle, then
        generates the bundle's config.json, resolv.conf and hosts from
        ``config`` the same way a create does, so that overrides apply and
        host paths point at this node's bundle and image cache.

        Args:
            config: The restored sandbox's configuration.
            checkpoint_path: The checkpoint bundle directory.
            sandbox_id: The restored sandbox's ID.
            root_dir: The restored sandbox's new bundle directory.
            image_fingerprint: The superblock fingerprint of the image the
                checkpoint was taken on, if recorded.

        Returns:
            The cached image directory, the host directory mounted at the
            workdir (or None), the sandbox's working directory, and the
            kernel overlay to restore the sandbox in (or None).
        """
        try:
            # Pin this node's copy of the image for the restored sandbox.
            image_dir = self._image_manager.pull_image(
                config.image,
                timeout_seconds=config.timeout_seconds,
                instance_id=sandbox_id,
            )
            local_fingerprint = (
                erofs_superblock_fingerprint(os.path.join(image_dir, ROOTFS_IMAGE))
                if isinstance(image_dir, str)
                else None
            )
            if image_fingerprint and local_fingerprint not in (None, image_fingerprint):
                # gVisor would panic on it ("superblock mismatch detected on
                # restore").
                raise SandboxError(
                    f"This node's build of image {config.image!r} differs from "
                    "the one the checkpoint was taken on, so gVisor can't "
                    "restore it. The tag may have moved, or the image was "
                    "evicted from the cache and rebuilt, since the checkpoint."
                )
            container_cwd = (
                config.workdir or self._image_manager.get_workdir(config.image) or "/"
            )

            # Step 4: Restore filesystem state (copy-on-write upper rootfs overlay layer & workdir).
            rootfs_dir = os.path.join(root_dir, "rootfs")
            os.makedirs(rootfs_dir, exist_ok=True)
            for cand in ("rootfs", "upper"):
                saved_rootfs = os.path.join(checkpoint_path, "fs", cand)
                if os.path.isdir(saved_rootfs):
                    copy_fs_tree(
                        saved_rootfs,
                        rootfs_dir,
                        ignore_patterns=[".gvisor.filestore*"],
                    )
                    break

            # The same rule as _start_sandbox: only an explicit workdir on a
            # readonly rootfs is host-backed. build_restored_config keeps
            # workdir and readonly as they were checkpointed.
            workdir_path = None
            if config.workdir and config.readonly:
                workdir_path = _safe_join_under(root_dir, config.workdir)
                os.makedirs(workdir_path, mode=0o777, exist_ok=True)
                saved_workdir = os.path.join(checkpoint_path, "fs", "workdir")
                if os.path.isdir(saved_workdir):
                    copy_fs_tree(saved_workdir, workdir_path)
        except Exception as err:
            raise SandboxCreationError(
                f"Failed to prepare restored sandbox '{sandbox_id}': {err}"
            ) from err

        # Step 5: Generate the OCI bundle from the restored configuration.
        rootfs_overlay = self._prepare_bundle(
            config, root_dir, workdir_path, container_cwd
        )
        # /etc/hosts is writable inside the sandbox; keep its checkpointed
        # contents over the freshly generated file.
        saved_hosts = os.path.join(checkpoint_path, "hosts")
        if os.path.isfile(saved_hosts):
            shutil.copy2(saved_hosts, os.path.join(root_dir, "hosts"))
        return image_dir, workdir_path, container_cwd, rootfs_overlay

    def _runsc_base_args(self, config: SandboxConfig) -> List[str]:
        """Build the runsc global flags shared by run/exec/kill/delete."""
        return _runsc_args(
            rootless=config.rootless,
            # Ray puts a sandbox with limits in a cgroup of its own, so runsc
            # must leave cgroups alone.
            ignore_cgroups=(
                config.cpu > 0
                or (parse_memory_bytes(config.memory) or 0) > 0
                or getattr(config, "_ignore_cgroups", False)
                or os.environ.get("RAY_SANDBOX_IGNORE_CGROUPS") == "1"
            ),
        )

    @staticmethod
    def _wrap_workload(meta, args):
        cgroup = meta.get("cgroup")
        return cgroup.wrap(args) if cgroup is not None else args

    def _delete_container_state(self, config: SandboxConfig, sandbox_id: str) -> None:
        """Best-effort ``runsc delete -force`` for teardown paths.

        Bounded by a timeout: a wedged gVisor (the usual reason a create
        timed out) must not block stopping the launch process, which is what
        actually frees the sandbox.
        """
        del_args = self._runsc_base_args(config) + ["delete", "-force", sandbox_id]
        try:
            subprocess.run(del_args, capture_output=True, timeout=10)
        except (subprocess.TimeoutExpired, OSError):
            pass

    def _prepare_rootfs_overlay(
        self,
        config: SandboxConfig,
        spec: Dict,
        root_dir: str,
    ) -> overlayfs.RootfsOverlay:
        """Check that this host can mount an overlayfs sandbox's kernel
        overlay, and set up the bundle directories for the kernel overlay
        and gVisor's writable layer.

        It runs after the bundle is written, since the kernel overlay mounts
        on root.path as written in its final ``spec``.
        """
        # The kernel overlay sits on the image's cached EROFS image.
        image = self._image_manager.get_rootfs_image(config.image)

        # root.path reflects any _oci_spec_transform_fn. A relative root.path
        # resolves against the bundle.
        mountpoint = os.path.join(root_dir, spec["root"]["path"])

        # gVisor's --overlay2 layer keeps writes made inside the sandbox here.
        # A readonly sandbox gets it too, where it stays empty and is removed
        # with the bundle.
        os.makedirs(os.path.join(root_dir, _OVERLAY2_DIRNAME), exist_ok=True)

        return overlayfs.prepare(
            image=image,
            mountpoint=mountpoint,
            bundle_dir=root_dir,
        )

    def _build_run_command(
        self,
        config: SandboxConfig,
        root_dir: str,
        sandbox_id: str,
        restore: Optional[_RestoreImage] = None,
        rootfs_overlay: Optional[overlayfs.RootfsOverlay] = None,
    ) -> List[str]:
        """Build the launch argv: `runsc run` under a shell that ignores SIGINT
        and SIGQUIT, namespace-wrapped for network="public".

        With ``restore``, the launch command is `runsc restore` from that
        checkpoint image instead, in the same wrapping: like `runsc run`, it
        stays in the foreground while the sandbox runs and deletes the
        container when it stops.

        For an overlayfs sandbox, the argv is also wrapped so that its kernel
        overlay gets mounted over the cached EROFS image, in a new mount
        namespace and a new user namespace (if needed), before runsc starts.

        Pure argv construction (no filesystem side effects) so tests can
        assert the exact command without runsc or slirp4netns installed.

        Two types of sandboxes can be started, an EROFS sandbox or an overlayfs
        sandbox. An EROFS sandbox boots straight from the cached EROFS image,
        which the sandbox's gVisor annotations point runsc at (see
        ``ImageManager.create_oci_spec``). If the sandbox is writable, those
        annotations also give it an overlay of its own, so runsc gets no
        ``--overlay2`` flag.

        An overlayfs sandbox, on the other hand, mounts the cached EROFS image
        on the host, and boots from a kernel overlay layered on top of it,
        mounted at the sandbox's root.path. The overlay's upper layer is a small
        tmpfs for host-side writes into the sandbox, such as those from OCI
        hooks and the mount points runsc creates. A writable sandbox's own
        writes go to gVisor's ``--overlay2`` layer.
        """
        # If an overlayfs sandbox needs a user namespace, rootfs_overlay.wrap()
        # creates it when wrapping the runsc command at the end of this method.
        # For an EROFS sandbox, the network="public" script creates it directly.
        create_userns_with_overlay = (
            rootfs_overlay is not None
            and rootfs_overlay.userns is overlayfs.UserNamespaceType.PRIVATE
        )
        create_userns_with_netns = config.network == "public" and rootfs_overlay is None

        # runsc runs in a new network namespace for network="public". It also
        # runs in a new user namespace if rootfs_overlay.wrap() or the
        # network="public" script creates one for it.
        with_netns = config.network == "public"
        with_userns = create_userns_with_overlay or create_userns_with_netns

        # Start from runsc's base arguments, then adjust them for this sandbox.
        args = self._runsc_base_args(config)
        if with_userns:
            # runsc gives every gofer an empty network namespace. By default,
            # all gofers under --root share one, created by the first gofer
            # that needs it, in whatever user namespace that gofer runs in.
            # However, a gofer in the user namespace created for this sandbox
            # can't join a network namespace created outside it, so it creates
            # an empty one of its own instead.
            args.append("--gofer-network-namespace=new")
        if with_userns and "--rootless" in args:
            # runsc runs as mapped root inside the user namespace that wraps it;
            # --rootless would nest a second user namespace whose
            # /proc/<pid>/root magic links the gofer cannot dereference.
            # Rootless mode also tolerates cgroup permission failures, so
            # keep that behavior explicitly.
            args = [a for a in args if a != "--rootless"]
            if "--ignore-cgroups" not in args:
                args.insert(1, "--ignore-cgroups")
        if config.network:
            # "public" = host egress + generated resolv.conf (handled in the
            # OCI bundle); runsc itself just sees host networking — of the
            # per-sandbox namespace when wrapped, of the worker otherwise.
            runsc_network = "host" if config.network == "public" else config.network
            args.extend(["--network", runsc_network])
        # Always pass --overlay2 for an overlayfs sandbox. runsc only layers
        # gVisor's overlay over a writable root and ignores the flag for a
        # read-only one, so a readonly sandbox's root stays read-only. Its size
        # caps the writes made inside the sandbox, as the rootfs annotations
        # do for an EROFS sandbox. Where the cap applies to a readonly
        # sandbox's workdir, the overlay covers that bind-mounted host
        # directory too, so the workdir's writes land in the bundle, capped.
        workdir_capped = _caps_workdir(config)
        if rootfs_overlay is not None or workdir_capped:
            scope = "all" if workdir_capped else "root"
            overlay2 = f"{scope}:dir={os.path.join(root_dir, _OVERLAY2_DIRNAME)}"
            disk_bytes = parse_memory_bytes(config.disk) or 0
            if disk_bytes > 0:
                overlay2 += f",size={disk_bytes}"
            args.extend(["--overlay2", overlay2])
        if restore is None:
            args.extend(["run", "--bundle", root_dir, sandbox_id])
        else:
            args.extend(
                ["restore", "--bundle", root_dir, "--image-path", restore.image_path]
            )
            if restore.background:
                args.append("--background")
            if restore.direct:
                args.append("--direct")
            args.append(sandbox_id)
        if with_netns:
            # When creating a new network namespace, we need to also create a
            # new user namespace to own it, unless the sandbox's kernel overlay
            # already runs in a user namespace of its own. slirp4netns and nft
            # then enter the new user namespace, which gives them CAP_NET_ADMIN
            # over the network namespace. For an EROFS sandbox, which has no
            # overlay, runsc enters it too. For an overlayfs sandbox, runsc
            # stays in the user namespace its overlay is mounted in, and joins
            # only the network namespace. Commands that need different flags
            # depending on which of these cases is true are built below using
            # extra_args, which encodes those flags for each command, and gives
            # an empty list for a command that needs none.
            extra_args = collections.defaultdict(list)
            if not create_userns_with_overlay:
                extra_args["unshare"] = ["--user", "--map-root-user"]
                extra_args["slirp4netns"] = ["--userns-path", "/proc/$NSPID/ns/user"]
                extra_args["nsenter_nft"] = ["-U"]
            if create_userns_with_netns:
                extra_args["nsenter_runsc"] = ["-U"]

            unshare = " ".join(
                ["unshare", *extra_args["unshare"], "--net --fork --kill-child"]
            )
            slirp = " ".join(
                [
                    "slirp4netns",
                    *_SLIRP4NETNS_FLAGS,
                    "--ready-fd=3 --netns-type=path /proc/$NSPID/ns/net tap0",
                    *extra_args["slirp4netns"],
                ]
            )
            nsenter_nft = " ".join(
                [
                    "nsenter --preserve-credentials",
                    *extra_args["nsenter_nft"],
                    "-n -t $NSPID --",
                ]
            )
            nsenter_runsc = " ".join(
                [
                    "nsenter --preserve-credentials",
                    *extra_args["nsenter_runsc"],
                    "-n -t $NSPID --",
                ]
            )
            netns_pidfile = shlex.quote(os.path.join(root_dir, "netns.pid"))
            ready_file = shlex.quote(os.path.join(root_dir, "slirp4netns.ready"))
            runsc = " ".join(shlex.quote(a) for a in args)
            rules_file = shlex.quote(os.path.join(root_dir, _EGRESS_RULES_FILE))
            egress = (
                f"{nsenter_nft} nft -f {rules_file} || "
                '{ echo "egress ruleset install failed" >&2; exit 1; }; '
            )
            script = (
                _IGNORE_INTERRUPTS
                # This shell stays the parent of the holder, slirp4netns and
                # runsc, and its exit trap kills and reaps the helpers. When it
                # exec'd runsc instead, the helpers outlived their parent and
                # became zombies of the pod's init, which often never reaps
                # them. Killing the holder's sleep (NSPID) lets unshare reap it.
                + "trap 'kill -KILL ${NSPID:-$HOLDER} $SLIRP 2>/dev/null; wait' EXIT; "
                # setpriv --pdeathsig kills each child with this shell, which
                # is what Ray kills when the owning worker exits.
                # The holder pins the namespaces for the sandbox's lifetime;
                # --kill-child ties its sleep to it.
                "setpriv --pdeathsig KILL -- "
                f"{unshare} bash -c 'echo $$ > {netns_pidfile}; exec sleep infinity' & "
                "HOLDER=$!; "
                # Stop waiting as soon as the holder dies, and refuse an
                # empty NSPID (which would resolve to /proc//ns/net).
                f"for i in $(seq 1 100); do [ -s {netns_pidfile} ] && break; "
                "kill -0 $HOLDER 2>/dev/null || break; sleep 0.1; done; "
                f"NSPID=$(cat {netns_pidfile} 2>/dev/null); "
                '[ -n "$NSPID" ] || { echo "netns holder failed to start" >&2; exit 1; }; '
                # slirp4netns attaches from the pod side and stays in the
                # foreground, so it lives and dies with this shell.
                # It writes "1" to --ready-fd once the tap is configured:
                # that is the go signal.
                f"setpriv --pdeathsig KILL -- {slirp} 3>{ready_file} & "
                "SLIRP=$!; "
                f"for i in $(seq 1 100); do [ -s {ready_file} ] && break; "
                "kill -0 $SLIRP 2>/dev/null || break; sleep 0.1; done; "
                f'[ -s {ready_file} ] || {{ echo "slirp4netns failed to start" >&2; exit 1; }}; '
                # The egress ruleset goes in after the tap exists and
                # before anything runs behind it; a failed install aborts
                # the start rather than leaving egress open.
                f"{egress}"
                f"{nsenter_runsc} setpriv --pdeathsig KILL -- {runsc} & "
                "wait $!"
            )
            args = ["bash", "-c", script]
        if rootfs_overlay is not None:
            args = rootfs_overlay.wrap(args)
        elif with_netns:
            return args
        # The shell execs the command, so the launch process is runsc itself,
        # or the overlay's wrapper around it, which then ignores SIGINT and
        # SIGQUIT too.
        return ["sh", "-c", _IGNORE_INTERRUPTS + 'exec "$@"', "sh", *args]

    def _terminate_tree(self, proc: subprocess.Popen) -> None:
        """Stop a sandbox's launch process, and everything it started, and reap it.

        Callers stop the container first, so the launch process normally exits
        on its own; a network="public" sandbox's shell reaps its helpers first.
        Otherwise it gets SIGKILL, and the processes it started die with it
        through their parent-death signals.
        """
        try:
            proc.wait(timeout=_TREE_EXIT_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            proc.kill()
        try:
            proc.communicate(timeout=2)
        except (subprocess.TimeoutExpired, ValueError):
            pass

    def _own_bundle(
        self, config: SandboxConfig, root_dir: str, cpus: Optional[List[int]] = None
    ) -> IO[str]:
        """Lock a new bundle's owner file for this backend and return it open.

        The backend keeps the file, and so its lock, until it deletes the
        sandbox. The file is locked before it takes its name, so a backend
        looking for unowned bundles never takes one that's being created. It
        also records what reclaiming the bundle takes, and the CPUs that the
        sandbox runs on, for ``_cpus_in_use``.
        """
        path = os.path.join(root_dir, _OWNER_FILE)
        runsc_args = self._runsc_base_args(config)
        owner_file = open(f"{path}.tmp", "x", encoding="utf-8")
        try:
            fcntl.flock(owner_file, fcntl.LOCK_EX)
            # Flags rather than a command line: a backend never runs one that
            # it reads from disk.
            json.dump(
                {
                    "pid": os.getpid(),
                    "image": config.image,
                    "rootless": "--rootless" in runsc_args,
                    "ignore_cgroups": "--ignore-cgroups" in runsc_args,
                    "cpus": cpus or [],
                },
                owner_file,
            )
            owner_file.flush()
            os.rename(f"{path}.tmp", path)
        except BaseException:
            owner_file.close()
            raise
        return owner_file

    def _cpus_in_use(self) -> List[List[int]]:
        """The CPUs that each sandbox on this node with a live owner runs on.

        A bundle counts while its owner holds the owner file's lock, so a
        sandbox stops counting as soon as its owner dies.
        """
        used = []
        for root_dir in glob.glob(os.path.join(_RAY_SANDBOX_DIR, "ray-sandbox-*")):
            if not _BUNDLE_NAME.fullmatch(os.path.basename(root_dir)):
                continue
            try:
                with open(os.path.join(root_dir, _OWNER_FILE), encoding="utf-8") as f:
                    try:
                        fcntl.flock(f, fcntl.LOCK_SH | fcntl.LOCK_NB)
                    except BlockingIOError:
                        owner = json.load(f)  # Its owner holds the lock.
                    else:
                        continue  # Nobody owns it.
            except (OSError, ValueError):
                continue
            cpus = owner.get("cpus") if isinstance(owner, dict) else None
            if isinstance(cpus, list):
                used.append([c for c in cpus if type(c) is int])
        return used

    def _maybe_reclaim_unowned_bundles(self) -> None:
        """Start ``_reclaim_unowned_bundles`` in the background, at most once a
        minute per process.

        A create never waits for it: with many dead owners, or a wedged runsc,
        a pass can take a while.
        """
        global _last_reclaim
        with _reclaim_lock:
            now = time.monotonic()
            if now - _last_reclaim < _RECLAIM_INTERVAL_SECONDS:
                return
            _last_reclaim = now
        threading.Thread(
            target=self._reclaim_in_background, name="sandbox-reclaim", daemon=True
        ).start()

    def _reclaim_in_background(self) -> None:
        try:
            self._reclaim_unowned_bundles()
        except Exception:
            logger.exception("Failed to reclaim sandboxes whose owner is gone")

    def _reclaim_unowned_bundles(self) -> List[str]:
        """Delete what sandboxes whose owner is gone left on this node.

        A sandbox that a Ray worker owns stops with the worker (see
        ``create_sandbox``), but its runsc state, its bundle (with the
        writable overlay's backing file) and its image pin stay on the node,
        and the pin keeps the image from being evicted. A sandbox whose owner
        wasn't a Ray worker can still be running. Either way its owner file
        is unlocked, and this backend holds the lock while it reclaims the
        bundle, so no other backend reclaims it too.

        Returns:
            The IDs of the sandboxes reclaimed.
        """
        reclaimed = []
        for root_dir in glob.glob(os.path.join(_RAY_SANDBOX_DIR, "ray-sandbox-*")):
            sandbox_id = os.path.basename(root_dir)
            if not _BUNDLE_NAME.fullmatch(sandbox_id):
                continue
            try:
                if self._reclaim_if_unowned(root_dir, sandbox_id):
                    reclaimed.append(sandbox_id)
            except Exception:
                logger.exception("Failed to reclaim sandbox %s", sandbox_id)
        if reclaimed:
            logger.info("Reclaimed sandboxes whose owner is gone: %s", reclaimed)
        return reclaimed

    def _reclaim_if_unowned(self, root_dir: str, sandbox_id: str) -> bool:
        """Reclaim one bundle if nobody owns it, and return whether it did."""
        try:
            owner_file = open(os.path.join(root_dir, _OWNER_FILE), encoding="utf-8")
        except OSError:
            # Being created, or made by a Ray that didn't lock it.
            return False
        with owner_file:
            try:
                fcntl.flock(owner_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return False  # Its owner is alive.
            if os.fstat(owner_file.fileno()).st_nlink == 0:
                return False  # Its owner deleted it since we opened the file.
            try:
                owner = json.load(owner_file)
            except ValueError:
                owner = None
            if not isinstance(owner, dict):
                owner = {}
            runsc_args = _runsc_args(
                rootless=owner.get("rootless") is True,
                ignore_cgroups=owner.get("ignore_cgroups") is True,
            )
            for action in (
                ["kill", sandbox_id, "SIGKILL"],
                ["delete", "-force", sandbox_id],
            ):
                try:
                    subprocess.run(
                        [*runsc_args, *action], capture_output=True, timeout=10
                    )
                except (OSError, subprocess.TimeoutExpired):
                    pass
            # A sandbox with limits ran in a cgroup of its own. One that
            # can't be removed raises, which leaves the bundle to a later pass.
            SandboxCgroupManager().remove(sandbox_id)
            # The owner file goes last: a pass that's cut short, say with the
            # process that runs it, leaves it, and a later pass finishes.
            with os.scandir(root_dir) as entries:
                for entry in entries:
                    if entry.name == _OWNER_FILE:
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        shutil.rmtree(entry.path, ignore_errors=True)
                    else:
                        try:
                            os.unlink(entry.path)
                        except OSError:
                            pass
            if isinstance(owner.get("image"), str):
                self._image_manager.release_image(owner["image"], sandbox_id)
            shutil.rmtree(root_dir, ignore_errors=True)
        return True

    def _resolve_path(self, root_dir: str, relative_or_abs_path: str) -> str:
        clean_path = relative_or_abs_path.lstrip("/")
        return os.path.join(root_dir, clean_path)

    def _get_metadata_or_raise(self, sandbox_id: str) -> Dict:
        if sandbox_id not in self._sandbox_metadata:
            raise SandboxNotFoundError(f"Sandbox ID '{sandbox_id}' not found.")
        reason = self.get_termination_reason(sandbox_id)
        if reason is not None:
            raise SandboxError(reason)
        return self._sandbox_metadata[sandbox_id]

    def _pull_and_extract_image(self, image: str) -> str:
        """Pull a container image and extract rootfs to local directory."""
        return self._image_manager.pull_image(image)

    def _prepare_oci_bundle(
        self,
        root_dir: str,
        workdir_path: str,
        container_cwd: str,
        image: str,
        env_dict: Optional[Dict[str, str]] = None,
        cpu: Optional[float] = None,
        memory: Optional[Union[str, int, float]] = None,
        readonly: bool = True,
        capabilities: Optional[List[str]] = None,
        network: str = "none",
        dns: Optional[List[str]] = None,
        _oci_spec_transform_fn: Optional[Callable[[Dict], Optional[Dict]]] = None,
        disk: Optional[Union[str, int, float]] = None,
    ) -> str:
        return self._image_manager.prepare_oci_bundle(
            root_dir=root_dir,
            workdir_path=workdir_path,
            container_cwd=container_cwd,
            image=image,
            env_dict=env_dict,
            cpu=cpu,
            memory=memory,
            readonly=readonly,
            capabilities=capabilities,
            network=network,
            dns=dns,
            _oci_spec_transform_fn=_oci_spec_transform_fn,
            disk=disk,
        )
