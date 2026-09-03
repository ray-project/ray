"""Multi-uid user-namespace mapping detection for network="public" sandboxes.

A single-uid user namespace (``unshare --map-root-user``) can express no
identity but its own: every in-sandbox file reads as root, and a chown to any
other uid fails because the id has no host representation. Mapping a subuid
range (``/etc/subuid`` + the setuid ``newuidmap``/``newgidmap`` helpers, the
rootless-Podman model) gives container uids 1..count host-side existence, so
images and workloads that spread ownership across users (postfix/mailman
style) behave as under Docker.

This module only *detects* whether the node can do that; the holder script in
``backend/gvisor.py`` performs the actual mapping.
"""

import logging
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Kill switch: set to "1" on workers to force single-uid namespaces (the
# pre-subuid behavior) without a code deploy.
SINGLE_UID_ENV = "RAY_SANDBOX_SINGLE_UID"

# A usable subordinate range must cover the uids images realistically ship
# (distro system users plus nobody at 65534).
_MIN_RANGE = 65536


@dataclass(frozen=True)
class IdMap:
    """One node-canonical uid/gid mapping for sandbox user namespaces.

    Container root maps to the worker's own ids (so the bundle and cache
    files it already owns stay accessible); container 1..count map onto the
    subordinate range, giving every other uid a host representation.
    ``sudo_mapfile`` records how mapping works on this node: False means
    the setuid newuidmap/newgidmap helpers elevate; True means the bits are
    stripped (some image builders do that) and privileged direct writes to
    /proc/<pid>/uid_map via passwordless sudo are used instead — shadow's
    helpers refuse cross-user targets, so sudo-ing *them* is never an option.
    """

    euid: int
    egid: int
    subuid_base: int
    subuid_count: int
    subgid_base: int
    subgid_count: int
    sudo_mapfile: bool = False


def parse_subid_file(
    path: str, user_name: Optional[str], uid: int
) -> Optional[Tuple[int, int]]:
    """First usable ``(base, count)`` range for the user in a subid file.

    Entries may be keyed by user name or numeric uid; malformed lines and
    ranges below the usable floor are skipped.
    """
    keys = {str(uid)}
    if user_name:
        keys.add(user_name)
    try:
        text = Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    for line in text.splitlines():
        parts = line.strip().split(":")
        if len(parts) != 3 or parts[0] not in keys:
            continue
        try:
            base, count = int(parts[1]), int(parts[2])
        except ValueError:
            continue
        if count >= _MIN_RANGE:
            return base, count
    return None


def _user_name() -> Optional[str]:
    try:
        import pwd

        return pwd.getpwuid(os.geteuid()).pw_name
    except (ImportError, KeyError, OSError):
        return None


def _no_new_privs() -> bool:
    """Whether this process runs with no_new_privs (setuid helpers no-op)."""
    try:
        status = Path("/proc/self/status").read_text(encoding="utf-8")
    except OSError:
        return False
    for line in status.splitlines():
        if line.startswith("NoNewPrivs:"):
            return line.split(":", 1)[1].strip() == "1"
    return False


def wait_for_userns(pid: int, timeout: float = 5.0) -> None:
    """Wait until *pid* has entered a user namespace different from ours."""
    own = os.readlink("/proc/self/ns/user")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if os.readlink(f"/proc/{pid}/ns/user") != own:
                return
        except OSError:
            pass
        time.sleep(0.02)
    raise RuntimeError(
        f"user-namespace holder (pid {pid}) never left the initial namespace"
    )


def map_ids_into(pid: int, idmap: "IdMap") -> None:
    """Write the canonical uid/gid maps into *pid*'s fresh user namespace.

    Native mode runs the setuid newuidmap/newgidmap helpers. sudo mode
    writes /proc/<pid>/{uid_map,gid_map} directly as root — the kernel lets
    a CAP_SETUID/CAP_SETGID holder in the parent namespace write arbitrary
    multi-line maps, which sidesteps both stripped setuid bits and shadow's
    invoker-must-own-the-target rule. Raises RuntimeError on failure.
    """
    if idmap.sudo_mapfile:
        script = (
            f"printf '0 {idmap.euid} 1\n1 {idmap.subuid_base} "
            f"{idmap.subuid_count}\n' > /proc/{pid}/uid_map && "
            f"printf '0 {idmap.egid} 1\n1 {idmap.subgid_base} "
            f"{idmap.subgid_count}\n' > /proc/{pid}/gid_map"
        )
        res = subprocess.run(["sudo", "-n", "sh", "-c", script], capture_output=True)
        if res.returncode != 0:
            raise RuntimeError(
                "sudo map-file write failed: "
                + res.stderr.decode(errors="replace").strip()
            )
        return
    for helper, own, base, count in (
        ("newuidmap", idmap.euid, idmap.subuid_base, idmap.subuid_count),
        ("newgidmap", idmap.egid, idmap.subgid_base, idmap.subgid_count),
    ):
        res = subprocess.run(
            [helper, str(pid), "0", str(own), "1", "1", str(base), str(count)],
            capture_output=True,
        )
        if res.returncode != 0:
            raise RuntimeError(
                f"{helper} failed: " + res.stderr.decode(errors="replace").strip()
            )


def _probe_sudo_mapfile(
    euid: int,
    egid: int,
    uid_range: Tuple[int, int],
    gid_range: Tuple[int, int],
) -> Optional[bool]:
    """How mapping works here: False native, True sudo map-file, None neither.

    A passing NoNewPrivs check does not guarantee the helpers elevate: image
    build pipelines can strip their setuid bits. uid_map is write-once, so
    each attempt gets a fresh throwaway namespace holder.
    """
    for sudo_mapfile in (False, True):
        candidate = IdMap(
            euid=euid,
            egid=egid,
            subuid_base=uid_range[0],
            subuid_count=uid_range[1],
            subgid_base=gid_range[0],
            subgid_count=gid_range[1],
            sudo_mapfile=sudo_mapfile,
        )
        try:
            holder = subprocess.Popen(
                ["unshare", "--user", "sleep", "5"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except OSError:
            # unshare is missing or unrunnable: there is no throwaway namespace
            # to probe with, so neither mapping method is usable here. Honor the
            # Optional[bool] contract (None = neither) instead of propagating.
            return None
        try:
            wait_for_userns(holder.pid)
            map_ids_into(holder.pid, candidate)
            return sudo_mapfile
        except (OSError, RuntimeError):
            pass
        finally:
            holder.kill()
            holder.communicate()
    return None


@lru_cache(maxsize=1)
def detect_idmap() -> Optional[IdMap]:
    """The node's multi-uid mapping, or None to fall back to single-uid.

    Cached per process; each fallback reason is logged once. The setuid
    newuidmap/newgidmap helpers silently become no-ops under no_new_privs
    (``allowPrivilegeEscalation: false``-style pod contexts), so that is
    detected here rather than discovered as a boot failure.
    """
    if os.environ.get(SINGLE_UID_ENV) == "1":
        logger.info("%s=1: sandboxes use single-uid user namespaces", SINGLE_UID_ENV)
        return None
    missing = [b for b in ("newuidmap", "newgidmap") if not shutil.which(b)]
    if missing:
        logger.warning(
            "%s not found in PATH; sandboxes fall back to single-uid user "
            "namespaces (in-sandbox files cannot be owned by distinct uids). "
            "Install the uidmap package on the node image.",
            ", ".join(missing),
        )
        return None
    if _no_new_privs():
        logger.warning(
            "This process runs with no_new_privs, which disables the setuid "
            "newuidmap/newgidmap helpers; sandboxes fall back to single-uid "
            "user namespaces. Remove allowPrivilegeEscalation=false (or "
            "equivalent) from the pod securityContext to enable multi-uid."
        )
        return None
    euid, egid = os.geteuid(), os.getegid()
    name = _user_name()
    # Both /etc/subuid and /etc/subgid are keyed by the login name or the
    # numeric *uid* (the shadow-utils convention) — subgid is not keyed by gid
    # — so the numeric lookup key is euid for both files.
    uid_range = parse_subid_file("/etc/subuid", name, euid)
    gid_range = parse_subid_file("/etc/subgid", name, euid)
    if uid_range is None or gid_range is None:
        logger.warning(
            "/etc/subuid or /etc/subgid has no range of at least %d ids for "
            "user %s (uid %d); sandboxes fall back to single-uid user "
            "namespaces. Add e.g. '%s:100000:65536' to both files.",
            _MIN_RANGE,
            name or "<unknown>",
            euid,
            name or euid,
        )
        return None
    sudo_mapfile = _probe_sudo_mapfile(euid, egid, uid_range, gid_range)
    if sudo_mapfile is None:
        logger.warning(
            "no way to write a subordinate mapping here (setuid "
            "newuidmap/newgidmap and privileged sudo map-file writes both "
            "failed) — commonly a restricted pod securityContext, or the "
            "pod itself running under a sandboxed runtime such as gVisor "
            "(GKE Sandbox), whose kernel only supports self-maps; "
            "sandboxes fall back to single-uid user namespaces."
        )
        return None
    return IdMap(
        euid=euid,
        egid=egid,
        subuid_base=uid_range[0],
        subuid_count=uid_range[1],
        subgid_base=gid_range[0],
        subgid_count=gid_range[1],
        sudo_mapfile=sudo_mapfile,
    )
