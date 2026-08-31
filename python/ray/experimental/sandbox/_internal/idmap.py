"""Multi-uid user-namespace mapping for sandboxes.

A single-uid user namespace (``unshare --map-root-user``) can express no
identity but its own: every in-sandbox file reads as root, and a chown to any
other uid fails because the id has no host representation. Mapping a subuid
range (``/etc/subuid`` + the setuid ``newuidmap``/``newgidmap`` helpers, the
rootless-Podman model) gives container uids 1..count host-side existence, so
images and workloads that spread ownership across users (postfix/mailman
style) behave as under Docker.

A node is either multi-uid (the helpers and ranges are usable) or single-uid;
``detect_idmap`` decides once per process. Image extraction and sandbox
creation both follow that decision, so the cached root filesystems on a node
always match the mapping its sandboxes run with.
"""

import logging
import os
import shutil
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Set to "1" on workers to force single-uid namespaces even where the node
# could map a subordinate range.
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
    """

    euid: int
    egid: int
    subuid_base: int
    subuid_count: int
    subgid_base: int
    subgid_count: int


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


def map_ids_into(pid: int, idmap: IdMap) -> None:
    """Write the canonical uid/gid maps into *pid*'s fresh user namespace.

    Runs the setuid ``newuidmap``/``newgidmap`` helpers, which shadow-utils
    authorizes against ``/etc/subuid`` and ``/etc/subgid``. Raises
    RuntimeError on failure.
    """
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


@contextmanager
def mapped_userns(idmap: IdMap, timeout: float = 60.0) -> Iterator[int]:
    """Yield the pid of a throwaway process in a user namespace mapped with ``idmap``.

    Raises RuntimeError when the namespace cannot be created or mapped.
    """
    try:
        holder = subprocess.Popen(
            ["unshare", "--user", "sleep", str(timeout)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError as err:
        raise RuntimeError(f"cannot start a user-namespace holder: {err}") from err
    try:
        wait_for_userns(holder.pid)
        map_ids_into(holder.pid, idmap)
        yield holder.pid
    finally:
        holder.kill()
        holder.communicate()


def run_as_mapped_root(
    pid: int, argv: List[str], timeout: Optional[float] = None
) -> subprocess.CompletedProcess:
    """Run ``argv`` as root inside the user namespace of ``pid``."""
    return subprocess.run(
        ["nsenter", "--preserve-credentials", "-U", "-t", str(pid), "--", *argv],
        capture_output=True,
        timeout=timeout,
    )


def remove_tree_as_mapped_root(path: str, idmap: IdMap) -> None:
    """Remove a tree with subordinate-owned entries, which only mapped root can."""
    try:
        with mapped_userns(idmap) as pid:
            res = run_as_mapped_root(pid, ["rm", "-rf", "--", path], timeout=120)
    except (RuntimeError, subprocess.TimeoutExpired) as err:
        logger.warning("Could not remove %s as mapped root: %s", path, err)
        return
    if res.returncode != 0:
        logger.warning(
            "Could not remove %s as mapped root: %s",
            path,
            res.stderr.decode(errors="replace").strip(),
        )


def _probe_mapping(idmap: IdMap) -> bool:
    """Whether the setuid helpers actually write ``idmap`` on this node.

    A passing NoNewPrivs check does not guarantee the helpers elevate: image
    build pipelines can strip their setuid bits, and sandboxed runtimes such
    as gVisor only accept single-entry self-maps.
    """
    try:
        with mapped_userns(idmap, timeout=5.0):
            return True
    except (RuntimeError, OSError):
        return False


@lru_cache(maxsize=1)
def detect_idmap() -> Optional[IdMap]:
    """The node's multi-uid mapping, or None to run single-uid.

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
            "%s not found in PATH; sandboxes use single-uid user namespaces "
            "(in-sandbox files cannot be owned by distinct uids). Install the "
            "uidmap package on the node image.",
            ", ".join(missing),
        )
        return None
    if _no_new_privs():
        logger.warning(
            "This process runs with no_new_privs, which disables the setuid "
            "newuidmap/newgidmap helpers; sandboxes use single-uid user "
            "namespaces. Remove allowPrivilegeEscalation=false (or equivalent) "
            "from the pod securityContext to enable multi-uid."
        )
        return None
    euid, egid = os.geteuid(), os.getegid()
    name = _user_name()
    # Both /etc/subuid and /etc/subgid are keyed by the login name or the
    # numeric *uid* (the shadow-utils convention), so the numeric lookup key
    # is euid for both files.
    uid_range = parse_subid_file("/etc/subuid", name, euid)
    gid_range = parse_subid_file("/etc/subgid", name, euid)
    if uid_range is None or gid_range is None:
        logger.warning(
            "/etc/subuid or /etc/subgid has no range of at least %d ids for "
            "user %s (uid %d); sandboxes use single-uid user namespaces. Add "
            "e.g. '%s:100000:65536' to both files.",
            _MIN_RANGE,
            name or "<unknown>",
            euid,
            name or euid,
        )
        return None
    idmap = IdMap(
        euid=euid,
        egid=egid,
        subuid_base=uid_range[0],
        subuid_count=uid_range[1],
        subgid_base=gid_range[0],
        subgid_count=gid_range[1],
    )
    if not _probe_mapping(idmap):
        logger.warning(
            "newuidmap/newgidmap could not write a subordinate mapping here "
            "(stripped setuid bits, a restricted pod securityContext, or the "
            "pod itself running under a sandboxed runtime such as gVisor, "
            "whose kernel only supports self-maps); sandboxes use single-uid "
            "user namespaces."
        )
        return None
    return idmap
