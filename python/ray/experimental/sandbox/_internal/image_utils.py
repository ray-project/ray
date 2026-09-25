import errno
import fcntl
import io
import json
import logging
import mmap
import os
import platform
import re
import shutil
import subprocess
import tarfile
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import deque
from functools import lru_cache
from typing import BinaryIO, Deque, Dict, List, Optional, Tuple, Union

from ray.experimental.sandbox._internal import fs_utils
from ray.experimental.sandbox.exceptions import SandboxCreationError

logger = logging.getLogger(__name__)

DEFAULT_IMAGES_DIR = "/tmp/ray/sandbox/images"
_USER_AGENT = "ray-sandbox/1.0 (python-urllib)"

# A cached image is an EROFS image of its root filesystem. gVisor mounts it
# inside the Sentry, so the image's uids and gids never need a host
# representation: files keep their real owners and in-sandbox chown works for
# any uid, all from an unprivileged worker.
ROOTFS_IMAGE = "rootfs.erofs"
# Cache format recorded in each image's ``.extracted`` marker. A mismatch (an
# extracted-directory cache left by an earlier Ray, say) re-pulls the image
# once; sandboxes already running on the old cache keep it until they exit.
_EXTRACT_FORMAT = 3
# The erofs-utils release that added ``mkfs.erofs --tar``.
_MKFS_EROFS_MIN_VERSION = "1.7"

# Name prefix of every copy of an image's rootfs.erofs unpacked next to it
# (see get_unpacked_rootfs).
_UNPACKED_PREFIX = f"{ROOTFS_IMAGE}.unpacked."

# Name prefix of a copy in the process of being unpacked.
_UNPACKED_TMP_PREFIX = f".{_UNPACKED_PREFIX}tmp."

# The directory inside an unpacked copy that holds the image's files.
_UNPACKED_ROOT_DIR = "root"

# Where an EROFS image's superblock starts, the magic number it opens with,
# and where its filesystem UUID sits within it.
_EROFS_SUPERBLOCK_OFFSET = 1024
_EROFS_MAGIC = 0xE0F5E1E2
_EROFS_MAGIC_SIZE = 4
_EROFS_UUID_OFFSET = 48
_EROFS_UUID_SIZE = 16


def _registry_request(
    url: str, headers: Dict[str, str], auth_header: Optional[str] = None
) -> urllib.request.Request:
    """Build a registry request, keeping the bearer token off redirects.

    Registries answer blob GETs with a redirect to presigned object storage.
    urllib does not copy unredirected headers onto a redirected request, so
    marking the token this way drops it at the hop. Sending it onward would
    trip S3's ``400 InvalidArgument: Only one auth mechanism allowed``, since
    the presigned URL already carries its own signature.
    """
    req = urllib.request.Request(url, headers=headers)
    if auth_header:
        req.add_unredirected_header("Authorization", auth_header)
    return req


def sanitize_image_name(image: str) -> str:
    """Sanitize container image name into a safe directory and filename."""
    if not isinstance(image, str):
        raise TypeError(f"Expected image to be a string, got {type(image).__name__}")

    if image.endswith(".tar"):
        image = os.path.basename(image)[:-4]

    safe = re.sub(r"[^a-zA-Z0-9_.-]", "_", image)
    safe = safe.lstrip(".")
    if not safe:
        raise ValueError(f"Invalid image name '{image}': cannot be safely sanitized.")
    return safe


_DOCKER_HUB_REGISTRIES = (
    "docker.io",
    "index.docker.io",
    "registry-1.docker.io",
    "registry.hub.docker.com",
)


_REGISTRY_MIRROR_ENV = "RAY_SANDBOX_REGISTRY_MIRROR"


def registry_base_url(registry: str) -> str:
    """Return the registry as a base URL.

    Bare hosts default to https. An explicit ``http://`` scheme is honored,
    which in-cluster pull-through proxies (a plain ``registry:2``) need.

    Args:
        registry: Registry host, optionally carrying an explicit scheme.

    Returns:
        The registry with a scheme, without a trailing slash.
    """
    if registry.startswith(("http://", "https://")):
        return registry
    return f"https://{registry}"


def apply_registry_mirror(registry: str, repo: str) -> Tuple[str, str]:
    """Route Docker Hub pulls through a configured pull-through mirror.

    ``RAY_SANDBOX_REGISTRY_MIRROR`` names a registry that mirrors Docker Hub
    as ``host[:port][/repo-prefix]`` — e.g. an ECR pull-through cache
    (``<acct>.dkr.ecr.<region>.amazonaws.com/dockerhub``), an Artifact
    Registry remote repository, or an in-cluster ``registry:2`` proxy. It
    avoids Docker Hub's anonymous rate limits and pulls over the local
    network instead of the WAN. Only Docker Hub pulls are rewritten; other
    registries pass through untouched. When set, the mirror is
    authoritative (no fallback to the upstream), and it is used with the
    same anonymous token flow as any registry.

    Args:
        registry: Registry host chosen by ``parse_image_ref``.
        repo: Repository path chosen by ``parse_image_ref``.

    Returns:
        The possibly rewritten ``(registry, repo)`` pair.
    """
    mirror = os.environ.get(_REGISTRY_MIRROR_ENV, "").strip().strip("/")
    if not mirror or registry != "registry-1.docker.io":
        return registry, repo
    scheme = ""
    for candidate in ("http://", "https://"):
        if mirror.startswith(candidate):
            scheme, mirror = candidate, mirror[len(candidate) :]
            break
    host, _, prefix = mirror.partition("/")
    if scheme:
        host = scheme + host
    return host, f"{prefix}/{repo}" if prefix else repo


def parse_image_ref(image_ref: str) -> Tuple[str, str, str]:
    """Parse image reference string into (registry, repository, tag_or_digest).

    Args:
        image_ref: Container image reference string (e.g. 'busybox:latest',
            'python:3.10-slim', 'docker.io/library/python:3.12-slim',
            'ghcr.io/org/repo:1.0').

    Returns:
        Tuple of (registry, repository, tag_or_digest).
    """
    tag = "latest"
    if "@" in image_ref:
        image_ref, tag = image_ref.split("@", 1)
    elif ":" in image_ref and not image_ref.endswith(":"):
        parts = image_ref.rsplit(":", 1)
        if "/" not in parts[1]:
            image_ref, tag = parts[0], parts[1]

    parts = image_ref.split("/", 1)
    if len(parts) == 1:
        registry = "registry-1.docker.io"
        repo = f"library/{parts[0]}"
    elif "." in parts[0] or ":" in parts[0] or parts[0] == "localhost":
        if parts[0] in _DOCKER_HUB_REGISTRIES:
            registry = "registry-1.docker.io"
            repo = f"library/{parts[1]}" if "/" not in parts[1] else parts[1]
        else:
            registry = parts[0]
            repo = parts[1]
    else:
        registry = "registry-1.docker.io"
        repo = image_ref

    return registry, repo, tag


def get_platform_arch() -> str:
    """Return normalized CPU architecture matching OCI/Docker conventions."""
    mach = platform.machine().lower()
    if mach in ("x86_64", "amd64"):
        return "amd64"
    if mach in ("aarch64", "arm64"):
        return "arm64"
    if mach in ("i386", "i686", "x86"):
        return "386"
    if mach.startswith("arm"):
        return "arm"
    return mach


def get_registry_auth_headers(
    registry: str,
    repo: str,
    reference: str = "latest",
    timeout: float = 30.0,
) -> Dict[str, str]:
    """Retrieve bearer authentication token headers for registry repository."""
    url = f"{registry_base_url(registry)}/v2/{repo}/manifests/{reference}"
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        urllib.request.urlopen(req, timeout=timeout)
        return {}
    except urllib.error.HTTPError as err:
        if err.code != 401:
            return {}
        auth_hdr = err.headers.get("Www-Authenticate", "")
        if not re.match(r"^\s*Bearer\b", auth_hdr, re.IGNORECASE):
            return {}

        realm_m = re.search(r'realm=["\']?([^"\',\s]+)["\']?', auth_hdr, re.IGNORECASE)
        if not realm_m:
            return {}
        realm = realm_m.group(1)

        service_m = re.search(
            r'service=["\']?([^"\',\s]+)["\']?', auth_hdr, re.IGNORECASE
        )
        service = service_m.group(1) if service_m else None

        scope_m = re.search(r'scope=["\']?([^"\',\s]+)["\']?', auth_hdr, re.IGNORECASE)
        scope = scope_m.group(1) if scope_m else f"repository:{repo}:pull"

        params = {}
        if service:
            params["service"] = service
        if scope:
            params["scope"] = scope

        sep = "&" if "?" in realm else "?"
        auth_url = f"{realm}{sep}{urllib.parse.urlencode(params)}" if params else realm
        auth_req = urllib.request.Request(auth_url, headers={"User-Agent": _USER_AGENT})
        try:
            with urllib.request.urlopen(auth_req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                token = data.get("token") or data.get("access_token")
                if token:
                    return {"Authorization": f"Bearer {token}"}
        except Exception as auth_err:
            logger.warning(
                f"Failed to obtain registry auth token from '{auth_url}': {auth_err}"
            )
            return {}
    except Exception as err:
        logger.debug(f"Failed to query registry '{url}' for auth challenge: {err}")
        return {}
    return {}


def _drop_ownership_children(
    ownership: Dict[str, Tuple[int, int]], parent: str
) -> None:
    """Forget recorded owners for everything under ``parent`` ("." for all)."""
    if parent == ".":
        ownership.clear()
        return
    prefix = parent + "/"
    for key in [k for k in ownership if k.startswith(prefix)]:
        del ownership[key]


def _drop_ownership_subtree(ownership: Dict[str, Tuple[int, int]], name: str) -> None:
    """Forget recorded owners for a deleted path and everything under it."""
    ownership.pop(name, None)
    _drop_ownership_children(ownership, name)


# Linux's MAXSYMLINKS: the hop count past which path resolution gives ELOOP.
_MAX_SYMLINK_HOPS = 40


def _resolve_in_root(root: str, rel: str) -> Tuple[str, str]:
    """Resolve ``rel`` under ``root`` the way a sandbox rooted there would.

    Every symlink met along the way is followed with the extracted tree as
    ``/``: an absolute target restarts at ``root`` and ``..`` never climbs
    above it, so the result stays inside the tree whatever the image's links
    point at on the host. Components that do not exist yet are kept as-is.
    Callers that must not dereference a path's last component resolve its
    parent and append the last component themselves.

    Args:
        root: Host directory holding the extracted tree.
        rel: Path to resolve, relative to ``root``; may be empty.

    Returns:
        The host path, and the canonical path relative to ``root`` ("." for
        ``root`` itself): the path the re-pack walk finds the entry at.

    Raises:
        OSError: ``ELOOP`` on a symlink chain longer than the kernel allows.
    """
    pending: Deque[str] = deque(p for p in rel.split("/") if p not in ("", "."))
    resolved: List[str] = []
    hops = 0
    while pending:
        part = pending.popleft()
        if part == "..":
            if resolved:
                resolved.pop()
            continue
        host = os.path.join(root, *resolved, part)
        if os.path.islink(host):
            hops += 1
            if hops > _MAX_SYMLINK_HOPS:
                raise OSError(errno.ELOOP, os.strerror(errno.ELOOP), host)
            target = os.readlink(host)
            if target.startswith("/"):
                resolved = []
            pending.extendleft(
                reversed([p for p in target.split("/") if p not in ("", ".")])
            )
            continue
        resolved.append(part)
    return os.path.join(root, *resolved), "/".join(resolved) or "."


def _create_nofollow(path: str) -> None:
    """Create ``path`` as an empty file if absent, never through a symlink at it."""
    os.close(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o644))


def extract_tar_layer(
    tar_input: Union[bytes, io.IOBase, BinaryIO],
    dest_dir: str,
    ownership: Optional[Dict[str, Tuple[int, int]]] = None,
) -> None:
    """Extract a tar archive layer onto dest_dir with OCI whiteout handling.

    Member paths are resolved as the sandbox will see them, with ``dest_dir``
    as ``/`` (see ``_resolve_in_root``), so a member under a symlinked
    directory (UsrMerge's ``bin -> usr/bin``, an absolute ``/var/run ->
    /run``) lands where the image means it to and never outside the tree.

    ``ownership``, shared by the caller across an image's layers, records the
    final {canonical path: (uid, gid)} of every member shipped with a
    non-root owner, keyed by the resolved path the re-pack walk finds the
    entry at; whiteouts and root-owned replacements drop entries. The
    extracted files themselves stay owned by the extracting user; the EROFS
    image build restores the recorded owners.
    """
    if isinstance(tar_input, bytes):
        tar_fileobj = io.BytesIO(tar_input)
    else:
        tar_fileobj = tar_input

    dir_mtimes = []
    with tarfile.open(fileobj=tar_fileobj, mode="r:*") as tar:
        for member in tar.getmembers():
            name = member.name.lstrip("/")

            # Prevent path traversal
            if ".." in name.split("/"):
                continue

            # The parent is resolved inside dest_dir, so every operation below
            # stays in the tree and ``rel`` is the canonical path. The last
            # component is never dereferenced: a symlink there is replaced or
            # kept as such, not written through.
            dirname, basename = os.path.split(os.path.normpath(name))
            try:
                parent_dir, parent_rel = _resolve_in_root(dest_dir, dirname)
            except OSError:
                continue
            target_path = os.path.join(parent_dir, basename)
            rel = os.path.normpath(os.path.join(parent_rel, basename))

            # Handle OCI opaque whiteout (.wh..wh..opq)
            if basename == ".wh..wh..opq":
                if ownership is not None:
                    _drop_ownership_children(ownership, parent_rel)
                if os.path.isdir(parent_dir):
                    for item in os.listdir(parent_dir):
                        item_path = os.path.join(parent_dir, item)
                        if os.path.isdir(item_path) and not os.path.islink(item_path):
                            fs_utils.rmtree(item_path, ignore_errors=True)
                        else:
                            try:
                                os.remove(item_path)
                            except OSError:
                                pass
                continue

            # Handle OCI deletion whiteout (.wh.<filename>)
            if basename.startswith(".wh."):
                del_name = basename[4:]
                if not del_name:
                    continue
                del_path = os.path.join(parent_dir, del_name)
                if ownership is not None:
                    _drop_ownership_subtree(
                        ownership, os.path.normpath(os.path.join(parent_rel, del_name))
                    )
                if os.path.isdir(del_path) and not os.path.islink(del_path):
                    fs_utils.rmtree(del_path, ignore_errors=True)
                elif os.path.exists(del_path) or os.path.islink(del_path):
                    try:
                        os.remove(del_path)
                    except OSError:
                        pass
                continue

            # Remove conflicting existing file/dir if member type differs
            if os.path.exists(target_path) or os.path.islink(target_path):
                if not (os.path.isdir(target_path) and member.isdir()):
                    try:
                        if os.path.isdir(target_path) and not os.path.islink(
                            target_path
                        ):
                            fs_utils.rmtree(target_path, ignore_errors=True)
                        else:
                            os.remove(target_path)
                    except OSError:
                        pass
                else:
                    # If target_path is a directory or a symlink to an existing directory
                    # inside dest_dir, preserve it (e.g. UsrMerge /bin -> usr/bin).
                    pass

            # Use safe extraction
            member.name = name
            if member.isreg():
                os.makedirs(parent_dir, exist_ok=True)
                # The conflict pass above removed any symlink at target_path;
                # O_NOFOLLOW turns a leftover one into an error, not a write
                # through it.
                fd = os.open(
                    target_path,
                    os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW,
                    0o666,
                )
                with open(fd, "wb") as f_out:
                    f_in = tar.extractfile(member)
                    if f_in:
                        shutil.copyfileobj(f_in, f_out)
                if member.mode:
                    os.chmod(target_path, member.mode)
                # Preserve the archived mtime: tools inside the sandbox rely
                # on it (apt revalidates its package lists with
                # If-Modified-Since from the file mtime, and a reset-to-now
                # mtime makes mirrors answer 304 for stale baked lists).
                # Best-effort, like the directory pass below.
                try:
                    os.utime(target_path, (member.mtime, member.mtime))
                except OSError:
                    pass
            elif member.isdir():
                os.makedirs(target_path, exist_ok=True)
                # Deferred to the post-loop pass: tar lists a directory
                # before its contents, so a restrictive archived mode (0500)
                # applied here would break extracting the children. Preserved
                # symlinks (UsrMerge) are skipped: chmod/utime follow them.
                if not os.path.islink(target_path):
                    dir_mtimes.append((target_path, member.mode, member.mtime))
            elif member.issym():
                os.makedirs(parent_dir, exist_ok=True)
                try:
                    os.symlink(member.linkname, target_path)
                except OSError:
                    pass
            elif member.islnk():
                os.makedirs(parent_dir, exist_ok=True)
                link_name = member.linkname.lstrip("/")
                if ".." not in link_name.split("/"):
                    link_dir, link_base = os.path.split(os.path.normpath(link_name))
                    try:
                        link_parent, _ = _resolve_in_root(dest_dir, link_dir)
                        # A hardlink names another member; if that is a
                        # symlink, link the symlink itself, not what it
                        # points at on the host.
                        os.link(
                            os.path.join(link_parent, link_base),
                            target_path,
                            follow_symlinks=False,
                        )
                    except OSError:
                        pass

            # Hardlinks are recorded under their own name too: the re-pack
            # looks owners up by path, and whichever name of the inode it
            # emits first decides the owner mkfs.erofs stores.
            if ownership is not None and rel != ".":
                if member.uid or member.gid:
                    ownership[rel] = (member.uid, member.gid)
                else:
                    # A later layer re-shipping the path as root wins.
                    ownership.pop(rel, None)

    # Children first, so a parent's restrictive mode cannot block them.
    for dir_path, mode, mtime in reversed(dir_mtimes):
        try:
            if mode:
                os.chmod(dir_path, mode)
            os.utime(dir_path, (mtime, mtime))
        except OSError:
            pass


@lru_cache(maxsize=1)
def mkfs_erofs_path() -> Optional[str]:
    """Path of a ``mkfs.erofs`` that can build images from tarballs, or None.

    Building from a tar (erofs-utils 1.7+) is what lets an unprivileged
    worker record the image's real uid/gid: a directory source would only
    carry the worker's own ownership.
    """
    path = shutil.which("mkfs.erofs")
    if path is None:
        return None
    try:
        res = subprocess.run(
            [path, "--help"], capture_output=True, text=True, timeout=30
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if "--tar" not in res.stdout + res.stderr:
        return None
    return path


def require_mkfs_erofs() -> str:
    """``mkfs_erofs_path()``, or a ``SandboxCreationError`` naming what to install."""
    path = mkfs_erofs_path()
    if path is not None:
        return path
    found = shutil.which("mkfs.erofs")
    if found is None:
        problem = "mkfs.erofs is not in PATH"
    else:
        problem = f"{found} predates --tar (erofs-utils {_MKFS_EROFS_MIN_VERSION})"
    raise SandboxCreationError(
        "Ray Sandbox caches container images as EROFS root filesystems, which "
        f"needs mkfs.erofs {_MKFS_EROFS_MIN_VERSION} or later (erofs-utils) on "
        f"every worker node, but {problem}. Install erofs-utils "
        f"{_MKFS_EROFS_MIN_VERSION}+ on the node image: on nodes with 4 KiB "
        "pages, use the Ubuntu 24.04 or Debian 13 package directly; on older "
        "distributions or nodes with 64 KiB pages, build it from source (with "
        "`./configure MAX_BLOCK_SIZE=65536` for 64 KiB pages)."
    )


def expected_extract_marker() -> str:
    """The ``.extracted`` content a cache entry of the current format carries."""
    return json.dumps({"format": _EXTRACT_FORMAT}, sort_keys=True)


def _owner_filter(ownership: Dict[str, Tuple[int, int]]):
    """``tar.add`` filter giving members the image's recorded owners."""

    def _filter(ti: tarfile.TarInfo) -> tarfile.TarInfo:
        ids = ownership.get(os.path.normpath(ti.name))
        ti.uid, ti.gid = ids if ids else (0, 0)
        ti.uname = ti.gname = ""
        return ti

    return _filter


def _seed_tmp(rootfs_dir: str) -> None:
    """Docker parity for /tmp: world-writable, sticky, and non-empty.

    runsc mounts a private tmpfs over an *empty* /tmp, which breaks
    rename(2) from /tmp with EXDEV; one dotfile keeps /tmp on the rootfs.
    A ``tmp`` symlink is followed inside the tree (``_resolve_in_root``):
    the directory it names in the image is what gets the mode and the file.
    """
    try:
        tmp_dir, _ = _resolve_in_root(rootfs_dir, "tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        os.chmod(tmp_dir, 0o1777)
        _create_nofollow(os.path.join(tmp_dir, ".ray-sandbox-keep"))
    except OSError:
        pass


# Mount points runsc needs in the root filesystem: its mandatory mounts plus
# the ones Ray's OCI spec adds. A read-only root gets no overlay (runsc drops
# it for spec.root.readonly), so an immutable EROFS image must ship them.
_MOUNTPOINT_DIRS = ("proc", "sys", "dev", "dev/pts", "dev/shm", "run", "tmp")
_MOUNTPOINT_FILES = ("etc/resolv.conf", "etc/hosts", "etc/hostname")


def _seed_mountpoints(rootfs_dir: str) -> None:
    """Create the mount points runsc expects, where the image lacks them.

    Each parent is resolved inside the tree (``_resolve_in_root``), so an
    image whose ``etc`` or ``dev`` is a symlink to a host directory gets its
    seeds where the sandbox will look for them, never on the host. A last
    component that already exists, as a symlink included, is left alone.
    """
    for rel in _MOUNTPOINT_DIRS + _MOUNTPOINT_FILES:
        try:
            parent, _ = _resolve_in_root(rootfs_dir, os.path.dirname(rel))
            path = os.path.join(parent, os.path.basename(rel))
            if os.path.lexists(path):
                continue
            if rel in _MOUNTPOINT_DIRS:
                os.makedirs(path, mode=0o755)
            else:
                os.makedirs(parent, exist_ok=True)
                _create_nofollow(path)
        except OSError:
            pass


def build_erofs_image(
    rootfs_dir: str, ownership: Dict[str, Tuple[int, int]], out_path: str
) -> None:
    """Build an EROFS image of ``rootfs_dir`` carrying the image's real owners.

    The tree is re-packed as a tar whose headers hold the recorded uid/gid
    (the files on disk belong to the worker), and ``mkfs.erofs --tar``
    turns that into the image. gVisor's EROFS reader maps the image and
    only reads the flat-plain data layout, hence ``-E^inline_data``. It also
    only mounts an image whose block size is a multiple of the host's page
    size, so we use the page size itself (4 KiB on most hosts, 64 KiB on
    64K-page kernels).

    Args:
        rootfs_dir: Extracted root filesystem.
        ownership: {path: (uid, gid)} recorded during extraction.
        out_path: Destination image file.
    """
    mkfs = require_mkfs_erofs()
    flat_tar = f"{out_path}.tar"
    try:
        with tarfile.open(flat_tar, "w") as tar:
            tar.add(rootfs_dir, arcname=".", filter=_owner_filter(ownership))
        res = subprocess.run(
            [
                mkfs,
                "--tar=f",
                f"-b{mmap.PAGESIZE}",
                "-E^inline_data",
                out_path,
                flat_tar,
            ],
            capture_output=True,
            text=True,
        )
    finally:
        try:
            os.remove(flat_tar)
        except OSError:
            pass
    if res.returncode != 0:
        raise SandboxCreationError(
            f"mkfs.erofs failed: {(res.stderr or res.stdout).strip()[-500:]}"
        )


@lru_cache(maxsize=1)
def fsck_erofs_path() -> Optional[str]:
    """Path of a ``fsck.erofs`` that can unpack images into directory trees, or None.

    Unpacking with ``--extract`` (erofs-utils 1.5+) is what gives an overlayfs
    sandbox the directory tree its kernel overlay sits on.
    """
    path = shutil.which("fsck.erofs")
    if path is None:
        return None
    try:
        res = subprocess.run(
            [path, "--help"], capture_output=True, text=True, timeout=30
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if "--extract" not in res.stdout + res.stderr:
        return None
    return path


def require_fsck_erofs() -> str:
    """``fsck_erofs_path()``, or a ``SandboxCreationError`` naming what to install."""
    path = fsck_erofs_path()
    if path is not None:
        return path
    found = shutil.which("fsck.erofs")
    if found is None:
        problem = "fsck.erofs is not in PATH"
    else:
        problem = f"{found} doesn't support --extract"
    raise SandboxCreationError(
        "Ray Sandbox unpacks cached images with fsck.erofs (erofs-utils) on "
        f"the worker node, but {problem}. Install erofs-utils on the node "
        "image."
    )


def unpack_erofs_image(erofs_image: str, dest: str, *, preserve_owners: bool) -> None:
    """Unpack an EROFS image into a new directory tree with ``fsck.erofs``.

    The tree keeps the image's file modes, and the root of ``dest`` takes the
    mode of the image's root directory. A failed unpack may leave a partial
    ``dest`` behind for the caller to delete.

    Args:
        erofs_image: The EROFS image.
        dest: Directory to unpack into. It must not exist yet.
        preserve_owners: Whether the tree keeps the image's file owners.
            Otherwise every file belongs to this process's user. Keeping
            them takes root in the initial user namespace, since
            ``fsck.erofs`` chowns each file to the image's uid and gid, and
            root in a nested user namespace (e.g. a rootless container) can
            only chown to the ids mapped into it.

    Raises:
        SandboxCreationError: If ``fsck.erofs`` is missing or fails.
    """
    fsck = require_fsck_erofs()
    res = subprocess.run(
        [
            fsck,
            f"--extract={dest}",
            "--preserve-perms",
            "--preserve-owner" if preserve_owners else "--no-preserve-owner",
            erofs_image,
        ],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        raise SandboxCreationError(
            f"fsck.erofs failed to unpack {erofs_image}: "
            f"{(res.stderr or res.stdout).strip()[-500:]}"
        )


_IMAGE_CACHE_MAX_BYTES_ENV = "RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES"
# Subdirectory of each cached image holding one marker file per live sandbox.
_USERS_SUBDIR = ".users"


def image_cache_max_bytes(images_dir: str) -> int:
    """Return the image cache size cap in bytes, or 0 for no cap.

    ``RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES`` sets the cap explicitly; ``0``
    disables eviction. Unset, the cap defaults to half of the filesystem
    that holds ``images_dir``.

    Args:
        images_dir: Root image cache directory.

    Returns:
        The cap in bytes; 0 disables eviction.
    """
    raw = os.environ.get(_IMAGE_CACHE_MAX_BYTES_ENV)
    if raw is not None and raw.strip():
        try:
            return max(int(raw), 0)
        except ValueError:
            logger.warning(
                "Ignoring %s=%r: expected an integer number of bytes.",
                _IMAGE_CACHE_MAX_BYTES_ENV,
                raw,
            )
    try:
        return shutil.disk_usage(images_dir).total // 2
    except OSError:
        return 0


def _mark_image_in_use(image_dir: str, instance_id: str) -> None:
    """Record ``instance_id`` as a live user of the cached image.

    Called by ``pull_and_extract_container_image`` while it holds the
    image's lock, so eviction (which re-checks users under the same lock)
    can never remove an image between its pull and its first use.
    """
    users_dir = os.path.join(image_dir, _USERS_SUBDIR)
    os.makedirs(users_dir, exist_ok=True)
    with open(os.path.join(users_dir, instance_id), "w", encoding="utf-8"):
        pass


def _release_image_use(image_dir: str, instance_id: str) -> None:
    """Drop ``instance_id``'s in-use record; a no-op if it was never marked."""
    try:
        os.remove(os.path.join(image_dir, _USERS_SUBDIR, instance_id))
    except OSError:
        pass


def _has_users(image_dir: str) -> bool:
    try:
        return bool(os.listdir(os.path.join(image_dir, _USERS_SUBDIR)))
    except OSError:
        return False


def _drop_stale_rootfs_tree(image_dir: str) -> None:
    """Delete an extracted ``rootfs/`` tree once no sandbox uses the image.

    A re-pull keeps the tree of an earlier cache format next to the new
    ``rootfs.erofs`` while sandboxes are still running on it.
    """
    stale = os.path.join(image_dir, "rootfs")
    if os.path.isdir(stale) and not _has_users(image_dir):
        fs_utils.rmtree(stale, ignore_errors=True)


def _is_unpacked(name: str) -> bool:
    """Whether an entry of an image's cache directory is an unpacked copy."""
    return name.startswith(_UNPACKED_PREFIX)


def _erofs_image_uuid(erofs_image: str) -> str:
    """The filesystem UUID in an EROFS image's superblock. ``mkfs.erofs``
    generates a random one for every image it builds.

    Args:
        erofs_image: The EROFS image.

    Returns:
        The UUID, in its canonical string form.

    Raises:
        FileNotFoundError: If ``erofs_image`` doesn't exist.
        SandboxCreationError: If it isn't an EROFS image.
    """
    # Read the superblock up to the end of its UUID.
    end = _EROFS_UUID_OFFSET + _EROFS_UUID_SIZE
    with open(erofs_image, "rb") as f:
        f.seek(_EROFS_SUPERBLOCK_OFFSET)
        superblock = f.read(end)

    # A real EROFS superblock opens with the EROFS magic number.
    if len(superblock) < end or (
        int.from_bytes(superblock[:_EROFS_MAGIC_SIZE], "little") != _EROFS_MAGIC
    ):
        raise SandboxCreationError(f"{erofs_image} is not an EROFS image.")

    return str(uuid.UUID(bytes=superblock[_EROFS_UUID_OFFSET:end]))


def _current_unpacked_prefix(image_dir: str) -> str:
    """Name prefix of the copies of the image's current ``rootfs.erofs``.

    Args:
        image_dir: The cached image's directory.

    Returns:
        The prefix, ending in a dot.

    Raises:
        FileNotFoundError: If the image has no ``rootfs.erofs``.
        SandboxCreationError: If its ``rootfs.erofs`` isn't an EROFS image.
    """
    image_uuid = _erofs_image_uuid(os.path.join(image_dir, ROOTFS_IMAGE))
    return f"{_UNPACKED_PREFIX}{image_uuid}."


def _unpacked_dir(image_dir: str, *, preserve_owners: bool) -> str:
    """Path of this process's copy of the image's current ``rootfs.erofs``.

    Args:
        image_dir: The cached image's directory.
        preserve_owners: Whether the copy keeps the image's file owners.

    Returns:
        The copy's absolute path, whether or not it exists yet.

    Raises:
        FileNotFoundError: If the image has no ``rootfs.erofs``.
        SandboxCreationError: If its ``rootfs.erofs`` isn't an EROFS image.
    """
    if preserve_owners:
        owners = "owners-preserved"
    else:
        owners = f"uid.{os.getuid()}.gid.{os.getgid()}"
    return os.path.join(image_dir, _current_unpacked_prefix(image_dir) + owners)


def _drop_stale_unpacked_copies(image_dir: str) -> None:
    """Delete copies unpacked from an earlier ``rootfs.erofs`` once no sandbox
    uses the image (see ``get_unpacked_rootfs``). With no image file, that's every
    copy."""
    # A running sandbox's overlay may sit on any of the copies.
    if _has_users(image_dir):
        return

    # Most images have no copies, so there's nothing to read the image for.
    copies = [name for name in os.listdir(image_dir) if _is_unpacked(name)]
    if not copies:
        return

    # Every user's copy of the current image file stays.
    try:
        current = _current_unpacked_prefix(image_dir)
    except (FileNotFoundError, SandboxCreationError):
        # No usable image file, so no copy is current.
        current = None
    for name in copies:
        if not (current and name.startswith(current)):
            fs_utils.rmtree(os.path.join(image_dir, name), ignore_errors=True)


def _dir_size_bytes(path: str) -> int:
    total = 0
    counted = set()
    for dirpath, _, filenames in os.walk(path):
        for name in filenames:
            try:
                st = os.lstat(os.path.join(dirpath, name))
            except OSError:
                continue
            # Hard links share one file, so each file counts once. Most files
            # have a single link, so only the others need remembering.
            if st.st_nlink <= 1:
                total += st.st_size
            elif (st.st_dev, st.st_ino) not in counted:
                counted.add((st.st_dev, st.st_ino))
                total += st.st_size
    return total


def _file_size_bytes(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _rename_for_deletion(image_dir: str) -> str:
    """Atomically rename a cached image's directory to
    ``.<name>.deleting.<random>``, and return the new path to delete.

    A crash midway through deleting it then never leaves a partly deleted
    image under the image's own name, and the next pull sweeps up what such a
    crash leaves behind (see ``_remove_stale_deletions``).
    """
    images_dir, name = os.path.split(image_dir)
    renamed = os.path.join(images_dir, f".{name}.deleting.{uuid.uuid4().hex}")
    os.replace(image_dir, renamed)
    return renamed


def _remove_stale_deletions(images_dir: str) -> None:
    """Finish deleting image directories whose deletion was interrupted (see
    ``_rename_for_deletion``)."""
    for name in os.listdir(images_dir):
        if name.startswith(".") and ".deleting." in name:
            fs_utils.rmtree(os.path.join(images_dir, name), ignore_errors=True)


def evict_least_recently_used_images(
    images_dir: str, max_bytes: int, keep: Optional[str] = None
) -> None:
    """Evict least-recently-extracted images until the cache fits ``max_bytes``.

    Nodes cache every image they ever ran, so without a cap a long-lived
    node eventually fills its disk. Candidates are fully extracted images
    (``.extracted`` marker present) and ``<name>.tar`` archives left behind
    by earlier Ray versions, oldest first. An image is skipped when a live sandbox uses it, when its
    per-image lock is held (a pull in progress), or when it is ``keep``. The
    in-use check is repeated under the lock, which is also where pulls
    register their users, so a marked image is never removed.

    Args:
        images_dir: Root image cache directory.
        max_bytes: The cache size cap in bytes.
        keep: Sanitized name of an image that must survive this pass.
    """
    try:
        names = os.listdir(images_dir)
    except OSError:
        return
    entries = []  # (mtime, name, image_dir or None, tar_path, size)
    for name in names:
        path = os.path.join(images_dir, name)
        if name.endswith(".tar") and os.path.isfile(path):
            stem = name[: -len(".tar")]
            if not os.path.isdir(os.path.join(images_dir, stem)):
                # Archive without an image: left by an earlier Ray version.
                try:
                    entries.append(
                        (
                            os.path.getmtime(path),
                            stem,
                            None,
                            path,
                            _file_size_bytes(path),
                        )
                    )
                except OSError:
                    pass
            continue
        # Only a sanitized image name is a cached image, which omits e.g.
        # image directories renamed for deletion (see _rename_for_deletion).
        if sanitize_image_name(name) != name:
            continue
        marker = os.path.join(path, ".extracted")
        try:
            if not (os.path.isdir(path) and os.path.exists(marker)):
                continue
            mtime = os.path.getmtime(marker)
        except OSError:
            continue  # Concurrently deleted; keep going.
        tar_path = os.path.join(images_dir, f"{name}.tar")
        size = _dir_size_bytes(path) + _file_size_bytes(tar_path)
        entries.append((mtime, name, path, tar_path, size))

    total = sum(entry[-1] for entry in entries)
    for _, name, img_dir, tar_path, size in sorted(entries):
        if total <= max_bytes:
            return
        if name == keep or (img_dir is not None and _has_users(img_dir)):
            continue
        lock_path = os.path.join(images_dir, f"{name}.lock")
        try:
            with open(lock_path, "w", encoding="utf-8") as f_lock:
                fcntl.flock(f_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # A pull may have registered a user since the scan.
                if img_dir is not None and _has_users(img_dir):
                    continue
                # Another pull may have evicted it since the scan.
                if img_dir is not None and os.path.isdir(img_dir):
                    fs_utils.rmtree(_rename_for_deletion(img_dir), ignore_errors=True)
                try:
                    os.remove(tar_path)
                except OSError:
                    pass
        except OSError:
            continue  # Locked by an in-progress pull; try the next one.
        total -= size
        logger.info("Evicted cached sandbox image %s (%d bytes)", name, size)


def get_unpacked_rootfs(image_dir: str, *, preserve_owners: bool) -> str:
    """Path of a directory tree holding a cached image's files, unpacked from
    its ``rootfs.erofs`` the first time it's needed.

    An overlayfs sandbox (see ``config.ROOTFS_OVERLAYFS``) boots from this
    tree rather than from ``rootfs.erofs``. The copy lives next to the image,
    so the image's pins and eviction cover it, and a new copy may evict other
    images to make room. Its name records the image's UUID (from its
    superblock) and who owns its files. That's either each file's owner from
    the image, or else the uid and gid of the process that unpacked it, so a
    re-pulled image, the two ownership modes, and workers running as
    different users each get a copy of their own. The copy is private to that user (0700), so the image's
    setuid and setgid files are out of reach of other users on the host,
    while its ``root/`` keeps the mode and owner of the image's root
    directory::

        rootfs.erofs                                   the EROFS image
        rootfs.erofs.unpacked.<uuid>.owners-preserved/ an unpacked copy,
            root/                                      with the image's owners
        rootfs.erofs.unpacked.<uuid>.uid.<n>.gid.<n>/  an unpacked copy,
            root/                                      owned by uid:gid
        .rootfs.erofs.unpacked.tmp.<random>/           an unpack in progress

    Unpacking holds the image's lock, so pulls of the same image wait for it.

    Args:
        image_dir: The cached image's directory.
        preserve_owners: Whether the tree keeps the image's file owners
            (see ``unpack_erofs_image``).

    Returns:
        The tree's absolute path.

    Raises:
        SandboxCreationError: If the image is missing, or ``fsck.erofs`` is
            missing or fails.
    """
    # Fast path without the lock, for a copy that already exists. The locked
    # re-check below handles a racing re-pull.
    try:
        tree = os.path.join(
            _unpacked_dir(image_dir, preserve_owners=preserve_owners),
            _UNPACKED_ROOT_DIR,
        )
        if os.path.isdir(tree):
            return tree
    except FileNotFoundError:
        pass  # Reported under the lock.

    # The same lock pull_and_extract_container_image takes for this image.
    with open(f"{image_dir}.lock", "w", encoding="utf-8") as f_lock:
        fcntl.flock(f_lock, fcntl.LOCK_EX)

        # Re-check under the lock, since another worker may have unpacked it.
        try:
            target_dir = _unpacked_dir(image_dir, preserve_owners=preserve_owners)
        except FileNotFoundError:
            # The caller checked the image, so it went away since (e.g.
            # evicted or re-pulled).
            raise SandboxCreationError(
                f"Cached image at {image_dir} has no {ROOTFS_IMAGE}; the cache "
                "entry is incomplete. Delete it to pull again."
            ) from None
        tree = os.path.join(target_dir, _UNPACKED_ROOT_DIR)
        if os.path.isdir(tree):
            return tree

        # Remove temporary copies left by unpacks that crashed.
        for name in os.listdir(image_dir):
            if name.startswith(_UNPACKED_TMP_PREFIX):
                fs_utils.rmtree(os.path.join(image_dir, name), ignore_errors=True)

        # Unpack into a temporary copy, and rename it into place only once
        # it's complete, so the copy's name always means a whole tree.
        tmp_dir = os.path.join(image_dir, _UNPACKED_TMP_PREFIX + uuid.uuid4().hex)
        os.mkdir(tmp_dir, 0o700)
        try:
            unpack_erofs_image(
                os.path.join(image_dir, ROOTFS_IMAGE),
                os.path.join(tmp_dir, _UNPACKED_ROOT_DIR),
                preserve_owners=preserve_owners,
            )
        except Exception:
            fs_utils.rmtree(tmp_dir, ignore_errors=True)
            raise
        os.replace(tmp_dir, target_dir)

    # A new copy grows the cache.
    images_dir = os.path.dirname(image_dir)
    max_cache = image_cache_max_bytes(images_dir)
    if max_cache > 0:
        evict_least_recently_used_images(
            images_dir, max_cache, keep=os.path.basename(image_dir)
        )
    return tree


def pull_and_extract_container_image(
    image: str,
    images_dir: str = DEFAULT_IMAGES_DIR,
    timeout_seconds: float = 120.0,
    instance_id: Optional[str] = None,
) -> str:
    """Pull a container image and cache it as an EROFS root filesystem.

    Args:
        image: Container image name (e.g. 'python:3.10-slim') or path to local tar archive.
        images_dir: Root directory for caching container images.
        timeout_seconds: Network request timeout.
        instance_id: When given, the sandbox instance is registered as a
            user of the image under the image lock, so cache eviction
            leaves the image alone until the instance releases it.

    Returns:
        Absolute path of the image's cache directory. It holds
        ``rootfs.erofs``, the image config, and the ``.extracted`` marker.
    """
    try:
        os.makedirs(images_dir, mode=0o777, exist_ok=True)
    except Exception as err:
        raise SandboxCreationError(
            f"Failed to create images directory '{images_dir}': {err}"
        ) from err

    safe_name = sanitize_image_name(image)
    target_dir = os.path.join(images_dir, safe_name)
    lock_path = os.path.join(images_dir, f"{safe_name}.lock")

    _remove_stale_deletions(images_dir)
    max_cache = image_cache_max_bytes(images_dir)
    if max_cache > 0:
        evict_least_recently_used_images(images_dir, max_cache, keep=safe_name)

    expected_marker = expected_extract_marker()

    def _finish() -> str:
        if instance_id is not None:
            _mark_image_in_use(target_dir, instance_id)
        return target_dir

    with open(lock_path, "w", encoding="utf-8") as f_lock:
        try:
            fcntl.flock(f_lock, fcntl.LOCK_EX)
            marker_path = os.path.join(target_dir, ".extracted")
            if os.path.isdir(target_dir) and os.path.exists(marker_path):
                try:
                    with open(marker_path, "r", encoding="utf-8") as f_mark:
                        marker_current = f_mark.read() == expected_marker
                except OSError:
                    marker_current = False
                # A cache of another format re-pulls once.
                if marker_current and (
                    not os.path.isfile(image)
                    or os.path.getmtime(marker_path) >= os.path.getmtime(image)
                ):
                    _drop_stale_rootfs_tree(target_dir)
                    _drop_stale_unpacked_copies(target_dir)
                    return _finish()

            # Checked before any download: without it the pull cannot finish.
            require_mkfs_erofs()

            tmp_extract_dir = os.path.join(
                images_dir, f"{safe_name}.tmp.{uuid.uuid4().hex}"
            )
            os.makedirs(tmp_extract_dir, mode=0o755, exist_ok=True)
            tmp_rootfs_dir = os.path.join(tmp_extract_dir, "rootfs")
            os.makedirs(tmp_rootfs_dir, mode=0o755, exist_ok=True)
            try:
                ownership = _extract_image_layers(
                    image, tmp_rootfs_dir, tmp_extract_dir, timeout_seconds, images_dir
                )
                _seed_tmp(tmp_rootfs_dir)
                _seed_mountpoints(tmp_rootfs_dir)
                build_erofs_image(
                    tmp_rootfs_dir,
                    ownership,
                    os.path.join(tmp_extract_dir, ROOTFS_IMAGE),
                )
            except Exception:
                fs_utils.rmtree(tmp_extract_dir, ignore_errors=True)
                raise
            # The image replaces the tree; nothing reads the tree once the
            # Sentry mounts the image.
            #
            # An overlayfs sandbox unpacks its own copy from the image instead
            # (see get_unpacked_rootfs).
            fs_utils.rmtree(tmp_rootfs_dir, ignore_errors=True)

            with open(
                os.path.join(tmp_extract_dir, ".extracted"), "w", encoding="utf-8"
            ) as f_mark:
                f_mark.write(expected_marker)

            # Swap the new cache in. A re-pull replaces a cache of another
            # format; sandboxes already running on it keep their pins, and an
            # extracted ``rootfs/`` tree their gofer still serves moves over
            # intact (a rename leaves its open root fd alone) and goes once no
            # sandbox uses the image.
            try:
                users = os.listdir(os.path.join(target_dir, _USERS_SUBDIR))
            except OSError:
                users = []

            # The image's old directory is deleted below, so first move over
            # the directory tree an older version of Ray cached the image as
            # and any unpacked copies, since a running sandbox may still use
            # them. Each is cleaned up once no sandbox uses it.
            if users:
                for name in os.listdir(target_dir):
                    if name == "rootfs" or _is_unpacked(name):
                        os.replace(
                            os.path.join(target_dir, name),
                            os.path.join(tmp_extract_dir, name),
                        )

            if os.path.exists(target_dir):
                fs_utils.rmtree(_rename_for_deletion(target_dir), ignore_errors=True)
            os.replace(tmp_extract_dir, target_dir)
            for user in users:
                _mark_image_in_use(target_dir, user)

            return _finish()
        finally:
            try:
                fcntl.flock(f_lock, fcntl.LOCK_UN)
            except Exception:
                pass


def _extract_image_layers(
    image: str,
    rootfs_dir: str,
    image_dir: str,
    timeout_seconds: float,
    blob_dir: str,
) -> Dict[str, Tuple[int, int]]:
    """Flatten ``image`` into ``rootfs_dir`` and return its recorded owners.

    ``image`` is a local tar archive or a registry reference. A registry
    image's config is written to ``image_dir/.image_config.json`` and its
    layer blobs are spooled through ``blob_dir``.

    Args:
        image: Container image name or path to a local tar archive.
        rootfs_dir: Empty directory that receives the flattened tree.
        image_dir: Directory that receives the image config.
        timeout_seconds: Network request timeout.
        blob_dir: Directory for temporary layer blobs.

    Returns:
        {path: (uid, gid)} for every path shipped with a non-root owner.

    Raises:
        SandboxCreationError: When the image cannot be fetched or extracted.
    """
    ownership: Dict[str, Tuple[int, int]] = {}
    if os.path.isfile(image):
        try:
            with open(image, "rb") as f:
                extract_tar_layer(f, rootfs_dir, ownership=ownership)
        except Exception as err:
            raise SandboxCreationError(
                f"Failed to extract local image archive '{image}': {err}"
            ) from err
        return ownership
    if (
        image.endswith(".tar")
        or image.startswith("/")
        or image.startswith("./")
        or image.startswith("../")
    ):
        raise SandboxCreationError(f"Local image archive '{image}' not found.")
    try:
        registry, repo, reference = parse_image_ref(image)
        registry, repo = apply_registry_mirror(registry, repo)
        auth_headers = get_registry_auth_headers(
            registry,
            repo,
            reference=reference,
            timeout=timeout_seconds,
        )
        headers = {
            "User-Agent": _USER_AGENT,
            "Accept": (
                "application/vnd.docker.distribution.manifest.v2+json, "
                "application/vnd.docker.distribution.manifest.list.v2+json, "
                "application/vnd.oci.image.manifest.v1+json, "
                "application/vnd.oci.image.index.v1+json"
            ),
        }
        auth_header = auth_headers.get("Authorization")

        manifest_url = f"{registry_base_url(registry)}/v2/{repo}/manifests/{reference}"
        req = _registry_request(manifest_url, headers, auth_header)
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            manifest_data = json.loads(resp.read().decode("utf-8"))

        # Resolve multi-architecture manifest list / OCI index
        if "manifests" in manifest_data:
            target_arch = get_platform_arch()
            chosen_digest = None
            for m in manifest_data["manifests"]:
                plat = m.get("platform", {})
                if (
                    plat.get("os") == "linux"
                    and plat.get("architecture") == target_arch
                ):
                    chosen_digest = m["digest"]
                    break
            if not chosen_digest:
                chosen_digest = manifest_data["manifests"][0]["digest"]

            sub_req = _registry_request(
                f"{registry_base_url(registry)}/v2/{repo}/manifests/{chosen_digest}",
                headers,
                auth_header,
            )
            with urllib.request.urlopen(sub_req, timeout=timeout_seconds) as resp:
                manifest_data = json.loads(resp.read().decode("utf-8"))

        # extract image config so we can reference metadata bout the image later.
        config_desc = manifest_data.get("config")
        if config_desc and "digest" in config_desc:
            config_digest = config_desc["digest"]
            config_url = (
                f"{registry_base_url(registry)}/v2/{repo}/blobs/{config_digest}"
            )
            config_req = _registry_request(config_url, headers, auth_header)
            try:
                with urllib.request.urlopen(
                    config_req, timeout=timeout_seconds
                ) as resp:
                    config_bytes = resp.read()
                    with open(
                        os.path.join(image_dir, ".image_config.json"),
                        "wb",
                    ) as f_cfg:
                        f_cfg.write(config_bytes)
            except Exception as e:
                logger.warning(f"Failed to fetch image config blob: {e}")

        layers = manifest_data.get("layers", [])
        if not layers:
            raise SandboxCreationError(
                f"No layers found in manifest for image '{image}'"
            )

        for layer in layers:
            digest = layer["digest"]
            blob_url = f"{registry_base_url(registry)}/v2/{repo}/blobs/{digest}"
            blob_req = _registry_request(blob_url, headers, auth_header)
            with urllib.request.urlopen(blob_req, timeout=timeout_seconds) as blob_resp:
                with tempfile.NamedTemporaryFile(
                    dir=blob_dir, delete=True
                ) as tmp_blob_file:
                    shutil.copyfileobj(blob_resp, tmp_blob_file, length=64 * 1024)
                    tmp_blob_file.seek(0)
                    extract_tar_layer(tmp_blob_file, rootfs_dir, ownership=ownership)

    except Exception as err:
        if isinstance(err, SandboxCreationError):
            raise
        raise SandboxCreationError(
            f"Failed to pull and extract container image '{image}': {err}"
        ) from err
    return ownership
