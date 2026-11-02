import concurrent.futures
import contextlib
import errno
import fcntl
import hashlib
import io
import json
import logging
import mmap
import os
import platform
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import deque
from functools import lru_cache
from typing import BinaryIO, Deque, Dict, Iterator, List, Optional, Tuple, Union

from ray.experimental.sandbox._internal import docker_config
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
# How many of an image's layers download at once, as in Docker.
_LAYER_DOWNLOADS = 3


def _registry_request(
    url: str, headers: Dict[str, str], auth_header: Optional[str] = None
) -> urllib.request.Request:
    """Build a registry request, keeping its credentials off redirects.

    Registries answer blob GETs with a redirect to presigned object storage.
    urllib does not copy unredirected headers onto a redirected request, so
    marking the credentials this way drops them at the hop. Sending them
    onward would hand them to the storage host and trip S3's ``400
    InvalidArgument: Only one auth mechanism allowed``, since the presigned
    URL already carries its own signature.
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
    authoritative (no fallback to the upstream), and it authenticates like
    any registry (see ``get_registry_auth_headers``).

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

    The registry comes back in lowercase, since host names are
    case-insensitive: ``Docker.io/...`` is a Docker Hub reference too.

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
    host = parts[0].lower()
    if len(parts) == 1:
        registry = "registry-1.docker.io"
        repo = f"library/{parts[0]}"
    elif "." in host or ":" in host or host == "localhost":
        if host in _DOCKER_HUB_REGISTRIES:
            registry = "registry-1.docker.io"
            repo = f"library/{parts[1]}" if "/" not in parts[1] else parts[1]
        else:
            registry = host
            repo = parts[1]
    else:
        registry = "registry-1.docker.io"
        repo = image_ref

    return registry, repo, tag


# Keeps a cache entry's name, and the ``.lock`` and ``.tmp.<uuid>`` names
# derived from it, well under the 255-byte limit on a file name.
_CACHE_NAME_READABLE_CHARS = 128


def image_cache_name(image: str) -> str:
    """Name of an image's entry in the image cache.

    A registry reference is normalized first, so every spelling of one image
    (``python:3.12-slim``, ``library/python:3.12-slim``,
    ``docker.io/library/python:3.12-slim``) shares one entry instead of each
    pulling and building its own. The name is the normalized reference made
    safe for a file name, then a hash of the reference: the first part turns
    ``/``, ``:`` and ``@`` all into ``_``, so ``repo@sha256:<hex>`` and a tag
    named ``sha256_<hex>`` differ only in the hash.

    The name depends on the string alone, not on which files exist, so the
    pin a pull takes and its later release always agree on it.

    Args:
        image: Container image reference or path to a local tar archive.

    Returns:
        A name that is safe as a directory and file name.
    """
    if (
        not isinstance(image, str)
        or not image.strip()
        or image.endswith(".tar")
        or image.startswith(("/", "./", "../"))
    ):
        return sanitize_image_name(image)
    registry, repo, reference = parse_image_ref(image)
    if registry == "registry-1.docker.io":
        registry = "docker.io"
    separator = "@" if "@" in image else ":"
    ref = f"{registry}/{repo}{separator}{reference}"
    ref_hash = hashlib.sha256(ref.encode("utf-8")).hexdigest()[:16]
    return f"{sanitize_image_name(ref)[:_CACHE_NAME_READABLE_CHARS]}-{ref_hash}"


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
    """Answer a registry's authentication challenge for pulling ``repo``.

    An anonymous manifest request shows whether, and how, the registry wants
    callers to authenticate. Only a challenge sends Ray to the node's Docker
    config for the registry's credentials (see
    ``docker_config.registry_credentials``), so a pull that draws no
    challenge runs no credential helper. Docker Hub and GHCR challenge every
    pull, even of a public image. A ``Basic`` challenge, Amazon ECR's, takes
    the credentials on every request; a ``Bearer`` challenge trades them, or
    nothing for a public repository, for a token from the realm it names.

    Args:
        registry: Registry host, optionally with a scheme.
        repo: Repository path within the registry.
        reference: Tag or digest whose manifest is requested.
        timeout: Network and credential helper timeout in seconds.

    Returns:
        The ``Authorization`` header for the pull's registry requests, or no
        headers when the registry asks for none.

    Raises:
        SandboxCreationError: If the registry demands credentials that the
            node's Docker config lacks, or turns down the ones it has.
    """
    url = f"{registry_base_url(registry)}/v2/{repo}/manifests/{reference}"
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        urllib.request.urlopen(req, timeout=timeout)
        return {}
    except urllib.error.HTTPError as err:
        if err.code != 401:
            return {}
        auth_hdr = err.headers.get("Www-Authenticate", "")
        scheme = auth_hdr.split(None, 1)[0].lower() if auth_hdr.strip() else ""
        if scheme not in ("basic", "bearer"):
            return {}

        creds = docker_config.registry_credentials(registry, timeout)
        if creds is not None and creds.registry_token:
            return {"Authorization": f"Bearer {creds.registry_token}"}
        if scheme == "basic":
            if creds is None or not creds.username:
                raise SandboxCreationError(
                    f"Registry '{registry}' requires credentials to pull "
                    f"'{repo}', and the Docker config "
                    f"{docker_config.config_path()} provides none for it."
                )
            return {"Authorization": creds.basic_authorization()}

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

        if creds is not None and creds.identity_token:
            # The token spec's OAuth2 refresh-token grant.
            auth_url = realm
            form = dict(
                params,
                grant_type="refresh_token",
                refresh_token=creds.identity_token,
                client_id="ray-sandbox",
            )
            auth_req = urllib.request.Request(
                auth_url,
                data=urllib.parse.urlencode(form).encode("utf-8"),
                headers={"User-Agent": _USER_AGENT},
            )
        else:
            sep = "&" if "?" in realm else "?"
            auth_url = (
                f"{realm}{sep}{urllib.parse.urlencode(params)}" if params else realm
            )
            basic = None
            if creds is not None and creds.username:
                basic = creds.basic_authorization()
            auth_req = _registry_request(auth_url, {"User-Agent": _USER_AGENT}, basic)
        try:
            with urllib.request.urlopen(auth_req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                token = data.get("token") or data.get("access_token")
                if token:
                    return {"Authorization": f"Bearer {token}"}
        except Exception as auth_err:
            if creds is not None:
                raise SandboxCreationError(
                    f"Failed to get a pull token for '{registry}' from "
                    f"'{realm}' with the credentials in "
                    f"{docker_config.config_path()}: "
                    f"{_describe_pull_error(auth_err)}"
                ) from auth_err
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
    # Iterating reads the layer once; getmembers() first would decompress it
    # twice, since extractfile() then rewinds a compressed stream. Not stream
    # mode (r|*): it doesn't check where a gzip stream ends, so a layer cut
    # short on a member boundary would extract without an error.
    with tarfile.open(fileobj=tar_fileobj, mode="r:*") as tar:
        for member in tar:
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
                            shutil.rmtree(item_path, ignore_errors=True)
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
                    shutil.rmtree(del_path, ignore_errors=True)
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
                            shutil.rmtree(target_path, ignore_errors=True)
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


# EROFS keeps its superblock at this offset, in this many bytes.
_EROFS_SUPERBLOCK_OFFSET = 1024
_EROFS_SUPERBLOCK_SIZE = 128


def erofs_superblock_fingerprint(image_path: str) -> Optional[str]:
    """SHA-256 of an EROFS image's superblock, or None if it can't be read.

    gVisor refuses to restore a checkpoint over an EROFS root filesystem
    whose superblock differs in any field from the one it was saved with
    (it panics with "superblock mismatch detected on restore"), so two
    builds of an image are interchangeable for restore exactly when their
    fingerprints match.

    Args:
        image_path: Path of the EROFS image.

    Returns:
        The hex digest, or None if the image can't be read.
    """
    try:
        with open(image_path, "rb") as f:
            f.seek(_EROFS_SUPERBLOCK_OFFSET)
            superblock = f.read(_EROFS_SUPERBLOCK_SIZE)
    except OSError:
        return None
    return hashlib.sha256(superblock).hexdigest()


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


def _stream_tree(tar: tarfile.TarFile, path: str, arcname: str, owner_filter) -> None:
    """Add ``path`` and everything under it to ``tar`` and delete each file
    once its bytes are in the stream.

    Members go in the order ``tar.add`` uses (a directory before its
    entries, names sorted), so the image is the same as before. A file with
    other hard links stays: deleting it would drop the others' link count
    to one, and tar would then store them as copies instead of links.
    """
    st = os.lstat(path)
    tar.add(path, arcname, recursive=False, filter=owner_filter)
    if stat.S_ISDIR(st.st_mode):
        if not st.st_mode & stat.S_IWUSR:
            # Images ship read-only directories (0555, say). The stream
            # already has this one's mode, so making it writable only lets a
            # worker that isn't root delete its files.
            try:
                os.chmod(path, stat.S_IMODE(st.st_mode) | stat.S_IWUSR)
            except OSError:
                pass
        for name in sorted(os.listdir(path)):
            _stream_tree(
                tar, os.path.join(path, name), os.path.join(arcname, name), owner_filter
            )
    elif stat.S_ISREG(st.st_mode) and st.st_nlink == 1:
        try:
            os.unlink(path)
        except OSError:
            pass  # It stays for the caller to remove, as before.


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

    The tar streams into mkfs.erofs's stdin, which it reads when given no
    source (erofs-utils 1.7+), and each file leaves the tree once it's in
    the stream. A build no longer holds the whole tree and a flat tar of it
    next to the image, two full copies, so concurrent cold pulls are much
    less likely to fill the node's disk. mkfs.erofs still spools file data
    under ``$TMPDIR`` (its default ``--sort=path``).

    Args:
        rootfs_dir: Extracted root filesystem. Its files are deleted as
            they're read; the caller removes what's left.
        ownership: {path: (uid, gid)} recorded during extraction.
        out_path: Destination image file.
    """
    mkfs = require_mkfs_erofs()
    # mkfs.erofs's output goes to a file: with a pipe, it could block on a
    # full pipe while this side blocks on a full stdin.
    with tempfile.TemporaryFile() as output:
        proc = subprocess.Popen(
            [mkfs, "--tar=f", f"-b{mmap.PAGESIZE}", "-E^inline_data", out_path],
            stdin=subprocess.PIPE,
            stdout=output,
            stderr=subprocess.STDOUT,
        )
        try:
            try:
                with tarfile.open(
                    fileobj=proc.stdin, mode="w|", bufsize=1 << 20
                ) as tar:
                    _stream_tree(tar, rootfs_dir, ".", _owner_filter(ownership))
                proc.stdin.close()
            except BrokenPipeError:
                # mkfs.erofs stopped reading; its exit status and output say why.
                pass
            returncode = proc.wait()
        except BaseException:
            proc.kill()
            proc.wait()
            raise
        finally:
            try:
                proc.stdin.close()
            except BrokenPipeError:
                pass
        if returncode != 0:
            output.seek(0)
            message = output.read().decode(errors="replace").strip()[-500:]
            raise SandboxCreationError(f"mkfs.erofs failed: {message}")


_IMAGE_CACHE_MAX_BYTES_ENV = "RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES"
_IMAGE_CACHE_MIN_FREE_BYTES_ENV = "RAY_SANDBOX_IMAGE_CACHE_MIN_FREE_BYTES"
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


def image_cache_min_free_bytes(images_dir: str) -> int:
    """Return the free space that eviction keeps on the cache's filesystem.

    The cache shares its filesystem with sandboxes' writable overlays, logs
    and spilled objects, so it gives way when they fill the disk.
    ``RAY_SANDBOX_IMAGE_CACHE_MIN_FREE_BYTES`` sets the floor in bytes; ``0``
    disables it. Unset, it is a tenth of the filesystem that holds
    ``images_dir``, the level at which the kubelet starts evicting Pods.

    Args:
        images_dir: Root image cache directory.

    Returns:
        The floor in bytes; 0 disables it.
    """
    raw = os.environ.get(_IMAGE_CACHE_MIN_FREE_BYTES_ENV)
    if raw is not None and raw.strip():
        try:
            return max(int(raw), 0)
        except ValueError:
            logger.warning(
                "Ignoring %s=%r: expected an integer number of bytes.",
                _IMAGE_CACHE_MIN_FREE_BYTES_ENV,
                raw,
            )
    try:
        return shutil.disk_usage(images_dir).total // 10
    except OSError:
        return 0


def _free_bytes(path: str) -> Optional[int]:
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return None


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
        shutil.rmtree(stale, ignore_errors=True)


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
    ``.deleting.<random>``, and return the new path to delete.

    A crash midway through deleting it then never leaves a partly deleted
    image under the image's own name, and the next pull sweeps up what such a
    crash leaves behind (see ``_remove_stale_deletions``). The new name
    leaves out the image's, so it stays short whatever the image's name.
    """
    images_dir = os.path.dirname(image_dir)
    renamed = os.path.join(images_dir, f".deleting.{uuid.uuid4().hex}")
    os.replace(image_dir, renamed)
    return renamed


def _remove_stale_deletions(images_dir: str) -> None:
    """Finish deleting image directories whose deletion was interrupted (see
    ``_rename_for_deletion``)."""
    for name in os.listdir(images_dir):
        if name.startswith(".deleting."):
            shutil.rmtree(os.path.join(images_dir, name), ignore_errors=True)


def evict_least_recently_used_images(
    images_dir: str,
    max_bytes: Optional[int],
    keep: Optional[str] = None,
    min_free_bytes: int = 0,
) -> None:
    """Evict least-recently-extracted images until the cache fits ``max_bytes``
    and its filesystem has ``min_free_bytes`` free.

    Nodes cache every image they ever ran, so without a cap a long-lived
    node eventually fills its disk, and the cache also gives way when
    sandboxes' overlays or anything else fill it. Candidates are fully extracted images
    (``.extracted`` marker present) and ``<name>.tar`` archives left behind
    by earlier Ray versions, oldest first. An image is skipped when a live sandbox uses it, when its
    per-image lock is held (a pull in progress), or when it is ``keep``. The
    in-use check is repeated under the lock, which is also where pulls
    register their users, so a marked image is never removed.

    Args:
        images_dir: Root image cache directory.
        max_bytes: The cache size cap in bytes, or None for no cap.
        keep: Sanitized name of an image that must survive this pass.
        min_free_bytes: Free space to keep on the cache's filesystem.
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
        # sanitize_image_name strips leading dots, so no cached image's name
        # starts with one. Skip image directories renamed for deletion,
        # whose names do (see _rename_for_deletion).
        if name.startswith("."):
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
    free = _free_bytes(images_dir)
    for _, name, img_dir, tar_path, size in sorted(entries):
        if (max_bytes is None or total <= max_bytes) and (
            free is None or free >= min_free_bytes
        ):
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
                    _rename_for_deletion(img_dir)
                try:
                    os.remove(tar_path)
                except OSError:
                    pass
        except OSError:
            continue  # Locked by an in-progress pull; try the next one.
        # Delete the image once its lock is free, so a pull of it doesn't wait
        # for the delete.
        _remove_stale_deletions(images_dir)
        total -= size
        free = _free_bytes(images_dir)
        logger.info("Evicted cached sandbox image %s (%d bytes)", name, size)


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

    safe_name = image_cache_name(image)
    target_dir = os.path.join(images_dir, safe_name)
    lock_path = os.path.join(images_dir, f"{safe_name}.lock")

    _remove_stale_deletions(images_dir)
    max_cache = image_cache_max_bytes(images_dir)
    min_free = image_cache_min_free_bytes(images_dir)
    if max_cache > 0 or min_free > 0:
        evict_least_recently_used_images(
            images_dir,
            max_cache if max_cache > 0 else None,
            keep=safe_name,
            min_free_bytes=min_free,
        )

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
                shutil.rmtree(tmp_extract_dir, ignore_errors=True)
                raise
            # The image replaces the tree; nothing reads the tree once the
            # Sentry mounts the image.
            shutil.rmtree(tmp_rootfs_dir, ignore_errors=True)

            with open(
                os.path.join(tmp_extract_dir, ".extracted"), "w", encoding="utf-8"
            ) as f_mark:
                f_mark.write(expected_marker)

            # Swap the new cache in. A re-pull replaces a cache of another
            # format; sandboxes already running on it keep their pins, and an
            # extracted ``rootfs/`` tree their gofer still serves moves over
            # intact (a rename leaves its open root fd alone) and goes once no
            # sandbox uses the image. The old image moves aside and the new one
            # takes its name before the old one is deleted, so a crash during
            # the delete leaves the new image in place.
            try:
                users = os.listdir(os.path.join(target_dir, _USERS_SUBDIR))
            except OSError:
                users = []
            old_tree = os.path.join(target_dir, "rootfs")
            if users and os.path.isdir(old_tree):
                os.replace(old_tree, tmp_rootfs_dir)
            if os.path.exists(target_dir):
                _rename_for_deletion(target_dir)
            os.replace(tmp_extract_dir, target_dir)
            for user in users:
                _mark_image_in_use(target_dir, user)

            return _finish()
        finally:
            try:
                fcntl.flock(f_lock, fcntl.LOCK_UN)
            except Exception:
                pass
            # Delete an image the pull moved aside once the lock is free, so
            # other pulls of this image don't wait for it.
            _remove_stale_deletions(images_dir)


def _download_blob(
    url: str,
    headers: Dict[str, str],
    auth_header: Optional[str],
    timeout_seconds: float,
    blob_dir: str,
    stop: threading.Event,
) -> BinaryIO:
    """Download a registry blob into an anonymous temporary file.

    Args:
        url: The blob's URL.
        headers: Request headers.
        auth_header: The registry's ``Authorization`` value, if any.
        timeout_seconds: Network request timeout.
        blob_dir: Directory for the temporary file.
        stop: Set to end the download early.

    Returns:
        The file, rewound; a partial one if ``stop`` was set meanwhile.
    """
    blob = tempfile.TemporaryFile(dir=blob_dir)
    if stop.is_set():
        return blob
    try:
        req = _registry_request(url, headers, auth_header)
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            while not stop.is_set():
                # read1 returns what has arrived, so a stop takes effect soon.
                chunk = resp.read1(1 << 20)
                if not chunk:
                    break
                blob.write(chunk)
        blob.seek(0)
    except BaseException:
        blob.close()
        raise
    return blob


def _download_blobs(
    urls: List[str],
    headers: Dict[str, str],
    auth_header: Optional[str],
    timeout_seconds: float,
    blob_dir: str,
) -> Iterator[BinaryIO]:
    """Yield registry blobs in order, downloading a few of them at a time.

    The caller extracts each layer while the ones after it download, and
    closes each blob it gets. Closing the generator, as on an error, returns
    at once: downloads still running stop at their next read, and the blobs
    nobody took get closed.

    Args:
        urls: Blob URLs, in the order to yield them.
        headers: Request headers.
        auth_header: The registry's ``Authorization`` value, if any.
        timeout_seconds: Network request timeout.
        blob_dir: Directory for the temporary files.

    Yields:
        BinaryIO: Each blob, as a rewound temporary file.
    """
    stop = threading.Event()
    pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=_LAYER_DOWNLOADS, thread_name_prefix="sandbox-layer-download"
    )
    downloads = [
        pool.submit(
            _download_blob, url, headers, auth_header, timeout_seconds, blob_dir, stop
        )
        for url in urls
    ]
    yielded = 0
    try:
        for download in downloads:
            blob = download.result()
            yielded += 1
            yield blob
    finally:
        stop.set()
        for download in downloads[yielded:]:
            download.cancel()
            download.add_done_callback(_close_blob)
        pool.shutdown(wait=False)


def _close_blob(download: "concurrent.futures.Future[BinaryIO]") -> None:
    if not download.cancelled() and download.exception() is None:
        download.result().close()


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

        blob_urls = [
            f"{registry_base_url(registry)}/v2/{repo}/blobs/{layer['digest']}"
            for layer in layers
        ]
        with contextlib.closing(
            _download_blobs(blob_urls, headers, auth_header, timeout_seconds, blob_dir)
        ) as blobs:
            for blob in blobs:
                with blob:
                    extract_tar_layer(blob, rootfs_dir, ownership=ownership)

    except Exception as err:
        if isinstance(err, SandboxCreationError):
            raise
        raise SandboxCreationError(
            f"Failed to pull and extract container image '{image}': "
            f"{_describe_pull_error(err)}"
        ) from err
    return ownership


def _describe_pull_error(err: Exception) -> str:
    """Describe a failed pull, with the registry's own reasons for an HTTP error.

    A registry's error body says why it refused a request: Amazon ECR's
    ``DENIED``, for one, names the IAM action that the node's role lacks.
    """
    if not isinstance(err, urllib.error.HTTPError):
        return str(err)
    try:
        errors = json.loads(err.read(65536)).get("errors") or []
        reasons = "; ".join(f"{e['code']}: {e['message']}" for e in errors)
    except Exception:
        reasons = ""
    return f"{err}: {reasons}" if reasons else str(err)
