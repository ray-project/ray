import fcntl
import io
import json
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import BinaryIO, Dict, Optional, Tuple, Union

from ray.experimental.sandbox._internal.idmap import (
    IdMap,
    detect_idmap,
    mapped_userns,
    remove_tree_as_mapped_root,
    run_as_mapped_root,
)
from ray.experimental.sandbox.exceptions import SandboxCreationError

logger = logging.getLogger(__name__)

DEFAULT_IMAGES_DIR = "/tmp/ray/sandbox/images"
_USER_AGENT = "ray-sandbox/1.0 (python-urllib)"

# Cache layout version recorded in each image's ``.extracted`` marker, next
# to the uid/gid mapping the rootfs was extracted for. A cache written under
# another version or mapping is re-extracted once.
_EXTRACT_FORMAT = 2

# Warn once per process about uids the node's subordinate range cannot map.
_UNMAPPED_ID_WARNED = False


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


def _drop_ownership_subtree(ownership: Dict[str, Tuple[int, int]], name: str) -> None:
    """Forget recorded owners for a deleted path and everything under it."""
    ownership.pop(name, None)
    prefix = name + "/"
    for key in [k for k in ownership if k.startswith(prefix)]:
        del ownership[key]


def _lchown_preserving(target_path: str, uid: int, gid: int) -> None:
    """lchown that tolerates ids outside the mapped subordinate range."""
    global _UNMAPPED_ID_WARNED
    try:
        os.lchown(target_path, uid, gid)
    except OSError as err:
        if not _UNMAPPED_ID_WARNED:
            _UNMAPPED_ID_WARNED = True
            logger.warning(
                "Could not chown '%s' to %d:%d (%s); ids outside the mapped "
                "subordinate range keep the extracting user's ownership "
                "(warning once).",
                target_path,
                uid,
                gid,
                err,
            )


def extract_tar_layer(
    tar_input: Union[bytes, io.IOBase, BinaryIO],
    dest_dir: str,
    ownership: Optional[Dict[str, Tuple[int, int]]] = None,
) -> None:
    """Extract a tar archive layer onto dest_dir with OCI whiteout handling.

    ``ownership`` (shared by the caller across an image's layers) records the
    final {path: (uid, gid)} for members shipped with a non-root owner;
    whiteouts drop entries. The extracted files themselves stay owned by the
    extracting user; the idmapped-rootfs build applies the recorded owners.
    Directory modes are applied children-first after the loop, since a
    restrictive parent written mid-extraction could block its own children.
    """
    if isinstance(tar_input, bytes):
        tar_fileobj = io.BytesIO(tar_input)
    else:
        tar_fileobj = tar_input

    # {dir target_path: (mode, mtime)} in final (last-layer-wins) state,
    # applied children-first after the loop. The mtime matters: apt inside
    # the sandbox revalidates package lists with If-Modified-Since from the
    # directory mtime, so a reset-to-now mtime makes mirrors answer 304 for
    # stale baked lists.
    deferred_dirs: Dict[str, Tuple[int, int]] = {}

    with tarfile.open(fileobj=tar_fileobj, mode="r:*") as tar:
        for member in tar.getmembers():
            name = member.name.lstrip("/")

            # Prevent path traversal
            if ".." in name.split(os.sep) or name.startswith(os.sep):
                continue

            target_path = os.path.abspath(os.path.join(dest_dir, name))
            dest_abs = os.path.abspath(dest_dir)

            # Prevent symlink traversal
            dirname = os.path.dirname(name)
            parent_dir = os.path.abspath(os.path.join(dest_dir, dirname))
            real_parent_dir = os.path.realpath(parent_dir)
            dest_real = os.path.realpath(dest_dir)

            if not (
                target_path == dest_abs or target_path.startswith(dest_abs + os.sep)
            ) or not (
                real_parent_dir == dest_real
                or real_parent_dir.startswith(dest_real + os.sep)
            ):
                continue

            basename = os.path.basename(name)

            # Handle OCI opaque whiteout (.wh..wh..opq)
            if basename == ".wh..wh..opq":
                if os.path.exists(parent_dir):
                    for item in os.listdir(parent_dir):
                        item_path = os.path.join(parent_dir, item)
                        if os.path.isdir(item_path) and not os.path.islink(item_path):
                            shutil.rmtree(item_path, ignore_errors=True)
                        else:
                            try:
                                os.remove(item_path)
                            except OSError:
                                pass
                if ownership is not None and dirname:
                    for key in [k for k in ownership if k.startswith(dirname + "/")]:
                        del ownership[key]
                continue

            # Handle OCI deletion whiteout (.wh.<filename>)
            if basename.startswith(".wh."):
                del_name = basename[4:]
                del_path = os.path.join(parent_dir, del_name)
                if os.path.isdir(del_path) and not os.path.islink(del_path):
                    shutil.rmtree(del_path, ignore_errors=True)
                elif os.path.exists(del_path) or os.path.islink(del_path):
                    try:
                        os.remove(del_path)
                    except OSError:
                        pass
                if ownership is not None:
                    _drop_ownership_subtree(
                        ownership,
                        os.path.join(dirname, del_name) if dirname else del_name,
                    )
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
                with open(target_path, "wb") as f_out:
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
                # Deferred to the post-loop pass: tar lists a directory before
                # its contents, so a restrictive archived mode (0500) applied
                # here would break extracting the children. Preserved symlinks
                # (UsrMerge) are skipped: chmod/utime follow them.
                if not os.path.islink(target_path):
                    deferred_dirs[target_path] = (member.mode, member.mtime)
            elif member.issym():
                os.makedirs(parent_dir, exist_ok=True)
                try:
                    os.symlink(member.linkname, target_path)
                except OSError:
                    pass
            elif member.islnk():
                os.makedirs(parent_dir, exist_ok=True)
                link_target = os.path.abspath(
                    os.path.join(dest_dir, member.linkname.lstrip("/"))
                )
                if link_target.startswith(dest_abs + os.sep):
                    try:
                        os.link(link_target, target_path)
                    except OSError:
                        pass
                # Hardlinks share the target's inode: no chown, and the
                # ownership record comes from the link target's own member.

            if ownership is not None and not member.islnk():
                if member.uid or member.gid:
                    ownership[name] = (member.uid, member.gid)
                else:
                    # A later layer re-shipping the path as root wins.
                    ownership.pop(name, None)

    if deferred_dirs:
        # Children first: a restrictive parent mode (0500) applied before its
        # children would block extracting them.
        for target_path in sorted(
            deferred_dirs, key=lambda p: p.count(os.sep), reverse=True
        ):
            mode, mtime = deferred_dirs[target_path]
            try:
                if mode:
                    os.chmod(target_path, mode)
                os.utime(target_path, (mtime, mtime))
            except OSError:
                pass


def expected_extract_marker(idmap: Optional[IdMap]) -> str:
    """The ``.extracted`` content a cache built for ``idmap`` must carry."""
    mapping = None
    if idmap is not None:
        mapping = [
            idmap.subuid_base,
            idmap.subuid_count,
            idmap.subgid_base,
            idmap.subgid_count,
        ]
    return json.dumps({"format": _EXTRACT_FORMAT, "idmap": mapping}, sort_keys=True)


def _apply_ownership_in_namespace(
    rootfs: str,
    ownership: Dict[str, Tuple[int, int]],
    idmap: IdMap,
    timeout_seconds: float,
) -> None:
    """Give a freshly extracted rootfs the image's real owners.

    Runs ``idmap_extract`` as root inside a user namespace mapped with
    ``idmap``: only there can ``lchown`` produce the subordinate host ids
    that read as the image's uids from inside a sandbox.
    """
    if not ownership:
        return
    ownership_path = f"{rootfs}.ownership.json"
    with open(ownership_path, "w", encoding="utf-8") as f:
        json.dump({p: list(ids) for p, ids in ownership.items()}, f)
    try:
        with mapped_userns(idmap, timeout=max(timeout_seconds, 60.0)) as pid:
            res = run_as_mapped_root(
                pid,
                [
                    sys.executable,
                    "-m",
                    "ray.experimental.sandbox._internal.idmap_extract",
                    rootfs,
                    ownership_path,
                ],
                timeout=timeout_seconds,
            )
    except (RuntimeError, subprocess.TimeoutExpired) as err:
        raise SandboxCreationError(f"applying image ownership failed: {err}") from err
    finally:
        try:
            os.remove(ownership_path)
        except OSError:
            pass
    if res.returncode != 0:
        raise SandboxCreationError(
            "applying image ownership failed: "
            + res.stderr.decode(errors="replace").strip()
        )


def _remove_image_tree(path: str) -> None:
    """Remove a cached image tree; multi-uid caches need the node's mapping."""
    shutil.rmtree(path, ignore_errors=True)
    if not os.path.lexists(path):
        return
    idmap = detect_idmap()
    if idmap is not None:
        remove_tree_as_mapped_root(path, idmap)
    if os.path.lexists(path):
        logger.warning(
            "Could not fully remove %s; it holds files owned by subordinate ids "
            "and the node's id mapping is unavailable.",
            path,
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


def _dir_size_bytes(path: str) -> int:
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for name in filenames:
            try:
                total += os.lstat(os.path.join(dirpath, name)).st_size
            except OSError:
                pass
    return total


def _file_size_bytes(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def evict_least_recently_used_images(
    images_dir: str, max_bytes: int, keep: Optional[str] = None
) -> None:
    """Evict least-recently-extracted images until the cache fits ``max_bytes``.

    Nodes cache every image they ever ran, so without a cap a long-lived
    node eventually fills its disk. Candidates are fully extracted images
    (``.extracted`` marker present) and stray ``<name>.tar`` archives left by
    earlier Ray versions, oldest first. An image is skipped when a live
    sandbox uses it, when its per-image lock is held (a pull in progress), or
    when it is ``keep``. The in-use check is repeated under the lock, which is
    also where pulls register their users, so a marked image is never removed.

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
        marker = os.path.join(path, ".extracted")
        try:
            if not (os.path.isdir(path) and os.path.exists(marker)):
                continue
            mtime = os.path.getmtime(marker)
        except OSError:
            continue  # Concurrently deleted; keep going.
        tar_path = os.path.join(images_dir, f"{name}.tar")
        # Subordinate-owned subtrees of a multi-uid cache are unreadable
        # here, so this is a lower bound for them.
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
                if img_dir is not None:
                    _remove_image_tree(img_dir)
                try:
                    os.remove(tar_path)
                except OSError:
                    pass
        except OSError:
            continue  # Locked by an in-progress pull; try the next one.
        total -= size
        logger.info("Evicted cached sandbox image %s (%d bytes)", name, size)


def pull_and_extract_container_image(
    image: str,
    images_dir: str = DEFAULT_IMAGES_DIR,
    timeout_seconds: float = 120.0,
    instance_id: Optional[str] = None,
) -> str:
    """Pull container image via Registry v2 HTTP API and extract rootfs into local directory.

    Args:
        image: Container image name (e.g. 'python:3.10-slim') or path to local tar archive.
        images_dir: Root directory for caching container images.
        timeout_seconds: Network request timeout.
        instance_id: When given, the sandbox instance is registered as a
            user of the image under the image lock, so cache eviction
            leaves the image alone until the instance releases it.

    Returns:
        Absolute directory path containing the extracted container filesystem.
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

    max_cache = image_cache_max_bytes(images_dir)
    if max_cache > 0:
        evict_least_recently_used_images(images_dir, max_cache, keep=safe_name)

    # The node's mapping decides the cache's ownership layout: multi-uid nodes
    # store the image's real owners (at subordinate host ids), single-uid
    # nodes store everything worker-owned.
    idmap = detect_idmap()
    expected_marker = expected_extract_marker(idmap)

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
                # A cache from another layout version or id mapping falls
                # through to a one-time re-pull.
                if marker_current:
                    if os.path.isfile(image):
                        if os.path.getmtime(marker_path) >= os.path.getmtime(image):
                            return _finish()
                    else:
                        return _finish()

            tmp_extract_dir = os.path.join(
                images_dir, f"{safe_name}.tmp.{uuid.uuid4().hex}"
            )
            os.makedirs(tmp_extract_dir, mode=0o755, exist_ok=True)

            tmp_rootfs_dir = os.path.join(tmp_extract_dir, "rootfs")
            os.makedirs(tmp_rootfs_dir, mode=0o755, exist_ok=True)

            ownership: Dict[str, Tuple[int, int]] = {}

            if os.path.isfile(image):
                try:
                    with open(image, "rb") as f:
                        extract_tar_layer(f, tmp_rootfs_dir, ownership=ownership)
                except Exception as err:
                    _remove_image_tree(tmp_extract_dir)
                    raise SandboxCreationError(
                        f"Failed to extract local image archive '{image}': {err}"
                    ) from err
            else:
                if (
                    image.endswith(".tar")
                    or image.startswith("/")
                    or image.startswith("./")
                    or image.startswith("../")
                ):
                    _remove_image_tree(tmp_extract_dir)
                    raise SandboxCreationError(
                        f"Local image archive '{image}' not found."
                    )
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

                    manifest_url = (
                        f"{registry_base_url(registry)}/v2/{repo}/manifests/{reference}"
                    )
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
                        with urllib.request.urlopen(
                            sub_req, timeout=timeout_seconds
                        ) as resp:
                            manifest_data = json.loads(resp.read().decode("utf-8"))

                    # extract image config so we can reference metadata bout the image later.
                    config_desc = manifest_data.get("config")
                    if config_desc and "digest" in config_desc:
                        config_digest = config_desc["digest"]
                        config_url = f"{registry_base_url(registry)}/v2/{repo}/blobs/{config_digest}"
                        config_req = _registry_request(config_url, headers, auth_header)
                        try:
                            with urllib.request.urlopen(
                                config_req, timeout=timeout_seconds
                            ) as resp:
                                config_bytes = resp.read()
                                with open(
                                    os.path.join(tmp_extract_dir, ".image_config.json"),
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
                        blob_url = (
                            f"{registry_base_url(registry)}/v2/{repo}/blobs/{digest}"
                        )
                        blob_req = _registry_request(blob_url, headers, auth_header)
                        with urllib.request.urlopen(
                            blob_req, timeout=timeout_seconds
                        ) as blob_resp:
                            with tempfile.NamedTemporaryFile(
                                dir=images_dir, delete=True
                            ) as tmp_blob_file:
                                shutil.copyfileobj(
                                    blob_resp, tmp_blob_file, length=64 * 1024
                                )
                                tmp_blob_file.seek(0)
                                extract_tar_layer(
                                    tmp_blob_file,
                                    tmp_rootfs_dir,
                                    ownership=ownership,
                                )

                except Exception as err:
                    _remove_image_tree(tmp_extract_dir)
                    if isinstance(err, SandboxCreationError):
                        raise
                    raise SandboxCreationError(
                        f"Failed to pull and extract container image '{image}': {err}"
                    ) from err

            if idmap is not None:
                try:
                    _apply_ownership_in_namespace(
                        tmp_rootfs_dir, ownership, idmap, timeout_seconds
                    )
                except Exception:
                    _remove_image_tree(tmp_extract_dir)
                    raise

            with open(
                os.path.join(tmp_extract_dir, ".extracted"), "w", encoding="utf-8"
            ) as f_mark:
                f_mark.write(expected_marker)

            # A re-extract (stale marker) replaces the directory; keep the
            # pins of sandboxes already running on the old extraction.
            try:
                users = os.listdir(os.path.join(target_dir, _USERS_SUBDIR))
            except OSError:
                users = []
            if os.path.exists(target_dir):
                _remove_image_tree(target_dir)
            os.replace(tmp_extract_dir, target_dir)
            for user in users:
                _mark_image_in_use(target_dir, user)

            return _finish()
        finally:
            try:
                fcntl.flock(f_lock, fcntl.LOCK_UN)
            except Exception:
                pass
