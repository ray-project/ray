import fcntl
import io
import json
import logging
import os
import platform
import re
import shutil
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import BinaryIO, Dict, Optional, Tuple, Union

from ray.experimental.sandbox.exceptions import SandboxCreationError

logger = logging.getLogger(__name__)

DEFAULT_IMAGES_DIR = "/tmp/ray/sandbox/images"
_USER_AGENT = "ray-sandbox/1.0 (python-urllib)"


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


def extract_tar_layer(
    tar_input: Union[bytes, io.IOBase, BinaryIO], dest_dir: str
) -> None:
    """Extract a tar archive layer onto dest_dir with OCI whiteout handling."""
    if isinstance(tar_input, bytes):
        tar_fileobj = io.BytesIO(tar_input)
    else:
        tar_fileobj = tar_input

    dir_mtimes = []
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
                link_target = os.path.abspath(
                    os.path.join(dest_dir, member.linkname.lstrip("/"))
                )
                if link_target.startswith(dest_abs + os.sep):
                    try:
                        os.link(link_target, target_path)
                    except OSError:
                        pass

    # Children first, so a parent's restrictive mode cannot block them.
    for dir_path, mode, mtime in reversed(dir_mtimes):
        try:
            if mode:
                os.chmod(dir_path, mode)
            os.utime(dir_path, (mtime, mtime))
        except OSError:
            pass


_IMAGE_CACHE_MAX_BYTES_ENV = "RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES"
_KEEP_IMAGE_TARBALL_ENV = "RAY_SANDBOX_KEEP_IMAGE_TARBALL"
_USERS_DIRNAME = ".users"
# Images extracted more recently than this are never evicted: it covers the
# window between a pull returning and its sandbox registering as a user.
_EVICTION_GRACE_SEC = 600


def mark_image_in_use(image_dir: str, instance_id: str) -> None:
    """Record that a live sandbox uses this cached image (blocks eviction).

    Args:
        image_dir: The image's cache directory.
        instance_id: The sandbox instance using it.
    """
    try:
        users_dir = os.path.join(image_dir, _USERS_DIRNAME)
        os.makedirs(users_dir, exist_ok=True)
        with open(os.path.join(users_dir, instance_id), "w", encoding="utf-8"):
            pass
    except OSError:
        pass


def release_image_use(image_dir: str, instance_id: str) -> None:
    """Drop a sandbox's in-use record for a cached image.

    Args:
        image_dir: The image's cache directory.
        instance_id: The sandbox instance releasing it.
    """
    try:
        os.remove(os.path.join(image_dir, _USERS_DIRNAME, instance_id))
    except OSError:
        pass


def _dir_size_bytes(path: str) -> int:
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for name in filenames:
            try:
                total += os.lstat(os.path.join(dirpath, name)).st_size
            except OSError:
                pass
    return total


def evict_images_over_cap(images_dir: str, max_bytes: int) -> None:
    """Evict least-recently-extracted images until the cache fits the cap.

    Sandbox images are large (a rootfs per image), nodes cache every image
    they ever ran, and nothing else reclaims the space — a long-lived node
    eventually fills its disk, which takes down far more than sandboxes.
    Called before each new pull when ``RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES``
    is set.

    Safety: only images with an ``.extracted`` marker, no live users
    (``mark_image_in_use``), and whose per-image lock can be taken without
    blocking are candidates; candidates are removed oldest-first.

    Args:
        images_dir: Root image cache directory.
        max_bytes: The cache size cap in bytes.
    """
    try:
        names = os.listdir(images_dir)
    except OSError:
        return
    now = time.time()
    entries = []
    for name in names:
        img_dir = os.path.join(images_dir, name)
        marker = os.path.join(img_dir, ".extracted")
        try:
            if not (os.path.isdir(img_dir) and os.path.exists(marker)):
                continue
            marker_mtime = os.path.getmtime(marker)
        except OSError:
            # Concurrently deleted: skip it, keep evicting the rest.
            continue
        if now - marker_mtime < _EVICTION_GRACE_SEC:
            # Freshly extracted: the puller may not have registered its
            # sandbox as a user yet.
            continue
        entries.append((marker_mtime, name, img_dir))

    total = sum(_dir_size_bytes(img_dir) for _, _, img_dir in entries)
    for _, name, img_dir in sorted(entries):
        if total <= max_bytes:
            return
        users_dir = os.path.join(img_dir, _USERS_DIRNAME)
        try:
            if os.path.isdir(users_dir) and os.listdir(users_dir):
                continue
        except OSError:
            continue
        lock_path = os.path.join(images_dir, f"{name}.lock")
        try:
            with open(lock_path, "w", encoding="utf-8") as f_lock:
                fcntl.flock(f_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                size = _dir_size_bytes(img_dir)
                shutil.rmtree(img_dir, ignore_errors=True)
                tar_path = os.path.join(images_dir, f"{name}.tar")
                if os.path.exists(tar_path):
                    size += os.path.getsize(tar_path)
                    os.remove(tar_path)
                total -= size
                logger.info("Evicted cached sandbox image %s (%d bytes)", name, size)
        except OSError:
            continue


def pull_and_extract_container_image(
    image: str,
    images_dir: str = DEFAULT_IMAGES_DIR,
    timeout_seconds: float = 120.0,
) -> str:
    """Pull container image via Registry v2 HTTP API and extract rootfs into local directory.

    Args:
        image: Container image name (e.g. 'python:3.10-slim') or path to local tar archive.
        images_dir: Root directory for caching container images.
        timeout_seconds: Network request timeout.

    Returns:
        Absolute directory path containing the extracted container filesystem.
    """
    try:
        os.makedirs(images_dir, mode=0o777, exist_ok=True)
    except Exception as err:
        raise SandboxCreationError(
            f"Failed to create images directory '{images_dir}': {err}"
        ) from err

    max_cache = int(os.environ.get(_IMAGE_CACHE_MAX_BYTES_ENV, "0") or "0")
    if max_cache > 0:
        evict_images_over_cap(images_dir, max_cache)

    safe_name = sanitize_image_name(image)
    target_dir = os.path.join(images_dir, safe_name)
    lock_path = os.path.join(images_dir, f"{safe_name}.lock")

    with open(lock_path, "w", encoding="utf-8") as f_lock:
        try:
            fcntl.flock(f_lock, fcntl.LOCK_EX)
            marker_path = os.path.join(target_dir, ".extracted")
            if os.path.isdir(target_dir) and os.path.exists(marker_path):
                if os.path.isfile(image):
                    if os.path.getmtime(marker_path) >= os.path.getmtime(image):
                        return target_dir
                else:
                    return target_dir

            tmp_extract_dir = os.path.join(
                images_dir, f"{safe_name}.tmp.{uuid.uuid4().hex}"
            )
            os.makedirs(tmp_extract_dir, mode=0o755, exist_ok=True)

            tmp_rootfs_dir = os.path.join(tmp_extract_dir, "rootfs")
            os.makedirs(tmp_rootfs_dir, mode=0o755, exist_ok=True)

            tar_path = os.path.join(images_dir, f"{safe_name}.tar")

            if os.path.isfile(image):
                try:
                    with open(image, "rb") as f:
                        extract_tar_layer(f, tmp_rootfs_dir)
                except Exception as err:
                    shutil.rmtree(tmp_extract_dir, ignore_errors=True)
                    raise SandboxCreationError(
                        f"Failed to extract local image archive '{image}': {err}"
                    ) from err
            elif os.path.isfile(tar_path):
                try:
                    with open(tar_path, "rb") as f:
                        extract_tar_layer(f, tmp_extract_dir)
                except Exception as err:
                    shutil.rmtree(tmp_extract_dir, ignore_errors=True)
                    raise SandboxCreationError(
                        f"Failed to extract cached image archive '{tar_path}': {err}"
                    ) from err
            else:
                if (
                    image.endswith(".tar")
                    or image.startswith("/")
                    or image.startswith("./")
                    or image.startswith("../")
                ):
                    shutil.rmtree(tmp_extract_dir, ignore_errors=True)
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
                                extract_tar_layer(tmp_blob_file, tmp_rootfs_dir)

                    # The uncompressed tarball doubles every image's disk
                    # footprint for a rarely-exercised restore path; keep it
                    # only on request.
                    if os.environ.get(_KEEP_IMAGE_TARBALL_ENV) == "1":
                        with tarfile.open(tar_path, "w") as tar:
                            tar.add(tmp_extract_dir, arcname=".")

                except Exception as err:
                    shutil.rmtree(tmp_extract_dir, ignore_errors=True)
                    if os.path.exists(tar_path):
                        try:
                            os.remove(tar_path)
                        except OSError:
                            pass
                    if isinstance(err, SandboxCreationError):
                        raise
                    raise SandboxCreationError(
                        f"Failed to pull and extract container image '{image}': {err}"
                    ) from err

            with open(
                os.path.join(tmp_extract_dir, ".extracted"), "w", encoding="utf-8"
            ) as f_mark:
                f_mark.write("ok")

            if os.path.exists(target_dir):
                shutil.rmtree(target_dir, ignore_errors=True)
            os.replace(tmp_extract_dir, target_dir)

            return target_dir
        finally:
            try:
                fcntl.flock(f_lock, fcntl.LOCK_UN)
            except Exception:
                pass
