import fcntl
import io
import json
import logging
import os
import platform
import re
import shutil
import tarfile
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Dict, Tuple

from ray.experimental.sandbox.exceptions import SandboxCreationError

logger = logging.getLogger(__name__)

DEFAULT_IMAGES_DIR = "/tmp/ray/sandbox/images"
_USER_AGENT = "ray-sandbox/1.0 (python-urllib)"


def sanitize_image_name(image: str) -> str:
    """Sanitize container image name into a safe directory and filename."""
    if not isinstance(image, str):
        raise TypeError(f"Expected image to be a string, got {type(image).__name__}")
    safe = re.sub(r"[^a-zA-Z0-9_.-]", "_", image)
    safe = safe.lstrip(".")
    if not safe:
        raise ValueError(f"Invalid image name '{image}': cannot be safely sanitized.")
    return safe


def parse_image_ref(image_ref: str) -> Tuple[str, str, str]:
    """Parse image reference string into (registry, repository, tag_or_digest).

    Args:
        image_ref: Container image reference string (e.g. 'busybox:latest',
            'python:3.10-slim', 'ghcr.io/org/repo:1.0').

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
    registry: str, repo: str, timeout: float = 30.0
) -> Dict[str, str]:
    """Retrieve bearer authentication token headers for registry repository."""
    url = f"https://{registry}/v2/{repo}/manifests/latest"
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        urllib.request.urlopen(req, timeout=timeout)
        return {}
    except urllib.error.HTTPError as err:
        if err.code != 401:
            return {}
        auth_hdr = err.headers.get("Www-Authenticate", "")
        if not auth_hdr.startswith("Bearer"):
            return {}

        realm_m = re.search(r'realm=["\']([^"\']+)["\']', auth_hdr)
        if not realm_m:
            return {}
        realm = realm_m.group(1)

        service_m = re.search(r'service=["\']([^"\']+)["\']', auth_hdr)
        service = service_m.group(1) if service_m else None

        scope_m = re.search(r'scope=["\']([^"\']+)["\']', auth_hdr)
        scope = scope_m.group(1) if scope_m else f"repository:{repo}:pull"

        params = {}
        if service:
            params["service"] = service
        if scope:
            params["scope"] = scope

        auth_url = f"{realm}?{urllib.parse.urlencode(params)}" if params else realm
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
    except Exception:
        return {}
    return {}


def extract_tar_layer(tar_bytes: bytes, dest_dir: str) -> None:
    """Extract a tar archive layer onto dest_dir with OCI whiteout handling."""
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:*") as tar:
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
                    # Clear out existing files/symlinks to avoid traversal via symlinks
                    if not os.path.isdir(target_path) or os.path.islink(target_path):
                        try:
                            os.remove(target_path)
                        except OSError:
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
            elif member.isdir():
                os.makedirs(target_path, exist_ok=True)
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

    safe_name = sanitize_image_name(image)
    target_dir = os.path.join(images_dir, safe_name)
    lock_path = os.path.join(images_dir, f"{safe_name}.lock")

    with open(lock_path, "w", encoding="utf-8") as f_lock:
        try:
            fcntl.flock(f_lock, fcntl.LOCK_EX)
            marker_path = os.path.join(target_dir, ".extracted")
            if os.path.isdir(target_dir) and os.path.exists(marker_path):
                return target_dir

            tmp_extract_dir = os.path.join(
                images_dir, f"{safe_name}.tmp.{uuid.uuid4().hex}"
            )
            os.makedirs(tmp_extract_dir, mode=0o755, exist_ok=True)

            tar_path = os.path.join(images_dir, f"{safe_name}.tar")

            if os.path.isfile(image):
                try:
                    with open(image, "rb") as f:
                        extract_tar_layer(f.read(), tmp_extract_dir)
                except Exception as err:
                    shutil.rmtree(tmp_extract_dir, ignore_errors=True)
                    raise SandboxCreationError(
                        f"Failed to extract local image archive '{image}': {err}"
                    ) from err
            elif os.path.isfile(tar_path):
                try:
                    with open(tar_path, "rb") as f:
                        extract_tar_layer(f.read(), tmp_extract_dir)
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
                    auth_headers = get_registry_auth_headers(
                        registry, repo, timeout=timeout_seconds
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
                    headers.update(auth_headers)

                    manifest_url = f"https://{registry}/v2/{repo}/manifests/{reference}"
                    req = urllib.request.Request(manifest_url, headers=headers)
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

                        sub_req = urllib.request.Request(
                            f"https://{registry}/v2/{repo}/manifests/{chosen_digest}",
                            headers=headers,
                        )
                        with urllib.request.urlopen(
                            sub_req, timeout=timeout_seconds
                        ) as resp:
                            manifest_data = json.loads(resp.read().decode("utf-8"))

                    # extract image config so we can reference metadata bout the image later.
                    config_desc = manifest_data.get("config")
                    if config_desc and "digest" in config_desc:
                        config_digest = config_desc["digest"]
                        config_url = (
                            f"https://{registry}/v2/{repo}/blobs/{config_digest}"
                        )
                        config_req = urllib.request.Request(config_url, headers=headers)
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
                        blob_url = f"https://{registry}/v2/{repo}/blobs/{digest}"
                        blob_req = urllib.request.Request(blob_url, headers=headers)
                        with urllib.request.urlopen(
                            blob_req, timeout=timeout_seconds
                        ) as blob_resp:
                            layer_bytes = blob_resp.read()

                        extract_tar_layer(layer_bytes, tmp_extract_dir)

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
