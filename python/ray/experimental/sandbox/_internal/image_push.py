"""Commit a sandbox: push its base image plus layers holding its changes.

The new layers are gVisor's tar of the sandbox's root filesystem upper layer,
rewritten to the OCI layer format, split at file boundaries into a few parts
so they compress and upload in parallel, and gzipped. Base layers are not
rebuilt: the target repository gets them by cross-repository mount when it
shares the base image's registry, else by upload from the node's layer cache.
"""

import concurrent.futures
import contextlib
import dataclasses
import datetime
import gzip
import hashlib
import json
import os
import tarfile
import urllib.parse
from typing import BinaryIO, Dict, List, Optional, Tuple, Union

import requests
import requests.adapters

from ray.experimental.sandbox._internal.image_utils import (
    _USER_AGENT,
    _download_blobs,
    apply_registry_mirror,
    get_registry_auth_headers,
    image_cache_name,
    parse_image_ref,
    registry_base_url,
)
from ray.experimental.sandbox.exceptions import SandboxError

_OPAQUE_XATTR = "SCHILY.xattr.trusted.overlay.opaque"
_OVERLAY_XATTR_PREFIX = "SCHILY.xattr.trusted.overlay."
_DOCKER_MANIFEST = "application/vnd.docker.distribution.manifest.v2+json"
_OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
_OCI_CONFIG = "application/vnd.oci.image.config.v1+json"
# Changes are split into parts of at least this many uncompressed bytes, and
# at most `_MAX_PARTS` parts: one registry upload stream is slow far from the
# registry, and parallel ones add up.
_PART_BYTES = 128 << 20
_MAX_PARTS = 8
_PARALLELISM = 16
_CHUNK = 1 << 20


@dataclasses.dataclass
class Layer:
    """A gzipped layer blob on disk."""

    path: str
    digest: str
    diff_id: str
    size: int


class _Sha256Writer:
    """A write-only stream that hashes and counts what passes through it."""

    def __init__(self, target: BinaryIO):
        self._target = target
        self.sha256 = hashlib.sha256()
        self.size = 0

    def write(self, data: bytes) -> int:
        self._target.write(data)
        self.sha256.update(data)
        self.size += len(data)
        return len(data)

    def flush(self) -> None:
        self._target.flush()

    @property
    def digest(self) -> str:
        return f"sha256:{self.sha256.hexdigest()}"


def build_layers(rootfs_tar: str, directory: str) -> List[Layer]:
    """Convert gVisor's rootfs tar into gzipped OCI layers in ``directory``.

    gVisor uses overlayfs markers, which OCI layers express differently:

    - Deleted path: gVisor writes a 0/0 character device; OCI uses a
      ``.wh.<name>`` file. All deletions share one inode, so the tar stores
      every deletion after the first as a hardlink to it.
    - Opaque directory: gVisor sets ``trusted.overlay.opaque=y``; OCI uses a
      ``.wh..wh..opq`` file inside the directory.

    Large changes are split into parts that apply in order. An opaque
    marker is written right after its directory, in the same part, so it
    can't hide the directory's new children.
    """
    part_bytes = max(_PART_BYTES, -(-os.path.getsize(rootfs_tar) // _MAX_PARTS))
    parts: List[tuple] = []
    whiteouts = set()
    out = raw = dst = None

    def close() -> None:
        dst.close()
        out.close()
        parts.append((out.name, raw.digest))

    with contextlib.ExitStack() as stack, open(
        rootfs_tar, "rb"
    ) as src_file, tarfile.open(fileobj=src_file, mode="r|") as src:
        for member in src:
            if dst is None:
                out = stack.enter_context(
                    open(os.path.join(directory, f"layer{len(parts)}.tar"), "wb")
                )
                raw = _Sha256Writer(out)
                dst = stack.enter_context(
                    tarfile.open(fileobj=raw, mode="w|", format=tarfile.PAX_FORMAT)
                )
            opaque = member.pax_headers.get(_OPAQUE_XATTR) == "y"
            member.pax_headers = {
                key: value
                for key, value in member.pax_headers.items()
                if not key.startswith(_OVERLAY_XATTR_PREFIX)
            }
            if (member.ischr() and member.devmajor == member.devminor == 0) or (
                member.islnk() and member.linkname in whiteouts
            ):
                whiteouts.add(member.name)
                parent, name = os.path.split(member.name.rstrip("/"))
                dst.addfile(_marker(os.path.join(parent, f".wh.{name}"), member))
            else:
                dst.addfile(member, src.extractfile(member) if member.isreg() else None)
                if opaque and member.isdir():
                    name = os.path.join(member.name.rstrip("/"), ".wh..wh..opq")
                    dst.addfile(_marker(name, member))
            if dst.offset >= part_bytes and len(parts) < _MAX_PARTS - 1:
                close()
                dst = None
        if dst is not None or not parts:
            if dst is None:
                out = stack.enter_context(
                    open(os.path.join(directory, "layer0.tar"), "wb")
                )
                raw = _Sha256Writer(out)
                dst = stack.enter_context(
                    tarfile.open(fileobj=raw, mode="w|", format=tarfile.PAX_FORMAT)
                )
            close()
    os.remove(rootfs_tar)
    with concurrent.futures.ThreadPoolExecutor(len(parts)) as pool:
        return list(pool.map(lambda part: _compress(*part), parts))


def _compress(path: str, diff_id: str) -> Layer:
    with open(path, "rb") as src, open(f"{path}.gz", "wb") as out:
        compressed = _Sha256Writer(out)
        with gzip.GzipFile(
            fileobj=compressed, mode="wb", compresslevel=1, mtime=0
        ) as gz:
            while block := src.read(_CHUNK):
                gz.write(block)
    os.remove(path)
    return Layer(f"{path}.gz", compressed.digest, diff_id, compressed.size)


def _marker(name: str, like: tarfile.TarInfo) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.mtime = like.mtime
    return info


def push_image(
    base_image: str,
    target: str,
    layers: List[Layer],
    env: Optional[Dict[str, str]],
    images_dir: str,
    timeout_seconds: float,
) -> str:
    """Push ``layers`` on top of ``base_image`` as ``target``; return a digest ref.

    The base manifest and config come from the local image cache, so the result
    builds on the exact image the sandbox runs, even if the tag has moved since.
    ``env`` overrides or adds entries in the image's Env.
    """
    registry, repo, tag = parse_image_ref(target)
    if tag.startswith("sha256:"):
        raise SandboxError(f"Commit target '{target}' needs a tag, not a digest")
    manifest, config = _load_base(images_dir, base_image)
    src_registry, src_repo, reference = parse_image_ref(base_image)
    src_registry, src_repo = apply_registry_mirror(src_registry, src_repo)
    # Base layers can be mounted from the source repository only on the same
    # registry, and only with a token that can also pull from it.
    can_mount = src_registry == registry and src_repo != repo
    scopes = [f"repository:{repo}:pull,push"]
    if can_mount:
        scopes.append(f"repository:{src_repo}:pull")
    auth = get_registry_auth_headers(
        registry, repo, reference=tag, timeout=timeout_seconds, scopes=scopes
    ).get("Authorization")

    with contextlib.closing(
        _Repository(registry, repo, auth, timeout_seconds)
    ) as dst, concurrent.futures.ThreadPoolExecutor(_PARALLELISM) as pool:
        uploads = [
            pool.submit(dst.upload, layer.digest, open(layer.path, "rb"))
            for layer in layers
        ]
        base_digests = list(dict.fromkeys(d["digest"] for d in manifest["layers"]))
        present = pool.map(
            lambda digest: dst.has_blob(digest)
            or (can_mount and dst.mount(digest, src_repo)),
            base_digests,
        )
        missing = [d for d, ok in zip(base_digests, present) if not ok]
        if missing:
            src_auth = get_registry_auth_headers(
                src_registry, src_repo, reference=reference, timeout=timeout_seconds
            ).get("Authorization")
            src_url = f"{registry_base_url(src_registry)}/v2/{src_repo}/blobs/"
            with contextlib.closing(
                _download_blobs(
                    missing,
                    src_url,
                    {"User-Agent": _USER_AGENT},
                    src_auth,
                    timeout_seconds,
                    images_dir,
                )
            ) as blobs:
                uploads += [
                    pool.submit(dst.upload, digest, blob)
                    for digest, blob in zip(missing, blobs)
                ]
        for future in uploads:
            future.result()

        config_bytes = _commit_config(config, layers, env)
        dst.upload(_sha256(config_bytes), config_bytes)
        media_type = manifest.get("mediaType") or _OCI_MANIFEST
        manifest_bytes = _commit_manifest(manifest, media_type, config_bytes, layers)
        dst.put_manifest(tag, manifest_bytes, media_type)
    return f"{registry}/{repo}@{_sha256(manifest_bytes)}"


def _sha256(data: bytes) -> str:
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


def _load_base(images_dir: str, base_image: str) -> Tuple[Dict, Dict]:
    """Return the manifest and config the image cache kept for ``base_image``."""
    image_dir = os.path.join(images_dir, image_cache_name(base_image))
    try:
        with open(os.path.join(image_dir, ".manifest.json")) as f:
            manifest = json.load(f)
        with open(os.path.join(image_dir, ".image_config.json")) as f:
            config = json.load(f)
    except (OSError, ValueError) as err:
        raise SandboxError(
            f"Cannot commit base image '{base_image}': its cached manifest or "
            "config is missing or invalid. Re-pull it into a fresh cache and "
            "recreate the sandbox before committing."
        ) from err
    return manifest, config


def _commit_config(
    config: Dict, layers: List[Layer], env: Optional[Dict[str, str]]
) -> bytes:
    """Return ``config`` with ``layers`` and ``env`` added, serialized."""
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    config["created"] = now
    rootfs = config.setdefault("rootfs", {"type": "layers", "diff_ids": []})
    rootfs["diff_ids"] += [layer.diff_id for layer in layers]
    config.setdefault("history", []).extend(
        {"created": now, "created_by": "ray sandbox commit"} for _ in layers
    )
    if env:
        image_config = config.setdefault("config", {})
        merged = dict(
            e.split("=", 1) for e in image_config.get("Env") or [] if "=" in e
        )
        merged.update(env)
        image_config["Env"] = [f"{k}={v}" for k, v in merged.items()]
    return json.dumps(config, separators=(",", ":")).encode()


def _commit_manifest(
    manifest: Dict, media_type: str, config_bytes: bytes, layers: List[Layer]
) -> bytes:
    """Return the base ``manifest`` pointing at the new config, plus ``layers``."""
    layer_type = (
        "application/vnd.docker.image.rootfs.diff.tar.gzip"
        if media_type == _DOCKER_MANIFEST
        else "application/vnd.oci.image.layer.v1.tar+gzip"
    )
    new_manifest = {
        "schemaVersion": 2,
        "mediaType": media_type,
        "config": {
            "mediaType": manifest["config"].get("mediaType") or _OCI_CONFIG,
            "digest": _sha256(config_bytes),
            "size": len(config_bytes),
        },
        "layers": manifest["layers"]
        + [
            {"mediaType": layer_type, "digest": layer.digest, "size": layer.size}
            for layer in layers
        ],
    }
    return json.dumps(new_manifest, separators=(",", ":")).encode()


class _Repository:
    """Blob and manifest uploads to one registry repository."""

    def __init__(
        self, registry: str, repo: str, auth: Optional[str], timeout_seconds: float
    ):
        self._url = f"{registry_base_url(registry)}/v2/{repo}"
        self._timeout = timeout_seconds
        # Reuses connections: far from the registry, a TLS handshake costs more
        # than most requests.
        self._session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_maxsize=_PARALLELISM)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)
        self._session.headers["User-Agent"] = _USER_AGENT
        if auth:
            self._session.headers["Authorization"] = auth

    def close(self) -> None:
        self._session.close()

    def _send(
        self, method: str, path: str, ok: Tuple[int, ...] = (), **kwargs
    ) -> requests.Response:
        url = f"{self._url}{path}" if path.startswith("/") else path
        response = self._session.request(method, url, timeout=self._timeout, **kwargs)
        if response.status_code >= 400 and response.status_code not in ok:
            raise SandboxError(
                f"Registry {method} {url.split('?')[0]} failed: "
                f"{response.status_code} {response.text[:500]}"
            )
        return response

    def has_blob(self, digest: str) -> bool:
        return self._send("HEAD", f"/blobs/{digest}", ok=(404,)).status_code != 404

    def mount(self, digest: str, from_repo: str) -> bool:
        query = urllib.parse.urlencode({"mount": digest, "from": from_repo})
        return self._send("POST", f"/blobs/uploads/?{query}").status_code == 201

    def upload(self, digest: str, body: Union[bytes, BinaryIO]) -> None:
        """Upload ``body`` unless the repository has it; close a file body."""
        try:
            if self.has_blob(digest):
                return
            location = self._send("POST", "/blobs/uploads/").headers["Location"]
            url = urllib.parse.urljoin(self._url, location)
            sep = "&" if "?" in url else "?"
            self._send(
                "PUT",
                f"{url}{sep}digest={urllib.parse.quote(digest)}",
                data=body,
                headers={"Content-Type": "application/octet-stream"},
            )
        finally:
            if not isinstance(body, bytes):
                body.close()

    def put_manifest(self, tag: str, data: bytes, media_type: str) -> None:
        self._send(
            "PUT", f"/manifests/{tag}", data=data, headers={"Content-Type": media_type}
        )
