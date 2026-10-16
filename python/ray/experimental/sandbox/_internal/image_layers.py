"""Image layers: the diffs build steps produce, and how they reach an image.

A layer is an OCI-style layer tarball: the files a build step created or
changed, ``.wh.<name>`` entries for what it deleted, and ``.wh..wh..opq``
markers in directories it replaced wholesale. Layers are applied in place to
an image's EROFS root filesystem with ``mkfs.erofs --incremental``, which
appends the new data and merges the directory trees, so applying a layer
costs about its own size rather than the image's.

Two sources produce layers:

* A RUN step's changes, read out of its build sandbox with ``runsc tar
  rootfs-upper``. That archive is gVisor's overlay upper layer, so it is
  rewritten into the OCI form by :func:`normalize_overlay_diff`.
* A COPY step's files, prepared by the client. Their destination is resolved
  against the image first by :func:`prepare_copy_layer`, as Docker resolves a
  ``COPY`` destination.

Every layer written here starts with a ``.`` entry that carries the image's
root directory metadata, and names every ancestor directory of its entries
explicitly: ``mkfs.erofs`` gives a directory the tar leaves implicit the
*builder's* uid and mode 0777, replacing the image's own.
"""

import contextlib
import errno
import fcntl
import glob
import hashlib
import json
import logging
import mmap
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import dataclass
from functools import lru_cache
from typing import IO, BinaryIO, Deque, Dict, List, Optional, Tuple

from ray.experimental.sandbox._internal.erofs_reader import (
    ErofsImage,
    ErofsStat,
)
from ray.experimental.sandbox._internal.image_utils import (
    _MKFS_EROFS_MIN_VERSION,
    _USERS_SUBDIR,
    LAYER_REFERENCE_PREFIX,
    _release_image_use,
    entry_lock_path,
    mkfs_erofs_path,
    mkfs_erofs_reproducible_flags,
)
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxImageBuildError,
)
from ray.experimental.sandbox.image_build import COPY_MTIME

logger = logging.getLogger(__name__)

LAYER_JSON = "layer.json"
LAYER_JSON_FORMAT = 1
# Force epochs live here, one tiny file per layer key ever force-rebuilt. The
# directory has no ``.extracted`` marker, so eviction and teardown pass it by,
# and ``sanitize_image_name`` strips leading dots, so no image maps onto it.
EPOCHS_DIR = ".epochs"
# Work copies of an image being built: ``raylayer@<leaf key>.work.<uuid>``.
WORK_INFIX = ".work."
# A work copy's owner file, which its build keeps locked (see own_work_dir).
BUILDER_FILE = ".builder"
_BUILD_TOKEN_PREFIX = "build-"
_BUILD_TOKEN = re.compile(r"build-[0-9a-f]{12}")
# A work copy with no owner file this old is one whose build died before it
# could take ownership; a younger one may be taking it right now.
_UNOWNED_WORK_DIR_GRACE_SECONDS = 300.0
# The node's secret for keying build-only environment values (see
# ``image_build.step_key``). Dot-prefixed like EPOCHS_DIR, so no image maps
# onto it, and a plain file, so eviction passes it by.
BUILD_ENV_KEY_FILE = ".build_env_key"

# The erofs-utils release that added ``mkfs.erofs --incremental``.
_MKFS_EROFS_INCREMENTAL_VERSION = "1.8"
_OPAQUE_XATTR = "SCHILY.xattr.trusted.overlay.opaque"
_CAPABILITY_XATTR = "SCHILY.xattr.security.capability"
_WHITEOUT_PREFIX = ".wh."
_OPAQUE_MARKER = ".wh..wh..opq"
_COPY_BUFSIZE = 1 << 20
# Linux's MAXSYMLINKS.
_MAX_SYMLINK_HOPS = 40


def layer_reference(key: str) -> str:
    """The image reference of the layer with ``key``."""
    return f"{LAYER_REFERENCE_PREFIX}{key}"


def is_layer_reference(image: str) -> bool:
    """Whether ``image`` names a built layer rather than a registry image."""
    return isinstance(image, str) and image.startswith(LAYER_REFERENCE_PREFIX)


def work_reference(leaf_key: str) -> str:
    """A fresh reference for a private work copy of the image ``leaf_key``."""
    return f"{layer_reference(leaf_key)}{WORK_INFIX}{uuid.uuid4().hex[:12]}"


# -- mkfs.erofs -------------------------------------------------------------


@lru_cache(maxsize=1)
def mkfs_erofs_supports_incremental() -> bool:
    """Whether the node's ``mkfs.erofs`` can append layers to an image."""
    path = mkfs_erofs_path()
    if path is None:
        return False
    try:
        res = subprocess.run(
            [path, "--help"], capture_output=True, text=True, timeout=30
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    text = res.stdout + res.stderr
    return all(flag in text for flag in ("--incremental", "--aufs", "--ovlfs-strip"))


def require_incremental_mkfs_erofs() -> str:
    """The ``mkfs.erofs`` path, or an error naming the release builds need."""
    path = mkfs_erofs_path()
    if path is not None and mkfs_erofs_supports_incremental():
        return path
    found = shutil.which("mkfs.erofs")
    raise SandboxCreationError(
        "Building images applies each layer with `mkfs.erofs --incremental`, "
        f"which needs erofs-utils {_MKFS_EROFS_INCREMENTAL_VERSION} or later on "
        "every node that builds, but "
        + (
            "mkfs.erofs is not in PATH"
            if found is None
            else f"{found} predates it (pulling images needs only "
            f"{_MKFS_EROFS_MIN_VERSION})"
        )
        + ". Install erofs-utils 1.8+ on the node image (the Debian 13 package, "
        "or a build from source)."
    )


def apply_layer_to_erofs(image_path: str, layer_path: str) -> None:
    """Append the layer tarball ``layer_path`` to the EROFS image in place.

    ``--aufs`` turns the layer's ``.wh.`` entries into overlay whiteouts and
    opaque directories, incremental mode merges each non-opaque directory
    with the image's, and ``--ovlfs-strip`` then drops the whiteouts, so
    what they delete is gone from the result. ``--sort=none`` keeps the
    tarball's order: sorting spools the data through a temporary file first,
    which measured several times slower. The block size and the flat data
    layout match :func:`~ray.experimental.sandbox._internal.image_utils.build_erofs_image`,
    as gVisor's EROFS reader requires. A fixed build time keeps the result
    the same bytes wherever the same layers are applied (see
    ``mkfs_erofs_reproducible_flags``); the UUID stays the base image's.

    Args:
        image_path: The EROFS image, modified in place.
        layer_path: The OCI layer tarball.

    Raises:
        SandboxCreationError: ``mkfs.erofs`` is missing, too old, or failed.
    """
    mkfs = require_incremental_mkfs_erofs()
    res = subprocess.run(
        [
            mkfs,
            "--quiet",
            "--incremental=data",
            "--tar=f",
            "--sort=none",
            "--aufs",
            "--ovlfs-strip=1",
            f"-b{mmap.PAGESIZE}",
            "-E^inline_data",
            *mkfs_erofs_reproducible_flags(),
            image_path,
            layer_path,
        ],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        raise SandboxCreationError(
            "mkfs.erofs failed to apply an image layer: "
            f"{(res.stderr or res.stdout).strip()[-500:]}"
        )


def fsck_erofs_path() -> Optional[str]:
    """``fsck.erofs`` from the erofs-utils that ``mkfs.erofs`` came from, or None.

    erofs-utils installs both; the one beside ``mkfs.erofs`` matches its
    version, so it is preferred over another found on PATH.
    """
    mkfs = mkfs_erofs_path()
    if mkfs is not None:
        sibling = os.path.join(os.path.dirname(mkfs), "fsck.erofs")
        if os.access(sibling, os.X_OK):
            return sibling
    return shutil.which("fsck.erofs")


def verify_erofs_image(image_path: str, key: Optional[str] = None) -> bool:
    """Check a finished image with ``fsck.erofs`` before any sandbox mounts it.

    Checks the superblock, every inode and how every file's data is laid
    out (``--extract`` with no target), which catches an image cut short or
    corrupted on its way to disk. It reads metadata rather than data, so it
    takes tens of milliseconds for a 250 MB image.

    Args:
        image_path: The EROFS image.
        key: The layer the image is for, for the error.

    Returns:
        False if the node has no ``fsck.erofs``, so nothing was checked.

    Raises:
        SandboxImageBuildError: The image failed the check.
    """
    fsck = fsck_erofs_path()
    if fsck is None:
        return False
    try:
        res = subprocess.run(
            [fsck, "--extract", image_path],
            capture_output=True,
            text=True,
            timeout=600,
        )
    except subprocess.TimeoutExpired as err:
        raise SandboxImageBuildError(
            "fsck.erofs did not finish checking the built image", key
        ) from err
    if res.returncode != 0:
        raise SandboxImageBuildError(
            "The built image failed fsck.erofs, so it was not cached: "
            f"{(res.stderr or res.stdout).strip()[-500:]}",
            key,
        )
    return True


# -- writing layers ---------------------------------------------------------


@dataclass(frozen=True)
class LayerInfo:
    """What was written: the layer's sha256 and size in bytes."""

    sha256: str
    size: int


class _HashingWriter:
    """A write-only file that hashes what passes through it."""

    def __init__(self, f: BinaryIO):
        self._f = f
        self._hash = hashlib.sha256()
        self._size = 0

    def write(self, data) -> int:
        self._hash.update(data)
        self._size += len(data)
        return self._f.write(data)

    def tell(self) -> int:
        return self._size

    def flush(self) -> None:
        self._f.flush()

    def info(self) -> LayerInfo:
        return LayerInfo(self._hash.hexdigest(), self._size)


def _open_layer_writer(hasher: _HashingWriter) -> tarfile.TarFile:
    return tarfile.open(
        fileobj=hasher,
        mode="w",
        format=tarfile.PAX_FORMAT,
        copybufsize=_COPY_BUFSIZE,
    )


def _dir_header(name: str, st: ErofsStat) -> tarfile.TarInfo:
    """A directory entry carrying ``st``'s exact metadata."""
    info = tarfile.TarInfo(name)
    info.type = tarfile.DIRTYPE
    info.mode = st.mode & 0o7777
    info.uid = st.uid
    info.gid = st.gid
    info.mtime = st.mtime
    return info


def _new_dir_header(name: str, mtime: float) -> tarfile.TarInfo:
    """A directory a COPY creates: root-owned, 0755, as Docker makes it."""
    info = tarfile.TarInfo(name)
    info.type = tarfile.DIRTYPE
    info.mode = 0o755
    info.mtime = int(mtime)
    return info


def _marker(name: str, mtime: float) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.mode = 0
    info.mtime = int(mtime)
    return info


def _strip_xattrs(member: tarfile.TarInfo, rel: str, dropped: List[str]) -> None:
    """Clear ``member``'s PAX records, its xattrs included.

    gVisor's EROFS reader refuses any inode that carries an xattr ("Operation
    not supported"), so a file keeping one would be unreadable in every
    sandbox on the image -- ``ping`` after ``apt-get install iputils-ping``,
    which gives it ``security.capability``, say. Pulled images carry none
    either. The other PAX records name the original path and link target,
    which would otherwise win over the names rewritten here; tarfile writes
    fresh ones where the rewritten entry needs them.

    Args:
        member: The entry, modified in place.
        rel: Its path, for the record of dropped capabilities.
        dropped: Paths whose file capabilities were dropped; extended.
    """
    if _CAPABILITY_XATTR in member.pax_headers:
        dropped.append(rel)
    member.pax_headers = {}


def _warn_dropped_capabilities(dropped: List[str]) -> None:
    if dropped:
        logger.warning(
            "Dropped file capabilities from %d file(s) in an image layer, "
            "first /%s: gVisor's EROFS root filesystem cannot serve them, "
            "and a file carrying one would be unreadable.",
            len(dropped),
            dropped[0],
        )


def _relative(name: str) -> str:
    """A member name relative to the root: no ``./`` prefix, no slashes at
    the ends, and "" for the root itself (which tarfile reads as ``.``)."""
    while name.startswith("./"):
        name = name[2:]
    name = name.strip("/")
    return "" if name == "." else name


def normalize_overlay_diff(src: BinaryIO, dst_path: str, root: ErofsStat) -> LayerInfo:
    """Rewrite gVisor's ``rootfs-upper`` archive as an OCI layer tarball.

    The archive is the sandbox overlay's upper layer, in overlayfs's own
    encoding:

    * A whiteout is a character device 0/0. gVisor keeps one whiteout inode,
      so only the first is written as a device and every other is a hard
      link to it -- a deleted ``/etc/issue`` arrives as a hard link to some
      other deleted path. Both forms become ``.wh.<name>``.
    * An opaque directory carries the ``trusted.overlay.opaque`` xattr. gVisor
      sets it on every directory created in the upper layer, not only on one
      that replaced a deleted directory; either way it becomes the directory
      plus ``<dir>/.wh..wh..opq``, which hides nothing for a new directory.
    * Every other xattr is dropped: overlay bookkeeping, and the rest too,
      since gVisor cannot read a file that carries one (see
      :func:`_strip_xattrs`).
    * The root entry describes the upper layer's own root rather than the
      image's, so it is replaced by ``root``.

    Parent directories always precede their children in gVisor's archive, so
    no directory is left implicit.

    Args:
        src: The archive, readable as a stream.
        dst_path: Where to write the layer.
        root: The image's root directory metadata, written as the ``.`` entry.

    Returns:
        The layer's sha256 and size.
    """
    whiteouts = set()
    dropped: List[str] = []
    with open(dst_path, "wb") as f:
        hasher = _HashingWriter(f)
        with tarfile.open(fileobj=src, mode="r|") as tin, _open_layer_writer(
            hasher
        ) as tout:
            tout.addfile(_dir_header(".", root))
            for member in tin:
                rel = _relative(member.name)
                if not rel:
                    continue
                if (
                    member.ischr() and member.devmajor == 0 and member.devminor == 0
                ) or (member.islnk() and member.linkname in whiteouts):
                    whiteouts.add(member.name)
                    parent, base = os.path.split(rel)
                    tout.addfile(
                        _marker(
                            os.path.join(parent, _WHITEOUT_PREFIX + base), member.mtime
                        )
                    )
                    continue
                opaque = member.isdir() and member.pax_headers.get(_OPAQUE_XATTR) == "y"
                _strip_xattrs(member, rel, dropped)
                member.name = rel
                if member.islnk():
                    member.linkname = _relative(member.linkname)
                tout.addfile(
                    member, tin.extractfile(member) if member.isreg() else None
                )
                if opaque:
                    tout.addfile(
                        _marker(os.path.join(rel, _OPAQUE_MARKER), member.mtime)
                    )
        info = hasher.info()
    _warn_dropped_capabilities(dropped)
    return info


def _resolve_parent(
    image: ErofsImage,
    rel_dir: str,
    ancestors: "OrderedDict[str, tarfile.TarInfo]",
    cache: Dict[str, str],
    now: float,
) -> str:
    """Resolve directory ``rel_dir`` inside the image, as a COPY would.

    Symlinks on the way are followed with the image as ``/`` -- ``bin`` on a
    UsrMerge image resolves to ``usr/bin`` -- and ``..`` never climbs above
    the root. Every directory on the resolved path is recorded in
    ``ancestors``, parents first: an existing one with its exact metadata,
    a missing one as a new root-owned 0755 directory.

    Args:
        image: The image the COPY applies to.
        rel_dir: The directory, relative to the root.
        ancestors: Directory entries to write, by resolved path; extended.
        cache: Resolutions already made for this archive; extended.
        now: Modification time for directories the COPY creates.

    Returns:
        The resolved path, relative to the root ("" for the root).

    Raises:
        SandboxImageBuildError: A component is a file, or symlinks loop.
    """
    if rel_dir in cache:
        return cache[rel_dir]
    pending: Deque[str] = deque(p for p in rel_dir.split("/") if p not in ("", "."))
    resolved: List[str] = []
    # nid of the deepest resolved component, or None once inside a directory
    # the image does not have.
    nids: List[Optional[int]] = [image.root_nid]
    hops = 0
    while pending:
        part = pending.popleft()
        if part == "..":
            if resolved:
                resolved.pop()
                nids.pop()
            continue
        parent_nid = nids[-1]
        child = image.lookup(parent_nid, part) if parent_nid is not None else None
        path = "/".join(resolved + [part])
        if child is None:
            resolved.append(part)
            nids.append(None)
            ancestors.setdefault(path, _new_dir_header(path, now))
            continue
        st = image.stat(child)
        if st.is_symlink:
            hops += 1
            if hops > _MAX_SYMLINK_HOPS:
                raise SandboxImageBuildError(
                    f"Cannot copy into '/{rel_dir}': too many levels of symbolic "
                    f"links ({os.strerror(errno.ELOOP)})."
                )
            target = image.readlink(child)
            if target.startswith("/"):
                resolved = []
                nids = [image.root_nid]
            pending.extendleft(
                reversed([p for p in target.split("/") if p not in ("", ".")])
            )
            continue
        if not st.is_dir:
            raise SandboxImageBuildError(
                f"Cannot copy into '/{rel_dir}': '/{path}' is not a directory in "
                "the image."
            )
        resolved.append(part)
        nids.append(child)
        ancestors.setdefault(path, _dir_header(path, st))
    result = "/".join(resolved)
    cache[rel_dir] = result
    return result


def prepare_copy_layer(image: ErofsImage, src_path: str, dst_path: str) -> LayerInfo:
    """Turn a COPY step's archive into a layer for ``image``.

    The client writes each file at its destination path, relative to ``/``,
    without directory entries. Here every destination directory is resolved
    against the image (see :func:`_resolve_parent`), entries are moved to
    their resolved paths, and every directory on those paths is written
    explicitly, so existing ones keep their metadata and ``mkfs.erofs`` never
    makes one up. Without this, a COPY into ``/bin`` on a UsrMerge image
    would replace the ``bin -> usr/bin`` symlink with a directory holding
    nothing but the copied file.

    Args:
        image: The image the layer will be applied to.
        src_path: The COPY archive.
        dst_path: Where to write the layer.

    Returns:
        The layer's sha256 and size.

    Raises:
        SandboxImageBuildError: A destination cannot be resolved, or the
            archive holds something other than files, directories and
            symlinks.
    """
    # Not the build's own time: the layer has to come out the same on every
    # node and in every build (see COPY_MTIME).
    now = COPY_MTIME
    ancestors: "OrderedDict[str, tarfile.TarInfo]" = OrderedDict()
    cache: Dict[str, str] = {}
    with tarfile.open(src_path, "r:") as tin:
        members = tin.getmembers()
        placed: List[Tuple[tarfile.TarInfo, str]] = []
        for member in members:
            rel = _relative(member.name)
            if not rel or ".." in rel.split("/"):
                raise SandboxImageBuildError(
                    f"COPY archive entry {member.name!r} is not a path below '/'."
                )
            if not (member.isreg() or member.isdir() or member.issym()):
                raise SandboxImageBuildError(
                    f"COPY archive entry {member.name!r} is not a file, "
                    "directory or symlink."
                )
            parent, base = os.path.split(rel)
            resolved_parent = _resolve_parent(image, parent, ancestors, cache, now)
            placed.append((member, os.path.join(resolved_parent, base)))
        explicit_dirs = {name for member, name in placed if member.isdir()}
        root = image.stat(image.root_nid)
        dropped: List[str] = []
        with open(dst_path, "wb") as f:
            hasher = _HashingWriter(f)
            with _open_layer_writer(hasher) as tout:
                tout.addfile(_dir_header(".", root))
                for name, header in ancestors.items():
                    if name not in explicit_dirs:
                        tout.addfile(header)
                for member, name in placed:
                    _strip_xattrs(member, name, dropped)
                    member.name = name
                    tout.addfile(
                        member, tin.extractfile(member) if member.isreg() else None
                    )
            _warn_dropped_capabilities(dropped)
            return hasher.info()


def archive_path_from_image(image_path: str, src: str, dest: str, dst_tar: str) -> None:
    """Write ``src`` from an EROFS image as a COPY archive placing it at ``dest``.

    Used for ``COPY --from=<image>``. The extracted files are written
    root-owned with their modes, as Docker copies them, and directories are
    left for :func:`prepare_copy_layer` to resolve like any COPY's.

    Args:
        image_path: The source image's EROFS file.
        src: Absolute path in the source image.
        dest: Absolute destination path.
        dst_tar: Where to write the archive.

    Raises:
        SandboxImageBuildError: ``src`` does not exist in the image.
    """
    with tempfile.TemporaryDirectory(dir=os.path.dirname(dst_tar)) as tmp:
        out = os.path.join(tmp, "x")
        res = subprocess.run(
            [
                shutil.which("fsck.erofs") or "fsck.erofs",
                f"--extract={out}",
                f"--path={src}",
                "--preserve-perms",
                "--no-preserve-owner",
                image_path,
            ],
            capture_output=True,
            text=True,
        )
        if res.returncode != 0 or not os.path.lexists(out):
            raise SandboxImageBuildError(
                f"Cannot copy {src!r} out of its source image: "
                f"{(res.stderr or res.stdout).strip()[-300:]}"
            )
        dest_rel = _relative(dest)

        def _owner(info: tarfile.TarInfo) -> tarfile.TarInfo:
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            return info

        with tarfile.open(dst_tar, "w", format=tarfile.PAX_FORMAT) as tout:
            tout.add(out, arcname=dest_rel, filter=_owner)


# -- layer metadata ---------------------------------------------------------


def read_layer_json(entry_dir: str) -> Optional[Dict]:
    """A layer entry's ``layer.json``, or None if absent or unreadable."""
    try:
        with open(os.path.join(entry_dir, LAYER_JSON), "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("format") != LAYER_JSON_FORMAT:
        return None
    return data


def write_layer_json(entry_dir: str, data: Dict) -> None:
    with open(os.path.join(entry_dir, LAYER_JSON), "w", encoding="utf-8") as f:
        json.dump({"format": LAYER_JSON_FORMAT, **data}, f, indent=2, sort_keys=True)


def _read_epoch_file(images_dir: str, key: str) -> Tuple[int, Optional[str]]:
    try:
        with open(
            os.path.join(images_dir, EPOCHS_DIR, key), "r", encoding="utf-8"
        ) as f:
            data = json.load(f)
        return int(data["epoch"]), data.get("token")
    except (OSError, ValueError, KeyError, TypeError):
        return 0, None


def read_epoch(images_dir: str, key: str) -> int:
    """The force epoch of layer ``key``: 0 until it is first force-rebuilt."""
    return _read_epoch_file(images_dir, key)[0]


def claim_epoch(images_dir: str, key: str, token: str) -> int:
    """Advance layer ``key``'s force epoch for ``token``, once.

    Advancing it re-keys every descendant of the layer, in every image. The
    token is recorded with the epoch, so concurrent or later builds forcing
    with the same token find it claimed and leave it alone -- they must all
    compute the same keys. Callers hold the layer's lock.

    Args:
        images_dir: The image cache.
        key: The layer's key.
        token: The force token rebuilding it.

    Returns:
        The epoch after the call.
    """
    epoch, claimed_by = _read_epoch_file(images_dir, key)
    if claimed_by == token:
        return epoch
    epochs = os.path.join(images_dir, EPOCHS_DIR)
    os.makedirs(epochs, exist_ok=True)
    tmp = os.path.join(epochs, f".{key}.{uuid.uuid4().hex}")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"epoch": epoch + 1, "token": token}, f)
    os.replace(tmp, os.path.join(epochs, key))
    return epoch + 1


def load_build_env_key(images_dir: str) -> bytes:
    """The node's secret for keying build-only environment values.

    Made on first use: 32 random bytes, readable by the worker's user alone.
    It is written under a private name and hard-linked into place, so
    processes racing to make it all end up reading the one that won.

    Args:
        images_dir: The image cache.

    Returns:
        The key.
    """
    path = os.path.join(images_dir, BUILD_ENV_KEY_FILE)
    try:
        with open(path, "rb") as f:
            key = f.read()
        if key:
            return key
    except FileNotFoundError:
        pass
    os.makedirs(images_dir, mode=0o777, exist_ok=True)
    tmp = f"{path}.{uuid.uuid4().hex}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(os.urandom(32))
        try:
            os.link(tmp, path)
        except FileExistsError:
            pass
    finally:
        os.remove(tmp)
    with open(path, "rb") as f:
        return f.read()


def base_build_id(image_dir: str, base_key: str) -> str:
    """The build id of a pulled base image, from its content where possible.

    A registry image's is the sha256 of its config blob -- its image id --
    so re-pulling identical content keeps the layers built on it
    replayable. A local tar has no config; its base key already covers the
    tar's size and mtime.
    """
    try:
        with open(os.path.join(image_dir, ".image_config.json"), "rb") as f:
            return "sha256:" + hashlib.sha256(f.read()).hexdigest()
    except OSError:
        return f"base:{base_key}"


def build_token() -> str:
    """A fresh in-use token for one build (see :func:`own_work_dir`)."""
    return f"{_BUILD_TOKEN_PREFIX}{uuid.uuid4().hex[:12]}"


def own_work_dir(work_dir: str, token: str) -> IO[str]:
    """Take ownership of a new work copy, and return its owner file open.

    The builder keeps the file, and so its ``flock``, until the work copy is
    gone. The kernel drops the lock when the builder exits or crashes, which
    is how :func:`sweep_stale_work_dirs` tells a dead build's work copy from a
    live one -- as the sandbox backend tells a dead owner's bundle. The file
    is locked before it takes its name, so a sweep never takes a work copy
    that is being created.

    Args:
        work_dir: The new work copy's directory.
        token: The build's in-use token (:func:`build_token`), which pins the
            work copy and the image the build starts from.

    Returns:
        The owner file. Close it only once the work copy is removed.
    """
    path = os.path.join(work_dir, BUILDER_FILE)
    owner = open(f"{path}.tmp", "x", encoding="utf-8")
    try:
        fcntl.flock(owner, fcntl.LOCK_EX)
        json.dump({"pid": os.getpid(), "token": token}, owner)
        owner.flush()
        os.rename(f"{path}.tmp", path)
    except BaseException:
        owner.close()
        raise
    return owner


def sweep_stale_work_dirs(images_dir: str) -> List[str]:
    """Remove the work copies of builds that died, and release their pins.

    A build that dies mid-way leaves its work copy, and its pin on the image
    it started from, which keeps that image from ever being evicted; nothing
    else releases either. A live build holds its work copy's owner file
    locked (see :func:`own_work_dir`), so a work copy whose lock this sweep
    can take is a dead build's. The sweep holds the lock while it removes the
    work copy, so no other sweep removes it too.

    Args:
        images_dir: The image cache.

    Returns:
        The names of the work copies removed.
    """
    try:
        names = os.listdir(images_dir)
    except OSError:
        return []
    swept = []
    for name in names:
        if not name.startswith("raylayer_") or WORK_INFIX not in name:
            continue
        try:
            if _sweep_if_unowned(images_dir, name):
                swept.append(name)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Failed to remove the work copy of a build that died: %s",
                name,
                exc_info=True,
            )
    if swept:
        logger.info("Removed the work copies of builds that died: %s", swept)
    return swept


def _sweep_if_unowned(images_dir: str, name: str) -> bool:
    """Remove one work copy if no build owns it, and return whether it did."""
    work_dir = os.path.join(images_dir, name)
    if os.path.islink(work_dir) or not os.path.isdir(work_dir):
        return False
    try:
        owner = open(os.path.join(work_dir, BUILDER_FILE), encoding="utf-8")
    except FileNotFoundError:
        # A build takes ownership right after creating the directory, so
        # one without an owner file this old died before it could.
        try:
            age = time.time() - os.path.getmtime(work_dir)
        except OSError:
            return False
        if age < _UNOWNED_WORK_DIR_GRACE_SECONDS:
            return False
        _remove_work_dir(images_dir, work_dir, keep=None)
        return True
    except OSError:
        return False
    with owner:
        try:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False  # Its build is alive.
        if os.fstat(owner.fileno()).st_nlink == 0:
            return False  # Its build removed it since we opened the file.
        try:
            record = json.load(owner)
        except ValueError:
            record = None
        token = record.get("token") if isinstance(record, dict) else None
        if isinstance(token, str) and _BUILD_TOKEN.fullmatch(token):
            # The token is the build's alone, so whatever entry holds it --
            # the image the build started from, wherever a republish carried
            # the pin -- holds it for this build. Pins inside the work copy go
            # with it.
            pattern = os.path.join(glob.escape(images_dir), "*", _USERS_SUBDIR, token)
            for pin in glob.glob(pattern):
                _release_image_use(os.path.dirname(os.path.dirname(pin)), token)
        _remove_work_dir(images_dir, work_dir, keep=BUILDER_FILE)
    return True


def _remove_work_dir(images_dir: str, work_dir: str, keep: Optional[str]) -> None:
    """Remove a work copy, its owner file ``keep`` last.

    A sweep cut short -- with the process running it, say -- leaves the owner
    file, and so the work copy, for a later sweep to finish.
    """
    if keep is not None:
        with os.scandir(work_dir) as entries:
            for entry in entries:
                if entry.name == keep:
                    continue
                if entry.is_dir(follow_symlinks=False):
                    shutil.rmtree(entry.path, ignore_errors=True)
                else:
                    with contextlib.suppress(OSError):
                        os.unlink(entry.path)
    shutil.rmtree(work_dir, ignore_errors=True)
    # Pulls of the work copy made its short lock file; nothing else ever uses
    # that name again.
    with contextlib.suppress(OSError):
        os.remove(entry_lock_path(images_dir, os.path.basename(work_dir)))
