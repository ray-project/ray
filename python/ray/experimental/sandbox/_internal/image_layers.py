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

import errno
import hashlib
import json
import logging
import mmap
import os
import shutil
import subprocess
import tarfile
import tempfile
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import dataclass
from functools import lru_cache
from typing import BinaryIO, Deque, Dict, List, Optional, Tuple

from ray.experimental.sandbox._internal.erofs_reader import (
    ErofsImage,
    ErofsStat,
)
from ray.experimental.sandbox._internal.image_utils import (
    _MKFS_EROFS_MIN_VERSION,
    LAYER_REFERENCE_PREFIX,
    mkfs_erofs_path,
)
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxImageBuildError,
)

logger = logging.getLogger(__name__)

LAYER_TAR = "layer.tar"
LAYER_JSON = "layer.json"
LAYER_JSON_FORMAT = 1
# Force epochs live here, one tiny file per layer key ever force-rebuilt. The
# directory has no ``.extracted`` marker, so eviction and teardown pass it by,
# and ``sanitize_image_name`` strips leading dots, so no image maps onto it.
EPOCHS_DIR = ".epochs"
# Work copies of an image being built: ``raylayer@<leaf key>.work.<uuid>``.
WORK_INFIX = ".work."
BUILDER_FILE = ".builder"

# The erofs-utils release that added ``mkfs.erofs --incremental``.
_MKFS_EROFS_INCREMENTAL_VERSION = "1.8"
_OPAQUE_XATTR = "SCHILY.xattr.trusted.overlay.opaque"
_OVERLAY_XATTR_PREFIX = "SCHILY.xattr.trusted.overlay."
_XATTR_PREFIX = "SCHILY.xattr."
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
    as gVisor's EROFS reader requires.

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
    * Other ``trusted.overlay.*`` xattrs are overlay bookkeeping and are dropped;
      every other xattr (``security.capability``, say) is kept.
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
                # Only xattrs survive: the original's path/linkpath records would
                # otherwise win over the names rewritten below.
                member.pax_headers = {
                    k: v
                    for k, v in member.pax_headers.items()
                    if k.startswith(_XATTR_PREFIX)
                    and not k.startswith(_OVERLAY_XATTR_PREFIX)
                }
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
    now = time.time()
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
        with open(dst_path, "wb") as f:
            hasher = _HashingWriter(f)
            with _open_layer_writer(hasher) as tout:
                tout.addfile(_dir_header(".", root))
                for name, header in ancestors.items():
                    if name not in explicit_dirs:
                        tout.addfile(header)
                for member, name in placed:
                    member.name = name
                    member.pax_headers = {
                        k: v
                        for k, v in member.pax_headers.items()
                        if k.startswith(_XATTR_PREFIX)
                    }
                    tout.addfile(
                        member, tin.extractfile(member) if member.isreg() else None
                    )
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


def write_builder_file(work_dir: str) -> None:
    """Record which process owns a work copy, for :func:`sweep_stale_work_dirs`."""
    with open(os.path.join(work_dir, BUILDER_FILE), "w", encoding="utf-8") as f:
        json.dump(
            {"pid": os.getpid(), "start_time": _process_start_time(os.getpid())}, f
        )


def _process_start_time(pid: int) -> Optional[str]:
    try:
        with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as f:
            # Field 22, after the parenthesized command, which may hold spaces.
            return f.read().rsplit(")", 1)[1].split()[19]
    except (OSError, IndexError):
        return None


def _builder_alive(work_dir: str) -> bool:
    try:
        with open(os.path.join(work_dir, BUILDER_FILE), "r", encoding="utf-8") as f:
            owner = json.load(f)
        pid = int(owner["pid"])
    except (OSError, ValueError, KeyError, TypeError):
        # Unreadable: either being written right now or garbage. Only call it
        # stale once it is old enough not to be the former.
        try:
            return time.time() - os.path.getmtime(work_dir) < 300
        except OSError:
            return False
    start = _process_start_time(pid)
    return start is not None and start == owner.get("start_time")


def sweep_stale_work_dirs(images_dir: str) -> None:
    """Remove work copies whose builder process is gone.

    A builder that dies mid-build leaves its work copy pinned by its own
    token, which nothing else ever releases; this is what does.
    """
    try:
        names = os.listdir(images_dir)
    except OSError:
        return
    for name in names:
        if not name.startswith("raylayer_") or WORK_INFIX not in name:
            continue
        path = os.path.join(images_dir, name)
        if os.path.isdir(path) and not _builder_alive(path):
            logger.info("Removing the work copy of a build that died: %s", name)
            shutil.rmtree(path, ignore_errors=True)
