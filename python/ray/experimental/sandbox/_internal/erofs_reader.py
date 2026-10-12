"""Read-only lookups in an EROFS image, without mounting it.

The image builder needs exact metadata of directories that already exist in
an image -- mode with its sticky and setid bits, owner, mtime -- and the
targets of symlinks on a path. ``dump.erofs`` masks the special mode bits, so
it cannot supply them, and mounting needs privileges a worker lacks. The
on-disk format is small enough to read directly.

Only what ``mkfs.erofs`` produces for the image cache is supported: plain or
inline flat data, 32-bit nids, no metabox. Anything else raises
:class:`ErofsError` rather than being misread.
"""

import os
import stat
import struct
from dataclasses import dataclass
from typing import BinaryIO, Iterator, Optional, Tuple

_SUPER_OFFSET = 1024
_SUPER_MAGIC = 0xE0F5E1E2
_FEATURE_INCOMPAT_48BIT = 0x00000080
_FEATURE_INCOMPAT_METABOX = 0x00000100
_INODE_FLAT_PLAIN = 0
_INODE_FLAT_INLINE = 2
_DIRENT = struct.Struct("<QHBB")
_XATTR_IBODY_HEADER_SIZE = 12


class ErofsError(Exception):
    """The image is malformed or uses a feature this reader does not support."""


@dataclass(frozen=True)
class ErofsStat:
    """The metadata of one inode.

    Attributes:
        nid: The inode's node number.
        mode: The full ``st_mode``: file type plus permission and special bits.
        uid: Owner.
        gid: Group.
        size: Data size in bytes.
        mtime: Modification time, seconds.
        mtime_nsec: Nanoseconds part of the modification time.
    """

    nid: int
    mode: int
    uid: int
    gid: int
    size: int
    mtime: int
    mtime_nsec: int

    @property
    def is_dir(self) -> bool:
        return stat.S_ISDIR(self.mode)

    @property
    def is_symlink(self) -> bool:
        return stat.S_ISLNK(self.mode)


@dataclass(frozen=True)
class _Inode:
    stat: ErofsStat
    layout: int
    startblk: int
    # Byte offset just past the inode and its inline xattrs, where an inline
    # data tail starts.
    inline_offset: int


class ErofsImage:
    """An EROFS image opened for metadata lookups.

    Args:
        path: The image file.

    Raises:
        ErofsError: The file is not an EROFS image this reader supports.
    """

    def __init__(self, path: str):
        self._file: BinaryIO = open(path, "rb")
        try:
            self._read_superblock()
        except BaseException:
            self._file.close()
            raise

    def close(self) -> None:
        self._file.close()

    def __enter__(self) -> "ErofsImage":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _read_superblock(self) -> None:
        self._file.seek(_SUPER_OFFSET)
        sb = self._file.read(128)
        if len(sb) < 128:
            raise ErofsError("image is too short for an EROFS superblock")
        magic, _checksum, _compat, blkszbits, _extslots, rootnid = struct.unpack_from(
            "<IIIBBH", sb, 0
        )
        if magic != _SUPER_MAGIC:
            raise ErofsError("not an EROFS image (bad superblock magic)")
        epoch, fixed_nsec, _blocks, meta_blkaddr = struct.unpack_from("<QIII", sb, 24)
        (feature_incompat,) = struct.unpack_from("<I", sb, 80)
        dirblkbits = sb[90]
        if feature_incompat & (_FEATURE_INCOMPAT_48BIT | _FEATURE_INCOMPAT_METABOX):
            raise ErofsError(
                f"unsupported EROFS features 0x{feature_incompat:x} (48-bit or metabox)"
            )
        if dirblkbits:
            raise ErofsError("unsupported EROFS directory block size")
        self._blksz = 1 << blkszbits
        self._meta_offset = meta_blkaddr << blkszbits
        self._root_nid = rootnid
        self._epoch = epoch
        self._fixed_nsec = fixed_nsec

    @property
    def root_nid(self) -> int:
        return self._root_nid

    def _inode(self, nid: int) -> _Inode:
        offset = self._meta_offset + (nid << 5)
        self._file.seek(offset)
        raw = self._file.read(64)
        if len(raw) < 32:
            raise ErofsError(f"inode {nid} lies past the end of the image")
        i_format, xattr_icount, mode = struct.unpack_from("<HHH", raw, 0)
        extended = i_format & 0x01
        layout = (i_format >> 1) & 0x07
        if extended:
            if len(raw) < 64:
                raise ErofsError(f"inode {nid} lies past the end of the image")
            size, startblk, _ino, uid, gid, mtime, mtime_nsec = struct.unpack_from(
                "<QIIIIQI", raw, 8
            )
            inode_size = 64
        else:
            size, rel_mtime, startblk, _ino, uid, gid = struct.unpack_from(
                "<IIIIHH", raw, 8
            )
            mtime = self._epoch + rel_mtime
            mtime_nsec = self._fixed_nsec
            inode_size = 32
        xattr_size = 0
        if xattr_icount:
            xattr_size = _XATTR_IBODY_HEADER_SIZE + (xattr_icount - 1) * 4
        return _Inode(
            stat=ErofsStat(
                nid=nid,
                mode=mode,
                uid=uid,
                gid=gid,
                size=size,
                mtime=mtime,
                mtime_nsec=mtime_nsec,
            ),
            layout=layout,
            startblk=startblk,
            inline_offset=offset + inode_size + xattr_size,
        )

    def _data(self, inode: _Inode) -> bytes:
        """The whole data of a small inode: a directory or a symlink."""
        size = inode.stat.size
        if inode.layout == _INODE_FLAT_PLAIN:
            self._file.seek(inode.startblk * self._blksz)
            data = self._file.read(size)
        elif inode.layout == _INODE_FLAT_INLINE:
            full = (size // self._blksz) * self._blksz
            data = b""
            if full:
                self._file.seek(inode.startblk * self._blksz)
                data = self._file.read(full)
            self._file.seek(inode.inline_offset)
            data += self._file.read(size - full)
        else:
            raise ErofsError(
                f"inode {inode.stat.nid} uses data layout {inode.layout}, which "
                "this reader does not support"
            )
        if len(data) != size:
            raise ErofsError(f"inode {inode.stat.nid} data is truncated")
        return data

    def _dirents(self, inode: _Inode) -> Iterator[Tuple[bytes, int]]:
        data = self._data(inode)
        for block_start in range(0, len(data), self._blksz):
            block = data[block_start : block_start + self._blksz]
            if len(block) < _DIRENT.size:
                break
            first_nameoff = _DIRENT.unpack_from(block, 0)[1]
            count = first_nameoff // _DIRENT.size
            if not count or first_nameoff > len(block):
                raise ErofsError(f"directory {inode.stat.nid} has a bad block")
            entries = [
                _DIRENT.unpack_from(block, i * _DIRENT.size) for i in range(count)
            ]
            for i, (nid, nameoff, _ftype, _reserved) in enumerate(entries):
                end = entries[i + 1][1] if i + 1 < count else len(block)
                name = block[nameoff:end]
                if i + 1 == count:
                    # The last name runs to the end of the block, padded with
                    # NULs when it ends before it.
                    name = name.split(b"\0", 1)[0]
                yield name, nid

    def stat(self, nid: int) -> ErofsStat:
        """The metadata of inode ``nid``."""
        return self._inode(nid).stat

    def lookup(self, dir_nid: int, name: str) -> Optional[int]:
        """Find ``name`` in a directory.

        Args:
            dir_nid: The directory's nid.
            name: An entry name, without slashes.

        Returns:
            The entry's nid, or None if the directory has no such entry.

        Raises:
            ErofsError: ``dir_nid`` is not a directory.
        """
        inode = self._inode(dir_nid)
        if not inode.stat.is_dir:
            raise ErofsError(f"inode {dir_nid} is not a directory")
        wanted = os.fsencode(name)
        for entry, nid in self._dirents(inode):
            if entry == wanted:
                return nid
        return None

    def readlink(self, nid: int) -> str:
        """The target of symlink ``nid``."""
        inode = self._inode(nid)
        if not inode.stat.is_symlink:
            raise ErofsError(f"inode {nid} is not a symlink")
        return os.fsdecode(self._data(inode))

    def lstat(self, path: str) -> Optional[ErofsStat]:
        """The metadata at ``path``, following no symlink; None if absent.

        Symlinks on the way are not followed either: a path through one is
        reported absent. Callers that must resolve them walk component by
        component with :meth:`lookup` and :meth:`readlink`.
        """
        nid = self._root_nid
        for part in [p for p in path.split("/") if p not in ("", ".")]:
            current = self.stat(nid)
            if not current.is_dir:
                return None
            child = self.lookup(nid, part)
            if child is None:
                return None
            nid = child
        return self.stat(nid)
