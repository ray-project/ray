import io
import os
import shutil
import tarfile
from filelock import FileLock

from typing import Optional, Tuple, Dict, Generator, Union

import ray


_DEFAULT_CHUNK_SIZE_BYTES = 500 * 1024 * 1024  # 500 MiB
_DEFAULT_MAX_SIZE_BYTES = 1 * 1024 * 1024 * 1024  # 1 GiB


def sync_dir_between_nodes(
    source_ip: str,
    source_path: str,
    target_ip: str,
    target_path: str,
    force_all: bool = False,
    chunk_size_bytes: int = _DEFAULT_CHUNK_SIZE_BYTES,
    max_size_bytes: Optional[int] = _DEFAULT_MAX_SIZE_BYTES,
    return_futures: bool = False,
) -> Union[
    None,
    Tuple[ray.ObjectRef, ray.ActorID, ray.ObjectRef],
    Tuple[ray.ObjectRef, None, None],
]:
    """Synchronize directory on source node to directory on target node.

    Per default, this function will collect information about already existing
    files in the target directory. Only files that differ in either mtime or
    filesize will be transferred, unless ``force_all=True``.

    If ``source_ip==target_ip``, shutil will be used to copy the directory. Otherwise,
    the directory will be packed and sent through the Ray Object Store to the target
    node.

    Args:
        source_ip: IP of source node.
        source_path: Path to directory on source node.
        target_ip: IP of target node.
        target_path: Path to directory on target node.
        force_all: If True, all files will be transferred (not just differing files).
            Ignored if ``source_ip==target_ip``.
        chunk_size_bytes: Chunk size for data transfer. Ignored if
            ``source_ip==target_ip``.
        max_size_bytes: If packed data exceeds this value, raise an error before
            transfer. If ``None``, no limit is enforced. Ignored if
            ``source_ip==target_ip``.
        return_futures: If True, returns a tuple of the unpack future,
            the pack actor, and the files_stats future. If False (default) will
            block until synchronization finished and return None.

    Returns:
        None, or Tuple of unpack future, pack actor, and files_stats future.
        If ``source_ip==target_ip``, pack actor and files_stats future will be None.

    """
    if source_ip != target_ip:
        return _sync_dir_between_different_nodes(
            source_ip=source_ip,
            source_path=source_path,
            target_ip=target_ip,
            target_path=target_path,
            force_all=force_all,
            chunk_size_bytes=chunk_size_bytes,
            max_size_bytes=max_size_bytes,
            return_futures=return_futures,
        )
    elif source_path != target_path:
        ret = _sync_dir_on_same_node(
            ip=source_ip,
            source_path=source_path,
            target_path=target_path,
            return_futures=return_futures,
        )
        if return_futures:
            return ret, None, None
        return ret


def _sync_dir_on_same_node(
    ip: str,
    source_path: str,
    target_path: str,
    return_futures: bool = False,
) -> Optional[ray.ObjectRef]:
    """Synchronize directory to another directory on the same node.

    Per default, this function will collect information about already existing
    files in the target directory. All files will be copied over.

    Args:
        ip: IP of the node.
        source_path: Path to source directory.
        target_path: Path to target directory.
        return_futures: If True, returns a future of the copy task.

    Returns:
        None, or future of the copy task.

    """
    copy_on_node = _copy_dir.options(
        num_cpus=0, resources={f"node:{ip}": 0.01}, placement_group=None
    )
    copy_future = copy_on_node.remote(source_dir=source_path, target_dir=target_path)

    if return_futures:
        return copy_future

    return ray.get(copy_future)


def _sync_dir_between_different_nodes(
    source_ip: str,
    source_path: str,
    target_ip: str,
    target_path: str,
    force_all: bool = False,
    chunk_size_bytes: int = _DEFAULT_CHUNK_SIZE_BYTES,
    max_size_bytes: Optional[int] = _DEFAULT_MAX_SIZE_BYTES,
    return_futures: bool = False,
) -> Union[None, Tuple[ray.ObjectRef, ray.ActorID, ray.ObjectRef]]:
    """Synchronize directory on source node to directory on target node.

    Per default, this function will collect information about already existing
    files in the target directory. Only files that differ in either mtime or
    filesize will be transferred, unless ``force_all=True``.

    Args:
        source_ip: IP of source node.
        source_path: Path to directory on source node.
        target_ip: IP of target node.
        target_path: Path to directory on target node.
        force_all: If True, all files will be transferred (not just differing files).
        chunk_size_bytes: Chunk size for data transfer.
        max_size_bytes: If packed data exceeds this value, raise an error before
            transfer. If ``None``, no limit is enforced.
        return_futures: If True, returns a tuple of the unpack future,
            the pack actor, and the files_stats future. If False (default) will
            block until synchronization finished and return None.

    Returns:
        None, or Tuple of unpack future, pack actor, and files_stats future.

    """
    pack_actor_on_source_node = _PackActor.options(
        num_cpus=0, resources={f"node:{source_ip}": 0.01}, placement_group=None
    )
    unpack_on_target_node = _unpack_from_actor.options(
        num_cpus=0, resources={f"node:{target_ip}": 0.01}, placement_group=None
    )

    if force_all:
        files_stats = None
    else:
        files_stats = _remote_get_recursive_files_and_stats.options(
            num_cpus=0, resources={f"node:{target_ip}": 0.01}, placement_group=None
        ).remote(target_path)

    pack_actor = pack_actor_on_source_node.remote(
        source_dir=source_path,
        files_stats=files_stats,
        chunk_size_bytes=chunk_size_bytes,
        max_size_bytes=max_size_bytes,
    )
    unpack_future = unpack_on_target_node.remote(pack_actor, target_path)

    if return_futures:
        return unpack_future, pack_actor, files_stats

    return ray.get(unpack_future)


def delete_on_node(
    node_ip: str, path: str, return_future: bool = False
) -> Union[bool, ray.ObjectRef]:
    """Delete path on node.

    Args:
        node_ip: IP of node to delete path on.
        path: Path to delete on remote node.
        return_future: If True, returns the delete future. Otherwise, blocks until
            the task finished and returns True if the path was deleted or False if not
            (e.g. if the path does not exist on the remote node).

    Returns:
        Boolean indicating if deletion succeeded, or Ray future
        for scheduled delete task.
    """
    delete_task = _remote_delete_path.options(
        num_cpus=0, resources={f"node:{node_ip}": 0.01}, placement_group=None
    )
    future = delete_task.remote(path)

    if return_future:
        return future

    return ray.get(future)


def _get_recursive_files_and_stats(path: str) -> Dict[str, Tuple[float, int]]:
    """Return dict of files mapping to stats in ``path``.

    This function scans a directory ``path`` recursively and returns a dict
    mapping each contained file to a tuple of (mtime, filesize).

    mtime and filesize are returned from ``os.lstat`` and are usually a
    floating point number (timestamp) and an int (filesize in bytes).
    """
    files_stats = {}
    for root, dirs, files in os.walk(path, topdown=False):
        rel_root = os.path.relpath(root, path)
        for file in files:
            key = os.path.join(rel_root, file)
            stat = os.lstat(os.path.join(path, key))
            files_stats[key] = stat.st_mtime, stat.st_size

    return files_stats


# Only export once
_remote_get_recursive_files_and_stats = ray.remote(_get_recursive_files_and_stats)


def _pack_dir(
    source_dir: str, files_stats: Optional[Dict[str, Tuple[float, int]]] = None
) -> io.BytesIO:
    """Pack whole directory contents into an uncompressed tarfile.

    This function accepts a ``files_stats`` argument. If given, only files
    whose stats differ from these stats will be packed.

    The main use case for this is that we can collect information about files
    already existing in the target directory, and only pack files that have
    been updated. This is similar to how cloud syncing utilities decide
    which files to transfer.

    Args:
        source_dir: Path to local directory to pack into tarfile.
        files_stats: Dict of relative filenames mapping to a tuple of
            (mtime, filesize). Only files that differ from these stats
            will be packed.

    Returns:
        Tarfile as a stream object.
    """
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.PAX_FORMAT) as tar:
        if not files_stats:
            # If no `files_stats` is passed, pack whole directory
            tar.add(source_dir, arcname="", recursive=True)
        else:
            # Otherwise, only pack differing files
            tar.add(source_dir, arcname="", recursive=False)
            for root, dirs, files in os.walk(source_dir, topdown=False):
                rel_root = os.path.relpath(root, source_dir)
                # Always add all directories
                for dir in dirs:
                    key = os.path.join(rel_root, dir)
                    tar.add(os.path.join(source_dir, key), arcname=key, recursive=False)
                # Add files where our information differs
                for file in files:
                    key = os.path.join(rel_root, file)
                    stat = os.lstat(os.path.join(source_dir, key))
                    file_stat = stat.st_mtime, stat.st_size
                    if key not in files_stats or file_stat != files_stats[key]:
                        tar.add(os.path.join(source_dir, key), arcname=key)

    return stream


def _gib_string(num_bytes: float) -> str:
    return f"{float(num_bytes / 1024 ** 3):.2f}GiB"


@ray.remote
class _PackActor:
    """Actor wrapping around a packing job.

    This actor is used for chunking the packed data into smaller chunks that
    can be transferred via the object store more efficiently.

    The actor will start packing the directory when initialized, and separate
    chunks can be received by calling the remote ``next()`` task.

    Args:
        source_dir: Path to local directory to pack into tarfile.
        files_stats: Dict of relative filenames mapping to a tuple of
            (mtime, filesize). Only files that differ from these stats
            will be packed.
        chunk_size_bytes: Cut bytes stream into chunks of this size in bytes.
        max_size_bytes: If packed data exceeds this value, raise an error before
            transfer. If ``None``, no limit is enforced.
    """

    def __init__(
        self,
        source_dir: str,
        files_stats: Optional[Dict[str, Tuple[float, int]]] = None,
        chunk_size_bytes: int = _DEFAULT_CHUNK_SIZE_BYTES,
        max_size_bytes: Optional[int] = _DEFAULT_MAX_SIZE_BYTES,
    ):
        self.stream = _pack_dir(source_dir=source_dir, files_stats=files_stats)

        # Get buffer size
        self.stream.seek(0, 2)
        file_size = self.stream.tell()

        if max_size_bytes and file_size > max_size_bytes:
            raise RuntimeError(
                f"Packed directory {source_dir} content has a size of "
                f"{_gib_string(file_size)}, which exceeds the limit "
                f"of {_gib_string(max_size_bytes)}. Please check the directory "
                f"contents. If you want to transfer everything, you can increase "
                f"or disable the limit by passing the `max_size` argument."
            )
        self.chunk_size = chunk_size_bytes
        self.max_size = max_size_bytes
        self.iter = None

    def get_full_data(self) -> bytes:
        return self.stream.getvalue()

    def _chunk_generator(self) -> Generator[bytes, None, None]:
        self.stream.seek(0)
        data = self.stream.read(self.chunk_size)
        while data:
            yield data
            data = self.stream.read(self.chunk_size)

    def next(self) -> Optional[bytes]:
        if not self.iter:
            self.iter = iter(self._chunk_generator())
        try:
            return next(self.iter)
        except StopIteration:
            return None


def _iter_remote(actor: ray.ActorID) -> Generator[bytes, None, None]:
    """Iterate over actor task and return as generator."""
    while True:
        buffer = ray.get(actor.next.remote())
        if buffer is None:
            return
        yield buffer


def _unpack_dir(stream: io.BytesIO, target_dir: str) -> None:
    """Unpack tarfile stream into target directory."""
    stream.seek(0)
    with FileLock(f"{target_dir}.lock"):
        with tarfile.open(fileobj=stream) as tar:
            tar.extractall(target_dir)


@ray.remote
def _unpack_from_actor(pack_actor: ray.ActorID, target_dir: str) -> None:
    """Iterate over chunks received from pack actor and unpack."""
    stream = io.BytesIO()
    for buffer in _iter_remote(pack_actor):
        stream.write(buffer)
    _unpack_dir(stream, target_dir=target_dir)


@ray.remote
def _copy_dir(source_dir: str, target_dir: str) -> None:
    """Copy dir with shutil on the actor."""
    with FileLock(f"{target_dir}.lock"):
        _delete_path_unsafe(target_dir)
        shutil.copytree(source_dir, target_dir)


def _delete_path(target_path: str) -> bool:
    """Delete path (files and directories)"""
    with FileLock(f"{target_path}.lock"):
        return _delete_path_unsafe(target_path)


def _delete_path_unsafe(target_path: str):
    """Delete path (files and directories). No filelock."""
    if os.path.exists(target_path):
        if os.path.isdir(target_path):
            shutil.rmtree(target_path)
        else:
            os.remove(target_path)
        return True
    return False


# Only export once
_remote_delete_path = ray.remote(_delete_path)
