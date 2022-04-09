import io
import os
import shutil
import tarfile

from typing import Optional, Tuple, Dict, Generator, Union

import ray


_DEFAULT_CHUNK_SIZE_BYTES = 500 * 1024 * 1024


def sync_dir_between_nodes(
    source_ip: str,
    source_path: str,
    target_ip: str,
    target_path: str,
    force_all: bool = False,
    chunk_size: int = _DEFAULT_CHUNK_SIZE_BYTES,
    _return_all_remotes: bool = False,
) -> Union[ray.ObjectRef, Tuple[ray.ObjectRef, ray.ActorID, ray.ObjectRef]]:
    """Synchronize directory on source node to directory on target node.

    Per default, this function will collect information about already existing
    files in the target directory. Only files that differ in either mtime or
    filesize will be transferred, unless ``force_all=True``.

    Args:
        source_ip: IP of source node.
        source_path: Path to file or directory on source node.
        target_ip: IP of target node.
        target_path: Path to file or directory on target node.
        force_all: If True, all files will be transferred (not just differing files).
        chunk_size: Chunk size for data transfer.
        _return_all_remotes: If True, returns a tuple of the unpack future,
            the pack actor, and the files_stats future.

    Returns:
        Ray future for scheduled unpacking task.

    """
    pack_actor_on_source_node = _PackActor.options(
        num_cpus=0, resources={f"node:{source_ip}": 0.01}
    )
    unpack_on_target_node = _unpack_from_actor.options(
        num_cpus=0, resources={f"node:{target_ip}": 0.01}
    )

    if force_all:
        files_stats = None
    else:
        files_stats = _remote_get_recursive_files_and_stats.options(
            num_cpus=0, resources={f"node:{target_ip}": 0.01}
        ).remote(target_path)

    pack_actor = pack_actor_on_source_node.remote(
        source_path, files_stats, chunk_size=chunk_size
    )
    unpack_future = unpack_on_target_node.remote(pack_actor, target_path)

    if _return_all_remotes:
        return unpack_future, pack_actor, files_stats

    return unpack_future


def delete_on_node(node_ip: str, path: str) -> ray.ObjectRef:
    """Delete path on node.

    Args:
        node_ip: IP of node to delete path on.
        path: Path to delete on remote node.

    Returns:
        Ray future for scheduled delete task.
    """
    delete_task = _delete_path.options(num_cpus=0, resources={f"node:{node_ip}": 0.01})
    return delete_task.remote(path)


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
    """Pack whole directory contents into a uncompressed tarfile.

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
        chunk_size: Cut bytes stream into chunks of this size in bytes.
    """

    def __init__(
        self,
        source_dir: str,
        files_stats: Optional[Dict[str, Tuple[float, int]]] = None,
        chunk_size: int = _DEFAULT_CHUNK_SIZE_BYTES,
    ):
        self.stream = _pack_dir(source_dir=source_dir, files_stats=files_stats)
        self.chunk_size = chunk_size
        self.iter = None

    def get_full_data(self) -> bytes:
        return self.stream.getvalue()

    def _chunk_generator(self) -> Generator[None, bytes, None]:
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


def _iter_remote(actor: ray.ActorID) -> Generator[None, bytes, None]:
    """Iterate over actor task and return as generator."""
    while True:
        buffer = ray.get(actor.next.remote())
        if buffer is None:
            return
        yield buffer


def _unpack_dir(stream: io.BytesIO, target_dir: str):
    """Unpack tarfile stream into target directory."""
    stream.seek(0)
    with tarfile.open(fileobj=stream) as tar:
        tar.extractall(target_dir)


@ray.remote
def _unpack_from_actor(pack_actor: ray.ActorID, target_dir: str):
    """Iterate over chunks received from pack actor and unpack."""
    stream = io.BytesIO()
    for buffer in _iter_remote(pack_actor):
        stream.write(buffer)
    _unpack_dir(stream, target_dir=target_dir)


@ray.remote
def _delete_path(target_path: str) -> bool:
    """Delete path (files and directories)"""
    if os.path.exists(target_path):
        if os.path.isdir(target_path):
            shutil.rmtree(target_path)
        else:
            os.remove(target_path)
        return True
    return False
