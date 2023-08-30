import contextlib
import glob
import json
import logging
import os
import platform
import shutil
import tempfile
import traceback
from typing import Any, Dict, Iterator, List, Optional, Union
import uuid

import pyarrow.fs

from ray.air._internal.filelock import TempFileLock
from ray.train._internal.storage import _download_from_fs_path, _exists_at_fs_path
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

# The filename of the file that stores user metadata set on the checkpoint.
_METADATA_FILE_NAME = ".metadata.json"

# The prefix of the temp checkpoint directory that `to_directory` downloads to
# on the local filesystem.
_CHECKPOINT_TEMP_DIR_PREFIX = "checkpoint_tmp_"


@PublicAPI(stability="beta")
class Checkpoint:
    """A reference to data persisted as a directory in local or remote storage.

    Access checkpoint contents locally using ``checkpoint.to_directory()``.

    Example creating a checkpoint using ``Checkpoint.from_directory``:

        >>> from ray.train import Checkpoint
        >>> checkpoint = Checkpoint.from_directory("/tmp/example_checkpoint_dir")
        >>> checkpoint.filesystem  # doctest: +ELLIPSIS
        <pyarrow._fs.LocalFileSystem object...
        >>> checkpoint.path
        '/tmp/example_checkpoint_dir'

    Example creating a checkpoint from a remote URI:

        >>> checkpoint = Checkpoint("s3://bucket/path/to/checkpoint")
        >>> checkpoint.filesystem  # doctest: +ELLIPSIS
        <pyarrow._s3fs.S3FileSystem object...
        >>> checkpoint.path
        'bucket/path/to/checkpoint'

    Attributes:
        path: A path on the filesystem containing the checkpoint contents.
        filesystem: PyArrow FileSystem that can be used to access data at the `path`.
    """

    def __init__(
        self,
        path: Union[str, os.PathLike],
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ):
        """Construct a Checkpoint.

        Args:
            path: A local path or remote URI containing the checkpoint data.
                If a filesystem is provided, then this path must NOT be a URI.
                It should be a path on the filesystem with the prefix already stripped.
            filesystem: PyArrow FileSystem to use to access data at the path.
                If not specified, this is inferred from the URI scheme.
        """
        self.path = str(path)
        self.filesystem = filesystem

        if path and not filesystem:
            self.filesystem, self.path = pyarrow.fs.FileSystem.from_uri(path)

        # This random UUID is used to create a temporary directory name on the
        # local filesystem, which will be used for downloading checkpoint data.
        # This ensures that if multiple processes download the same checkpoint object
        # only one process performs the actual download while the others wait.
        # This prevents duplicated download efforts and data.
        # NOTE: Calling `to_directory` from multiple `Checkpoint` objects
        # that point to the same (fs, path) will still download the data multiple times.
        # This only ensures a canonical temp directory name for a single `Checkpoint`.
        self._uuid = uuid.uuid4()

    def __repr__(self):
        return f"Checkpoint(filesystem={self.filesystem.type_name}, path={self.path})"

    def get_metadata(self) -> Dict[str, Any]:
        """Return the metadata dict stored with the checkpoint.

        If no metadata is stored, an empty dict is returned.
        """
        metadata_path = os.path.join(self.path, _METADATA_FILE_NAME)
        if not _exists_at_fs_path(self.filesystem, metadata_path):
            return {}

        with self.filesystem.open_input_file(metadata_path) as f:
            return json.loads(f.readall().decode("utf-8"))

    def set_metadata(self, metadata: Dict[str, Any]) -> None:
        """Set the metadata stored with this checkpoint.

        This will overwrite any existing metadata stored with this checkpoint.
        """
        metadata_path = os.path.join(self.path, _METADATA_FILE_NAME)
        with self.filesystem.open_output_stream(metadata_path) as f:
            f.write(json.dumps(metadata).encode("utf-8"))

    def update_metadata(self, metadata: Dict[str, Any]) -> None:
        """Update the metadata stored with this checkpoint.

        This will update any existing metadata stored with this checkpoint.
        """
        existing_metadata = self.get_metadata()
        existing_metadata.update(metadata)
        self.set_metadata(existing_metadata)

    @classmethod
    def from_directory(cls, path: Union[str, os.PathLike]) -> "Checkpoint":
        """Create checkpoint object from a local directory.

        Args:
            path: Local directory containing checkpoint data. The caller should not
                modify the contents of this directory after creating the Checkpoint.
                If passing this checkpoint to `train.report`, Ray will take control
                of the checkpoint directory.

        Returns:
            A ray.train.Checkpoint object.
        """
        return cls(path, filesystem=pyarrow.fs.LocalFileSystem())

    def to_directory(self, path: Optional[Union[str, os.PathLike]] = None) -> str:
        """Write checkpoint data to directory.

        Args:
            path: Target directory to restore data in. If not specified,
                will create a temporary directory.

        Returns:
            str: Directory containing checkpoint data.
        """
        user_provided_path = path is not None
        local_path = (
            path if user_provided_path else self._get_temporary_checkpoint_dir()
        )
        local_path = os.path.normpath(os.path.expanduser(str(local_path)))
        os.makedirs(local_path, exist_ok=True)

        try:
            # Timeout 0 means there will be only one attempt to acquire
            # the file lock. If it cannot be acquired, throw a TimeoutError
            with TempFileLock(local_path, timeout=0):
                _download_from_fs_path(
                    fs=self.filesystem, fs_path=self.path, local_path=local_path
                )
        except TimeoutError:
            # if the directory is already locked, then wait but do not do anything.
            with TempFileLock(local_path, timeout=-1):
                pass
            if not os.path.exists(local_path):
                raise RuntimeError(
                    f"Checkpoint directory {local_path} does not exist, "
                    "even though it should have been created by "
                    "another process. Please raise an issue on GitHub: "
                    "https://github.com/ray-project/ray/issues"
                )

        return local_path

    @contextlib.contextmanager
    def as_directory(self) -> Iterator[str]:
        """Return checkpoint directory path in a context.

        This function makes checkpoint data available as a directory while avoiding
        unnecessary copies and left-over temporary data.

        If the current path is local, it will return the existing path. If it is
        not, it will create a temporary directory,
        which will be deleted after the context is exited.

        Users should treat the returned checkpoint directory as read-only and avoid
        changing any data within it, as it might get deleted when exiting the context.

        Example:

        .. testcode::
            :hide:

            from pathlib import Path
            import tempfile

            from ray.train import Checkpoint

            temp_dir = tempfile.mkdtemp()
            (Path(temp_dir) / "example.txt").write_text("example checkpoint data")
            checkpoint = Checkpoint.from_directory(temp_dir)

        .. testcode::

            with checkpoint.as_directory() as checkpoint_dir:
                # Do some read-only processing of files within checkpoint_dir
                pass

            # At this point, if a temporary directory was created, it will have
            # been deleted.

        """
        if isinstance(self.filesystem, pyarrow.fs.LocalFileSystem):
            yield self.path
        else:
            del_lock_path = _get_del_lock_path(self._get_temporary_checkpoint_dir())
            open(del_lock_path, "a").close()

            try:
                temp_dir = self.to_directory()
                yield temp_dir
            finally:
                # Always cleanup the del lock after we're done with the directory.
                # This avoids leaving a lock file behind in the case of an exception
                # in the user code.
                try:
                    os.remove(del_lock_path)
                except Exception:
                    logger.warning(
                        f"Could not remove {del_lock_path} deletion file lock. "
                        f"Traceback:\n{traceback.format_exc()}"
                    )

                # In the edge case (process crash before del lock file is removed),
                # we do not remove the directory at all.
                # Since it's in /tmp, this is not that big of a deal.
                # check if any lock files are remaining
                remaining_locks = _list_existing_del_locks(temp_dir)
                if not remaining_locks:
                    try:
                        # Timeout 0 means there will be only one attempt to acquire
                        # the file lock. If it cannot be acquired, a TimeoutError
                        # will be thrown.
                        with TempFileLock(temp_dir, timeout=0):
                            shutil.rmtree(temp_dir, ignore_errors=True)
                    except TimeoutError:
                        pass

    def _get_temporary_checkpoint_dir(self) -> str:
        """Return the name for the temporary checkpoint dir that this checkpoint
        will get downloaded to, if accessing via `to_directory` or `as_directory`.
        """
        tmp_dir_path = tempfile.gettempdir()
        checkpoint_dir_name = _CHECKPOINT_TEMP_DIR_PREFIX + self._uuid.hex
        if platform.system() == "Windows":
            # Max path on Windows is 260 chars, -1 for joining \
            # Also leave a little for the del lock
            del_lock_name = _get_del_lock_path("")
            checkpoint_dir_name = (
                _CHECKPOINT_TEMP_DIR_PREFIX
                + self._uuid.hex[
                    -259
                    + len(_CHECKPOINT_TEMP_DIR_PREFIX)
                    + len(tmp_dir_path)
                    + len(del_lock_name) :
                ]
            )
            if not checkpoint_dir_name.startswith(_CHECKPOINT_TEMP_DIR_PREFIX):
                raise RuntimeError(
                    "Couldn't create checkpoint directory due to length "
                    "constraints. Try specifying a shorter checkpoint path."
                )
        return os.path.join(tmp_dir_path, checkpoint_dir_name)

    def __fspath__(self):
        raise TypeError(
            "You cannot use `Checkpoint` objects directly as paths. "
            "Use `Checkpoint.to_directory()` or `Checkpoint.as_directory()` instead."
        )


def _get_del_lock_path(path: str, suffix: str = None) -> str:
    """Get the path to the deletion lock file for a file/directory at `path`.

    Example:

        >>> _get_del_lock_path("/tmp/checkpoint_tmp")  # doctest: +ELLIPSIS
        '/tmp/checkpoint_tmp.del_lock_...
        >>> _get_del_lock_path("/tmp/checkpoint_tmp/")  # doctest: +ELLIPSIS
        '/tmp/checkpoint_tmp.del_lock_...
        >>> _get_del_lock_path("/tmp/checkpoint_tmp.txt")  # doctest: +ELLIPSIS
        '/tmp/checkpoint_tmp.txt.del_lock_...

    """
    suffix = suffix if suffix is not None else str(os.getpid())
    return f"{path.rstrip('/')}.del_lock_{suffix}"


def _list_existing_del_locks(path: str) -> List[str]:
    """List all the deletion lock files for a file/directory at `path`.

    For example, if 2 checkpoints are being read via `as_directory`,
    then this should return a list of 2 deletion lock files.
    """
    return list(glob.glob(f"{_get_del_lock_path(path, suffix='*')}"))
