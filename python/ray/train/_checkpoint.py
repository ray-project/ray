import contextlib
import glob
import json
import logging
import os
import platform
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

import pyarrow.fs

from ray.air._internal.filelock import TempFileLock
from ray.train._internal.storage import _download_from_fs_path, _exists_at_fs_path
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

# The filename of the file that stores user metadata set on the checkpoint.
_METADATA_FILE_NAME = ".metadata.json"

# The prefix of the temporary directory that `to_directory` and `as_directory`
# download to on the local filesystem.
_CHECKPOINT_TEMP_DIR_PREFIX = "checkpoint_tmp_"

# Siblings of the temporary checkpoint directory. See
# `Checkpoint._download_to_temp_dir` and `Checkpoint.as_directory`.
_STAGING_SUFFIX = ".staging"
_DELETING_SUFFIX = ".deleting"
_READER_SUFFIX = ".reader_"
# Marks a temporary directory that `to_directory` returned, so it's never deleted.
_KEEP_SUFFIX = ".keep"


class _CheckpointMetaClass(type):
    def __getattr__(self, item):
        try:
            return super().__getattribute__(item)
        except AttributeError as exc:
            if item in {
                "from_dict",
                "to_dict",
                "from_bytes",
                "to_bytes",
                "get_internal_representation",
            }:
                raise _get_migration_error(item) from exc
            elif item in {
                "from_uri",
                "to_uri",
                "uri",
            }:
                raise _get_uri_error(item) from exc
            elif item in {"get_preprocessor", "set_preprocessor"}:
                raise _get_preprocessor_error(item) from exc

            raise exc


@PublicAPI(stability="beta")
class Checkpoint(metaclass=_CheckpointMetaClass):
    """A reference to data persisted as a directory in local or remote storage.

    Access the checkpoint contents locally using ``checkpoint.to_directory()``
    or ``checkpoint.as_directory``.

    Attributes
    ----------
    path: A path on the filesystem containing the checkpoint contents.
    filesystem: PyArrow FileSystem that can be used to access data at the `path`.

    See Also
    --------
    ray.train.report : Report a checkpoint during training (with Ray Train/Tune).
    ray.train.get_checkpoint : Get the latest checkpoint during training
        (for restoration).

    :ref:`train-checkpointing`
    :ref:`persistent-storage-guide`

    Examples
    --------

    Creating a checkpoint using ``Checkpoint.from_directory``:

        >>> from ray.train import Checkpoint
        >>> checkpoint = Checkpoint.from_directory("/tmp/example_checkpoint_dir")
        >>> checkpoint.filesystem  # doctest: +ELLIPSIS
        <pyarrow._fs.LocalFileSystem object...
        >>> checkpoint.path
        '/tmp/example_checkpoint_dir'

    Creating a checkpoint from a remote URI:

        >>> checkpoint = Checkpoint("s3://bucket/path/to/checkpoint")
        >>> checkpoint.filesystem  # doctest: +ELLIPSIS
        <pyarrow._s3fs.S3FileSystem object...
        >>> checkpoint.path
        'bucket/path/to/checkpoint'

    Creating a checkpoint with a custom filesystem:

        >>> checkpoint = Checkpoint(
        ...     path="bucket/path/to/checkpoint",
        ...     filesystem=pyarrow.fs.S3FileSystem(),
        ... )
        >>> checkpoint.filesystem  # doctest: +ELLIPSIS
        <pyarrow._s3fs.S3FileSystem object...
        >>> checkpoint.path
        'bucket/path/to/checkpoint'

    Accessing a checkpoint's contents:

        >>> import os  # doctest: +SKIP
        >>> with checkpoint.as_directory() as local_checkpoint_dir:  # doctest: +SKIP
        ...    print(os.listdir(local_checkpoint_dir))  # doctest: +SKIP
        ['model.pt', 'optimizer.pt', 'misc.pt']
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

        # This random UUID is used to create a temporary directory name for
        # `as_directory()` when downloading checkpoint data.
        # This allows multiple workers to download the same checkpoint object
        # while only one worker actually performs the download while the
        # others wait, preventing duplicated download efforts.
        self._uuid = uuid.uuid4()

    def __repr__(self):
        return f"Checkpoint(filesystem={self.filesystem.type_name}, path={self.path})"

    def get_metadata(self) -> Dict[str, Any]:
        """Return the metadata dict stored with the checkpoint.

        If no metadata is stored, an empty dict is returned.

        Returns:
            The metadata dict, or an empty dict if none is stored.
        """
        metadata_path = Path(self.path, _METADATA_FILE_NAME).as_posix()
        if not _exists_at_fs_path(self.filesystem, metadata_path):
            return {}

        with self.filesystem.open_input_file(metadata_path) as f:
            return json.loads(f.readall().decode("utf-8"))

    def set_metadata(self, metadata: Dict[str, Any]) -> None:
        """Set the metadata stored with this checkpoint.

        This will overwrite any existing metadata stored with this checkpoint.

        Args:
            metadata: The metadata to store. Must be JSON-serializable.
        """
        metadata_path = Path(self.path, _METADATA_FILE_NAME).as_posix()
        with self.filesystem.open_output_stream(metadata_path) as f:
            f.write(json.dumps(metadata).encode("utf-8"))

    def update_metadata(self, metadata: Dict[str, Any]) -> None:
        """Update the metadata stored with this checkpoint.

        This will update any existing metadata stored with this checkpoint.

        Args:
            metadata: The keys and values to merge into the existing metadata.
                Must be JSON-serializable.
        """
        existing_metadata = self.get_metadata()
        existing_metadata.update(metadata)
        self.set_metadata(existing_metadata)

    @classmethod
    def from_directory(cls, path: Union[str, os.PathLike]) -> "Checkpoint":
        """Create checkpoint object from a local directory.

        Args:
            path: Local directory containing checkpoint data.

        Returns:
            A ray.train.Checkpoint object.
        """
        return cls(path, filesystem=pyarrow.fs.LocalFileSystem())

    def to_directory(self, path: Optional[Union[str, os.PathLike]] = None) -> str:
        """Write checkpoint data to a local directory.

        If multiple workers on the same node call this method simultaneously,
        only a single one will perform the download, while the others wait for
        the download to finish. Once the download finishes, all workers
        receive the same local directory to read from.

        If no path is given, the checkpoint is downloaded to a temporary directory
        that is shared with every ``to_directory()`` and ``as_directory()`` call
        for this checkpoint on the node, so the checkpoint is downloaded only
        once. If it's already downloaded, the path is returned immediately.
        Once this method returns the temporary directory, Ray never deletes it,
        so treat it as read-only.

        Args:
            path: Target directory to download data to. If not specified,
                this method will use a temporary directory.

        Returns:
            str: Directory containing checkpoint data.
        """
        if path is None:
            temp_dir = self._get_temp_dirname()
            with TempFileLock(temp_dir):
                # Check if the checkpoint has been downloaded already
                if not os.path.isdir(temp_dir):
                    self._download_to_temp_dir(temp_dir)

                # Mark the directory to prevent a future `as_directory()` from deleting it
                Path(f"{temp_dir}{_KEEP_SUFFIX}").touch()
            return temp_dir

        # Download to user-provided path
        local_path = os.path.normpath(os.path.expanduser(str(path)))
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
        """Returns checkpoint contents in a local directory as a context.

        If the checkpoint is stored in a local directory, this method just
        returns the checkpoint path without making a copy, and nothing is
        cleaned up.

        If the checkpoint is stored remotely, this method will
        download the checkpoint to a local temporary directory and return the
        path. Additionally, if multiple workers on the same node call this
        method simultaneously, only a single worker will perform the download.
        Meanwhile, the other workers will wait for the download to finish and
        return the shared checkpoint path. Once all workers have left this
        function's context, the temporary directory will be deleted and future
        workers will need to redownload the checkpoint.

        Examples:
            >>> from pathlib import Path
            >>> import tempfile
            >>> from ray.train import Checkpoint
            >>> temp_dir = tempfile.mkdtemp()
            >>> (Path(temp_dir) / "example.txt").write_text("example checkpoint data")
            23
            >>> checkpoint = Checkpoint.from_directory(temp_dir)
            >>> with checkpoint.as_directory() as checkpoint_dir:
            ...     # Do some read-only processing of files within checkpoint_dir
            ...     pass
            >>> # At this point, if a temporary directory was created, it will have
            >>> # been deleted.

        Yields:
            str: The local path to the checkpoint directory.
        """
        if isinstance(self.filesystem, pyarrow.fs.LocalFileSystem):
            yield self.path
        else:
            # Load remote checkpoint
            temp_dir = self._get_temp_dirname()
            # Each call registers its own reader file, so nested calls and threads
            # in one process count as separate readers.
            reader_path = _get_reader_path(temp_dir)

            with TempFileLock(temp_dir):
                # Check if the checkpoint has been downloaded already
                if not os.path.isdir(temp_dir):
                    self._download_to_temp_dir(temp_dir)

                Path(reader_path).touch()

            try:
                yield temp_dir
            finally:
                # Cleanup failures are logged rather than raised, so that they
                # never hide an exception raised inside the user's `with` block.
                try:
                    # Unregistering, checking for readers and deleting all happen
                    # under the lock, so no reader can register between the
                    # check and the deletion. If a process dies before removing
                    # its reader file, the directory is never deleted.
                    with TempFileLock(temp_dir):
                        os.remove(reader_path)

                        if not _list_readers(temp_dir) and not os.path.exists(
                            f"{temp_dir}{_KEEP_SUFFIX}"
                        ):
                            # Rename before deleting, so that a crash partway
                            # through never leaves a partial directory at
                            # `temp_dir`, where it would look complete.
                            deleting_path = f"{temp_dir}{_DELETING_SUFFIX}"
                            shutil.rmtree(deleting_path, ignore_errors=True)
                            os.rename(temp_dir, deleting_path)
                            shutil.rmtree(deleting_path, ignore_errors=True)
                except Exception:
                    logger.warning(
                        "Could not clean up the temporary checkpoint directory "
                        f"{temp_dir}.",
                        exc_info=True,
                    )

    def _download_to_temp_dir(self, temp_dir: str) -> None:
        """Download the checkpoint to ``temp_dir``, unless it's already there.

        The caller must hold ``TempFileLock(temp_dir)``. The download goes to
        ``<temp_dir>.staging``, which is renamed to ``temp_dir`` once complete,
        so ``temp_dir`` exists only when it holds the complete checkpoint, even if
        a process crashes partway through. Never create ``temp_dir`` any other
        way, such as with ``os.makedirs``, or a partial download would look
        complete.

        Args:
            temp_dir: The temporary checkpoint directory.
        """
        staging_path = f"{temp_dir}{_STAGING_SUFFIX}"

        # Delete a staging path in case an old download failed midway through
        shutil.rmtree(staging_path, ignore_errors=True)
        # Delete all old deleting suffixes
        shutil.rmtree(f"{temp_dir}{_DELETING_SUFFIX}", ignore_errors=True)

        # Download to the staging path then rename to the temporary directory
        _download_from_fs_path(
            fs=self.filesystem,
            fs_path=self.path,
            local_path=staging_path,
            filelock=False,
        )
        os.rename(staging_path, temp_dir)

    def _get_temp_dirname(self) -> str:
        """Return the temporary directory that ``to_directory()`` and
        ``as_directory()`` download this checkpoint to.

        Returns:
            The temporary checkpoint directory path.
        """
        tmp_dir_path = tempfile.gettempdir()
        checkpoint_dir_name = _CHECKPOINT_TEMP_DIR_PREFIX + self._uuid.hex
        if platform.system() == "Windows":
            # Max path on Windows is 260 chars, -1 for joining \
            # Also leave a little for the reader file
            reader_name = _get_reader_path("")
            checkpoint_dir_name = (
                _CHECKPOINT_TEMP_DIR_PREFIX
                + self._uuid.hex[
                    -259
                    + len(_CHECKPOINT_TEMP_DIR_PREFIX)
                    + len(tmp_dir_path)
                    + len(reader_name) :
                ]
            )
            if not checkpoint_dir_name.startswith(_CHECKPOINT_TEMP_DIR_PREFIX):
                raise RuntimeError(
                    "Couldn't create checkpoint directory due to length "
                    "constraints. Try specifying a shorter checkpoint path."
                )
        return os.path.normpath(Path(tmp_dir_path, checkpoint_dir_name).as_posix())

    def __fspath__(self):
        raise TypeError(
            "You cannot use `Checkpoint` objects directly as paths. "
            "Use `Checkpoint.to_directory()` or `Checkpoint.as_directory()` instead."
        )


def _get_reader_path(temp_dir: str, reader_id: Optional[str] = None) -> str:
    """Get the path of a reader file for the shared temporary directory.

    Args:
        temp_dir: The shared temporary checkpoint directory.
        reader_id: Identifies the reader. Defaults to the process ID followed by
            a random ID, so that each call returns a new path, even within one
            process.

    Returns:
        The reader file path, a sibling of ``temp_dir``.
    """
    if reader_id is None:
        reader_id = f"{os.getpid()}_{uuid.uuid4().hex}"
    return f"{temp_dir.rstrip('/')}{_READER_SUFFIX}{reader_id}"


def _list_readers(temp_dir: str) -> List[str]:
    """List the reader files for ``temp_dir``.

    Args:
        temp_dir: The shared temporary checkpoint directory.

    Returns:
        The paths of the reader files.
    """
    return glob.glob(_get_reader_path(temp_dir, reader_id="*"))


def _get_migration_error(name: str):
    return AttributeError(
        f"The new `ray.train.Checkpoint` class does not support `{name}()`. "
        f"Instead, only directories are supported.\n\n"
        f"Example to store a dictionary in a checkpoint:\n\n"
        f"import os, tempfile\n"
        f"import ray.cloudpickle as pickle\n"
        f"from ray import train\n"
        f"from ray.train import Checkpoint\n\n"
        f"with tempfile.TemporaryDirectory() as checkpoint_dir:\n"
        f"  with open(os.path.join(checkpoint_dir, 'data.pkl'), 'wb') as fp:\n"
        f"    pickle.dump({{'data': 'value'}}, fp)\n\n"
        f"  checkpoint = Checkpoint.from_directory(checkpoint_dir)\n"
        f"  train.report(..., checkpoint=checkpoint)\n\n"
        f"Example to load a dictionary from a checkpoint:\n\n"
        f"if train.get_checkpoint():\n"
        f"  with train.get_checkpoint().as_directory() as checkpoint_dir:\n"
        f"    with open(os.path.join(checkpoint_dir, 'data.pkl'), 'rb') as fp:\n"
        f"      data = pickle.load(fp)"
    )


def _get_uri_error(name: str):
    return AttributeError(
        f"The new `ray.train.Checkpoint` class does not support `{name}()`. "
        f"To create a checkpoint from remote storage, create a `Checkpoint` using its "
        f"constructor instead of `from_directory`.\n"
        f'Example: `Checkpoint(path="s3://a/b/c")`.\n'
        f"Then, access the contents of the checkpoint with "
        f"`checkpoint.as_directory()` / `checkpoint.to_directory()`.\n"
        f"To upload data to remote storage, use e.g. `pyarrow.fs.FileSystem` "
        f"or your client of choice."
    )


def _get_preprocessor_error(name: str):
    return AttributeError(
        f"The new `ray.train.Checkpoint` class does not support `{name}()`. "
        f"To include preprocessor information in checkpoints, "
        f"pass it as metadata in the <Framework>Trainer constructor.\n"
        f"Example: `TorchTrainer(..., metadata={{...}})`.\n"
        f"After training, access it in the checkpoint via `checkpoint.get_metadata()`. "
        f"See here: https://docs.ray.io/en/master/train/user-guides/"
        f"data-loading-preprocessing.html#preprocessing-structured-data"
    )
