import contextlib
import logging
import os
import platform
import shutil
import tempfile
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional, Union

from ray import cloudpickle as pickle
from ray.air._internal.filelock import TempFileLock
from ray.util.annotations import PublicAPI, Deprecated

if TYPE_CHECKING:
    import pyarrow
    from ray.data.preprocessor import Preprocessor

logger = logging.getLogger(__name__)

_METADATA_FILE_NAME = ".metadata.pkl"
_CHECKPOINT_DIR_PREFIX = "checkpoint_tmp_"


@PublicAPI(stability="beta")
class Checkpoint:
    """A reference to data persisted in local or remote storage.

    Access checkpoint contents locally using `chkpt.to_directory()`.

    Attributes:
        path: A local path or remote URI containing the checkpoint data.
        filesystem: PyArrow FileSystem to use to access data at the path.
    """

    def __init__(
        self,
        path: Union[str, os.PathLike],
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ):
        """Construct a Checkpoint.

        Args:
            path: A local path or remote URI containing the checkpoint data.
            filesystem: PyArrow FileSystem to use to access data at the path. If not
                specified, this is inferred from the URI scheme.
        """
        self.path = path
        self.filesystem = filesystem

        if path and not filesystem:
            import pyarrow

            self.filesystem, self.path = pyarrow.fs.FileSystem.from_uri(path)

    def __repr__(self):
        return f"Checkpoint(path={self.path}, filesystem={self.filesystem})"

    def get_metadata(self) -> Dict[str, Any]:
        """Return the metadata dict stored with the checkpoint.

        If no metadata is stored, an empty dict is returned."""

        path = os.path.join(self.path, _METADATA_FILE_NAME)
        with self.filesystem.open_input_file(path) as f:
            return pickle.loads(f.readall())

    def set_metadata(self, metadata: Dict[str, Any]) -> None:
        """Update the metadata stored with this checkpoint.

        This will overwrite any existing metadata stored with this checkpoint.
        """

        path = os.path.join(self.path, _METADATA_FILE_NAME)
        with self.filesystem.open_output_path(path) as f:
            f.write(pickle.dumps(metadata))

    def to_directory(self, path: Optional[str] = None) -> str:
        """Write checkpoint data to directory.

        Args:
            path: Target directory to restore data in. If not specified,
                will create a temporary directory.

        Returns:
            str: Directory containing checkpoint data.
        """
        import pyarrow

        user_provided_path = path is not None
        path = path if user_provided_path else self._get_temporary_checkpoint_dir()
        path = os.path.normpath(str(path))
        _make_dir(path, acquire_del_lock=not user_provided_path)

        try:
            # Timeout 0 means there will be only one attempt to acquire
            # the file lock. If it cannot be aquired, a TimeoutError
            # will be thrown.
            with TempFileLock(f"{path}.lock", timeout=0):
                pyarrow.fs.copy_files(self.path, path)
        except TimeoutError:
            # if the directory is already locked, then wait but do not do anything.
            with TempFileLock(f"{path}.lock", timeout=-1):
                pass
            if not os.path.exists(path):
                raise RuntimeError(
                    f"Checkpoint directory {path} does not exist, "
                    "even though it should have been created by "
                    "another process. Please raise an issue on GitHub: "
                    "https://github.com/ray-project/ray/issues"
                )

        return path

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

        .. code-block:: python

            with checkpoint.as_directory() as checkpoint_dir:
                # Do some read-only processing of files within checkpoint_dir
                pass

            # At this point, if a temporary directory was created, it will have
            # been deleted.

        """
        import pyarrow

        if isinstance(self.path, pyarrow.fs.LocalFileSystem):
            yield self._local_path
        else:
            temp_dir = self.to_directory()
            del_lock_path = _get_del_lock_path(temp_dir)
            yield temp_dir

            # Cleanup
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
            temp_dir_base_name = Path(temp_dir).name
            if not list(
                Path(temp_dir).parent.glob(_get_del_lock_path(temp_dir_base_name, "*"))
            ):
                try:
                    # Timeout 0 means there will be only one attempt to acquire
                    # the file lock. If it cannot be aquired, a TimeoutError
                    # will be thrown.
                    with TempFileLock(f"{temp_dir}.lock", timeout=0):
                        shutil.rmtree(temp_dir, ignore_errors=True)
                except TimeoutError:
                    pass

    def _get_temporary_checkpoint_dir(self) -> str:
        """Return the name for the temporary checkpoint dir."""
        tmp_dir_path = tempfile.gettempdir()
        checkpoint_dir_name = _CHECKPOINT_DIR_PREFIX + self._uuid.hex
        if platform.system() == "Windows":
            # Max path on Windows is 260 chars, -1 for joining \
            # Also leave a little for the del lock
            del_lock_name = _get_del_lock_path("")
            checkpoint_dir_name = (
                _CHECKPOINT_DIR_PREFIX
                + self._uuid.hex[
                    -259
                    + len(_CHECKPOINT_DIR_PREFIX)
                    + len(tmp_dir_path)
                    + len(del_lock_name) :
                ]
            )
            if not checkpoint_dir_name.startswith(_CHECKPOINT_DIR_PREFIX):
                raise RuntimeError(
                    "Couldn't create checkpoint directory due to length "
                    "constraints. Try specifing a shorter checkpoint path."
                )
        return os.path.join(tmp_dir_path, checkpoint_dir_name)

    def __fspath__(self):
        raise TypeError(
            "You cannot use `Checkpoint` objects directly as paths. "
            "Use `Checkpoint.to_directory()` or `Checkpoint.as_directory()` instead."
        )

    @Deprecated
    def get_preprocessor(self) -> Optional["Preprocessor"]:
        return self.get_metadata().get("preprocessor")

    @Deprecated
    def set_preprocessor(self, preprocessor: Optional["Preprocessor"]):
        metadata = self.get_metadata()
        metadata["preprocessor"] = preprocessor
        self.set_metadata(metadata)


def _make_dir(path: str, acquire_del_lock: bool = False) -> None:
    """Create the temporary checkpoint dir in ``path``."""
    if acquire_del_lock:
        # Each process drops a deletion lock file it then cleans up.
        # If there are no lock files left, the last process
        # will remove the entire directory.
        del_lock_path = _get_del_lock_path(path)
        open(del_lock_path, "a").close()

    os.makedirs(path, exist_ok=True)


def _get_del_lock_path(path: str, pid: str = None) -> str:
    """Get the path to the deletion lock file."""
    pid = pid if pid is not None else os.getpid()
    return f"{path}.del_lock_{pid}"
