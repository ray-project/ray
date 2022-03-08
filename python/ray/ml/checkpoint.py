import shutil
import tarfile
import tempfile

import os
from typing import Optional

from ray import cloudpickle as pickle
from ray.util.annotations import DeveloperAPI


DICT_CHECKPOINT_FILE_NAME = "dict_checkpoint.pkl"
FS_CHECKPOINT_KEY = "fs_checkpoint"
BYTES_DATA_KEY = "bytes_data"


class Checkpoint:
    """Ray ML Checkpoint.
    This implementation provides interfaces to translate between
    different checkpoint storage locations: Local FS storage, remote
    node FS storage, data object, and cloud storage location.
    The constructor is a private API, instead the ``from_`` methods should
    be used to create checkpoint objects
    (e.g. ``Checkpoint.from_directory()``).
    """

    @DeveloperAPI
    def __init__(
        self,
        local_path: Optional[str] = None,
        data_dict: Optional[dict] = None,
    ):
        # Only one data type can be set at any time
        if local_path:
            assert not data_dict
            if not isinstance(local_path, (str, os.PathLike)) or not os.path.exists(
                local_path
            ):
                raise RuntimeError(
                    f"Cannot create checkpoint from path as it does "
                    f"not exist on local node: {local_path}"
                )
        elif data_dict:
            assert not local_path
            if not isinstance(data_dict, dict):
                raise RuntimeError(
                    f"Cannot create checkpoint from dict as no "
                    f"dict was passed: {data_dict}"
                )
        else:
            raise ValueError("Cannot create checkpoint without data.")

        self._local_path = local_path
        self._data_dict = data_dict

    def __eq__(self, other):
        return isinstance(other, Checkpoint) and self.__dict__ == other.__dict__

    @classmethod
    def from_bytes(cls, data: bytes) -> "Checkpoint":
        """Create Checkpoint object from bytes string.
        Args:
            data (bytes): Data object containing pickled checkpoint data.
                The checkpoint data is assumed to be a pickled dict.
        Returns:
            Checkpoint: checkpoint object.
        """
        bytes_data = pickle.loads(data)
        if isinstance(bytes_data, dict):
            data_dict = bytes_data
        else:
            data_dict = {BYTES_DATA_KEY: bytes_data}
        return cls.from_dict(data_dict)

    def to_bytes(self) -> bytes:
        """Return Checkpoint serialized as bytes object.
        Converts the checkpoint into a dict and returns a pickled
        bytes string of that dict.
        Returns:
            bytes: Bytes object containing checkpoint data.
        """
        # Todo: Add support for stream in the future (to_bytes(file_like))
        data_dict = self.to_dict()
        if "bytes_data" in data_dict:
            return data_dict["bytes_data"]
        return pickle.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict) -> "Checkpoint":
        """Create checkpoint object from dictionary.
        Args:
            data (dict): Dictionary containing checkpoint data.
        Returns:
            Checkpoint: checkpoint object.
        """
        return Checkpoint(data_dict=data)

    def to_dict(self) -> dict:
        """Return checkpoint data as dictionary.
        Returns:
            dict: Dictionary containing checkpoint data.
        """
        if self._data_dict:
            # If the checkpoint data is already a dict, return
            return self._data_dict
        elif self._local_path:
            # Else, checkpoint is either on FS or external storage
            cleanup = False

            local_path = self._local_path

            checkpoint_data_path = os.path.join(local_path, DICT_CHECKPOINT_FILE_NAME)
            if os.path.exists(checkpoint_data_path):
                # If we are restoring a dict checkpoint, load the dict
                # from the checkpoint file.
                with open(checkpoint_data_path, "rb") as f:
                    checkpoint_data = pickle.load(f)
            else:
                data = _pack(local_path)

                checkpoint_data = {
                    FS_CHECKPOINT_KEY: data,
                }

            if cleanup:
                shutil.rmtree(local_path)

            return checkpoint_data
        else:
            raise RuntimeError(f"Empty data for checkpoint {self}")

    @classmethod
    def from_directory(cls, path: str) -> "Checkpoint":
        """Create checkpoint object from directory.
        Args:
            path (str): Directory containing checkpoint data.
        Returns:
            Checkpoint: checkpoint object.
        """
        return Checkpoint(local_path=path)

    def to_directory(self, path: Optional[str] = None) -> str:
        """Write checkpoint data to directory.
        Args:
            path (str): Target directory to restore data in.
        Returns:
            str: Directory containing checkpoint data.
        """
        path = path if path is not None else _temporary_checkpoint_dir()

        os.makedirs(path, exist_ok=True)
        # Drop marker
        open(os.path.join(path, ".is_checkpoint"), "a").close()

        if self._data_dict or self._obj_ref:
            # This is a object ref or dict
            data_dict = self.to_dict()

            if FS_CHECKPOINT_KEY in data_dict:
                # This used to be a true fs checkpoint, so restore
                _unpack(data_dict[FS_CHECKPOINT_KEY], path)
            else:
                # This is a dict checkpoint. Dump data into checkpoint.pkl
                checkpoint_data_path = os.path.join(path,
                                                    DICT_CHECKPOINT_FILE_NAME)
                with open(checkpoint_data_path, "wb") as f:
                    pickle.dump(data_dict, f)
        else:
            # This is either a local fs, remote node fs, or external fs
            local_path = self._local_path
            if local_path:
                if local_path != path:
                    # If this exists on the local path, just copy over
                    if path and os.path.exists(path):
                        shutil.rmtree(path)
                    shutil.copytree(local_path, path)
            else:
                raise RuntimeError(
                    f"No valid location found for checkpoint {self}: {self._uri}"
                )

        return path

def _get_local_path(path: Optional[str]) -> Optional[str]:
    """Check if path is a local path. Otherwise return None."""
    if path is None:
        return None
    if path.startswith("file://"):
        path = path[7:]
    if os.path.exists(path):
        return path
    return None


def _pack(path: str) -> bytes:
    """Pack directory in ``path`` into an archive, return as bytes string."""
    _, tmpfile = tempfile.mkstemp()
    with tarfile.open(tmpfile, "w:gz") as tar:
        tar.add(path, arcname="")

    with open(tmpfile, "rb") as f:
        stream = f.read()

    os.remove(tmpfile)
    return stream

def _temporary_checkpoint_dir() -> str:
    """Create temporary checkpoint dir."""
    return tempfile.mkdtemp(prefix="checkpoint_tmp_", dir=os.getcwd())

def _unpack(stream: bytes, path: str) -> str:
    """Unpack archive in bytes string into directory in ``path``."""
    _, tmpfile = tempfile.mkstemp()

    with open(tmpfile, "wb") as f:
        f.write(stream)

    with tarfile.open(tmpfile) as tar:
        tar.extractall(path)

    os.remove(tmpfile)
    return path