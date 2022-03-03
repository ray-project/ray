import shutil
import tarfile
import tempfile

import cloudpickle as pickle
import os
from typing import Optional

import ray
from ray.util.annotations import DeveloperAPI
from ray.util.ml_utils.cloud import (
    upload_to_bucket,
    is_cloud_target,
    download_from_bucket,
)


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
        uri: Optional[str] = None,
        obj_ref: Optional[ray.ObjectRef] = None,
    ):
        # First, resolve file:// URIs to local paths
        if uri:
            local_path = _get_local_path(uri)
            if local_path:
                uri = None

        # Only one data type can be set at any time
        if local_path:
            assert not data_dict and not uri and not obj_ref
            if not isinstance(local_path, (str, os.PathLike)) or not os.path.exists(
                local_path
            ):
                raise RuntimeError(
                    f"Cannot create checkpoint from path as it does "
                    f"not exist on local node: {local_path}"
                )
        elif data_dict:
            assert not local_path and not uri and not obj_ref
            if not isinstance(data_dict, dict):
                raise RuntimeError(
                    f"Cannot create checkpoint from dict as no "
                    f"dict was passed: {data_dict}"
                )
        elif obj_ref:
            assert not local_path and not data_dict and not uri
            if not isinstance(obj_ref, ray.ObjectRef):
                raise RuntimeError(
                    f"Cannot create checkpoint from object ref as no "
                    f"object ref was passed: {obj_ref}"
                )
        elif uri:
            assert not local_path and not data_dict and not obj_ref
            uri = _get_external_path(uri)
            if not uri:
                raise RuntimeError(
                    f"Cannot create checkpoint from URI as it is not "
                    f"supported: {uri}"
                )
        else:
            raise ValueError("Cannot create checkpoint without data.")

        self._local_path = local_path
        self._data_dict = data_dict
        self._uri = uri
        self._obj_ref = obj_ref

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
        elif self._obj_ref:
            # If the checkpoint data is an object reference, resolve
            return ray.get(self._obj_ref)
        elif self._local_path or self._uri:
            # Else, checkpoint is either on FS or external storage
            cleanup = False

            local_path = self._local_path
            if not local_path:
                # Checkpoint does not exist on local path. Save
                # in temporary directory, but clean up later
                local_path = self.to_directory()
                cleanup = True

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
    def from_object_ref(cls, obj_ref: ray.ObjectRef) -> "Checkpoint":
        """Create checkpoint object from object reference.

        The object reference is assumed to point to a dictionary containing
        the checkpoint data.

        Args:
            obj_ref (ray.ObjectRef): ObjectRef pointing to checkpoint data.

        Returns:
            Checkpoint: checkpoint object.
        """
        return Checkpoint(obj_ref=obj_ref)

    def to_object_ref(self) -> ray.ObjectRef:
        """Return checkpoint data as object reference.

        Returns:
            ray.ObjectRef: ObjectRef pointing to checkpoint data.
        """
        if self._obj_ref:
            return self._obj_ref
        else:
            return ray.put(self.to_dict())

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
                checkpoint_data_path = os.path.join(path, DICT_CHECKPOINT_FILE_NAME)
                with open(checkpoint_data_path, "wb") as f:
                    pickle.dump(data_dict, f)
        else:
            # This is either a local fs, remote node fs, or external fs
            local_path = self._local_path
            external_path = _get_external_path(self._uri)
            if local_path:
                if local_path != path:
                    # If this exists on the local path, just copy over
                    if path and os.path.exists(path):
                        shutil.rmtree(path)
                    shutil.copytree(local_path, path)
            elif external_path:
                # If this exists on external storage (e.g. cloud), download
                download_from_bucket(bucket=external_path, local_path=path)
            else:
                raise RuntimeError(
                    f"No valid location found for checkpoint {self}: {self._uri}"
                )

        return path

    @classmethod
    def from_uri(cls, uri: str) -> "Checkpoint":
        """Create checkpoint object from location URI (e.g. cloud storage).

        Valid locations currently include AWS S3 (``s3://``),
        Google cloud storage (``gs://``), HDFS (``hdfs://``), and
        local files (``file://``).

        Args:
            uri (str): Source location URI to read data from.

        Returns:
            Checkpoint: checkpoint object.
        """
        return Checkpoint(uri=uri)

    def to_uri(self, uri: str) -> str:
        """Write checkpoint data to location URI (e.g. cloud storage).

        ARgs:
            uri (str): Target location URI to write data to.

        Returns:
            str: Cloud location containing checkpoint data.
        """
        if uri.startswith("file://"):
            local_path = uri[7:]
            return self.to_directory(local_path)

        assert is_cloud_target(uri)

        cleanup = False

        local_path = self._local_path
        if not local_path:
            cleanup = True
            local_path = self.to_directory()

        upload_to_bucket(bucket=uri, local_path=local_path)

        if cleanup:
            shutil.rmtree(local_path)

        return uri


def _get_local_path(path: Optional[str]) -> Optional[str]:
    """Check if path is a local path. Otherwise return None."""
    if path is None or is_cloud_target(path):
        return None
    if path.startswith("file://"):
        path = path[7:]
    if os.path.exists(path):
        return path
    return None


def _get_external_path(path: Optional[str]) -> Optional[str]:
    """Check if path is an external path. Otherwise return None."""
    if not isinstance(path, str) or not is_cloud_target(path):
        return None
    return path


def _temporary_checkpoint_dir() -> str:
    """Create temporary checkpoint dir."""
    return tempfile.mkdtemp(prefix="checkpoint_tmp_", dir=os.getcwd())


def _pack(path: str) -> bytes:
    """Pack directory in ``path`` into an archive, return as bytes string."""
    _, tmpfile = tempfile.mkstemp()
    with tarfile.open(tmpfile, "w:gz") as tar:
        tar.add(path, arcname="")

    with open(tmpfile, "rb") as f:
        stream = f.read()

    os.remove(tmpfile)
    return stream


def _unpack(stream: bytes, path: str) -> str:
    """Unpack archive in bytes string into directory in ``path``."""
    _, tmpfile = tempfile.mkstemp()

    with open(tmpfile, "wb") as f:
        f.write(stream)

    with tarfile.open(tmpfile) as tar:
        tar.extractall(path)

    os.remove(tmpfile)
    return path
