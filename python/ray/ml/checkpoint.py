import json
import shutil
import tarfile
import tempfile

import cloudpickle as pickle
import os
from typing import Any, Optional, Dict

import ray
from ray.util.ml_utils.cloud import (
    upload_to_bucket,
    is_cloud_target,
    download_from_bucket,
)


DICT_CHECKPOINT_FILE_NAME = "dict_checkpoint.pkl"


class Checkpoint:
    """Ray ML Checkpoint.

    This implementation provides interfaces to translate between
    different checkpoint storage locations: Local FS storage, remote
    node FS storage, data object, and cloud storage location.
    """

    def __init__(self, data: Any, metadata: Optional[Dict] = None):
        self.data = data
        self.metadata = metadata or {}

    def __eq__(self, other):
        return (
            isinstance(other, Checkpoint)
            and self.data == other.data
            and self.metadata == other.metadata
        )

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
            data_dict = {"data": bytes_data}
        return cls.from_dict(data_dict)

    def to_bytes(self) -> bytes:
        """Return Checkpoint serialized as bytes object.

        Will convert the checkpoint into a dict and return a pickled
        bytes string.

        Returns:
            bytes: Bytes object containing checkpoint data.
        """
        # Todo: Add support for stream in the future (to_bytes(file_like))
        return pickle.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict) -> "Checkpoint":
        """Create checkpoint object from dictionary.

        Args:
            data (dict): Dictionary containing checkpoint data.

        Returns:
            Checkpoint: checkpoint object.
        """
        metadata = data.get("metadata", {})
        return Checkpoint(data=data, metadata=metadata)

    def to_dict(self) -> dict:
        """Return checkpoint data as dictionary.

        Returns:
            dict: Dictionary containing checkpoint data.
        """
        if isinstance(self.data, dict):
            # If the checkpoint data is already a dict, return
            return self.data
        elif isinstance(self.data, ray.ObjectRef):
            # If the checkpoint data is an object reference, resolve
            return ray.get(self.data)
        elif isinstance(self.data, str):
            # Else, checkpoint is either on FS or external storage
            cleanup = False

            load_path = _get_local_path(self.data)
            if not load_path:
                # Checkpoint does not exist on local path. Save
                # in temporary directory, but clean up later
                load_path = self.to_directory()
                cleanup = True

            metadata = _load_metadata(load_path)

            checkpoint_data_path = os.path.join(load_path, DICT_CHECKPOINT_FILE_NAME)
            if os.path.exists(checkpoint_data_path):
                # If we are restoring a dict checkpoint, load the dict
                # from the checkpoint file.
                with open(checkpoint_data_path, "rb") as f:
                    checkpoint_data = pickle.load(f)
            else:
                # Else, we have a true FS checkpoint.
                # Serialize directory into data blob.
                data = _pack(load_path)

                checkpoint_data = {
                    "fs_checkpoint": data,
                    "metadata": metadata,
                }

            if cleanup:
                shutil.rmtree(load_path)

            return checkpoint_data
        else:
            raise RuntimeError(f"Invalid data type for checkpoint {self}: {self.data}")

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
        return Checkpoint(data=obj_ref, metadata=None)

    def to_object_ref(self) -> ray.ObjectRef:
        """Return checkpoint data as object reference.

        Returns:
            ray.ObjectRef: ObjectRef pointing to checkpoint data.
        """
        if isinstance(self.data, ray.ObjectRef):
            return self.data
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
        if not os.path.exists(path):
            raise RuntimeError(
                f"Cannot create checkpoint from directory, because path does "
                f"not exist on local node: {path}"
            )

        metadata = _load_metadata(path)

        local_checkpoint = Checkpoint(data=path, metadata=metadata)
        return local_checkpoint

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

        if isinstance(self.data, (ray.ObjectRef, dict)):
            # This is a object ref or dict
            data_dict = self.to_dict()

            if "fs_checkpoint" in data_dict:
                # This used to be a true fs checkpoint, so restore
                _unpack(data_dict["fs_checkpoint"], path)
                _write_metadata(path, self.metadata)
            else:
                # This is a dict checkpoint. Dump data into checkpoint.pkl
                checkpoint_data_path = os.path.join(path, DICT_CHECKPOINT_FILE_NAME)
                with open(checkpoint_data_path, "wb") as f:
                    pickle.dump(data_dict, f)
                _write_metadata(path, self.metadata)
        else:
            # This is either a local fs, remote node fs, or external fs
            local_path = _get_local_path(self.data)
            external_path = _get_external_path(self.data)
            if local_path:
                # If this exists on the local path, just copy over
                if path and os.path.exists(path):
                    shutil.rmtree(path)
                shutil.copytree(local_path, path)
            elif external_path:
                # If this exists on external storage (e.g. cloud), download
                download_from_bucket(bucket=external_path, local_path=path)
            else:
                raise RuntimeError(
                    f"No valid location found for checkpoint {self}: " f"{self.data}"
                )

        return path

    @classmethod
    def from_uri(cls, location: str) -> "Checkpoint":
        """Create checkpoint object from location URI (e.g. cloud storage).

        Args:
            location (str): Source location URI to read data from.

        Returns:
            Checkpoint: checkpoint object.
        """
        local_path = _get_local_path(location)
        if local_path:
            return Checkpoint.from_directory(local_path)
        return Checkpoint(data=location, metadata=None)

    def to_uri(self, location: str) -> str:
        """Write checkpoint data to location URI (e.g. cloud storage).

        ARgs:
            location (str): Target location URI to write data to.

        Returns:
            str: Cloud location containing checkpoint data.
        """
        if location.startswith("file://"):
            local_path = location[7:]
            return self.to_directory(local_path)

        assert is_cloud_target(location)

        cleanup = False
        local_path = None

        if isinstance(self.data, str):
            local_path = _get_local_path(self.data)

        if not local_path:
            cleanup = True
            local_path = self.to_directory()

        upload_to_bucket(bucket=location, local_path=local_path)

        if cleanup:
            shutil.rmtree(local_path)

        return location


def _get_local_path(path: str) -> Optional[str]:
    if is_cloud_target(path):
        return None
    if path.startswith("file://"):
        path = path[7:]
    if os.path.exists(path):
        return path
    return None


def _get_external_path(path: str) -> Optional[str]:
    if not is_cloud_target(path):
        return None
    return path


def _write_metadata(path: str, metadata: Dict[str, Any]) -> None:
    metadata_file = os.path.join(path, ".checkpoint_metadata")
    with open(metadata_file, "wt") as fp:
        json.dump(metadata, fp)


def _load_metadata(path: str) -> Dict[str, Any]:
    metadata_file = os.path.join(path, ".checkpoint_metadata")
    if os.path.exists(metadata_file):
        with open(metadata_file, "rt") as fp:
            return json.load(fp)
    return {}


def _temporary_checkpoint_dir() -> str:
    return tempfile.mkdtemp(prefix="checkpoint_tmp_", dir=os.getcwd())


def _pack(path: str) -> bytes:
    _, tmpfile = tempfile.mkstemp()
    with tarfile.open(tmpfile, "w:gz") as tar:
        tar.add(path, arcname="")

    with open(tmpfile, "rb") as f:
        stream = f.read()

    os.remove(tmpfile)
    return stream


def _unpack(stream: bytes, path: str) -> str:
    _, tmpfile = tempfile.mkstemp()

    with open(tmpfile, "wb") as f:
        f.write(stream)

    with tarfile.open(tmpfile) as tar:
        tar.extractall(path)

    os.remove(tmpfile)
    return path
