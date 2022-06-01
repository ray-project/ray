import contextlib
import io
import os
import shutil
import tarfile
import tempfile
from typing import Iterator, Optional, Tuple, Union

import ray
from ray import cloudpickle as pickle
from ray.ml.utils.remote_storage import (
    download_from_uri,
    fs_hint,
    is_non_local_path_uri,
    upload_to_uri,
)
from ray.util.annotations import DeveloperAPI, PublicAPI

_DICT_CHECKPOINT_FILE_NAME = "dict_checkpoint.pkl"
_FS_CHECKPOINT_KEY = "fs_checkpoint"
_BYTES_DATA_KEY = "bytes_data"


@PublicAPI
class Checkpoint:
    """Ray ML Checkpoint.

    This implementation provides methods to translate between
    different checkpoint storage locations: Local storage, external storage
    (e.g. cloud storage), and data dict representations.

    The constructor is a private API, instead the ``from_`` methods should
    be used to create checkpoint objects
    (e.g. ``Checkpoint.from_directory()``).

    When converting between different checkpoint formats, it is guaranteed
    that a full round trip of conversions (e.g. directory --> dict -->
    obj ref --> directory) will recover the original checkpoint data.
    There are no guarantees made about compatibility of intermediate
    representations.

    Examples:

        Example for an arbitrary data checkpoint:

        .. code-block:: python

            from ray.ml.checkpoint import Checkpoint

            # Create checkpoint data dict
            checkpoint_data = {"data": 123}

            # Create checkpoint object from data
            checkpoint = Checkpoint.from_dict(checkpoint_data)

            # Save checkpoint to temporary location
            path = checkpoint.to_directory()

            # This path can then be passed around, e.g. to a different function

            # At some other location, recover Checkpoint object from path
            checkpoint = Checkpoint.from_directory(path)

            # Convert into dictionary again
            recovered_data = checkpoint.to_dict()

            # It is guaranteed that the original data has been recovered
            assert recovered_data == checkpoint_data

        Example using MLflow for saving and loading a classifier:

        .. code-block:: python

            from ray.ml.checkpoint import Checkpoint
            from sklearn.ensemble import RandomForestClassifier
            import mlflow.sklearn

            # Create an sklearn classifier
            clf = RandomForestClassifier(max_depth=7, random_state=0)
            # ... e.g. train model with clf.fit()
            # Save model using MLflow
            mlflow.sklearn.save_model(clf, "model_directory")

            # Create checkpoint object from path
            checkpoint = Checkpoint.from_directory("model_directory")

            # Convert into dictionary
            checkpoint_dict = checkpoint.to_dict()

            # This dict can then be passed around, e.g. to a different function

            # At some other location, recover checkpoint object from dict
            checkpoint = Checkpoint.from_dict(checkpoint_dict)

            # Convert into a directory again
            checkpoint.to_directory("other_directory")

            # We can now use MLflow to re-load the model
            clf = mlflow.sklearn.load_model("other_directory")

            # It is guaranteed that the original data was recovered
            assert isinstance(clf, RandomForestClassifier)

        Checkpoints can be pickled and sent to remote processes.
        Please note that checkpoints pointing to local directories will be
        pickled as data representations, so the full checkpoint data will be
        contained in the checkpoint object. If you want to avoid this,
        consider passing only the checkpoint directory to the remote task
        and re-construct your checkpoint object in that function. Note that
        this will only work if the "remote" task is scheduled on the
        same node or a node that also has access to the local data path (e.g.
        on a shared file system like NFS).

        Checkpoints pointing to object store references will keep the
        object reference in tact - this means that these checkpoints cannot
        be properly deserialized on other Ray clusters or outside a Ray
        cluster. If you need persistence across clusters, use the ``to_uri()``
        or ``to_directory()`` methods to persist your checkpoints to disk.

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
            elif not os.path.isdir(local_path):
                raise RuntimeError(
                    f"Cannot create checkpoint from path as it does "
                    f"not point to a directory: {local_path}. If your checkpoint "
                    f"is a single file, consider passing the enclosing directory "
                    f"instead."
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
            resolved = _get_external_path(uri)
            if not resolved:
                raise RuntimeError(
                    f"Cannot create checkpoint from URI as it is not "
                    f"supported: {resolved}"
                )
            uri = resolved
        else:
            raise ValueError("Cannot create checkpoint without data.")

        self._local_path = local_path
        self._data_dict = data_dict
        self._uri = uri
        self._obj_ref = obj_ref

    @classmethod
    def from_bytes(cls, data: bytes) -> "Checkpoint":
        """Create a checkpoint from the given byte string.

        Args:
            data: Data object containing pickled checkpoint data.

        Returns:
            Checkpoint: checkpoint object.
        """
        bytes_data = pickle.loads(data)
        if isinstance(bytes_data, dict):
            data_dict = bytes_data
        else:
            data_dict = {_BYTES_DATA_KEY: bytes_data}
        return cls.from_dict(data_dict)

    def to_bytes(self) -> bytes:
        """Return Checkpoint serialized as bytes object.

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
            data: Dictionary containing checkpoint data.

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
            with self.as_directory() as local_path:
                checkpoint_data_path = os.path.join(
                    local_path, _DICT_CHECKPOINT_FILE_NAME
                )
                if os.path.exists(checkpoint_data_path):
                    # If we are restoring a dict checkpoint, load the dict
                    # from the checkpoint file.
                    with open(checkpoint_data_path, "rb") as f:
                        checkpoint_data = pickle.load(f)
                else:
                    data = _pack(local_path)

                    checkpoint_data = {
                        _FS_CHECKPOINT_KEY: data,
                    }

            return checkpoint_data
        else:
            raise RuntimeError(f"Empty data for checkpoint {self}")

    @classmethod
    def from_object_ref(cls, obj_ref: ray.ObjectRef) -> "Checkpoint":
        """Create checkpoint object from object reference.

        Args:
            obj_ref: ObjectRef pointing to checkpoint data.

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
            path: Directory containing checkpoint data. The caller promises to
                not delete the directory (gifts ownership of the directory to this
                Checkpoint).

        Returns:
            Checkpoint: checkpoint object.
        """
        return Checkpoint(local_path=path)

    def to_directory(self, path: Optional[str] = None, dedup: bool = True) -> str:
        """Write checkpoint data to directory.

        Args:
            path: Target directory to restore data in. If not specified,

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

            if _FS_CHECKPOINT_KEY in data_dict:
                # This used to be a true fs checkpoint, so restore
                _unpack(data_dict[_FS_CHECKPOINT_KEY], path)
            else:
                # This is a dict checkpoint. Dump data into checkpoint.pkl
                checkpoint_data_path = os.path.join(path, _DICT_CHECKPOINT_FILE_NAME)
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
                download_from_uri(uri=external_path, local_path=path)
            else:
                raise RuntimeError(
                    f"No valid location found for checkpoint {self}: {self._uri}"
                )

        return path

    @contextlib.contextmanager
    def as_directory(self) -> Iterator[str]:
        """Return checkpoint directory path in a context.

        This function makes checkpoint data available as a directory while avoiding
        unnecessary copies and left-over temporary data.

        If the checkpoint is already a directory checkpoint, it will return
        the existing path. If it is not, it will create a temporary directory,
        which will be deleted after the context is exited.

        Users should treat the returned checkpoint directory as read-only and avoid
        changing any data within it, as it might get deleted when exiting the context.

        Example:

            with checkpoint.as_directory() as checkpoint_dir:
                # Do some read-only processing of files within checkpoint_dir
                pass

            # At this point, if a temporary directory was created, it will have
            # been deleted.

        """
        if self._local_path:
            yield self._local_path
        else:
            temp_dir = self.to_directory()
            yield temp_dir
            shutil.rmtree(temp_dir, ignore_errors=True)

    @classmethod
    def from_uri(cls, uri: str) -> "Checkpoint":
        """Create checkpoint object from location URI (e.g. cloud storage).

        Valid locations currently include AWS S3 (``s3://``),
        Google cloud storage (``gs://``), HDFS (``hdfs://``), and
        local files (``file://``).

        Args:
            uri: Source location URI to read data from.

        Returns:
            Checkpoint: checkpoint object.
        """
        return Checkpoint(uri=uri)

    def to_uri(self, uri: str) -> str:
        """Write checkpoint data to location URI (e.g. cloud storage).

        Args:
            uri: Target location URI to write data to.

        Returns:
            str: Cloud location containing checkpoint data.
        """
        if uri.startswith("file://"):
            local_path = uri[7:]
            return self.to_directory(local_path)

        if not is_non_local_path_uri(uri):
            raise RuntimeError(
                f"Cannot upload checkpoint to URI: Provided URI "
                f"does not belong to a registered storage provider: `{uri}`. "
                f"Hint: {fs_hint(uri)}"
            )

        with self.as_directory() as local_path:
            upload_to_uri(local_path=local_path, uri=uri)

        return uri

    @DeveloperAPI
    def get_internal_representation(
        self,
    ) -> Tuple[str, Union[dict, str, ray.ObjectRef]]:
        """Return tuple of (type, data) for the internal representation.

        The internal representation can be used e.g. to compare checkpoint
        objects for equality or to access the underlying data storage.

        The returned type is a string and one of
        ``["local_path", "data_dict", "uri", "object_ref"]``.

        The data is the respective data value.

        Note that paths converted from ``file://...`` will be returned
        as ``local_path`` (without the ``file://`` prefix) and not as ``uri``.

        Returns:
            Tuple of type and data.
        """
        if self._local_path:
            return "local_path", self._local_path
        elif self._data_dict:
            return "data_dict", self._data_dict
        elif self._uri:
            return "uri", self._uri
        elif self._obj_ref:
            return "object_ref", self._obj_ref
        else:
            raise RuntimeError(
                "Cannot get internal representation of empty checkpoint."
            )

    def __getstate__(self):
        if self._local_path:
            blob = self.to_bytes()
            return Checkpoint.from_bytes(blob).__getstate__()
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)


def _get_local_path(path: Optional[str]) -> Optional[str]:
    """Check if path is a local path. Otherwise return None."""
    if path is None or is_non_local_path_uri(path):
        return None
    if path.startswith("file://"):
        path = path[7:]
    if os.path.exists(path):
        return path
    return None


def _get_external_path(path: Optional[str]) -> Optional[str]:
    """Check if path is an external path. Otherwise return None."""
    if not isinstance(path, str) or not is_non_local_path_uri(path):
        return None
    return path


def _temporary_checkpoint_dir() -> str:
    """Create temporary checkpoint dir."""
    return tempfile.mkdtemp(prefix="checkpoint_tmp_")


def _pack(path: str) -> bytes:
    """Pack directory in ``path`` into an archive, return as bytes string."""
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.PAX_FORMAT) as tar:
        tar.add(path, arcname="")

    return stream.getvalue()


def _unpack(stream: bytes, path: str) -> str:
    """Unpack archive in bytes string into directory in ``path``."""
    with tarfile.open(fileobj=io.BytesIO(stream)) as tar:
        tar.extractall(path)
    return path
