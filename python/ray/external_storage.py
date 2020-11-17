import abc
import os
import re
from collections import namedtuple
from typing import List

import ray
from ray.ray_constants import DEFAULT_OBJECT_PREFIX


class ExternalStorageURLProtocol:
    """Class to parse/generate urls for object spilling.
    
    There will be 2 types of URLs.
    1. URL used internally. These urls are stored in GCS.
    2. URL used for external storage.

    This class is used as a translator layer between 1 and 2.
    """

    def __init__(self, prefix: str):
        self._prefix = prefix
        # NOTE(sang): To optimize the performance, Ray fusions small
        # objects when it stored them. object_id_stored is
        # an object_id that is used as a filename.
        # (job_id)-(object_id_stored)-(object_id)
        # -(object_size_in_bytes)-(offsets_in_bytes)
        self.internal_url_pattern = re.compile(
            "([0-9a-f]{8})-([0-9a-f]{40})-([0-9a-f]{40})-(\d+)-(\d+)")
        # (prefix)-(object_id_stored)
        self.external_url_pattern = re.compile(
            "([0-9a-zA-Z_]+)-([0-9a-f]{40})")

    def produce_external_url_from_object_ref(self, object_ref):
        external_url = f"{self._prefix}-{object_ref.hex()}"
        match = self.external_url_pattern.match(external_url)
        self._check_external_url_match(match, external_url)
        return external_url

    def produce_external_url(self, internal_url: str):
        """internal_url -> external_url"""
        match = self.internal_url_pattern.match(internal_url)
        self._check_internal_url_match(match, internal_url)
        object_id_stored = match.group(2)
        external_url = f"{self._prefix}-{object_id_stored}"
        match = self.external_url_pattern.match(external_url)
        self._check_external_url_match(match, external_url)
        return external_url

    def create_internal_url(self, *, job_id: str, object_id_stored: str,
                            object_id: str, object_size_in_bytes: int,
                            offsets_in_bytes: int):
        """Create an internal url from a given input.
        
        Follow the self.internal_url_pattern protocol.

        Args:
            job_id(str): JobID in hex string.
            object_id_stored(str): Object ID in hex that is used
                as a key to store this object.
            object_id(str): Object ID in hex.
            object_size_in_bytes(int): The size of objects in bytes.
            offsets_in_bytes(int): The offsets from the beginning
                of the file in bytes
        """
        internal_url = (
            f"{job_id}-{object_id_stored}-{object_id}"
            f"-{str(object_size_in_bytes)}-{str(offsets_in_bytes)}")
        match = self.internal_url_pattern.match(internal_url)
        self._check_internal_url_match(match, internal_url)
        return internal_url

    def get_url_info(self, internal_url) -> namedtuple:
        match = self.internal_url_pattern.match(internal_url)
        self._check_internal_url_match(match, internal_url)
        URLInfo = namedtuple(
            "URLInfo", "job_id object_id_stored "
            "object_id object_size_in_bytes "
            "offsets_in_bytes")
        return URLInfo(
            job_id=match.group(1),
            object_id_stored=match.group(2),
            object_id=match.group(3),
            object_size_in_bytes=int(match.group(4)),
            offsets_in_bytes=int(match.group(5)))

    def _check_internal_url_match(self, match, internal_url):
        assert match, (f"Given external url, {internal_url}, doesn't "
                       "follow the protocol. Please follow this pattern: "
                       f"{self.internal_url_pattern}")
    def _check_external_url_match(self, match, external_url):
        assert match, (f"Given external url, {external_url}, doesn't "
                       "follow the protocol. Please follow this pattern: "
                       f"{self.external_url_pattern}")


class ExternalStorage(metaclass=abc.ABCMeta):
    """The base class for external storage.

    This class provides some useful functions for zero-copy object
    put/get from plasma store. Also it specifies the interface for
    object spilling.

    When inheriting this class, please make sure to implement validation
    logic inside __init__ method. When ray instance starts, it will
    instantiating external storage to validate the config.

    Raises:
        ValueError: when given configuration for
            the external storage is invalid.
    """

    def _get_objects_from_store(self, object_refs):
        worker = ray.worker.global_worker
        ray_object_pairs = worker.core_worker.get_objects(
            object_refs,
            worker.current_task_id,
            timeout_ms=0,
            plasma_objects_only=True)
        return ray_object_pairs

    def _put_object_to_store(self, metadata, data_size, file_like, object_ref):
        worker = ray.worker.global_worker
        worker.core_worker.put_file_like_object(metadata, data_size, file_like,
                                                object_ref)

    def _fusion_objects(
            self,
            f,
            object_refs,
            object_id_stored,
            url_protocol) -> List[str]:
        """Fusion all objects into a given file handle/return internal urls.
        
        Args:
            f: File handle to fusion all given object refs.
            object_refs: Object references to fusion to a single file.
            object_id_stored: The ID that will be used to store the file f.
            url_protocol: instance of ExternalStorageURLProtocol. 
        
        Return:
            List of internal urls of fusioned objects.
        """
        keys = []
        offsets = 0
        ray_object_pairs = self._get_objects_from_store(object_refs)
        for ref, (buf, metadata) in zip(object_refs, ray_object_pairs):
            metadata_len = len(metadata)
            buf_len = len(buf)
            object_size_in_bytes = metadata_len + buf_len + 16
            internal_url = url_protocol.create_internal_url(
                job_id=ref.job_id().hex(),
                object_id_stored=object_id_stored,
                object_id=ref.hex(),
                object_size_in_bytes=object_size_in_bytes,
                offsets_in_bytes=offsets)
            f.write(metadata_len.to_bytes(8, byteorder="little"))
            f.write(buf_len.to_bytes(8, byteorder="little"))
            f.write(metadata)
            f.write(memoryview(buf))
            offsets += object_size_in_bytes
            keys.append(internal_url.encode())
        return keys

    @abc.abstractmethod
    def spill_objects(self, object_refs) -> List[str]:
        """Spill objects to the external storage. Objects are specified
        by their object refs.

        Args:
            object_refs: The list of the refs of the objects to be spilled.
        Returns:
            A list of internal URLs following ExternalStorageURLProtocol.
        """

    @abc.abstractmethod
    def restore_spilled_objects(self, keys: List[str]):
        """Spill objects to the external storage. Objects are specified
        by their object refs.

        Args:
            keys: A list of internal URL following ExternalStorageURLProtocol.
        """


class NullStorage(ExternalStorage):
    """The class that represents an uninitialized external storage."""

    def spill_objects(self, object_refs) -> List[str]:
        raise NotImplementedError("External storage is not initialized")

    def restore_spilled_objects(self, keys):
        raise NotImplementedError("External storage is not initialized")


class FileSystemStorage(ExternalStorage):
    """The class for filesystem-like external storage.

    Raises:
        ValueError: Raises directory path to
            spill objects doesn't exist.
    """

    def __init__(self, directory_path):
        self.directory_path = directory_path
        self.prefix = DEFAULT_OBJECT_PREFIX
        os.makedirs(self.directory_path, exist_ok=True)
        if not os.path.exists(self.directory_path):
            raise ValueError("The given directory path to store objects, "
                             f"{self.directory_path}, could not be created.")
        self.url_protocol = ExternalStorageURLProtocol(self.prefix)

    def spill_objects(self, object_refs) -> List[str]:
        if len(object_refs) == 0:
            return []
        keys = []
        # Always use the first object ref as a key when fusioning objects.
        first_ref = object_refs[0]
        object_id_stored = first_ref.hex()
        filename = self.url_protocol.produce_external_url_from_object_ref(
            first_ref)
        offsets = 0
        with open(os.path.join(self.directory_path, filename), "wb") as f:
            return self._fusion_objects(
                f,
                object_refs,
                object_id_stored,
                self.url_protocol)

    def restore_spilled_objects(self, keys):
        for k in keys:
            internal_url = k.decode()
            url_info = self.url_protocol.get_url_info(internal_url)
            filename = self.url_protocol.produce_external_url(internal_url)
            ref = ray.ObjectRef(bytes.fromhex(url_info.object_id))
            with open(os.path.join(self.directory_path, filename), "rb") as f:
                f.seek(url_info.offsets_in_bytes)
                metadata_len = int.from_bytes(f.read(8), byteorder="little")
                buf_len = int.from_bytes(f.read(8), byteorder="little")
                metadata = f.read(metadata_len)
                # read remaining data to our buffer
                self._put_object_to_store(metadata, buf_len, f, ref)


class ExternalStorageSmartOpenImpl(ExternalStorage):
    """The external storage class implemented by smart_open.
    (https://github.com/RaRe-Technologies/smart_open)

    Smart open supports multiple backend with the same APIs.

    To use this implementation, you should pre-create the given uri.
    For example, if your uri is a local file path, you should pre-create
    the directory.

    Args:
        uri(str): Storage URI used for smart open.
        prefix(str): Prefix of objects that are stored.
        override_transport_params(dict): Overriding the default value of
            transport_params for smart-open library.

    Raises:
        ModuleNotFoundError: If it fails to setup.
            For example, if smart open library
            is not downloaded, this will fail.
    """

    def __init__(self,
                 uri: str,
                 prefix: str = DEFAULT_OBJECT_PREFIX,
                 override_transport_params: dict = None):
        try:
            from smart_open import open  # noqa
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(
                "Smart open is chosen to be a object spilling "
                "external storage, but smart_open "
                f"is not downloaded. Original error: {e}")

        self.uri = uri.strip("/")
        self.prefix = prefix
        self.override_transport_params = override_transport_params or {}
        self.transport_params = {}.update(self.override_transport_params)
        self.url_protocol = ExternalStorageURLProtocol(self.prefix)

    def spill_objects(self, object_refs) -> List[str]:
        if len(object_refs) == 0:
            return []
        from smart_open import open
        # Always use the first object ref as a key when fusioning objects.
        first_ref = object_refs[0]
        object_id_stored = first_ref.hex()
        external_url = self.url_protocol.produce_external_url_from_object_ref(
            first_ref)
        offsets = 0
        from time import perf_counter
        start  = perf_counter()
        with open(
                self._build_uri(external_url), "wb",
                transport_params=self.transport_params) as file_like:
            keys = self._fusion_objects(
                file_like,
                object_refs,
                object_id_stored,
                self.url_protocol)
            end = perf_counter()
            print("sang ", end - start)
            return keys

    def restore_spilled_objects(self, keys):
        from smart_open import open
        for k in keys:
            internal_url = k.decode()
            url_info = self.url_protocol.get_url_info(internal_url)
            external_url = self.url_protocol.produce_external_url(internal_url)
            ref = ray.ObjectRef(bytes.fromhex(url_info.object_id))
            first_bytes = url_info.offsets_in_bytes
            end_bytes = url_info.offsets_in_bytes + url_info.object_size_in_bytes
            with open(
                    self._build_uri(external_url), "wb",
                    transport_params=self.transport_params) as f:
                metadata_len = int.from_bytes(f.read(8), byteorder="little")
                buf_len = int.from_bytes(f.read(8), byteorder="little")
                metadata = f.read(metadata_len)
                # read remaining data to our buffer
                self._put_object_to_store(metadata, buf_len, f, ref)

    def _get_restore_transport_param(self, first_bytes, end_bytes):
        return self.transport_params.copy().update({
            "object_kwargs": {
                "Range": f"bytes={first_bytes}-{end_bytes}"
            }
        })

    def _build_uri(self, key):
        return f"{self.uri}/{key}"


_external_storage = NullStorage()


def setup_external_storage(config):
    """Setup the external storage according to the config."""
    global _external_storage
    if config:
        storage_type = config["type"]
        if storage_type == "filesystem":
            _external_storage = FileSystemStorage(**config["params"])
        elif storage_type == "smart_open":
            _external_storage = ExternalStorageSmartOpenImpl(
                **config["params"])
        else:
            raise ValueError(f"Unknown external storage type: {storage_type}")
    else:
        _external_storage = NullStorage()


def reset_external_storage():
    global _external_storage
    _external_storage = NullStorage()


def spill_objects(object_refs):
    """Spill objects to the external storage. Objects are specified
    by their object refs.

    Args:
        object_refs: The list of the refs of the objects to be spilled.
    Returns:
        A list of keys corresponding to the input object refs.
    """
    return _external_storage.spill_objects(object_refs)


def restore_spilled_objects(keys: List[bytes]):
    """Spill objects to the external storage. Objects are specified
    by their object refs.

    Args:
        keys: A list of bytes corresponding to the spilled objects.
    """
    _external_storage.restore_spilled_objects(keys)
