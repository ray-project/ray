import abc
import logging
import os
import shutil
import urllib
from collections import namedtuple
from typing import List, IO, Tuple

import ray
from ray.ray_constants import DEFAULT_OBJECT_PREFIX
from ray._raylet import ObjectRef

ParsedURL = namedtuple("ParsedURL", "base_url, offset, size")
logger = logging.getLogger(__name__)


def create_url_with_offset(*, url: str, offset: int, size: int) -> str:
    """Methods to create a URL with offset.

    When ray spills objects, it fuses multiple objects
    into one file to optimize the performance. That says, each object
    needs to keep tracking of its own special url to store metadata.

    This method creates an url_with_offset, which is used internally
    by Ray.

    Created url_with_offset can be passed to the self._get_base_url method
    to parse the filename used to store files.

    Example) file://path/to/file?offset=""&size=""

    Args:
        url(str): url to the object stored in the external storage.
        offset(int): Offset from the beginning of the file to
            the first bytes of this object.
        size(int): Size of the object that is stored in the url.
            It is used to calculate the last offset.

    Returns:
        url_with_offset stored internally to find
        objects from external storage.
    """
    return f"{url}?offset={offset}&size={size}"


def parse_url_with_offset(url_with_offset: str) -> Tuple[str, int, int]:
    """Parse url_with_offset to retrieve information.

    base_url is the url where the object ref
    is stored in the external storage.

    Args:
        url_with_offset(str): url created by create_url_with_offset.

    Returns:
        named tuple of base_url, offset, and size.
    """
    parsed_result = urllib.parse.urlparse(url_with_offset)
    query_dict = urllib.parse.parse_qs(parsed_result.query)
    # Split by ? to remove the query from the url.
    base_url = parsed_result.geturl().split("?")[0]
    offset = int(query_dict["offset"][0])
    size = int(query_dict["size"][0])
    return ParsedURL(base_url=base_url, offset=offset, size=size)


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

    HEADER_LENGTH = 24

    def _get_objects_from_store(self, object_refs):
        worker = ray.worker.global_worker
        # Since the object should always exist in the plasma store before
        # spilling, it can directly get the object from the local plasma
        # store.
        # issue: https://github.com/ray-project/ray/pull/13831
        ray_object_pairs = worker.core_worker.get_if_local(object_refs)
        return ray_object_pairs

    def _put_object_to_store(self, metadata, data_size, file_like, object_ref,
                             owner_address):
        worker = ray.worker.global_worker
        worker.core_worker.put_file_like_object(metadata, data_size, file_like,
                                                object_ref, owner_address)

    def _write_multiple_objects(self, f: IO, object_refs: List[ObjectRef],
                                owner_addresses: List[str],
                                url: str) -> List[str]:
        """Fuse all given objects into a given file handle.

        Args:
            f(IO): File handle to fusion all given object refs.
            object_refs(list): Object references to fusion to a single file.
            owner_addresses(list): Owner addresses for the provided objects.
            url(str): url where the object ref is stored
                in the external storage.

        Return:
            List of urls_with_offset of fusioned objects.
            The order of returned keys are equivalent to the one
            with given object_refs.
        """
        keys = []
        offset = 0
        ray_object_pairs = self._get_objects_from_store(object_refs)
        for ref, (buf, metadata), owner_address in zip(
                object_refs, ray_object_pairs, owner_addresses):
            address_len = len(owner_address)
            metadata_len = len(metadata)
            buf_len = len(buf)
            # 24 bytes to store owner address, metadata, and buffer lengths.
            data_size_in_bytes = (
                address_len + metadata_len + buf_len + self.HEADER_LENGTH)
            f.write(address_len.to_bytes(8, byteorder="little"))
            f.write(metadata_len.to_bytes(8, byteorder="little"))
            f.write(buf_len.to_bytes(8, byteorder="little"))
            f.write(owner_address)
            f.write(metadata)
            f.write(memoryview(buf))
            url_with_offset = create_url_with_offset(
                url=url, offset=offset, size=data_size_in_bytes)
            keys.append(url_with_offset.encode())
            offset += data_size_in_bytes
        return keys

    def _size_check(self, address_len, metadata_len, buffer_len,
                    obtained_data_size):
        """Check whether or not the obtained_data_size is as expected.

        Args:
             metadata_len(int): Actual metadata length of the object.
             buffer_len(int): Actual buffer length of the object.
             obtained_data_size(int): Data size specified in the
                url_with_offset.

        Raises:
            ValueError if obtained_data_size is different from
            address_len + metadata_len + buffer_len +
            24 (first 8 bytes to store length).
        """
        data_size_in_bytes = (
            address_len + metadata_len + buffer_len + self.HEADER_LENGTH)
        if data_size_in_bytes != obtained_data_size:
            raise ValueError(
                f"Obtained data has a size of {data_size_in_bytes}, "
                "although it is supposed to have the "
                f"size of {obtained_data_size}.")

    @abc.abstractmethod
    def spill_objects(self, object_refs, owner_addresses) -> List[str]:
        """Spill objects to the external storage. Objects are specified
        by their object refs.

        Args:
            object_refs: The list of the refs of the objects to be spilled.
        Returns:
            A list of internal URLs with object offset.
        """

    @abc.abstractmethod
    def restore_spilled_objects(self, object_refs: List[ObjectRef],
                                url_with_offset_list: List[str]) -> int:
        """Restore objects from the external storage.

        Args:
            object_refs: List of object IDs (note that it is not ref).
            url_with_offset_list: List of url_with_offset.

        Returns:
            The total number of bytes restored.
        """

    @abc.abstractmethod
    def delete_spilled_objects(self, urls: List[str]):
        """Delete objects that are spilled to the external storage.

        Args:
            urls: URLs that store spilled object files.
        """

    @abc.abstractmethod
    def destroy_external_storage(self):
        """Destroy external storage when a head node is down.

        NOTE: This is currently working when the cluster is
        started by ray.init
        """


class NullStorage(ExternalStorage):
    """The class that represents an uninitialized external storage."""

    def spill_objects(self, object_refs, owner_addresses) -> List[str]:
        raise NotImplementedError("External storage is not initialized")

    def restore_spilled_objects(self, object_refs, url_with_offset_list):
        raise NotImplementedError("External storage is not initialized")

    def delete_spilled_objects(self, urls: List[str]):
        raise NotImplementedError("External storage is not initialized")

    def destroy_external_storage(self):
        raise NotImplementedError("External storage is not initialized")


class FileSystemStorage(ExternalStorage):
    """The class for filesystem-like external storage.

    Raises:
        ValueError: Raises directory path to
            spill objects doesn't exist.
    """

    def __init__(self, directory_path):
        self.spill_dir_name = DEFAULT_OBJECT_PREFIX
        self.directory_path = os.path.join(directory_path, self.spill_dir_name)
        os.makedirs(self.directory_path, exist_ok=True)
        if not os.path.exists(self.directory_path):
            raise ValueError("The given directory path to store objects, "
                             f"{self.directory_path}, could not be created.")

    def spill_objects(self, object_refs, owner_addresses) -> List[str]:
        if len(object_refs) == 0:
            return []
        # Always use the first object ref as a key when fusioning objects.
        first_ref = object_refs[0]
        filename = f"{first_ref.hex()}-multi-{len(object_refs)}"
        url = f"{os.path.join(self.directory_path, filename)}"
        with open(url, "wb") as f:
            return self._write_multiple_objects(f, object_refs,
                                                owner_addresses, url)

    def restore_spilled_objects(self, object_refs: List[ObjectRef],
                                url_with_offset_list: List[str]):
        total = 0
        for i in range(len(object_refs)):
            object_ref = object_refs[i]
            url_with_offset = url_with_offset_list[i].decode()
            # Retrieve the information needed.
            parsed_result = parse_url_with_offset(url_with_offset)
            base_url = parsed_result.base_url
            offset = parsed_result.offset
            # Read a part of the file and recover the object.
            with open(base_url, "rb") as f:
                f.seek(offset)
                address_len = int.from_bytes(f.read(8), byteorder="little")
                metadata_len = int.from_bytes(f.read(8), byteorder="little")
                buf_len = int.from_bytes(f.read(8), byteorder="little")
                self._size_check(address_len, metadata_len, buf_len,
                                 parsed_result.size)
                total += buf_len
                owner_address = f.read(address_len)
                metadata = f.read(metadata_len)
                # read remaining data to our buffer
                self._put_object_to_store(metadata, buf_len, f, object_ref,
                                          owner_address)
        return total

    def delete_spilled_objects(self, urls: List[str]):
        for url in urls:
            filename = parse_url_with_offset(url.decode()).base_url
            os.remove(os.path.join(self.directory_path, filename))

    def destroy_external_storage(self):
        # Q: Should we add stdout here to
        # indicate we are deleting a directory?

        # There's a race condition where IO workers are still
        # deleting each objects while we try deleting the
        # whole directory. So we should keep trying it until
        # The directory is actually deleted.
        while os.path.isdir(self.directory_path):
            try:
                shutil.rmtree(self.directory_path)
            except FileNotFoundError:
                # If excpetion occurs when other IO workers are
                # deleting the file at the same time.
                pass
            except Exception:
                logger.exception("Error cleaning up spill files")
                break


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
        # smart_open always seek to 0 if we don't set this argument.
        # This will lead us to call a Object.get when it is not necessary,
        # so defer seek and call seek before reading objects instead.
        self.transport_params = {"defer_seek": True}
        self.transport_params.update(self.override_transport_params)

    def spill_objects(self, object_refs, owner_addresses) -> List[str]:
        if len(object_refs) == 0:
            return []
        from smart_open import open
        # Always use the first object ref as a key when fusioning objects.
        first_ref = object_refs[0]
        key = f"{self.prefix}-{first_ref.hex()}-multi-{len(object_refs)}"
        url = f"{self.uri}/{key}"
        with open(
                url, "wb",
                transport_params=self.transport_params) as file_like:
            return self._write_multiple_objects(file_like, object_refs,
                                                owner_addresses, url)

    def restore_spilled_objects(self, object_refs: List[ObjectRef],
                                url_with_offset_list: List[str]):
        from smart_open import open
        total = 0
        for i in range(len(object_refs)):
            object_ref = object_refs[i]
            url_with_offset = url_with_offset_list[i].decode()

            # Retrieve the information needed.
            parsed_result = parse_url_with_offset(url_with_offset)
            base_url = parsed_result.base_url
            offset = parsed_result.offset

            with open(
                    base_url, "rb",
                    transport_params=self.transport_params) as f:
                # smart open seek reads the file from offset-end_of_the_file
                # when the seek is called.
                f.seek(offset)
                address_len = int.from_bytes(f.read(8), byteorder="little")
                metadata_len = int.from_bytes(f.read(8), byteorder="little")
                buf_len = int.from_bytes(f.read(8), byteorder="little")
                self._size_check(metadata_len, buf_len, parsed_result.size)
                owner_address = f.read(address_len)
                total += buf_len
                metadata = f.read(metadata_len)
                # read remaining data to our buffer
                self._put_object_to_store(metadata, buf_len, f, object_ref,
                                          owner_address)
        return total

    def delete_spilled_objects(self, urls: List[str]):
        pass

    def destroy_external_storage(self):
        pass


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
        elif storage_type == "mock_distributed_fs":
            # This storage is used to unit test distributed external storages.
            # TODO(sang): Delete it after introducing the mock S3 test.
            _external_storage = FileSystemStorage(**config["params"])
        else:
            raise ValueError(f"Unknown external storage type: {storage_type}")
    else:
        _external_storage = NullStorage()
    return _external_storage


def reset_external_storage():
    global _external_storage
    _external_storage = NullStorage()


def spill_objects(object_refs, owner_addresses):
    """Spill objects to the external storage. Objects are specified
    by their object refs.

    Args:
        object_refs: The list of the refs of the objects to be spilled.
        owner_addresses: The owner addresses of the provided object refs.
    Returns:
        A list of keys corresponding to the input object refs.
    """
    return _external_storage.spill_objects(object_refs, owner_addresses)


def restore_spilled_objects(object_refs: List[ObjectRef],
                            url_with_offset_list: List[str]):
    """Restore objects from the external storage.

    Args:
        object_refs: List of object IDs (note that it is not ref).
        url_with_offset_list: List of url_with_offset.
    """
    return _external_storage.restore_spilled_objects(object_refs,
                                                     url_with_offset_list)


def delete_spilled_objects(urls: List[str]):
    """Delete objects that are spilled to the external storage.

    Args:
        urls: URLs that store spilled object files.
    """
    _external_storage.delete_spilled_objects(urls)
