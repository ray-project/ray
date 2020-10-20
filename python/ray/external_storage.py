import abc
import os
from contextlib import closing
from io import BytesIO, BufferedReader
from typing import List

import ray


class ExternalStorageConfigInvalidError(Exception):
    """Exception that indicates external storage is not properly setup.

    This exception should be thrown when external storage setup is not
    successful. For example, if the directory path for file system object
    spilling doesn't exist, that means object spilling wouldn't work. In
    that case, this exception should be thrown. This should be thrown
    only from __init__ method inside External storage inherited classes.
    """
    pass


class ExternalStorage(metaclass=abc.ABCMeta):
    """The base class for external storage.

    This class provides some useful functions for zero-copy object
    put/get from plasma store. Also it specifies the interface for
    object spilling.

    When inheriting this class, please make sure to implement validation
    logic inside __init__ method. When ray instance starts, it will
    instantiating external storage to validate the config.

    Raises:
        ExternalStorageConfigInvalidError: when given configuration for
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

    @abc.abstractmethod
    def spill_objects(self, object_refs):
        """Spill objects to the external storage. Objects are specified
        by their object refs.

        Args:
            object_refs: The list of the refs of the objects to be spilled.
        Returns:
            A list of keys corresponding to the input object refs.
        """

    @abc.abstractmethod
    def restore_spilled_objects(self, keys: List[bytes]):
        """Spill objects to the external storage. Objects are specified
        by their object refs.

        Args:
            keys: A list of bytes corresponding to the spilled objects.
        """

    @abc.abstractmethod
    def get_stats(self) -> dict:
        """Return object spilling internal stats in dict."""


class NullStorage(ExternalStorage):
    """The class that represents an uninitialized external storage."""

    def spill_objects(self, object_refs):
        raise NotImplementedError("External storage is not initialized")

    def restore_spilled_objects(self, keys):
        raise NotImplementedError("External storage is not initialized")

    def get_stats(self):
        raise NotImplementedError("External storage is not initialized")


class FileSystemStorage(ExternalStorage):
    """The class for filesystem-like external storage.

    Raises:
        ExternalStorageConfigInvalidError: Raises directory path to
            spill objects doesn't exist.
    """

    def __init__(self, directory_path):
        self.directory_path = directory_path
        self.prefix = "ray_spilled_object_"
        if not os.path.exists(self.directory_path):
            raise ExternalStorageConfigInvalidError(
                "The given directory path to store objects, "
                f"{self.directory_path}, doesn't exist.")

    def spill_objects(self, object_refs):
        keys = []
        ray_object_pairs = self._get_objects_from_store(object_refs)
        for ref, (buf, metadata) in zip(object_refs, ray_object_pairs):
            filename = self.prefix + ref.hex()
            with open(os.path.join(self.directory_path, filename), "wb") as f:
                metadata_len = len(metadata)
                buf_len = len(buf)
                f.write(metadata_len.to_bytes(8, byteorder="little"))
                f.write(buf_len.to_bytes(8, byteorder="little"))
                f.write(metadata)
                f.write(memoryview(buf))
            keys.append(filename.encode())
        return keys

    def restore_spilled_objects(self, keys):
        for k in keys:
            filename = k.decode()
            ref = ray.ObjectRef(bytes.fromhex(filename[len(self.prefix):]))
            with open(os.path.join(self.directory_path, filename), "rb") as f:
                metadata_len = int.from_bytes(f.read(8), byteorder="little")
                buf_len = int.from_bytes(f.read(8), byteorder="little")
                metadata = f.read(metadata_len)
                # read remaining data to our buffer
                self._put_object_to_store(metadata, buf_len, f, ref)

    def get_stats(self):
        # TODO(sang): Implement it for benchmark.
        return {}


class S3StorageBotoCoreImpl(ExternalStorage):
    """The external storage class implemented by botocore.

    Args:
        bucket_name(str): Name of the bucket to store objects.
        prefix(str): Prefix of objects that are stored.
        get_config_override(dict): Configuration dict that will override
            the default boto3.resource('s3').Object(bucket_name, key).get()
        put_config_override(dict): Configuration dict that will override
            the default boto3.resource('s3').Object(bucket_name, key).put()

    Raises:
        ExternalStorageConfigInvalidError: If it fails to setup a S3 client.
            For example, if boto3 is not downloaded.
    """

    def __init__(self,
                 bucket_name: str,
                 prefix: str = "ray_spilled_object_",
                 get_config_override: dict = None,
                 put_config_override: dict = None):
        try:
            import boto3
        except ModuleNotFoundError:
            raise ExternalStorageConfigInvalidError(
                "S3 storage is chosen to be a object spilling "
                "external storage, but boto3 is not downloaded.")

        self.bucket_name = bucket_name
        self.prefix = prefix
        self._get_config_override = get_config_override or {}
        self._put_config_override = put_config_override or {}

        self.s3 = boto3.resource("s3")
        bucket = self.s3.Bucket(self.bucket_name)
        if not bucket.creation_date:
            raise ExternalStorageConfigInvalidError(
                f"Bucket name {self.bucket_name} doesn't "
                "exist or is not reachable.")

    def spill_objects(self, object_refs):
        keys = []
        ray_object_pairs = self._get_objects_from_store(object_refs)
        for ref, (buf, metadata) in zip(object_refs, ray_object_pairs):
            key = self.prefix + ref.hex()
            self._spill_object(key, ref, buf, metadata)
            keys.append(key.encode())
        return keys

    def restore_spilled_objects(self, keys):
        for k in keys:
            key = k.decode()
            ref = ray.ObjectRef(bytes.fromhex(key[len(self.prefix):]))
            self._restore_spilled_object(key, ref)

    def get_stats(self):
        # TODO(sang): Implement it for benchmark.
        return {}

    def _spill_object(self, key, ref, buf, metadata):
        s3_object = self.s3.Object(self.bucket_name, key)

        with BytesIO() as file_like:
            metadata_len = len(metadata)
            buf_len = len(buf)
            file_like.write(metadata_len.to_bytes(8, byteorder="little"))
            file_like.write(buf_len.to_bytes(8, byteorder="little"))
            file_like.write(metadata)
            file_like.write(memoryview(buf))
            file_like.seek(0)
            # TODO(sang): Should we check integrity here?
            # TODO(sang): Add a job id to a tagging field for deletion.
            s3_object.put(Body=file_like)

    def _restore_spilled_object(self, key, ref):
        resp = self.s3.Object(self.bucket_name, key).get()
        if "Body" not in resp:
            raise IOError("Failed to get a proper response from S3. "
                          "The response should contain a Body field, "
                          f"but it only got {resp}")
        with closing(resp["Body"]) as streaming_body:
            # botocore.response.StreamingBody doesn't provide readinto,
            # which we'd need to put objects to avoid unnecessary copy.
            # Solution 1: Use BytesIO and pass bytes using
            #   streaming_body.read(). This solution requires a byte copy
            #   returned by the read API.
            # Solution 2: Use underlying _raw_stream attribute that is
            #   a true file-like object. This seems to be fine because
            #   StreamingBody class seems to be a simple wrapper around
            #   this raw stream. Look at source code;
            #   https://gemfury.com/squarecapadmin/python:botocore/-/content/botocore/response.py # noqa
            #   BufferedReader allows us to copy underlying bytes
            #   directly to Ray object buffer.
            # We are going with solution 2 to reduce memory footprint.
            with BufferedReader(streaming_body._raw_stream) as file_like:
                metadata_len = int.from_bytes(
                    file_like.read(8), byteorder="little")
                buf_len = int.from_bytes(file_like.read(8), byteorder="little")
                metadata = file_like.read(metadata_len)
                # read remaining data to our buffer
                self._put_object_to_store(metadata, buf_len, file_like, ref)


class S3StorageSmartOpenImpl(ExternalStorage):
    """The external storage class implemented by smart_open.
    (https://github.com/RaRe-Technologies/smart_open)

    Args:
        bucket_name(str): Name of the bucket to store objects.
        prefix(str): Prefix of objects that are stored.
        override_transport_params(dict): Overriding the default value of
            transport_params for smart-open library.
        track_tail_latency(bool): If True, get_stats will return tail latency.
            Tracking tail latency requires more memory.

    Raises:
        ExternalStorageConfigInvalidError: If it fails to setup a S3 client.
            For example, if boto3 is not downloaded.
    """

    def __init__(self,
                 bucket_name: str,
                 prefix: str = "ray_spilled_object_",
                 override_transport_params: dict = None,
                 track_tail_latency: bool = False):
        try:
            import boto3
            from smart_open import open  # noqa
        except ModuleNotFoundError:
            raise ExternalStorageConfigInvalidError(
                "S3 storage is chosen to be a object spilling "
                "external storage, but boto3 and smart_open[s3] "
                "is not downloaded.")

        self.bucket_name = bucket_name
        self.prefix = prefix
        self.override_transport_params = override_transport_params or {}
        self.track_tail_latency = track_tail_latency

        self.session = boto3.Session()
        self.transport_params = {
            "session": self.session
        }.update(self.override_transport_params)

        # --validation--
        s3 = self.session.resource("s3")
        # Make sure the bucket exists.
        bucket = s3.Bucket(self.bucket_name)
        if not bucket.creation_date:
            raise ExternalStorageConfigInvalidError(
                f"Bucket name {self.bucket_name} doesn't "
                "exist or is not reachable.")

        # --stats--
        # SANG-TODO Implement it for benchmark.
        self.download_count = 0
        self.upload_count = 0
        self.download_avg_latency = 0
        self.upload_avg_latency = 0

        if self.track_tail_latency:
            self.download_latencies = []
            self.upload_latencies = []

    def spill_objects(self, object_refs):
        keys = []
        ray_object_pairs = self._get_objects_from_store(object_refs)
        for ref, (buf, metadata) in zip(object_refs, ray_object_pairs):
            key = self.prefix + ref.hex()
            self._spill_object(key, ref, buf, metadata)
            keys.append(key.encode())
        return keys

    def restore_spilled_objects(self, keys):
        for k in keys:
            key = k.decode()
            ref = ray.ObjectRef(bytes.fromhex(key[len(self.prefix):]))
            self._restore_spilled_object(key, ref)

    def get_stats(self):
        # TODO(sang): Implement it for benchmark.
        return {}

    def _spill_object(self, key, ref, buf, metadata):
        from smart_open import open
        url = self._build_s3_url(key)
        with open(
                url, "wb",
                transport_params=self.transport_params) as file_like:
            metadata_len = len(metadata)
            buf_len = len(buf)
            file_like.write(metadata_len.to_bytes(8, byteorder="little"))
            file_like.write(buf_len.to_bytes(8, byteorder="little"))
            file_like.write(metadata)
            file_like.write(memoryview(buf))

    def _restore_spilled_object(self, key, ref):
        from smart_open import open
        url = self._build_s3_url(key)
        with open(
                url, "rb",
                transport_params=self.transport_params) as file_like:
            metadata_len = int.from_bytes(
                file_like.read(8), byteorder="little")
            buf_len = int.from_bytes(file_like.read(8), byteorder="little")
            metadata = file_like.read(metadata_len)
            # read remaining data to our buffer
            self._put_object_to_store(metadata, buf_len, file_like, ref)

    def _build_s3_url(self, key):
        return f"s3://{self.bucket_name}/{key}"


_external_storage = NullStorage()


def setup_external_storage(config):
    """Setup the external storage according to the config."""
    global _external_storage
    if config:
        storage_type = config["type"]
        if storage_type == "filesystem":
            _external_storage = FileSystemStorage(**config["params"])
        elif storage_type == "s3":
            _external_storage = S3StorageSmartOpenImpl(**config["params"])
        elif storage_type == "s3_botocore":
            _external_storage = S3StorageBotoCoreImpl(**config["params"])
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
