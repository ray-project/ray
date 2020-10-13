import abc
import os
from contextlib import closing
from io import BytesIO, BufferedReader
from typing import List

import ray


class ExternalStorage(metaclass=abc.ABCMeta):
    """The base class for external storage.

    This class provides some useful functions for zero-copy object
    put/get from plasma store. Also it specifies the interface for
    object spilling.
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

    def validate(self):
        """Check if the external storage configuration is valid.

        Raises:
            Raise various exceptions if it fails to validate.
        """


class NullStorage(ExternalStorage):
    """The class that represents an uninitialized external storage."""

    def spill_objects(self, object_refs):
        raise NotImplementedError("External storage is not initialized")

    def restore_spilled_objects(self, keys):
        raise NotImplementedError("External storage is not initialized")

    def validate(self):
        pass


class FileSystemStorage(ExternalStorage):
    """The class for filesystem-like external storage."""

    def __init__(self, directory_path):
        self.directory_path = directory_path
        self.prefix = "ray_spilled_object_"

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

    def validate(self):
        if not os.path.exists(self.directory_path):
            raise ValueError("The give directory path to store objects, "
                             f"{self.directory_path}, doesn't exist.")


class S3Storage(ExternalStorage):
    """The class for AWS S3.

    Args:
        bucket_name(str): Name of the bucket to store objects.
        prefix(str): Prefix of objects that are stored.
        get_config_override(dict): Configuration dict that will override
            the default boto3.resource('s3').Object(bucket_name, key).get()
        put_config_override(dict): Configuration dict that will override
            the default boto3.resource('s3').Object(bucket_name, key).put()
    Raises:
        RayError if it fails to setup a S3 client. For example, if boto3 is
        not downloaded, it raises an RayError. Or if it doesn't have a bucket,
        it raises the same error.
    """

    def __init__(self,
                 bucket_name: str,
                 prefix: str = "ray_spilled_object_",
                 get_config_override: dict = None,
                 put_config_override: dict = None):
        try:
            import boto3
        except ModuleNotFoundError:
            raise ray.exceptions.RayError(
                "S3 storage is chosen to be a object spilling "
                "external storage, but boto3 is not downloaded.")

        self.bucket_name = bucket_name
        self.prefix = prefix
        self._get_config_override = get_config_override or {}
        self._put_config_override = put_config_override or {}

        # Setup the S3 client.
        self.s3 = boto3.resource("s3")

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
            raise IOError("Failed to get a proper response from S3.")
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

    def validate(self):
        try:
            import boto3
        except ModuleNotFoundError:
            raise ray.exceptions.RayError(
                "S3 storage is chosen to be a object spilling "
                "external storage, but boto3 is not downloaded.")
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket_name)
        if not bucket.creation_date:
            raise ValueError(f"Bucket name {self.bucket_name} doesn't "
                             "exist or is not reachable.")


_external_storage = NullStorage()


def setup_external_storage(config):
    """Setup the external storage according to the config."""
    global _external_storage
    if config:
        storage_type = config["type"]
        if storage_type == "filesystem":
            _external_storage = FileSystemStorage(**config["params"])
        elif storage_type == "s3":
            _external_storage = S3Storage(**config["params"])
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


def validate():
    """Check if external storage configuration is valid.

    Raises:
        Raise various exceptions if it fails to validate.
    """
    _external_storage.validate()
