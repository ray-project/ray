import boto3
from botocore.exceptions import ClientError

import ray.experimental.internal_kv as ray_kv
from ray.serve.utils import logger


def format_key(name, key):
    return "{ns}-{key}".format(ns=name, key=key)


class RayInternalKVStore:
    """Wraps ray's internal_kv with a namespace to avoid collisions.

    Supports string keys and bytes values, caller must handle serialization.
    """

    def __init__(self, namespace=None):
        assert ray_kv._internal_kv_initialized()
        if namespace is not None and not isinstance(namespace, str):
            raise TypeError("namespace must a string, got: {}.".format(
                type(namespace)))

        self.namespace = namespace or ""

    def _format_key(self, key):
        return format_key(
            self.namespace,
            key)  # "{ns}-{key}".format(ns=self.namespace, key=key)

    def put(self, key, val):
        """Put the key-value pair into the store.

        Args:
            key (str)
            val (Union[str, bytes])
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))
        if not isinstance(val, bytes) and not isinstance(val, str):
            raise TypeError("val must be a string or bytes, got: {}.".format(
                type(val)))

        ray_kv._internal_kv_put(self._format_key(key), val, overwrite=True)

    def get(self, key):
        """Get the value associated with the given key from the store.

        Args:
            key (str)

        Returns:
            The bytes value. If the key wasn't found, returns None.
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))

        return ray_kv._internal_kv_get(self._format_key(key))

    def delete(self, key):
        """Delete the value associated with the given key from the store.

        Args:
            key (str)
        """

        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))
        return ray_kv._internal_kv_del(self._format_key(key))


class RayExternalKVStore:
    """Persistent version of RayInternalKVStore for cluster fault
    tolerance. Writes to local disk in local / test mode, also external
    storage (such as amazon S3) with provided path.

    Supports string keys and bytes values, caller must handle serialization.
    """

    def __init__(
            self,
            namepsace: str,
            bucket="",
            s3_path="",
            region_name="us-west-2",
            aws_access_key_id=None,
            aws_secret_access_key=None,
            aws_session_token=None,
            local_mode=True,
    ):
        self._namespace = namepsace
        self._bucket = bucket
        self._s3_path = s3_path
        self._local_mode = local_mode
        self._tombstone = b"\##DELETED\##"

        self._s3 = boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token)

    def get_key_with_namespace(self, key):
        return "{ns}-{key}".format(ns=self._namespace, key=key)

    def put(self, key, val):
        """Put the key-value pair into the store.

        Args:
            key (str)
            val (bytes)
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))
        if not isinstance(val, bytes):
            raise TypeError("val must be bytes, got: {}.".format(type(val)))

        if self._local_mode:
            with open("/tmp/ray_serve_checkpoint_key.txt",
                      "a+") as key_file, open(
                          "/tmp/ray_serve_checkpoint_val.txt",
                          "ab+") as val_file:
                key_file.write(self.get_key_with_namespace(key) + "\n")
                val_file.write(val)
                val_file.write(b"\n")

        else:
            try:
                self._s3.put_object(
                    Body=val,
                    Bucket=self._bucket,
                    Key=self.get_key_with_namespace(key))
            except ClientError as e:
                message = e.response["Error"]["Message"]
                logger.error(f"Encountered ClientError while calling put() "
                             f"in RayExternalKVStore: {message}")
                raise e

    def get(self, key):
        """Get the value associated with the given key from the store.

        Args:
            key (str)

        Returns:
            The bytes value. If the key wasn't found, returns None.
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))

        if self._local_mode:
            with open("/tmp/ray_serve_checkpoint_key.txt",
                      "r") as key_file, open(
                          "/tmp/ray_serve_checkpoint_val.txt",
                          "rb") as val_file:
                for key_line, val_line in zip(
                        reversed(key_file.readlines()),
                        reversed(val_file.readlines())):
                    if key_line.strip() == self.get_key_with_namespace(key):
                        if val_line.strip() == self._tombstone:
                            # Key deleted
                            return None
                        else:
                            return val_line.strip()
            return None
        else:
            try:
                response = self._s3.get_object(
                    Bucket=self._bucket, Key=self.get_key_with_namespace(key))
                return response["Body"].read()
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    logger.warning(f"No such key in s3 for key = {key}")
                    return None
                else:
                    message = e.response["Error"]["Message"]
                    logger.error(
                        f"Encountered ClientError while calling get() "
                        f"in RayExternalKVStore: {message}")
                    raise e

    def delete(self, key):
        """Delete the value associated with the given key from the store.

        Args:
            key (str)
        """

        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))

        if self._local_mode:
            with open("/tmp/ray_serve_checkpoint_key.txt",
                      "a+") as key_file, open(
                          "/tmp/ray_serve_checkpoint_val.txt",
                          "ab+") as val_file:
                key_file.write(self.get_key_with_namespace(key) + "\n")
                val_file.write(self._tombstone)
                val_file.write(b"\n")
        else:
            try:
                self._s3.delete_object(
                    Bucket=self._bucket, Key=self.get_key_with_namespace(key))
            except ClientError as e:
                message = e.response["Error"]["Message"]
                logger.error(f"Encountered ClientError while calling get() "
                             f"in RayExternalKVStore: {message}")
                raise e
