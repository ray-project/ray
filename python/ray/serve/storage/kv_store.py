import logging
import os
import sqlite3
from typing import Optional

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None

from ray import ray_constants
import ray.experimental.internal_kv as ray_kv

from ray.serve.constants import SERVE_LOGGER_NAME
from ray.serve.storage.kv_store_base import KVStoreBase

logger = logging.getLogger(SERVE_LOGGER_NAME)


def get_storage_key(namespace: str, storage_key: str) -> str:
    """In case we need to access kvstore"""
    return "{ns}-{key}".format(ns=namespace, key=storage_key)


class RayInternalKVStore(KVStoreBase):
    """Wraps ray's internal_kv with a namespace to avoid collisions.

    Supports string keys and bytes values, caller must handle serialization.
    """

    def __init__(self, namespace: str = None):
        assert ray_kv._internal_kv_initialized()
        if namespace is not None and not isinstance(namespace, str):
            raise TypeError("namespace must a string, got: {}.".format(type(namespace)))

        self.namespace = namespace or ""

    def get_storage_key(self, key: str) -> str:
        return "{ns}-{key}".format(ns=self.namespace, key=key)

    def put(self, key: str, val: bytes) -> bool:
        """Put the key-value pair into the store.

        Args:
            key (str)
            val (bytes)
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))
        if not isinstance(val, bytes):
            raise TypeError("val must be bytes, got: {}.".format(type(val)))

        ray_kv._internal_kv_put(
            self.get_storage_key(key),
            val,
            overwrite=True,
            namespace=ray_constants.KV_NAMESPACE_SERVE,
        )

    def get(self, key: str) -> Optional[bytes]:
        """Get the value associated with the given key from the store.

        Args:
            key (str)

        Returns:
            The bytes value. If the key wasn't found, returns None.
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))

        return ray_kv._internal_kv_get(
            self.get_storage_key(key), namespace=ray_constants.KV_NAMESPACE_SERVE
        )

    def delete(self, key: str):
        """Delete the value associated with the given key from the store.

        Args:
            key (str)
        """

        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))
        return ray_kv._internal_kv_del(
            self.get_storage_key(key), namespace=ray_constants.KV_NAMESPACE_SERVE
        )


class RayLocalKVStore(KVStoreBase):
    """Persistent version of KVStoreBase for cluster fault
    tolerance. Writes to local disk by sqlite3 with given db_path.

    Supports only string type for key and bytes type for value,
    caller must handle serialization.
    """

    def __init__(
        self,
        namepsace: str,
        db_path: str,
    ):
        if len(db_path) == 0:
            raise ValueError("LocalKVStore's path shouldn't be empty.")

        # Ensture that parent directory is created.
        parent_dir = os.path.split(db_path)[0]
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        self._namespace = namepsace
        self._conn = sqlite3.connect(db_path)
        cursor = self._conn.cursor()
        cursor.execute(
            f'CREATE TABLE IF NOT EXISTS "{self._namespace}"'
            "(key TEXT UNIQUE, value BLOB)"
        )
        self._conn.commit()

    def get_storage_key(self, key: str) -> str:
        return "{ns}-{key}".format(ns=self._namespace, key=key)

    def put(self, key: str, val: bytes) -> bool:
        """Put the key-value pair into the store.

        Args:
            key (str)
            val (bytes)
        """
        if not isinstance(key, str):
            raise TypeError(f"key must be a string, got: {type(key)}.")
        if not isinstance(val, bytes):
            raise TypeError(f"val must be bytes, got: {type(val)}.")

        cursor = self._conn.cursor()
        cursor.execute(
            f'INSERT OR REPLACE INTO "{self._namespace}" ' "(key, value) VALUES (?,?)",
            (self.get_storage_key(key), val),
        )
        self._conn.commit()
        return True

    def get(self, key: str) -> Optional[bytes]:
        """Get the value associated with the given key from the store.

        Args:
            key (str)

        Returns:
            The bytes value. If the key wasn't found, returns None.
        """
        if not isinstance(key, str):
            raise TypeError(f"key must be a string, got: {type(key)}.")

        cursor = self._conn.cursor()
        result = list(
            cursor.execute(
                f'SELECT value FROM "{self._namespace}" WHERE key = (?)',
                (self.get_storage_key(key),),
            )
        )
        if len(result) == 0:
            return None
        else:
            # Due to UNIQUE constraint, there can only be one value.
            value, *_ = result[0]
            return value

    def delete(self, key: str):
        """Delete the value associated with the given key from the store.

        Args:
            key (str)
        """

        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))

        cursor = self._conn.cursor()
        cursor.execute(
            f'DELETE FROM "{self._namespace}" ' "WHERE key = (?)",
            (self.get_storage_key(key),),
        )
        self._conn.commit()


class RayS3KVStore(KVStoreBase):
    """Persistent version of KVStoreBase for cluster fault
    tolerance. Writes to S3 bucket with provided path and credentials.

    Supports only string type for key and bytes type for value,
    caller must handle serialization.
    """

    def __init__(
        self,
        namespace: str,
        bucket="",
        prefix="",
        region_name="us-west-2",
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
    ):
        self._namespace = namespace
        self._bucket = bucket
        self._prefix = prefix
        if not boto3:
            raise ImportError(
                "You tried to use S3KVstore client without boto3 installed."
                "Please run `pip install boto3`"
            )
        self._s3 = boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

    def get_storage_key(self, key: str) -> str:
        return f"{self._prefix}/{self._namespace}-{key}"

    def put(self, key: str, val: bytes) -> bool:
        """Put the key-value pair into the store.

        Args:
            key (str)
            val (bytes)
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))
        if not isinstance(val, bytes):
            raise TypeError("val must be bytes, got: {}.".format(type(val)))

        try:
            self._s3.put_object(
                Body=val, Bucket=self._bucket, Key=self.get_storage_key(key)
            )
        except ClientError as e:
            message = e.response["Error"]["Message"]
            logger.error(
                f"Encountered ClientError while calling put() "
                f"in RayExternalKVStore: {message}"
            )
            raise e

    def get(self, key: str) -> Optional[bytes]:
        """Get the value associated with the given key from the store.

        Args:
            key (str)

        Returns:
            The bytes value. If the key wasn't found, returns None.
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))

        try:
            response = self._s3.get_object(
                Bucket=self._bucket, Key=self.get_storage_key(key)
            )
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"No such key in s3 for key = {key}")
                return None
            else:
                message = e.response["Error"]["Message"]
                logger.error(
                    f"Encountered ClientError while calling get() "
                    f"in RayExternalKVStore: {message}"
                )
                raise e

    def delete(self, key: str):
        """Delete the value associated with the given key from the store.

        Args:
            key (str)
        """

        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))

        try:
            self._s3.delete_object(Bucket=self._bucket, Key=self.get_storage_key(key))
        except ClientError as e:
            message = e.response["Error"]["Message"]
            logger.error(
                f"Encountered ClientError while calling delete() "
                f"in RayExternalKVStore: {message}"
            )
            raise e
