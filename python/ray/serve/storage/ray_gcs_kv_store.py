import io
import logging
from typing import Optional

try:
    from google.cloud import storage
    from google.cloud.exceptions import NotFound
except ImportError:
    storage = None

from ray.serve.constants import SERVE_LOGGER_NAME
from ray.serve.storage.kv_store_base import KVStoreBase

logger = logging.getLogger(SERVE_LOGGER_NAME)


class RayGcsKVStore(KVStoreBase):
    """Persistent version of KVStoreBase for cluster fault
    tolerance. Writes to GCS bucket with provided path and credentials.

    Supports only string type for key and bytes type for value,
    caller must handle serialization.
    """

    def __init__(
        self,
        namespace: str,
        bucket="",
        prefix="",
    ):
        self._namespace = namespace
        self._bucket = bucket
        self._prefix = prefix + "/" if prefix else ""
        if not storage:
            raise ImportError(
                "You tried to use RayGcsKVStore client without"
                "google-cloud-storage installed."
                "Please run `pip install google-cloud-storage`"
            )
        self._gcs = storage.Client()
        self._bucket = self._gcs.bucket(bucket)

    def get_storage_key(self, key: str) -> str:
        return f"{self._prefix}{self._namespace}-{key}"

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
            blob = self._bucket.blob(blob_name=self.get_storage_key(key))
            f = io.BytesIO(val)
            blob.upload_from_file(f, num_retries=5)
        except Exception as e:
            message = str(e)
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
            blob = self._bucket.blob(blob_name=self.get_storage_key(key))
            return blob.download_as_bytes()
        except NotFound:
            logger.warning(f"No such key in GCS for key = {key}")
            return None

    def delete(self, key: str):
        """Delete the value associated with the given key from the store.

        Args:
            key (str)
        """

        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))

        try:
            blob_name = self.get_storage_key(key)
            blob = self._bucket.blob(blob_name=blob_name)
            blob.delete()
        except NotFound:
            logger.error(
                f"Encountered ClientError while calling delete() "
                f"in RayExternalKVStore - "
                f"Blob {blob_name} was not found!"
            )
