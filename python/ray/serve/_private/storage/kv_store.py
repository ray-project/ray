import logging
from typing import Optional

import ray
from ray._private import ray_constants
from ray._private.gcs_utils import GcsClient
from ray.serve._private.constants import RAY_SERVE_KV_TIMEOUT_S, SERVE_LOGGER_NAME
from ray.serve._private.storage.kv_store_base import KVStoreBase


logger = logging.getLogger(SERVE_LOGGER_NAME)


def get_storage_key(namespace: str, storage_key: str) -> str:
    """In case we need to access kvstore"""
    return "{ns}-{key}".format(ns=namespace, key=storage_key)


class KVStoreError(Exception):
    pass


class RayInternalKVStore(KVStoreBase):
    """Wraps ray's internal_kv with a namespace to avoid collisions.

    Supports string keys and bytes values, caller must handle serialization.
    """

    def __init__(
        self,
        namespace: Optional[str] = None,
        gcs_client: Optional[GcsClient] = None,
    ):
        if namespace is not None and not isinstance(namespace, str):
            raise TypeError("namespace must a string, got: {}.".format(type(namespace)))
        if gcs_client is not None:
            self.gcs_client = gcs_client
        else:
            self.gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
        self.timeout = RAY_SERVE_KV_TIMEOUT_S
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

        try:
            return self.gcs_client.internal_kv_put(
                self.get_storage_key(key).encode(),
                val,
                overwrite=True,
                namespace=ray_constants.KV_NAMESPACE_SERVE,
                timeout=self.timeout,
            )
        except Exception as e:
            raise KVStoreError(e.code())

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
            return self.gcs_client.internal_kv_get(
                self.get_storage_key(key).encode(),
                namespace=ray_constants.KV_NAMESPACE_SERVE,
                timeout=self.timeout,
            )
        except Exception as e:
            raise KVStoreError(e.code())

    def delete(self, key: str):
        """Delete the value associated with the given key from the store.

        Args:
            key (str)
        """

        if not isinstance(key, str):
            raise TypeError("key must be a string, got: {}.".format(type(key)))

        try:
            return self.gcs_client.internal_kv_del(
                self.get_storage_key(key).encode(),
                False,
                namespace=ray_constants.KV_NAMESPACE_SERVE,
                timeout=self.timeout,
            )
        except Exception as e:
            raise KVStoreError(e.code())
