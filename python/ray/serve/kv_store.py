import ray.experimental.internal_kv as ray_kv


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
        return "{ns}-{key}".format(ns=self.namespace, key=key)

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
