import json
import sqlite3
from abc import ABC

import ray.experimental.internal_kv as ray_kv


class NamespacedKVStore(ABC):
    """Abstract base class for a namespaced key-value store.

    The idea is that multiple key-value stores can be created while sharing
    the same storage system. The keys of each instance are namespaced to avoid
    object_id key collision.

    Example:

    >>> store_ns1 = NamespacedKVStore(namespace="ns1")
    >>> store_ns2 = NamespacedKVStore(namespace="ns2")
    # Two stores can share the same connection like Redis or SQL Table
    >>> store_ns1.put("same-key", 1)
    >>> store_ns1.get("same-key")
    1
    >>> store_ns2.put("same-key", 2)
    >>> store_ns2.get("same-key", 2)
    2
    """

    def __init__(self, namespace):
        raise NotImplementedError()

    def get(self, key):
        """Retrieve the value for the given key.

        Args:
            key (str)
        """
        raise NotImplementedError()

    def put(self, key, value):
        """Serialize the value and store it under the given key.

        Args:
            key (str)
            value (object): any serializable object. The serialization method
                is determined by the subclass implementation.
        """
        raise NotImplementedError()

    def as_dict(self):
        """Return the entire namespace as a dictionary.

        Returns:
            data (dict): key value pairs in current namespace
        """
        raise NotImplementedError()


class InMemoryKVStore(NamespacedKVStore):
    """A reference implementation used for testing."""

    def __init__(self, namespace):
        self.data = dict()

        # Namespace is ignored, because each namespace is backed by
        # an in-memory Python dictionary.
        self.namespace = namespace

    def get(self, key):
        return self.data[key]

    def put(self, key, value):
        self.data[key] = value

    def as_dict(self):
        return self.data.copy()


class RayInternalKVStore(NamespacedKVStore):
    """A NamespacedKVStore implementation using ray's `internal_kv`."""

    def __init__(self, namespace):
        assert ray_kv._internal_kv_initialized()
        self.index_key = "RAY_SERVE_INDEX"
        self.namespace = namespace
        self._put(self.index_key, [])

    def _format_key(self, key):
        return "{ns}-{key}".format(ns=self.namespace, key=key)

    def _remove_format_key(self, formatted_key):
        return formatted_key.replace(self.namespace + "-", "", 1)

    def _serialize(self, obj):
        return json.dumps(obj)

    def _deserialize(self, buffer):
        return json.loads(buffer)

    def _put(self, key, value):
        ray_kv._internal_kv_put(
            self._format_key(self._serialize(key)),
            self._serialize(value),
            overwrite=True,
        )

    def _get(self, key):
        return self._deserialize(
            ray_kv._internal_kv_get(self._format_key(self._serialize(key))))

    def get(self, key):
        return self._get(key)

    def put(self, key, value):
        assert isinstance(key, str), "Key must be a string."

        self._put(key, value)

        all_keys = set(self._get(self.index_key))
        all_keys.add(key)
        self._put(self.index_key, list(all_keys))

    def as_dict(self):
        data = {}
        all_keys = self._get(self.index_key)
        for key in all_keys:
            data[self._remove_format_key(key)] = self._get(key)
        return data


class SQLiteKVStore(NamespacedKVStore):
    def __init__(self, namespace, db_path):
        self.namespace = namespace
        self.conn = sqlite3.connect(db_path)

        cursor = self.conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS {} (key TEXT UNIQUE, value TEXT)".
            format(self.namespace))
        self.conn.commit()

    def put(self, key, value):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO {} (key, value) VALUES (?,?)".format(
                self.namespace), (key, value))
        self.conn.commit()

    def get(self, key, default=None):
        cursor = self.conn.cursor()
        result = list(
            cursor.execute(
                "SELECT value FROM {} WHERE key = (?)".format(self.namespace),
                (key, )))
        if len(result) == 0:
            return default
        else:
            # Due to UNIQUE constraint, there can only be one value.
            value, *_ = result[0]
            return value

    def as_dict(self):
        cursor = self.conn.cursor()
        result = list(
            cursor.execute("SELECT key, value FROM {}".format(self.namespace)))
        return dict(result)
