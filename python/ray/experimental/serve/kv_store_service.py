import json
import sqlite3
from abc import ABC

from ray import cloudpickle as pickle

import ray.experimental.internal_kv as ray_kv
from ray.experimental.serve.utils import logger


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


# Tables
class RoutingTable:
    def __init__(self, kv_connector):
        self.routing_table = kv_connector("routing_table")
        self.request_count = 0

    def register_service(self, route: str, service: str):
        """Create an entry in the routing table

        Args:
            route: http path name. Must begin with '/'.
            service: service name. This is the name http actor will push
                the request to.
        """
        logger.debug("[KV] Registering route {} to service {}.".format(
            route, service))
        self.routing_table.put(route, service)

    def list_service(self):
        """Returns the routing table."""
        self.request_count += 1
        table = self.routing_table.as_dict()
        return table

    def get_request_count(self):
        """Return the number of requests that fetched the routing table.

        This method is used for two purpose:

        1. Make sure HTTP server has started and healthy. Incremented request
           count means HTTP server is actively fetching routing table.

        2. Make sure HTTP server does not have stale routing table. This number
           should be incremented every HTTP_ROUTER_CHECKER_INTERVAL_S seconds.
           Supervisor should check this number as indirect indicator of http
           server's health.
        """
        return self.request_count


class BackendTable:
    def __init__(self, kv_connector):
        self.backend_table = kv_connector("backend_creator")
        self.replica_table = kv_connector("replica_table")

    def register_backend(self, backend_tag: str, backend_creator):
        backend_creator_serialized = pickle.dumps(backend_creator)
        self.backend_table.put(backend_tag, backend_creator_serialized)

    def get_backend_creator(self, backend_tag):
        return pickle.loads(self.backend_table.get(backend_tag))

    def list_backends(self):
        return list(self.backend_table.as_dict().keys())

    def list_replicas(self, backend_tag: str):
        return json.loads(self.replica_table.get(backend_tag, "[]"))

    def add_replica(self, backend_tag: str, new_replica_tag: str):
        replica_tags = self.list_replicas(backend_tag)
        replica_tags.append(new_replica_tag)
        self.replica_table.put(backend_tag, json.dumps(replica_tags))

    def remove_replica(self, backend_tag):
        replica_tags = self.list_replicas(backend_tag)
        removed_replica = replica_tags.pop()
        self.replica_table.put(backend_tag, json.dumps(replica_tags))
        return removed_replica


class TrafficPolicyTable:
    def __init__(self, kv_connector):
        self.traffic_policy_table = kv_connector("traffic_policy")

    def register_traffic_policy(self, service_name, policy_dict):
        self.traffic_policy_table.put(service_name, json.dumps(policy_dict))

    def list_traffic_policy(self):
        return {
            service: json.loads(policy)
            for service, policy in self.traffic_policy_table.as_dict()
        }
