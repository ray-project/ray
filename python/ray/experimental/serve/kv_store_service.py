import json
from abc import ABC

import ray
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

    def __init__(self, index_key,namespace):
        assert ray_kv._internal_kv_initialized()
        # self.index_key = "RAY_SERVE_INDEX"
        self.index_key = index_key
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
    def _hexists(self,key):
        return ray_kv._internal_kv_exists(self._format_key(self._serialize(key)))

    def _get(self, key):
        return self._deserialize(
            ray_kv._internal_kv_get(self._format_key(self._serialize(key))))

    def exists(self,key):
        return self._hexists(key)

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

# A class which stores pipeline name and it's dependent service
class KVPipelineProxy:
    def __init__(self, kv_class=RayInternalKVStore):
        self.pipeline_storage = kv_class(index_key = "RAY_PIPELINE_INDEX",namespace="pipelines")
        self.request_count = 0

    def add_node(self,pipeline: str, service_no_http: str):
        if self.pipeline_storage.exists(pipeline):
            l = set(self.pipeline_storage.get(pipeline))
            l.add(service_no_http)
            self.pipeline_storage.put(pipeline,list(l))
        else:
            self.pipeline_storage.put(pipeline,[l])

    def list_pipeline_service(self):
        self.request_count += 1
        table = self.pipeline_storage.as_dict()
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

    
class KVStoreProxy:
    def __init__(self, kv_class=InMemoryKVStore):
        self.routing_table = kv_class(index_key = "RAY_SERVE_INDEX",namespace="routes")
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


@ray.remote
class KVStoreProxyActor(KVStoreProxy):
    def __init__(self, kv_class=RayInternalKVStore):
        super().__init__(kv_class=kv_class)

@ray.remote
class KVPipelineProxyActor(KVPipelineProxy):
    def __init__(self, kv_class=RayInternalKVStore):
        super().__init__(kv_class=kv_class)
