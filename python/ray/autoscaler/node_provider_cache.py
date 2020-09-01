import threading
from typing import Any, Dict


class NodeProviderCache:
    """Base class for storing cached node information, used to avoid excessive
    API calls to cloud providers in some cases.

    This implementation stores all information in memory. 
    """

    def __init__(self) -> None:
        # tags_map and node_map are not guaranteed to be in sync in current
        # implementation. node_ids in tags_map is supposed to be the superset
        # of those in node_map.
        self.node_map: Dict[str, Any]  = {}
        self.tags_map: Dict[str, Dict[str, str]] = {}
        self.lock = threading.Lock()

    def get_node(self, node_id: str) -> Any:
        """Returns a cached node obj with given node ID."""
        with self.lock:
            return self.node_map.get(node_id)

    def set_node(self, node_id: str, node: Any) -> None:
        """Stores a node obj into cache with given node ID."""
        with self.lock:
            self.node_map[node_id] = node
            self.tags_map.setdefault(node_id, {})

    def node_exists(self, node_id: str) -> bool:
        """Returns whether a node obj with given node ID exists in cache."""
        with self.lock:
            return node_id in self.node_map

    def get_tags(self, node_id: str) -> Dict[str, str]:
        """Returns a dict of tags associated with given node ID."""
        with self.lock:
            return self.tags_map.get(node_id, {})

    def set_tags(self, node_id: str, tags: Dict[str, str]) -> None:
        """Stores the tags into cache with given node ID."""
        with self.lock:
            self.tags_map[node_id] = tags

    def update_tags(self, node_id: str, tags: Dict[str, str]) -> None:
        """Update the tags in cache with given node ID."""
        with self.lock:
            self.tags_map[node_id].update(tags)

    def tags_exist(self, node_id: str) -> bool:
        """Returns whether tags with given node ID exists in cache."""
        with self.lock:
            return node_id in self.tags_map

    def delete_node_and_tags(self, node_id: str) -> None:
        """Deletes nodes and tags with given node ID from cache."""
        with self.lock:
            if node_id in self.node_map:
                del self.node_map[node_id]
            if node_id in self.tags_map:
                del self.tags_map[node_id]

    def cleanup(self) -> None:
        """Deletes all nodes and tags from cache."""
        with self.lock:
            self.node_map = {}
            self.tags_map = {}
