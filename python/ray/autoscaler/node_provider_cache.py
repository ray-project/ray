import threading
from typing import Any, Dict


class NodeProviderCache:
    """Base class for storing cached node information, used to avoid excessive
    API calls to cloud providers in some cases.

    This implementation stores all information in memory. 
    """

    def __init__(self):
        # tag_map and node_map are not guaranteed to be in sync in current
        # implementation. node_ids in tag_map is supposed to be the superset
        # of those in node_map.
        self.node_map = {}
        self.tag_map = {}
        self.lock = threading.Lock()

    def get_node(self, node_id: str) -> Any:
        """Returns a cached node obj with given node ID."""
        with lock:
            return self.node_map.get(node_id)

    def set_node(self, node_id: str, node: Any) -> None:
        """Stores a node obj into cache with given node ID."""
        with lock:
            self.node_map[node_id] = node
            self.tag_map.setdefault(node_id, {})

    def node_exists(self, node_id: str) -> bool:
        """Returns whether a node obj with given node ID exists in cache."""
        with lock:
            return node_id in self.node_map

    def get_tags(self, node_id: str) -> Dict[str, str]:
        """Returns a dict of tags associated with given node ID."""
        with lock:
            return self.tag_map.get(node_id)

    def set_tags(self, node_id: str, tags: Dict[str, str]):
        """Stores the tags into cache with given node ID."""
        with lock:
            self.tag_map[node_id] = tags

    def update_tags(self, node_id: str, tags: Dict[str, str]):
        """Update the tags in cache with given node ID."""
        with lock:
            self.tag_map[node_id].update(tags)

    def tags_exist(self, node_id: str) -> bool:
        """Returns whether tags with given node ID exists in cache."""
        with lock:
            return node_id in self.tags_map

    def delete_node_and_tags(self, node_id: str) -> None:
        """Deletes nodes and tags with given node ID from cache."""
        with lock:
            if node_id in self.node_map:
                del self.node_map[node_id]
            if node_id in self.tag_map:
                del self.tag_map[node_id]

    def cleanup(self) -> None:
        """Deletes all nodes and tags from cache."""
        with lock:
            self.node_map = {}
            self.tag_map = {}