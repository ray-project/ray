from __future__ import annotations

import logging
import os
from threading import RLock
from typing import Dict, List, Optional, Set, Tuple, Any

import ray

logger = logging.getLogger(__name__)


class Node:
    """
    Node in a prefix tree that represents a segment of text and can belong to multiple tenants.
    Each node also tracks the last access time for each tenant.
    Simple example of root node connected to two children Nodes:
        root = Node(text="", parent=None, edge_label_to_child={"f": fooNode, "b": barNode}, tenant_to_last_access_time={"tenant_1": 2})
        fooNode = Node(text="foo", parent=root, edge_label_to_child={}, tenant_to_last_access_time={"tenant_1": 1})
        barNode = Node(text="bar", parent=root, edge_label_to_child={}, tenant_to_last_access_time={"tenant_1": 2})

        In the above example, "foo" was inserted at time 1, and "bar" was inserted at time 2.
        It follows that root was last accessed at time 2.
    """

    def __init__(self, text: str = "", parent: Optional[Node] = None) -> None:
        """
        Initialize a node in the prefix tree.

        Args:
            text: The text segment this node represents
            parent: The parent node of this node
        """
        self.text: str = text
        self.parent: Optional[Node] = parent  # The parent node of this node
        self.edge_label_to_child: Dict[
            str, Node
        ] = {}  # Maps first character to child node
        self.tenant_to_last_access_time: Dict[
            str, float
        ] = (
            {}
        )  # For each tenant that has inserted text matching this node, maps tenant to the last access timestamp (in seconds)
        # Doubly linked list pointers for LRU tracking per tenant
        self.tenant_to_older_node: Dict[
            str, Optional[Node]
        ] = {}  # Points to the less recently used node (toward tail for eviction)
        self.tenant_to_newer_node: Dict[
            str, Optional[Node]
        ] = {}  # Points to the more recently used node (toward head for retention)


class TimestampedNode:
    """
    Wrapper class for storing nodes in a min-heap, ordered by timestamp.
    Used for efficient LRU eviction of nodes.
    """

    def __init__(self, node: Node, time_sec: float) -> None:
        """
        Initialize a heap node for efficient LRU eviction of nodes.

        Args:
            node: The prefix tree node this heap node refers to
            time_sec: The timestamp this heap uses to order nodes
        """
        self.node = node
        self.time_sec = time_sec

    def __lt__(self, other: TimestampedNode) -> bool:
        """
        Compare heap nodes based on timestamp.

        Args:
            other: Another TimestampedNode to compare with

        Returns:
            True if this node's timestamp is earlier than the other's
        """
        return self.time_sec < other.time_sec


class PrefixTree:
    """
    Thread-safe multi-tenant prefix tree (approximate radix tree).

    Features:
    1. Stores data for multiple tenants in the same tree structure
    2. Thread-safe with node-level locking for concurrent access
    3. LRU eviction based on tenant access time
    4. Efficient prefix matching across multiple tenants


    Example tree structure:
        Representing the strings inserted in order:
            - "helloworld"  at time 1 by tenant_1
            - "hellothere"  at time 2 by tenant_2
            - "hellothomas" at time 3 by tenant_2

        root: [] {tenant_1: 1, tenant_2: 3}
            (h) → [hello] {tenant_1: 1, tenant_2: 3}
                (w) → [world] {tenant_1: 1}
                (t) → [th]    {tenant_2: 3}
                    (e) → [ere] {tenant_2: 2}
                    (o) → [omas] {tenant_2: 3}

            Legend for each node:
            - [text] = Node.text
            - {tenant, timestamp} = Node.tenant_to_last_access_time
            - (x) = edge label (first character used as key for parent's children)

        PrefixTree instance variables:
            self.tenant_to_char_count = {"tenant_1": 10, "tenant_2": 14}
            self.tenant_to_nodes = {"tenant_1": {root, Node("hello"), Node("world")}, "tenant_2": {root, Node("hello"), Node("th"), Node("ere"), Node("omas")}}
    """

    def __init__(self) -> None:
        """Initialize an empty prefix tree."""
        self.lock: RLock = RLock()
        self.root: Node = Node()
        self.tenant_to_char_count: Dict[
            str, int
        ] = (
            {}
        )  # Tracks total character count per tenant. Used by the replica scheduler to determine which tenant to evict, and by how much.
        self.tenant_to_nodes: Dict[
            str, Set[Node]
        ] = (
            {}
        )  # Maps tenant to set of nodes. Used for O(1) testing if a node belongs to a tenant. The keys are the active tenants in the tree.

        # LRU tracking - head is the most recently used node, tail is the least recently used
        self.tenant_to_lru_head: Dict[str, Optional[Node]] = {}
        self.tenant_to_lru_tail: Dict[str, Optional[Node]] = {}

    @staticmethod
    def _shared_prefix_count(a: str, b: str) -> int:
        """
        Count the number of shared characters at the beginning of two strings.

        Args:
            a: First string
            b: Second string

        Returns:
            Number of matching characters at the beginning
        """
        return len(os.path.commonprefix([a, b]))

    def _reset(self) -> None:
        """
        Reset the tree to an empty state.

        Note: This method is intended to be used only in tests.
        """
        with self.lock:
            self.root = Node()
            self.tenant_to_char_count = {}
            self.tenant_to_nodes = {}
            self.tenant_to_lru_head = {}
            self.tenant_to_lru_tail = {}

    def _add_tenant(self, tenant: str) -> None:
        """
        Add a new tenant to the tree.

        If the tenant already exists, this is a no-op with a warning log.

        Args:
            tenant: Tenant to add
        """
        with self.lock:
            if tenant in self.tenant_to_nodes:
                logger.warning(f"Tenant '{tenant}' already exists. No action taken.")
                return

            self.tenant_to_char_count[tenant] = 0
            self.tenant_to_nodes[tenant] = set()
            self.tenant_to_lru_head[tenant] = None
            self.tenant_to_lru_tail[tenant] = None

    def _move_node_to_head(self, node: Node, tenant: str) -> None:
        """
        Move a node to the head of the tenant's LRU list.

        Args:
            node: Node to move
            tenant: Tenant that owns the node
        """
        # If this is the first node, initialize the LRU list
        if self.tenant_to_lru_head.get(tenant) is None:
            self.tenant_to_lru_head[tenant] = node
            self.tenant_to_lru_tail[tenant] = node
            node.tenant_to_older_node[tenant] = None
            node.tenant_to_newer_node[tenant] = None
            return

        # If node is already the head, nothing to do
        if node == self.tenant_to_lru_head[tenant]:
            return

        # If node is already in the list, remove it
        if tenant in node.tenant_to_older_node or tenant in node.tenant_to_newer_node:
            # Connect older and newer nodes directly (skip this node)
            older = node.tenant_to_older_node.get(
                tenant
            )  # Less recently used (toward tail)
            newer = node.tenant_to_newer_node.get(
                tenant
            )  # More recently used (toward head)

            if older:
                older.tenant_to_newer_node[tenant] = newer

            if newer:
                newer.tenant_to_older_node[tenant] = older

            # If this is the tail, update tail pointer
            if node == self.tenant_to_lru_tail[tenant]:
                self.tenant_to_lru_tail[tenant] = newer

        # Place at head of list
        current_head = self.tenant_to_lru_head[tenant]
        node.tenant_to_newer_node[tenant] = None  # Head has no newer node
        node.tenant_to_older_node[
            tenant
        ] = current_head  # Old head becomes older than new head
        current_head.tenant_to_newer_node[tenant] = node  # Connect old head to new head
        self.tenant_to_lru_head[tenant] = node  # Update head pointer

    def _remove_tenant_single_node(self, tenant: str, node: Node) -> int:
        """
        Remove a tenant from a single node.

        Args:
            tenant: Tenant to remove
            node: Node to remove tenant from

        Returns:
            Number of characters removed (0 if preconditions not met)
        """
        with self.lock:
            if tenant not in self.tenant_to_nodes:
                logger.warning(f"Tenant '{tenant}' does not exist. No action taken.")
                return 0
            if tenant not in node.tenant_to_last_access_time:
                logger.warning(
                    f"Tenant '{tenant}' does not have node '{node.text}'. No action taken."
                )
                return 0
            if node not in self.tenant_to_nodes[tenant]:
                logger.warning(
                    f"Node '{node.text}' does not belong to tenant '{tenant}'. No action taken."
                )
                return 0

            removed_chars_len: int = len(node.text)
            self.tenant_to_char_count[tenant] -= removed_chars_len
            self.tenant_to_nodes[tenant].remove(node)
            node.tenant_to_last_access_time.pop(tenant, None)

            # Remove from LRU list
            older = node.tenant_to_older_node.get(
                tenant
            )  # Less recently used (toward tail)
            newer = node.tenant_to_newer_node.get(
                tenant
            )  # More recently used (toward head)

            if older:
                older.tenant_to_newer_node[tenant] = newer

            if newer:
                newer.tenant_to_older_node[tenant] = older

            # Update head/tail pointers if necessary
            if node == self.tenant_to_lru_head[tenant]:
                self.tenant_to_lru_head[tenant] = older  # Older becomes new head
            if node == self.tenant_to_lru_tail[tenant]:
                self.tenant_to_lru_tail[tenant] = newer  # Newer becomes new tail

            node.tenant_to_older_node.pop(tenant, None)
            node.tenant_to_newer_node.pop(tenant, None)

            # Clean up empty nodes
            if not node.tenant_to_last_access_time and node.parent:
                if (
                    node.text and node.text[0] in node.parent.edge_label_to_child
                ):  # Defensive check
                    node.parent.edge_label_to_child.pop(node.text[0], None)

            return removed_chars_len

    def insert(self, text: str, tenant: str, time_sec: float) -> None:
        """
        Insert text into tree for a specific tenant.

        If the tenant doesn't exist, it will be automatically added.

        Args:
            text: Text to insert
            tenant: Tenant
            time_sec: Current timestamp in seconds

        Loop structure:
            1. At the start of each iteration, curr_node is a node we potentially update.
                e.g. Update node.tenant_to_last_access_time[tenant], self.tenant_to_char_count,
                self.tenant_to_nodes
            2. Each iteration then either:
                a. Breaks (if we've processed the entire string).
                b. Processes the next segment of text by:
                    1. If no child exists for the first character, create a new leaf node that matches the current text.
                    2. Then, match the current text with the child's text:
                        a. If they share a prefix (partial match), split the node and traverse into the new parent.
                        b. If they fully match, traverse into the child node.
        """
        with self.lock:
            if tenant not in self.tenant_to_nodes:
                self._add_tenant(tenant)

            curr_node: Node = self.root
            i: int = 0

            while i <= len(text):
                # Invariant at beginning of each iteration: assume curr_node has not been visited by tenant yet.
                # Update tenant info for current node.
                if tenant not in curr_node.tenant_to_last_access_time:
                    self.tenant_to_char_count[tenant] += len(curr_node.text)
                    self.tenant_to_nodes[tenant].add(curr_node)

                curr_node.tenant_to_last_access_time[tenant] = time_sec
                self._move_node_to_head(curr_node, tenant)

                if i == len(text):
                    break

                first_char: str = text[i]
                curr_text: str = text[i:]

                if first_char not in curr_node.edge_label_to_child:
                    # No match, create new node. Don't update new node as "visited" by tenant yet; it will be done at the beginning of the next iteration.
                    # e.g. curr_node.edge_label_to_child = {}, curr_text = "hello" -> curr_node.edge_label_to_child = {"h": Node("hello")}
                    new_node: Node = Node(text=curr_text, parent=curr_node)
                    curr_node.edge_label_to_child[first_char] = new_node

                # Match found, check if we need to split
                matched_node: Node = curr_node.edge_label_to_child[first_char]
                shared_count: int = self._shared_prefix_count(
                    matched_node.text, curr_text
                )

                if shared_count < len(matched_node.text):
                    # Partial match, split node at matched point
                    # Example:
                    ## Before update:
                    ### curr_node.edge_label_to_child = {"h": Node("helloworld")}, curr_text = "hellothere" -> shared_count = 5
                    ### matched_node = Node("helloworld")

                    ## After update:
                    ### curr_node.edge_label_to_child = {"h": Node("hello", edge_label_to_child = {"w": Node("world")})}
                    ### parent_node = Node("hello"), matched_node = Node("world")
                    ### Copy matched_node.tenant_to_last_access_time to parent_node.tenant_to_last_access_time
                    ### (new) curr_text = "there", (new) curr_node = parent_node
                    ### Continue adding "there" to tree in next iteration

                    matched_text: str = matched_node.text[:shared_count]
                    remaining_text: str = matched_node.text[shared_count:]

                    # Create new intermediate node
                    # Note that we don't update new_parent.tenant_to_last_access_time yet; it will be done at the beginning of the next iteration.
                    new_parent: Node = Node(text=matched_text, parent=curr_node)
                    new_parent.tenant_to_last_access_time = (
                        matched_node.tenant_to_last_access_time.copy()
                    )
                    for existing_tenant in new_parent.tenant_to_last_access_time:
                        self.tenant_to_nodes[existing_tenant].add(new_parent)
                        # Initialize LRU list pointers
                        new_parent.tenant_to_older_node[existing_tenant] = None
                        new_parent.tenant_to_newer_node[existing_tenant] = None
                        # Move to head of LRU list for each tenant
                        self._move_node_to_head(new_parent, existing_tenant)

                    # Update existing matched node
                    matched_node.text = remaining_text
                    matched_node.parent = new_parent

                    # Connect nodes
                    new_parent.edge_label_to_child[remaining_text[0]] = matched_node
                    curr_node.edge_label_to_child[first_char] = new_parent

                    # Continue traversal
                    curr_node = new_parent
                    i += shared_count
                else:
                    # Full match, continue down the tree
                    curr_node = matched_node
                    i += shared_count

    def prefix_match(
        self, text: str, available_tenants: Optional[List[str]] = None
    ) -> Tuple[str, Optional[List[str]]]:
        """
        Match text against tree and return matched text and matching tenants.

        Args:
            text: Text to match
            available_tenants: List of tenants to match against (or None for all)

        Returns:
            Tuple of (matched_text, matched_tenants):
                If the list of available tenants doesn't match any tenants in the tree: returns ("", None)
                When no prefix match is found (does not traverse further than the root node): returns ("", list of available tenants)
                When a prefix match is found: returns (matched_prefix, list of tenants that own the matched node)

        Note:
            A tenant is unable to be returned by prefix_match until it has inserted text into the tree, even if _add_tenant is called.
            The replica scheduler is responsible for inserting text into new replicas; it should not only rely on prefix_match to select replicas.
        """
        if available_tenants:
            # Filter available_tenants to only include those in the tree
            available_tenants = [
                tenant for tenant in available_tenants if tenant in self.tenant_to_nodes
            ]
            if not available_tenants:
                return "", None
        else:
            available_tenants = list(self.tenant_to_nodes.keys())

        with self.lock:
            curr_node: Node = self.root
            i: int = 0
            text_len: int = len(text)

            while i < text_len:
                first_char: str = text[i]
                curr_text: str = text[i:]

                if first_char in curr_node.edge_label_to_child:
                    matched_node: Node = curr_node.edge_label_to_child[first_char]

                    # Check if any available tenants match this node
                    if not any(
                        tenant in matched_node.tenant_to_last_access_time
                        for tenant in available_tenants
                    ):
                        break

                    shared_count: int = self._shared_prefix_count(
                        matched_node.text, curr_text
                    )
                    i += shared_count
                    curr_node = matched_node

                    if shared_count < len(matched_node.text):
                        # Partial match, stop here
                        break
                else:
                    # No match found, stop here
                    break

            # Find tenants in current node that match available tenants
            matched_tenants = [
                tenant
                for tenant in available_tenants
                if tenant in curr_node.tenant_to_last_access_time
            ] or None

            matched_text: str = text[:i]

            return matched_text, matched_tenants

    def remove_tenant(self, tenant: str) -> int:
        """
        Remove a tenant and all its nodes from the tree.
        Time complexity: O(n) where n is the number of nodes owned by the tenant.

        Args:
            tenant: Tenant to remove

        Returns:
            Number of characters removed (0 if tenant doesn't exist)
        """
        with self.lock:
            if tenant not in self.tenant_to_nodes:
                logger.warning(f"Tenant '{tenant}' does not exist. No action taken.")
                return 0

            total_chars_removed: int = 0
            for node in self.tenant_to_nodes[tenant].copy():
                total_chars_removed += self._remove_tenant_single_node(tenant, node)

            self.tenant_to_nodes.pop(tenant, None)
            self.tenant_to_char_count.pop(tenant, None)
            self.tenant_to_lru_head.pop(tenant, None)
            self.tenant_to_lru_tail.pop(tenant, None)

            return total_chars_removed

    def evict_tenant_by_lru(self, tenant: str, min_remove_size: int) -> int:
        """
        Evict least recently used nodes for a tenant until minimum size is freed.
        Time complexity: O(m) where m is the number of nodes removed.

        Args:
            tenant: The tenant to evict nodes from
            min_remove_size: Minimum number of characters to remove

        Returns:
            Actual number of characters removed (0 if tenant doesn't exist)

        Note:
            - All nodes with the same oldest access time are removed together to maintain tree integrity, even if only removing a subset of them satisfies the min_remove_size.
            - This behavior is expected in the case when an input was split into multiple nodes by a different tenant (e.g. insert("helloworld", "tenant_1", 1) and insert("hellothere", "tenant_2", 2)).
              because "hello" and "world" were inserted as a package, and so should be removed as a package.
            - However, if two inputs happen to be inserted at the same time (e.g. insert("helloworld", "tenant_1", 1) and insert("hellothere", "tenant_2", 1)),
              then both "chains" will be removed by our method. This may not reflect the actual KV cache eviction policy.
            - For more predictable eviction, use unique timestamps for each insertion.
        """
        with self.lock:
            if tenant not in self.tenant_to_nodes or not self.tenant_to_nodes[tenant]:
                logger.warning(
                    f"Cannot evict tenant '{tenant}': tenant does not exist or has no nodes. No action taken."
                )
                return 0

            if self.tenant_to_char_count[tenant] < min_remove_size:
                logger.warning(
                    f"Cannot evict {min_remove_size} characters from tenant '{tenant}', which has only "
                    f"{self.tenant_to_char_count[tenant]} characters. Will remove all available characters."
                )
                min_remove_size = self.tenant_to_char_count[tenant]

            total_chars_removed: int = 0

            # Start removing from the tail (least recently used)
            tail = self.tenant_to_lru_tail.get(tenant)

            # Continue until we've freed enough space or run out of nodes
            while total_chars_removed < min_remove_size and tail:
                # Get the current timestamp to remove all nodes with this timestamp
                current_timestamp = tail.tenant_to_last_access_time[tenant]
                nodes_with_same_timestamp = []

                # Collect all nodes with the same timestamp (guaranteed to be contiguous in our LRU list)
                current = tail
                while (
                    current
                    and current.tenant_to_last_access_time[tenant] == current_timestamp
                ):
                    nodes_with_same_timestamp.append(current)
                    current = current.tenant_to_newer_node.get(tenant)

                # Set the new tail to continue from for the next iteration (if needed)
                tail = current

                # Remove all collected nodes with the same timestamp
                for node in nodes_with_same_timestamp:
                    total_chars_removed += self._remove_tenant_single_node(tenant, node)

            return total_chars_removed

    def get_smallest_tenant(self) -> Optional[str]:
        """
        Get the tenant with the smallest total character count.

        Returns:
            Tenant with smallest character count, or None if no tenants
        """
        with self.lock:
            if not self.tenant_to_char_count:
                return None

            return min(
                self.tenant_to_char_count,
                key=self.tenant_to_char_count.get,
                default=None,
            )


@ray.remote
class PrefixTreeActor(PrefixTree):
    def _to_dict(self) -> Dict[str, Any]:
        """
        Convert tree to dictionary for serialization.

        Returns:
            Dictionary representation of the tree

        Note: This method is intended to be used only in tests.
        """
        return {
            "root": self.root,
            "tenant_to_char_count": self.tenant_to_char_count,
            "tenant_to_nodes": self.tenant_to_nodes,
        }
