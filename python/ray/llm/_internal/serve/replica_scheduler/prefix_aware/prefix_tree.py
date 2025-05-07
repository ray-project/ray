from __future__ import annotations

import heapq
import logging
import os
from threading import RLock
from typing import Dict, List, Optional, Set, Tuple, Any

from ray import serve

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
        self.edge_label_to_child: Dict[str, Node] = {}  # Maps first character to child node
        self.tenant_to_last_access_time: Dict[
            str, int
        ] = (
            {}
        )  # For each tenant that has inserted text matching this node, maps tenant to the last access timestamp (in milliseconds)

class TenantHeapNode:
    """
    Wrapper class for storing nodes in a min-heap, ordered by tenant access time.
    Used for efficient LRU eviction of tenant nodes.
    """

    def __init__(self, node: Node, tenant_ordering_key: str) -> None:
        """
        Initialize a heap node for efficient LRU tenant management.

        Args:
            node: The prefix tree node this heap node refers to
            tenant_ordering_key: The tenant this heap uses to order nodes
        """
        self.node = node
        self.tenant_ordering_key = tenant_ordering_key

    def __lt__(self, other: TenantHeapNode) -> bool:
        """
        Compare heap nodes based on tenant's last access time.

        Args:
            other: Another TenantHeapNode to compare with

        Returns:
            True if this node's tenant access time is earlier than the other's
        """
        return (
            self.node.tenant_to_last_access_time[self.tenant_ordering_key]
            < other.node.tenant_to_last_access_time[other.tenant_ordering_key]
        )


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
        ] = {}  # Tracks total character count per tenant. Used by the client to determine which tenant to evict, and by how much.
        self.tenant_to_nodes: Dict[
            str, Set[Node]
        ] = {}  # Maps tenant to set of nodes. Used for O(1) testing if a node belongs to a tenant. The keys are the active tenants in the tree.

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

    def _remove_tenant_single_node(self, tenant: str, node: Node) -> int:
        """
        Remove a tenant from a single node.

        This function expects valid input where:
        - tenant exists in self.tenant_to_nodes
        - tenant exists in node.tenant_to_last_access_time
        - node exists in self.tenant_to_nodes[tenant]
        
        These preconditions are guaranteed to be satisfied if the user is using the public methods of this class.
        They may be violated if the user manipulates the internal state of the tree directly.

        Args:
            tenant: Tenant to remove
            node: Node to remove tenant from

        Does:
            Decrements self.tenant_to_char_count[tenant] by the length of the node's text.
            Removes the tenant from node.tenant_to_last_access_time.
            Removes the node from self.tenant_to_nodes[tenant].

        Returns:
            Number of characters removed (0 if preconditions not met)
        """
        with self.lock:
            if tenant not in self.tenant_to_nodes:
                logger.warning(f"Tenant '{tenant}' does not exist. No action taken.")
                return 0
            if tenant not in node.tenant_to_last_access_time:
                logger.warning(f"Tenant '{tenant}' does not have node '{node.text}'. No action taken.")
                return 0
            if node not in self.tenant_to_nodes[tenant]:
                logger.warning(f"Node '{node.text}' does not belong to tenant '{tenant}'. No action taken.")
                return 0

            removed_chars_len: int = len(node.text)
            self.tenant_to_char_count[tenant] -= removed_chars_len
            self.tenant_to_nodes[tenant].remove(node)
            node.tenant_to_last_access_time.pop(tenant, None)

            # Clean up empty nodes
            if not node.tenant_to_last_access_time and node.parent:
                if (
                    node.text and node.text[0] in node.parent.edge_label_to_child
                ):  # Defensive check
                    node.parent.edge_label_to_child.pop(node.text[0], None)

            return removed_chars_len

    def insert(self, text: str, tenant: str, time_sec: float) -> Node:
        """
        Insert text into tree for a specific tenant.

        If the tenant doesn't exist, it will be automatically added.

        Args:
            text: Text to insert
            tenant: Tenant
            time_sec: Current timestamp in seconds

        Returns:
            The node that was inserted or updated

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

                if i == len(text):
                    break

                first_char: str = text[i]
                curr_text: str = text[i:]

                if first_char not in curr_node.edge_label_to_child:
                    # No match, create new node. Don't update new node as "visited" by tenant yet; it will be done in the code below.
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

                    ## During update:
                    ### Increment tenant_to_char_count[tenant] by shared_count if matched_node has not seen this tenant before

                    ## After update:
                    ### curr_node.edge_label_to_child = {"h": Node("hello", edge_label_to_child = {"w": Node("world")})}
                    ### parent_node = Node("hello"), matched_node = Node("world")
                    ### Update tenant_to_last_access_time for parent_node, NOT matched_node
                    ### (new) curr_text = "there", (new) curr_node = parent_node
                    ### Continue adding "there" to tree in next iteration

                    matched_text: str = matched_node.text[:shared_count]
                    remaining_text: str = matched_node.text[shared_count:]

                    # Create new intermediate node
                    new_parent: Node = Node(text=matched_text, parent=curr_node)
                    new_parent.tenant_to_last_access_time = (
                        matched_node.tenant_to_last_access_time.copy()
                    )
                    for existing_tenant in new_parent.tenant_to_last_access_time:
                        self.tenant_to_nodes[existing_tenant].add(new_parent)

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

            return curr_node

    def prefix_match(
        self, text: str, available_tenants: Optional[List[str]] = None
    ) -> Tuple[str, Optional[List[str]]]:
        """
        Match text against tree and return matched text and matching tenants.

        Args:
            text: Text to match
            available_tenants: List of tenants to match against (or None for all)

        Returns:
            Tuple of (matched_text, matched_tenants)
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

        Args:
            tenant: Tenant to remove

        Returns:
            Number of characters removed
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

            return total_chars_removed

    def evict_tenant_by_lru(self, tenant: str, min_remove_size: int) -> int:
        """
        Evict least recently used nodes for a tenant until minimum size is freed.

        Args:
            tenant: The tenant to evict nodes from
            min_remove_size: Minimum number of characters to remove

        Returns:
            Actual number of characters removed
        
        Note:
            - All nodes with the same oldest access time are removed together to maintain tree integrity, even if only removing a subset of them satisfies the min_remove_size.
            - This behavior is expected in the case when an input was split into multiple nodes by a different tenant (e.g. insert("helloworld", "tenant_1", 1) and insert("hellothere", "tenant_2", 2)).
              because there is no reason to only remove "world" from tenant 1. So we remove the "chain" of "hello" and "world" from tenant 1.
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

            # Create a min-heap of nodes ordered by access time
            # Each entry is a tuple of (access_time, node) so heapq sorts by access_time first
            nodes_by_access_time = []
            for node in self.tenant_to_nodes[tenant]:
                access_time = node.tenant_to_last_access_time[tenant]
                nodes_by_access_time.append((access_time, node))
            heapq.heapify(nodes_by_access_time)
            
            # Remove nodes until we've freed enough characters
            while (
                total_chars_removed < min_remove_size
                and nodes_by_access_time
            ):
                # Get the oldest (minimum) access time from the top of the heap
                oldest_access_time = nodes_by_access_time[0][0]

                # Remove ALL nodes with this same access time to maintain tree consistency
                # (partial removals could break prefix relationships)
                while (
                    nodes_by_access_time
                    and nodes_by_access_time[0][0] == oldest_access_time
                ):
                    _, node_to_remove = heapq.heappop(nodes_by_access_time)
                    total_chars_removed += self._remove_tenant_single_node(
                        tenant, node_to_remove
                    )

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

            return min(self.tenant_to_char_count, key=self.tenant_to_char_count.get, default=None)


@serve.deployment(name="TreeDeployment")
class PrefixTreeDeployment(PrefixTree):
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
