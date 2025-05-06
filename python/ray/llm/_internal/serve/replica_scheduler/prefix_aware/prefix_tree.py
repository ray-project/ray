from __future__ import annotations

import heapq
import logging
import os
from threading import RLock
from typing import Dict, List, Optional, Set, Tuple

from ray import serve

# Logger for this module
logger = logging.getLogger(__name__)


class Node:
    """
    Node in a prefix tree that tracks tenant access time.

    Each node represents a segment of text and can belong to multiple tenants.

    Example tree structure:
        Representing the strings inserted in order:
            - "helloworld"  at time 1 by tenant_1
            - "hellothere"  at time 2 by tenant_2
            - "hellothomas" at time 3 by tenant_2

               root: []
         {tenant_1: 1, tenant_2: 3}
                      |
                   (h)|
                      |
                   [hello]
          {tenant_1: 1, tenant_2: 3}
                   /     \
               (w)/       \\(t)
                 /         \
             [world]      [th]
         {tenant_1: 1} {tenant_2: 3}
                          /  \
                      (e)/    \\(o)
                        /      \
                     [ere]   [omas]
              {tenant_2: 2} {tenant_2: 3}
          
        Legend for each node:
        - [text] = Node.text
        - {tenant, timestamp} = Node.tenant_last_access_time
        - (x) = edge label (first character used as key for parent's children)
    """

    def __init__(self, text: str = "", parent: Optional[Node] = None) -> None:
        """
        Initialize a node in the prefix tree.

        Args:
            text: The text segment this node represents
            parent: The parent node of this node
        """
        self.text: str = text
        self.parent: Optional[Node] = parent
        self.children: Dict[str, Node] = {}  # Maps first character to child node
        self.tenant_last_access_time: Dict[str, int] = (
            {}
        )  # Maps tenant ID to last access timestamp (in milliseconds)

    def __repr__(self) -> str:
        return f"Node(text='{self.text}', children={list(self.children.keys())}, tenants={list(self.tenant_last_access_time.keys())})"


class TenantHeapNode:
    """
    Wrapper class for storing nodes in a min-heap, ordered by tenant access time.
    Used for efficient LRU eviction of tenant nodes.
    """

    def __init__(self, node: Node, tenant: str) -> None:
        """
        Initialize a heap node for efficient LRU tenant management.

        Args:
            node: The prefix tree node this heap node refers to
            tenant: The tenant ID this heap node is associated with
        """
        self.node = node
        self.tenant_ordering_key = tenant

    def __lt__(self, other: TenantHeapNode) -> bool:
        """
        Compare heap nodes based on tenant's last access time.

        Args:
            other: Another TenantHeapNode to compare with

        Returns:
            True if this node's tenant access time is earlier than the other's
        """
        return (
            self.node.tenant_last_access_time[self.tenant_ordering_key]
            < other.node.tenant_last_access_time[other.tenant_ordering_key]
        )

    def __repr__(self) -> str:
        return f"TenantHeapNode(node={self.node}, tenant_ordering_key={self.tenant_ordering_key})"


@serve.deployment(name="TreeDeployment")
class PrefixTree:
    """
    Thread-safe multi-tenant prefix tree (approximate radix tree).

    Features:
    1. Stores data for multiple tenants in the same tree structure
    2. Thread-safe with node-level locking for concurrent access
    3. LRU eviction based on tenant access time
    4. Efficient prefix matching across multiple tenants
    """

    def __init__(self) -> None:
        """Initialize an empty prefix tree."""
        self.lock: RLock = RLock()
        self.root: Node = Node()
        self.tenants: Set[str] = set()  # Set of tenant IDs
        self.tenant_char_count: Dict[str, int] = (
            {}
        )  # Tracks total character count per tenant
        self.tenant_nodes: Dict[str, Set[Node]] = (
            {}
        )  # Maps tenant ID to set of nodes belonging to that tenant
        self.tenant_nodes_sorted: Dict[str, List[TenantHeapNode]] = (
            {}
        )  # Maps tenant ID to heap of nodes for LRU eviction

    def reset(self) -> None:
        """Reset the tree to an empty state."""
        with self.lock:
            self.root = Node()
            self.tenants = set()
            self.tenant_char_count = {}
            self.tenant_nodes = {}
            self.tenant_nodes_sorted = {}

    def to_dict(self) -> Dict:
        """
        Convert tree to dictionary for serialization.

        Returns:
            Dictionary representation of the tree
        """
        return {
            "root": self.root,
            "tenants": self.tenants,
            "tenant_char_count": self.tenant_char_count,
            "tenant_nodes": self.tenant_nodes,
            "tenant_nodes_sorted": self.tenant_nodes_sorted,
        }

    def to_string(self) -> str:
        """String representation of the tree."""
        return f"PrefixTree(tenants={self.tenants}, tenant_char_count={self.tenant_char_count}, tenant_nodes={self.tenant_nodes}, tenant_nodes_sorted={self.tenant_nodes_sorted})"

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

    def insert(self, text: str, tenant: str, timestamp_ms: int) -> Node:
        """
        Insert text into tree for a specific tenant.

        If the tenant doesn't exist, it will be automatically added.

        Args:
            text: Text to insert
            tenant: Tenant ID
            timestamp_ms: Current timestamp in milliseconds

        Returns:
            The node that was inserted or updated
        """
        with self.lock:
            if tenant not in self.tenants:
                self._add_tenant(tenant)

            curr_node: Node = self.root
            i: int = 0

            while i <= len(text):
                # Invariant: assume curr_node has not been visited by tenant yet
                # Update tenant info for current node
                if tenant not in curr_node.tenant_last_access_time:
                    self.tenant_nodes[tenant].add(curr_node)
                    self.tenant_char_count[tenant] += len(curr_node.text)
                    self.tenant_nodes_sorted[tenant].append(
                        TenantHeapNode(curr_node, tenant)
                    )

                curr_node.tenant_last_access_time[tenant] = timestamp_ms
                heapq.heapify(self.tenant_nodes_sorted[tenant])

                if i == len(text):
                    break

                first_char: str = text[i]
                curr_text: str = text[i:]

                if first_char not in curr_node.children:
                    # No match, create new node. Don't update new node as "visited" by tenant yet; it will be done in the code below.
                    # e.g. curr_node.children = {}, curr_text = "hello" -> curr_node.children = {"h": Node("hello")}
                    new_node: Node = Node(text=curr_text, parent=curr_node)
                    curr_node.children[first_char] = new_node

                # Match found, check if we need to split
                matched_node: Node = curr_node.children[first_char]
                shared_count: int = self._shared_prefix_count(
                    matched_node.text, curr_text
                )

                if shared_count < len(matched_node.text):
                    # Partial match, split node at matched point
                    # Example:
                    ## Before update:
                    ### curr_node.children = {"h": Node("helloworld")}, curr_text = "hellothere" -> shared_count = 5
                    ### matched_node = Node("helloworld")

                    ## During update:
                    ### Increment tenant_char_count[tenant] by shared_count if matched_node has not seen this tenant before

                    ## After update:
                    ### curr_node.children = {"h": Node("hello", children = {"w": Node("world")})}
                    ### parent_node = Node("hello"), matched_node = Node("world")
                    ### Update tenant_last_access_time for parent_node, NOT matched_node
                    ### (new) curr_text = "there", (new) curr_node = parent_node
                    ### Continue adding "there" to tree in next iteration

                    matched_text: str = matched_node.text[:shared_count]
                    remaining_text: str = matched_node.text[shared_count:]

                    # Create new intermediate node
                    new_parent: Node = Node(text=matched_text, parent=curr_node)
                    new_parent.tenant_last_access_time = (
                        matched_node.tenant_last_access_time.copy()
                    )

                    # Update existing matched node
                    matched_node.text = remaining_text
                    matched_node.parent = new_parent

                    # Connect nodes
                    new_parent.children[remaining_text[0]] = matched_node
                    curr_node.children[first_char] = new_parent

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
            Tuple of (matched_text, matched_tenant_ids)
        """
        if available_tenants:
            # Filter available_tenants to only include those in the tree
            available_tenants = [
                tenant for tenant in available_tenants if tenant in self.tenants
            ]
            if not available_tenants:
                return "", None
        else:
            available_tenants = list(self.tenants)

        with self.lock:
            curr_node: Node = self.root
            i: int = 0
            text_len: int = len(text)

            while i < text_len:
                first_char: str = text[i]
                curr_text: str = text[i:]

                if first_char in curr_node.children:
                    matched_node: Node = curr_node.children[first_char]

                    # Check if any available tenants match this node
                    if not any(
                        tenant in matched_node.tenant_last_access_time
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
            matching_tenants = [
                tenant
                for tenant in available_tenants
                if tenant in curr_node.tenant_last_access_time
            ]

            selected_tenants: Optional[List[str]] = (
                matching_tenants if matching_tenants else None
            )
            matched_text: str = text[:i]

            return matched_text, selected_tenants

    def remove_tenant(self, tenant: str) -> int:
        """
        Remove a tenant and all its nodes from the tree.

        Args:
            tenant: Tenant ID to remove

        Returns:
            Number of characters removed

        Raises:
            ValueError: If tenant does not exist
        """
        with self.lock:
            if tenant not in self.tenants:
                raise ValueError(
                    f"Cannot remove tenant '{tenant}': tenant does not exist"
                )

            total_chars_removed: int = 0
            for node in self.tenant_nodes[tenant].copy():
                total_chars_removed += self._remove_tenant_single_node(tenant, node)

            self.tenants.remove(tenant)
            self.tenant_nodes.pop(tenant, None)
            self.tenant_char_count.pop(tenant, None)
            self.tenant_nodes_sorted.pop(tenant, None)

            return total_chars_removed

    def _remove_tenant_single_node(self, tenant: str, node: Node) -> int:
        """
        Remove a tenant from a single node.

        Args:
            tenant: Tenant ID to remove
            node: Node to remove tenant from

        Returns:
            Number of characters removed

        Raises:
            ValueError: If tenant does not exist or node doesn't belong to tenant
        """
        with self.lock:
            if tenant not in self.tenants:
                raise ValueError(
                    f"Cannot remove tenant '{tenant}': tenant does not exist"
                )

            if (
                node not in self.tenant_nodes[tenant]
                or tenant not in node.tenant_last_access_time
            ):
                raise ValueError(
                    f"Cannot remove node '{node.text}' from tenant '{tenant}': "
                    f"tenant does not have this node"
                )

            removed_chars_len: int = len(node.text)
            self.tenant_char_count[tenant] -= removed_chars_len
            self.tenant_nodes[tenant].remove(node)
            node.tenant_last_access_time.pop(tenant, None)

            # Clean up empty nodes
            if not node.tenant_last_access_time and node.parent:
                if (
                    node.text and node.text[0] in node.parent.children
                ):  # Defensive check
                    node.parent.children.pop(node.text[0], None)

            return removed_chars_len

    def evict_tenant_by_LRU(self, tenant: str, min_remove_size: int) -> int:
        """
        Evict least recently used nodes for a tenant until minimum size is freed.

        Args:
            tenant: The tenant to evict nodes from
            min_remove_size: Minimum number of characters to remove

        Returns:
            Actual number of characters removed

        Raises:
            ValueError: If tenant doesn't exist or has insufficient nodes
        """
        with self.lock:
            if tenant not in self.tenant_nodes or not self.tenant_nodes[tenant]:
                raise ValueError(
                    f"Cannot evict tenant '{tenant}': tenant does not exist or has no nodes"
                )

            if self.tenant_char_count[tenant] < min_remove_size:
                raise ValueError(
                    f"Cannot evict tenant '{tenant}': total character count "
                    f"({self.tenant_char_count[tenant]}) is less than min_remove_size "
                    f"({min_remove_size})"
                )

            total_chars_removed: int = 0

            # Directly use the tenant's priority queue
            while (
                total_chars_removed < min_remove_size
                and self.tenant_nodes_sorted[tenant]
            ):
                heap_node: TenantHeapNode = heapq.heappop(
                    self.tenant_nodes_sorted[tenant]
                )
                total_chars_removed += self._remove_tenant_single_node(
                    tenant, heap_node.node
                )

            return total_chars_removed

    def get_smallest_tenant(self) -> Optional[str]:
        """
        Get the tenant with the smallest total character count.

        Returns:
            Tenant ID with smallest character count, or None if no tenants
        """
        with self.lock:
            if not self.tenant_char_count:
                return None

        return min(self.tenant_char_count, key=self.tenant_char_count.get, default=None)

    def _add_tenant(self, tenant: str) -> None:
        """
        Add a new tenant to the tree.

        If the tenant already exists, this is a no-op with a warning log.

        Args:
            tenant: Tenant ID to add
        """
        with self.lock:
            if tenant in self.tenants:
                logger.warning(f"Tenant '{tenant}' already exists. No action taken.")
                return

            self.tenants.add(tenant)
            self.tenant_char_count[tenant] = 0
            self.tenant_nodes[tenant] = set()
            self.tenant_nodes_sorted[tenant] = []
