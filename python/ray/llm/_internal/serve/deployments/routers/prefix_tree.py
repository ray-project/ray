from ray import serve
import time
from collections import defaultdict
from threading import RLock
from typing import Optional, List, Tuple, Dict, Set


class Node:
    """
    Node in a prefix tree that tracks tenant access time.

    Each node represents a segment of text and can belong to multiple tenants.
    """

    def __init__(self, text: str = "", parent: Optional["Node"] = None):
        self.text: str = text
        self.parent: Optional["Node"] = parent
        self.children: Dict[str, "Node"] = {}  # Maps char -> Node
        self.tenant_last_access_time: Dict[
            str, int
        ] = {}  # Maps tenant -> timestamp in ms (int)

    def to_string(self) -> str:
        return f"Node(text='{self.text}', parent={self.parent}, children={self.children}, tenant_last_access_time={self.tenant_last_access_time})"


@serve.deployment(name="TreeDeployment")
class PrefixTree:
    """
    Thread-safe multi-tenant prefix tree (approximate radix tree).

    Features:
    1. Stores data for multiple tenants in the same tree structure
    2. Node-level locking for concurrent access
    3. Leaf LRU eviction based on tenant access time
    """

    def __init__(self) -> None:
        self.lock: RLock = RLock()
        self.root: Node = Node()
        self.tenants: Set[str] = set()
        self.tenant_char_count: Dict[str, int] = {}
        self.tenant_nodes: Dict[str, Set[Node]] = {}

    def reset(self) -> None:
        """Reset the tree to an empty state."""
        with self.lock:
            self.root = Node()
            self.tenants = set()
            self.tenant_char_count = {}
            self.tenant_nodes = {}

    def to_dict(self) -> Dict:
        return {
            "root": self.root,
            "tenants": self.tenants,
            "tenant_char_count": self.tenant_char_count,
            "tenant_nodes": self.tenant_nodes
        }

    def to_string(self) -> str:
        return f"PrefixTree(root={self.root.__str__()}, tenants={self.tenants}, tenant_char_count={self.tenant_char_count}, tenant_nodes={self.tenant_nodes})"

    @staticmethod
    def shared_prefix_count(a: str, b: str) -> int:
        """Count the number of shared characters at the beginning of two strings."""
        i: int = 0
        for char_a, char_b in zip(a, b):
            if char_a == char_b:
                i += 1
            else:
                break
        return i

    # def insert(self, text: str, tenant: str) -> Node:
    #     """Insert text into tree with given tenant. Returns the node that was inserted (or the existing node if it was updated)."""
    #     with self.lock:
    #         if tenant not in self.tenants:
    #             raise ValueError(f"Cannot insert text for tenant '{tenant}': tenant does not exist")

    #         curr_node: Node = self.root
    #         timestamp_ms: int = int(time.time() * 1000)
    #         i: int = 0
    #         while i < len(text):
    #             self.tenant_nodes[tenant].add(curr_node)

    #             first_char: str = text[i]
    #             curr_text: str = text[i:]
    #             if first_char not in curr_node.children:
    #                 # No match, create new node
    #                 # e.g. curr_node.children = {}, curr_text = "hello" -> curr_node.children = {"h": Node("hello")}
    #                 new_node: Node = Node(text=curr_text, parent=curr_node)
    #                 new_node.tenant_last_access_time[tenant] = timestamp_ms
    #                 curr_node.children[first_char] = new_node

    #             # Match found, check if need to split
    #             matched_node: Node = curr_node.children[first_char]
    #             shared_count: int = self.shared_prefix_count(
    #                 matched_node.text, curr_text
    #             )
    #             if shared_count == len(matched_node.text):
    #                 # Full match, move down the tree
    #                 if tenant not in matched_node.tenant_last_access_time:
    #                     self.tenant_char_count[tenant] += shared_count
    #                 matched_node.tenant_last_access_time[tenant] = timestamp_ms
    #                 curr_node = matched_node
    #             else:
    #                 # Partial match, split at matched point
    #                 matched_text: str = matched_node.text[:shared_count]
    #                 remaining_text: str = matched_node.text[shared_count:]
    #                 new_parent: Node = Node(text=matched_text, parent=curr_node)
    #                 matched_node.text = remaining_text
    #                 matched_node.parent = new_parent
    #                 new_parent.children[remaining_text[0]] = matched_node
    #                 if tenant not in new_parent.tenant_last_access_time:
    #                     self.tenant_char_count[tenant] += shared_count
    #                 new_parent.tenant_last_access_time[tenant] = timestamp_ms
    #                 curr_node = new_parent

    #             i += shared_count

    #         self.tenant_nodes[tenant].add(curr_node)
    #         return curr_node
            
    def insert(self, text: str, tenant: str) -> Node:
        """Insert text into tree with given tenant. Returns the node that was inserted (or the existing node if it was updated)."""
        with self.lock:
            if tenant not in self.tenants:
                raise ValueError(f"Cannot insert text for tenant '{tenant}': tenant does not exist")

            curr_node: Node = self.root
            timestamp_ms: int = int(time.time() * 1000)
            i: int = 0
            while i < len(text):
                curr_node.tenant_last_access_time[tenant] = timestamp_ms
                self.tenant_nodes[tenant].add(curr_node)

                first_char: str = text[i]
                curr_text: str = text[i:]
                if first_char not in curr_node.children:
                    # No match, create new node
                    # e.g. curr_node.children = {}, curr_text = "hello" -> curr_node.children = {"h": Node("hello")}
                    new_node: Node = Node(text=curr_text, parent=curr_node)
                    new_node.tenant_last_access_time[tenant] = timestamp_ms

                    # Increment char count for tenant and add node to tenant_nodes
                    self.tenant_char_count[tenant] += len(curr_text)
                    self.tenant_nodes[tenant].add(new_node)

                    curr_node.children[first_char] = new_node
                else:
                    # Match found, check if need to split
                    matched_node: Node = curr_node.children[first_char]
                    shared_count: int = self.shared_prefix_count(
                        matched_node.text, curr_text
                    )

                    if shared_count < len(matched_node.text):
                        # Partial match, split at matched point
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

                        # Update tenant char count for the new split node
                        if tenant not in matched_node.tenant_last_access_time:
                            self.tenant_char_count[tenant] += shared_count

                        # Create new parent node
                        new_parent: Node = Node(text=matched_text, parent=curr_node)
                        new_parent.tenant_last_access_time = (
                            matched_node.tenant_last_access_time.copy()
                        )
                        # Update matched_node
                        matched_node.text = remaining_text
                        matched_node.parent = new_parent

                        # Connect new parent node to matched_node
                        new_parent.children[remaining_text[0]] = matched_node

                        # Connect current node to new parent
                        curr_node.children[first_char] = new_parent

                        # Move down the tree
                        curr_node = new_parent
                        i += shared_count
                    else:
                        # Full match

                        # Update tenant char count if this is a new tenant for this node
                        if tenant not in matched_node.tenant_last_access_time:
                            self.tenant_char_count[tenant] += shared_count

                        # # Update tenant last access time
                        # matched_node.tenant_last_access_time[tenant] = timestamp_ms

                        # Move down the tree
                        curr_node = matched_node
                        i += shared_count
            curr_node.tenant_last_access_time[tenant] = timestamp_ms
            self.tenant_nodes[tenant].add(curr_node)
            return curr_node
    def prefix_match(
        self, text: str, available_tenants: Optional[List[str]] = None
    ) -> Tuple[str, Optional[List[str]]]:
        """
        Match text against tree and return (matched_text, matched_tenants).
        Does not update access time for the matched tenants (only updates when insert() is called).
        If available_tenants is not provided, all tenants are considered.
        """
        if available_tenants:
            # Filter available_tenants to only include those that exist in the tree
            available_tenants = [
                tenant for tenant in available_tenants 
                if tenant in self.tenants
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

                    # Check if any of the available tenants match this node
                    if not any(
                        tenant in matched_node.tenant_last_access_time
                        for tenant in available_tenants
                    ):
                        break

                    shared_count: int = self.shared_prefix_count(
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

            # Select the tenants in available_tenants that are in the current node
            selected_tenants: Optional[List[str]] = None
            matching_tenants = [
                tenant
                for tenant in available_tenants
                if tenant in curr_node.tenant_last_access_time
            ]
            if matching_tenants:
                selected_tenants = matching_tenants

            ret_text: str = text[:i]
            return ret_text, selected_tenants


    def add_tenant(self, tenant: str) -> None:
        """Add a tenant to the tree."""
        with self.lock:
            if tenant in self.tenants:
                raise ValueError(f"Cannot add tenant '{tenant}': tenant already exists")

            self.tenants.add(tenant)
            self.tenant_char_count[tenant] = 0
            self.tenant_nodes[tenant] = set()


    def remove_tenant(self, tenant: str) -> int:
        """Remove a tenant's nodes from the tree, returns the number of characters removed. Also removes the tenant from tenants, tenant_char_count, and tenant_nodes."""
        with self.lock:
            if tenant not in self.tenants:
                raise ValueError(f"Cannot remove tenant '{tenant}': tenant does not exist")

            total_chars_removed: int = 0
            for node in self.tenant_nodes[tenant].copy():
                total_chars_removed += self.remove_tenant_single_node(tenant, node)
            
            self.tenants.remove(tenant)
            self.tenant_nodes.pop(tenant, None)
            self.tenant_char_count.pop(tenant, None)
            return total_chars_removed


    def remove_tenant_single_node(self, tenant: str, node: Node) -> int:
        """Remove a single node belonging to a tenant, returns the number of characters removed."""
        with self.lock:
            if tenant not in self.tenants:
                raise ValueError(f"Cannot remove tenant '{tenant}': tenant does not exist")
            if node not in self.tenant_nodes[tenant] or tenant not in node.tenant_last_access_time:
                raise ValueError(f"Cannot remove node '{node.text}' from tenant '{tenant}': tenant does not have this node")

            removed_chars_len: int = len(node.text)
            self.tenant_char_count[tenant] -= removed_chars_len
            self.tenant_nodes[tenant].remove(node)
            node.tenant_last_access_time.pop(tenant, None)
            # If this node has no more tenants, remove it from the parent
            if not node.tenant_last_access_time and node.parent:
                node.parent.children.pop(node.text[0], None)

            return removed_chars_len


    def evict_tenant_by_LRU(self, tenant: str, min_remove_size: int) -> int:
        """Evict nodes from a tenant until the removed character count is at least min_remove_size.

        Args:
            tenant: The tenant to evict nodes from
            min_remove_size: Minimum number of characters to remove

        Returns:
            int: The actual number of characters removed
        """
        with self.lock:
            if tenant not in self.tenant_nodes or not self.tenant_nodes[tenant]:
                raise ValueError(f"Cannot evict tenant '{tenant}': tenant does not exist or has no nodes")

            if self.tenant_char_count[tenant] < min_remove_size:
                raise ValueError(f"Cannot evict tenant '{tenant}': total character count is less than min_remove_size")

            # Sort nodes by last access time (oldest first)
            nodes_to_evict = sorted(
                self.tenant_nodes[tenant],
                key=lambda node: node.tenant_last_access_time.get(tenant, 0),
            )


            total_chars_removed: int = 0

            # Remove nodes until we've reached the minimum removal size
            for node in nodes_to_evict.copy():
                # Use existing function to remove tenant from node
                total_chars_removed += self.remove_tenant_single_node(tenant, node)

                # Check if we've removed enough characters
                if total_chars_removed >= min_remove_size:
                    break

            return total_chars_removed


    def get_smallest_tenant(self) -> Optional[str]:
        """Get the tenant with the smallest total character count."""
        with self.lock:
            if not self.tenant_char_count:
                return None

            return min(self.tenant_char_count.items(), key=lambda x: x[1])[0]