from ray import serve
import time
from collections import defaultdict
from threading import Lock
from typing import Optional, List, Tuple, Dict


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

    def __str__(self) -> str:
        return f"Node(text='{self.text}', tenants={list(self.tenant_last_access_time.keys())})"


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
        self.root: Node = Node()
        self.tenant_char_count: Dict[str, int] = defaultdict(
            int
        )  # Maps tenant -> character count
        self.lock: Lock = Lock()  # For operations that need to lock the entire tree
        self.tenant_nodes: Dict[str, List[Node]] = defaultdict(
            list
        )  # Maps tenant -> list of nodes it belongs to

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

    def insert(self, text: str, tenant: str) -> None:
        """Insert text into tree with given tenant."""
        with self.lock:
            curr_node: Node = self.root
            timestamp_ms: int = int(time.time() * 1000)
            i: int = 0
            while i < len(text):
                curr_node.tenant_last_access_time[tenant] = timestamp_ms
                first_char: str = text[i]
                curr_text: str = text[i:]
                if first_char not in curr_node.children:
                    # No match, create new node
                    # e.g. curr_node.children = {}, curr_text = "hello" -> curr_node.children = {"h": Node("hello")}
                    new_node: Node = Node(text=curr_text, parent=curr_node)
                    new_node.tenant_last_access_time[tenant] = timestamp_ms

                    # Increment char count for tenant and add node to tenant_nodes
                    self.tenant_char_count[tenant] += len(curr_text)
                    self.tenant_nodes[tenant].append(new_node)

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
                        self.tenant_nodes[tenant].append(new_parent)
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

    def prefix_match(
        self, text: str, available_tenants: Optional[List[str]] = None
    ) -> Tuple[str, Optional[List[str]]]:
        """
        Match text against tree and return (matched_text, matched_tenants).
        Does not update access time for the matched tenants (only updates when insert() is called).
        """
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
                    if available_tenants:
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
            if available_tenants:
                matching_tenants = [
                    tenant
                    for tenant in available_tenants
                    if tenant in curr_node.tenant_last_access_time
                ]
                if matching_tenants:
                    selected_tenants = matching_tenants
            else:
                if curr_node.tenant_last_access_time:
                    selected_tenants = list(curr_node.tenant_last_access_time)

            ret_text: str = text[:i]
            return ret_text, selected_tenants

    def get_smallest_tenant(self) -> Optional[str]:
        """Get the tenant with the smallest total character count."""
        with self.lock:
            if not self.tenant_char_count:
                # Return first worker if no data yet
                return None

            return min(self.tenant_char_count.items(), key=lambda x: x[1])[0]

    # def evict_tenant_by_size(self, max_size: int) -> None:
    #     """Evict nodes for tenants that exceed the maximum tree size."""
    #     with self.lock:
    #         # Get total tree size
    #         total_size: int = sum(self.tenant_char_count.values())

    #         # If tree is smaller than max size, no need to evict
    #         if total_size <= max_size:
    #             return

    #         # Calculate how much we need to evict
    #         excess: int = total_size - max_size

    #         # Sort tenants by size (largest first)
    #         sorted_tenants: List[Tuple[str, int]] = sorted(
    #             self.tenant_char_count.items(),
    #             key=lambda x: x[1],
    #             reverse=True
    #         )

    #         # Evict from largest tenants first
    #         for tenant, size in sorted_tenants:
    #             # If we've evicted enough, stop
    #             if excess <= 0:
    #                 break

    #             # Calculate how much to evict from this tenant
    #             # Evict at most half of the tenant's size
    #             evict_amount: int = min(excess, size // 2)

    #             if evict_amount > 0:
    #                 # print(f"Evicting {evict_amount} chars from tenant {tenant}")
    #                 self.tenant_char_count[tenant] -= evict_amount
    #                 excess -= evict_amount

    #         # print(f"Tree eviction complete. New size: {sum(self.tenant_char_count.values())}")

    def get_tenant_char_count(self) -> Dict[str, int]:
        """Get character count for each tenant."""
        with self.lock:
            return dict(self.tenant_char_count)

    def remove_tenant(self, tenant: str) -> None:
        """Remove all nodes belonging to a tenant."""
        # Would require a traversal of the tree and removing the tenant
        # from tenant_last_access_time. Simplifying for now.
        with self.lock:
            for node in self.tenant_nodes[tenant]:
                node.tenant_last_access_time.pop(tenant, None)
                if not node.tenant_last_access_time:
                    node.parent.children.pop(node.text[0], None)
            self.tenant_char_count.pop(tenant, None)
            self.tenant_nodes.pop(tenant, None)
