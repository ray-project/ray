from __future__ import annotations

import logging

from transformers.models.auto.tokenization_auto import AutoTokenizer

from ray.llm._internal.serve.request_router.prefix_aware.prefix_tree import (
    Node,
    PrefixTree,
)
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class PrefixTokenTree(PrefixTree):
    def __init__(self):
        super().__init__()
        self.tokenizer = AutoTokenizer.from_pretrained(
            "meta-llama/Meta-Llama-3-8B-Instruct"
        )

    def insert(self, text: str, tenant: str, time_s: float) -> None:
        """
        Insert text into tree for a specific tenant, but only if the tenant already exists.

        If the tenant doesn't exist in the tree, this will log a warning and return without
        inserting anything. Use add_tenants() first to add a new tenant.

        Args:
            text: Text to insert
            tenant: Tenant
            time_s: Current timestamp in seconds

        Loop structure:
            1. We update the current node at the start of each iteration of the while loop.
            This includes updating tenant_to_char_count and tenant_to_last_access_time, and moving the node to the front of the LRU list.
            2. Each iteration then either:
                a. Breaks (if we've processed the entire string).
                b. Processes the next segment of text by:
                    1. If no child exists for the first character, create a new leaf node that matches the current text.
                    2. Then, match the current text with the child's text:
                        a. If they share a prefix (partial match), split the node and traverse into the new parent.
                        b. If they fully match, traverse into the child node.
        """

        tokens = self.tokenizer.encode(text)
        tokens = ["<" + str(token) + ">" for token in tokens]

        with self.lock:
            if tenant not in self.tenant_to_char_count:
                logger.debug(
                    f"[_insert] Tenant '{tenant}' does not exist. Use add_tenants() first."
                )
                return

            curr_node: Node = self.root
            i: int = 0
            while i <= len(tokens):
                # Invariant at beginning of each iteration: assume curr_node has not been visited by tenant yet.
                # Update tenant info for current node.
                if tenant not in curr_node.tenant_to_last_access_time:
                    self.tenant_to_char_count[tenant] += len(curr_node.text)

                curr_node.tenant_to_last_access_time[tenant] = time_s
                if curr_node != self.root:
                    self._remove_node_from_linked_list(curr_node, tenant)
                    self._insert_node_into_linked_list(
                        curr_node,
                        self.root,
                        self.root.tenant_to_older_node.get(tenant),
                        tenant,
                    )
                if i == len(text):
                    break

                first_char: str = text[i]
                curr_text: str = "".join(text[i:])

                if first_char not in curr_node.edge_label_to_child:
                    # No match, create new node. Don't update new node as "visited" by tenant yet; it will be done at the beginning of the next iteration.
                    # e.g. curr_node.edge_label_to_child = {}, curr_text = "hello" -> curr_node.edge_label_to_child = {"h": Node("hello")}
                    new_node: Node = Node(text=curr_text, parent=curr_node)
                    curr_node.edge_label_to_child[first_char] = new_node
                    # Add the node to the back of the LRU list; it will be moved to the front in the next iteration.
                    self._insert_node_into_linked_list(
                        new_node, self.tenant_to_lru_tail[tenant], None, tenant
                    )

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
                    ### Insert parent_node into the back of the LRU list; it will be moved to the front in the next iteration. (for the current tenant)
                    ### Insert parent_node between matched_node and matched_node's newer neighbor (for all other tenants)
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
                    # Insert new_parent into the back of the LRU list; it will be moved to the front in the next iteration. (for the current tenant)
                    self._insert_node_into_linked_list(
                        new_parent, self.tenant_to_lru_tail[tenant], None, tenant
                    )
                    # Insert new_parent between matched_node and matched_node's newer neighbor (for all other tenants)
                    for existing_tenant in new_parent.tenant_to_last_access_time:
                        if existing_tenant != tenant:
                            self._insert_node_into_linked_list(
                                new_parent,
                                matched_node.tenant_to_newer_node.get(existing_tenant),
                                matched_node,
                                existing_tenant,
                            )

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
