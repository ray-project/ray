import asyncio
from typing import List, Set

import pytest

import ray
from ray.llm._internal.serve.request_router.prefix_aware.prefix_tree import (
    Node,
    PrefixTree,
    PrefixTreeActor,
)


# Fixtures
@pytest.fixture
def tree() -> PrefixTree:
    """Create a fresh PrefixTree instance for each local test."""
    return PrefixTree()


@pytest.fixture
def tree_actor():
    """Create a fresh PrefixTreeActor instance for each ray.remote test."""
    return PrefixTreeActor.remote()


# Helper to get LRU chain texts
def get_lru_texts_from_tree(tree: PrefixTree, tenant_id: str) -> List[str]:
    """Gets LRU chain texts directly from a PrefixTree instance."""
    chain = tree._get_lru_chain(tenant_id)
    return [node.text for node in chain]


async def get_lru_texts_from_tree_actor(
    tree_actor: PrefixTreeActor, tenant_id: str
) -> List[str]:
    """Gets LRU chain texts from a PrefixTreeActor."""
    chain = ray.get(tree_actor._get_lru_chain.remote(tenant_id))
    return [node.text for node in chain]


class TestPrefixTreeInitialization:
    """Tests for the PrefixTree class initialization and basic tenant management."""

    def test_initial_state(self, tree: PrefixTree) -> None:
        """Test the initial state of a new PrefixTree."""
        assert tree.tenant_to_char_count == {}
        assert tree.tenant_to_lru_tail == {}
        assert tree.root is not None
        assert tree.root.text == ""
        assert tree.root.parent is None
        assert tree.root.tenant_to_last_access_time == {}
        assert tree.root.edge_label_to_child == {}

    def test_add_tenant(self, tree: PrefixTree) -> None:
        """Test adding a new tenant via add_tenants."""
        tree.add_tenants(["tenant_1"], 0)
        assert tree.tenant_to_char_count == {"tenant_1": 0}
        assert tree.tenant_to_lru_tail.get("tenant_1") == tree.root
        assert tree.root.tenant_to_last_access_time == {"tenant_1": 0}
        assert get_lru_texts_from_tree(tree, "tenant_1") == [""]

    def test_add_existing_tenant_noop(self, tree: PrefixTree) -> None:
        """Test that adding an existing tenant via add_tenants is a no-op."""
        tree.add_tenants(["tenant_1"], 0)
        assert tree.tenant_to_char_count == {"tenant_1": 0}
        assert tree.tenant_to_lru_tail.get("tenant_1") == tree.root
        assert tree.root.tenant_to_last_access_time == {"tenant_1": 0}
        assert get_lru_texts_from_tree(tree, "tenant_1") == [""]

        tree.add_tenants(["tenant_1"], 0)  # Add again

        assert tree.tenant_to_char_count == {"tenant_1": 0}
        assert tree.tenant_to_lru_tail.get("tenant_1") == tree.root
        assert tree.root.tenant_to_last_access_time == {"tenant_1": 0}
        assert get_lru_texts_from_tree(tree, "tenant_1") == [""]

    def test_add_multiple_tenants(self, tree: PrefixTree) -> None:
        """Test adding multiple tenants at once."""
        tree.add_tenants(["tenant_1", "tenant_2", "tenant_3"], 0)

        assert tree.tenant_to_char_count == {
            "tenant_1": 0,
            "tenant_2": 0,
            "tenant_3": 0,
        }
        for tenant in ["tenant_1", "tenant_2", "tenant_3"]:
            assert tree.tenant_to_lru_tail.get(tenant) == tree.root
            assert tree.root.tenant_to_newer_node.get(tenant) is None
            assert tree.root.tenant_to_older_node.get(tenant) is None
            assert tree.root.tenant_to_last_access_time == {
                "tenant_1": 0,
                "tenant_2": 0,
                "tenant_3": 0,
            }
            assert get_lru_texts_from_tree(tree, tenant) == [""]

    def test_add_multiple_tenants_with_existing(self, tree: PrefixTree) -> None:
        """Test adding multiple tenants when some already exist."""
        tree.add_tenants(["tenant_1"], 0)
        assert tree.root.tenant_to_last_access_time == {"tenant_1": 0}
        assert tree.tenant_to_char_count == {"tenant_1": 0}
        assert "tenant_1" in tree.tenant_to_lru_tail

        # Add a mix of new and existing tenants
        tree.add_tenants(["tenant_1", "tenant_2", "tenant_3"], 0)
        # Existing tenants should remain unchanged
        assert tree.root.tenant_to_last_access_time == {
            "tenant_1": 0,
            "tenant_2": 0,
            "tenant_3": 0,
        }
        assert tree.tenant_to_char_count == {
            "tenant_1": 0,
            "tenant_2": 0,
            "tenant_3": 0,
        }
        assert all(
            tenant in tree.tenant_to_lru_tail
            for tenant in ["tenant_1", "tenant_2", "tenant_3"]
        )


class TestPrefixTreeInsert:
    def test_insert_non_existent_tenant(self, tree: PrefixTree) -> None:
        """Test inserting a string for a non-existent tenant fails."""
        # Insert without adding tenant first
        tree.insert("hello", "nonexistent", 1)

        # Verify insert did nothing since tenant doesn't exist
        assert "nonexistent" not in tree.tenant_to_char_count
        assert get_lru_texts_from_tree(tree, "nonexistent") == []
        assert "h" not in tree.root.edge_label_to_child

    def test_insert_single_string(self, tree: PrefixTree) -> None:
        """Test inserting a single string after adding a tenant."""
        tree.add_tenants(["tenant_1"], 0)
        tree.insert("hello", "tenant_1", 1)
        assert tree.tenant_to_char_count == {"tenant_1": 5}
        assert get_lru_texts_from_tree(tree, "tenant_1") == ["", "hello"]

        root_node = tree.root
        assert root_node.tenant_to_last_access_time == {"tenant_1": 1}
        assert set(root_node.edge_label_to_child.keys()) == {"h"}

        hello_node = root_node.edge_label_to_child["h"]
        assert hello_node.text == "hello"
        assert hello_node.parent == root_node
        assert hello_node.tenant_to_last_access_time == {"tenant_1": 1}
        assert hello_node.edge_label_to_child == {}

    def test_insert_duplicate_string(self, tree: PrefixTree) -> None:
        """Test inserting a duplicate string for the same tenant."""
        tree.add_tenants(["tenant_1"], 0)
        tree.insert("hello", "tenant_1", 1)  # Initial insert
        tree.insert("hello", "tenant_1", 1)  # Duplicate insert with the same timestamp

        assert tree.tenant_to_char_count == {"tenant_1": 5}  # Char count unchanged
        assert get_lru_texts_from_tree(tree, "tenant_1") == [
            "",
            "hello",
        ]  # LRU order same

        hello_node = tree.root.edge_label_to_child["h"]
        assert tree.root.tenant_to_last_access_time == {"tenant_1": 1}
        assert hello_node.tenant_to_last_access_time == {"tenant_1": 1}

        tree.insert("hello", "tenant_1", 2)  # Duplicate insert with new timestamp

        assert tree.tenant_to_char_count == {"tenant_1": 5}  # Char count unchanged
        assert get_lru_texts_from_tree(tree, "tenant_1") == [
            "",
            "hello",
        ]  # LRU order same

        hello_node = tree.root.edge_label_to_child["h"]
        assert tree.root.tenant_to_last_access_time == {"tenant_1": 2}
        assert hello_node.tenant_to_last_access_time == {"tenant_1": 2}

    def test_insert_multiple_tenants(self, tree: PrefixTree) -> None:
        """Test inserting the same string for different tenants."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("hello", "tenant_1", 1)
        tree.insert("hello", "tenant_2", 2)

        assert tree.tenant_to_char_count == {"tenant_1": 5, "tenant_2": 5}
        assert get_lru_texts_from_tree(tree, "tenant_1") == ["", "hello"]
        assert get_lru_texts_from_tree(tree, "tenant_2") == ["", "hello"]

        hello_node = tree.root.edge_label_to_child["h"]
        assert tree.root.tenant_to_last_access_time == {"tenant_1": 1, "tenant_2": 2}
        assert hello_node.tenant_to_last_access_time == {"tenant_1": 1, "tenant_2": 2}

    def test_insert_node_split(self, tree: PrefixTree) -> None:
        """Test insertion that causes an existing node to split due to differing suffixes."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("helloworld", "tenant_1", 1)
        tree.insert("hellothere", "tenant_2", 2)  # "hello" is common prefix

        assert tree.tenant_to_char_count == {"tenant_1": 10, "tenant_2": 10}
        assert get_lru_texts_from_tree(tree, "tenant_1") == ["", "hello", "world"]
        assert get_lru_texts_from_tree(tree, "tenant_2") == ["", "there", "hello"]

        hello_node = tree.root.edge_label_to_child["h"]
        assert hello_node.text == "hello"
        assert hello_node.tenant_to_last_access_time == {"tenant_1": 1, "tenant_2": 2}
        assert set(hello_node.edge_label_to_child.keys()) == {"w", "t"}

        world_node = hello_node.edge_label_to_child["w"]
        assert world_node.text == "world"
        assert world_node.tenant_to_last_access_time == {"tenant_1": 1}

        there_node = hello_node.edge_label_to_child["t"]
        assert there_node.text == "there"
        assert there_node.tenant_to_last_access_time == {"tenant_2": 2}

    def test_insert_longer_string_with_shared_prefix(self, tree: PrefixTree) -> None:
        """Test inserting a longer string that shares a prefix with an existing node string."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("hello", "tenant_1", 1)
        tree.insert("helloworld", "tenant_2", 2)  # "hello" is prefix of "helloworld"

        assert tree.tenant_to_char_count == {"tenant_1": 5, "tenant_2": 10}
        assert get_lru_texts_from_tree(tree, "tenant_1") == ["", "hello"]
        assert get_lru_texts_from_tree(tree, "tenant_2") == ["", "world", "hello"]

        hello_node = tree.root.edge_label_to_child["h"]
        assert hello_node.text == "hello"
        assert hello_node.tenant_to_last_access_time == {"tenant_1": 1, "tenant_2": 2}
        assert set(hello_node.edge_label_to_child.keys()) == {"w"}

        world_node = hello_node.edge_label_to_child["w"]
        assert world_node.text == "world"
        assert world_node.tenant_to_last_access_time == {"tenant_2": 2}

        # Ensure no empty non-root nodes created
        empty_text_nodes: List[Node] = []
        nodes_to_check: List[Node] = [tree.root]
        visited_nodes: Set[Node] = {tree.root}
        while nodes_to_check:
            node: Node = nodes_to_check.pop()
            if node.text == "" and node != tree.root:  # check for non-root empty nodes
                empty_text_nodes.append(node)
            for child in node.edge_label_to_child.values():
                if child not in visited_nodes:
                    nodes_to_check.append(child)
                    visited_nodes.add(child)
        assert not empty_text_nodes

    def test_insert_shorter_string_with_shared_prefix(self, tree: PrefixTree) -> None:
        """Test inserting a shorter string that is a prefix of an existing longer string, causing split."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("helloworld", "tenant_1", 1)
        tree.insert(
            "hello", "tenant_2", 2
        )  # "hello" is prefix, causes "helloworld" to split

        assert tree.tenant_to_char_count == {"tenant_1": 10, "tenant_2": 5}
        assert get_lru_texts_from_tree(tree, "tenant_1") == ["", "hello", "world"]
        assert get_lru_texts_from_tree(tree, "tenant_2") == ["", "hello"]

        hello_node = tree.root.edge_label_to_child["h"]
        assert hello_node.text == "hello"
        assert hello_node.tenant_to_last_access_time == {"tenant_1": 1, "tenant_2": 2}
        assert set(hello_node.edge_label_to_child.keys()) == {"w"}

        world_node = hello_node.edge_label_to_child["w"]
        assert world_node.text == "world"
        assert world_node.tenant_to_last_access_time == {"tenant_1": 1}


class TestPrefixTreeMatch:
    def test_prefix_match_empty_tree(self, tree: PrefixTree) -> None:
        """Test prefix_match on an empty tree returns empty string and None tenants."""
        matched_text, matched_tenants = tree.prefix_match("hello")
        assert matched_text == ""
        assert matched_tenants is None

    def test_prefix_match_no_match(self, tree: PrefixTree) -> None:
        """Test prefix_match for a non-matching prefix returns empty string and all tenants."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("hello", "tenant_1", 1)
        tree.insert("world", "tenant_2", 2)
        matched_text, matched_tenants = tree.prefix_match("foobar")
        assert matched_text == ""
        assert matched_tenants is not None
        assert sorted(matched_tenants) == sorted(["tenant_1", "tenant_2"])

    def test_prefix_match_query_longer_than_stored_strings(
        self, tree: PrefixTree
    ) -> None:
        """Test prefix_match where query is longer than any stored string but matches a full path."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("helloworld", "tenant_1", 1)
        tree.insert("hellothere", "tenant_2", 2)
        matched_text, matched_tenants = tree.prefix_match("hellothereextra")
        assert matched_text == "hellothere"
        assert matched_tenants == ["tenant_2"]

    def test_prefix_match_exact_match(self, tree: PrefixTree) -> None:
        """Test prefix_match with an exact match for a single tenant."""
        tree.add_tenants(["tenant_1"], 0)
        tree.insert("hello", "tenant_1", 1)
        matched_text, matched_tenants = tree.prefix_match("hello")
        assert matched_text == "hello"
        assert matched_tenants == ["tenant_1"]

    def test_prefix_match_partial_match(self, tree: PrefixTree) -> None:
        """Test prefix_match with a partial query matching the longest common part of a branch."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("apple", "tenant_1", 1)
        tree.insert("apricot", "tenant_2", 2)
        matched_text, matched_tenants = tree.prefix_match("application")
        assert matched_text == "appl"  # Longest of ("appl", "ap")
        assert matched_tenants == ["tenant_1"]

    def test_prefix_match_with_tenant_filter(self, tree: PrefixTree) -> None:
        """Test prefix_match with a tenant filter selecting a specific branch."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("apple", "tenant_1", 1)
        tree.insert("apricot", "tenant_2", 2)
        matched_text, matched_tenants = tree.prefix_match("application", ["tenant_2"])
        assert matched_text == "ap"
        assert matched_tenants == ["tenant_2"]

    def test_prefix_match_with_shared_prefix_tenant_filter(
        self, tree: PrefixTree
    ) -> None:
        """Test prefix_match with a tenant filter when one tenant has a prefix of a longer string."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("apple", "tenant_1", 1)
        tree.insert("applepie", "tenant_2", 2)

        # Match the longer string but only allow tenant_1
        matched_text, matched_tenants = tree.prefix_match("applepie", ["tenant_1"])

        # Should only match up to "apple" as that's what tenant_1 owns
        assert matched_text == "apple"
        assert matched_tenants == ["tenant_1"]

        # Verify that using both tenants would match the full string for tenant_2 only
        matched_text, matched_tenants = tree.prefix_match(
            "applepie", ["tenant_1", "tenant_2"]
        )
        assert matched_text == "applepie"
        assert matched_tenants == ["tenant_2"]

        # And both tenants should be returned for "apple"
        matched_text, matched_tenants = tree.prefix_match(
            "apple", ["tenant_1", "tenant_2"]
        )
        assert matched_text == "apple"
        assert set(matched_tenants) == {"tenant_1", "tenant_2"}

    def test_prefix_match_with_non_existent_tenant_filter(
        self, tree: PrefixTree
    ) -> None:
        """Test prefix_match with a filter for a non-existent tenant returns no match."""
        tree.add_tenants(["tenant_1"], 0)
        tree.insert("apple", "tenant_1", 1)
        matched_text, matched_tenants = tree.prefix_match(
            "application", ["non_existent_tenant"]
        )
        assert matched_text == ""
        assert matched_tenants is None


class TestPrefixTreeRemove:
    def test_remove_single_leaf_node_pruned(self, tree: PrefixTree) -> None:
        """Test _remove_tenant_single_node for a leaf node; node should be pruned."""
        tree.add_tenants(["tenant_1"], 0)
        tree.insert("hello", "tenant_1", 1)
        hello_node = tree.root.edge_label_to_child["h"]
        assert hello_node.tenant_to_last_access_time == {"tenant_1": 1}
        assert tree.tenant_to_char_count == {"tenant_1": 5}
        assert tree.root.edge_label_to_child == {"h": hello_node}

        removed_chars = tree._remove_tenant_single_node("tenant_1", hello_node)
        assert removed_chars == 5
        assert hello_node.tenant_to_last_access_time == {}
        assert tree.tenant_to_char_count == {"tenant_1": 0}
        assert tree.root.edge_label_to_child == {}  # Node pruned

    def test_remove_single_leaf_node_not_pruned(self, tree: PrefixTree) -> None:
        """Test _remove_tenant_single_node for a leaf node; node should not be pruned."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("hello", "tenant_1", 1)
        tree.insert("hello", "tenant_2", 2)
        hello_node = tree.root.edge_label_to_child["h"]
        assert hello_node.tenant_to_last_access_time == {"tenant_1": 1, "tenant_2": 2}
        assert tree.tenant_to_char_count == {"tenant_1": 5, "tenant_2": 5}
        assert tree.root.edge_label_to_child == {"h": hello_node}

        removed_chars = tree._remove_tenant_single_node("tenant_1", hello_node)
        assert removed_chars == 5
        assert hello_node.tenant_to_last_access_time == {"tenant_2": 2}
        assert tree.tenant_to_char_count == {"tenant_1": 0, "tenant_2": 5}
        assert tree.root.edge_label_to_child == {"h": hello_node}  # Node not pruned

    def test_remove_single_node_with_non_existent_tenant(
        self, tree: PrefixTree
    ) -> None:
        """Test _remove_tenant_single_node for a non-existent tenant is a no-op."""
        tree.add_tenants(["tenant_1"], 0)
        tree.insert("hello", "tenant_1", 1)
        hello_node = tree.root.edge_label_to_child["h"]
        removed_chars = tree._remove_tenant_single_node(
            "non_existent_tenant", hello_node
        )
        assert removed_chars == 0

    def test_remove_single_node_with_non_matching_tenant(
        self, tree: PrefixTree
    ) -> None:
        """Test _remove_tenant_single_node if node doesn't belong to specified tenant is a no-op."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("hello", "tenant_1", 1)
        tree.insert("world", "tenant_2", 2)  # Node for tenant_2
        hello_node = tree.root.edge_label_to_child["h"]  # Belongs to tenant_1
        removed_chars = tree._remove_tenant_single_node(
            "tenant_2", hello_node
        )  # Try removing tenant_2 from tenant_1's node
        assert removed_chars == 0

    def test_remove_tenant(self, tree: PrefixTree) -> None:
        """Test remove_tenant for a tree with multiple tenants only removes the specified tenant."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("hello", "tenant_1", 1)
        tree.insert("foobar", "tenant_1", 2)
        tree.insert("helloworld", "tenant_2", 3)
        removed_chars = tree.remove_tenants(["tenant_1"])
        assert removed_chars == {"tenant_1": 11}
        hello_node = tree.root.edge_label_to_child["h"]
        assert hello_node.tenant_to_last_access_time == {"tenant_2": 3}
        assert tree.tenant_to_char_count == {"tenant_2": 10}
        assert set(tree.tenant_to_lru_tail.keys()) == {"tenant_2"}
        tenant_2_lru_texts = get_lru_texts_from_tree(tree, "tenant_2")
        assert tenant_2_lru_texts == ["", "world", "hello"]

    def test_remove_non_existent_tenant(self, tree: PrefixTree) -> None:
        """Test remove_tenant for a non-existent tenant returns 0."""
        tree.add_tenants(["tenant_1"], 0)
        tree.insert("hello", "tenant_1", 1)
        removed_chars = tree.remove_tenants(["non_existent_tenant"])
        assert removed_chars == {"non_existent_tenant": 0}

    def test_remove_tenant_prunes_nodes(self, tree: PrefixTree) -> None:
        """Test remove_tenant prunes nodes that become tenant-less and childless."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("helloworld", "tenant_1", 1)  # Creates "helloworld"
        tree.insert(
            "hellothere", "tenant_2", 2
        )  # Splits into "hello" -> "world" and "hello" -> "there"

        tree.remove_tenants(["tenant_1"])

        # "world" node should be pruned. "hello" and "there" remain for tenant_2.
        hello_node = tree.root.edge_label_to_child["h"]
        assert set(hello_node.edge_label_to_child.keys()) == {"t"}
        assert hello_node.edge_label_to_child["t"].text == "there"
        assert hello_node.edge_label_to_child["t"].tenant_to_last_access_time == {
            "tenant_2": 2
        }

    def test_remove_tenants(self, tree: PrefixTree) -> None:
        """Test remove_tenants for multiple tenants with different structures."""
        tree.add_tenants(["tenant_1", "tenant_2", "tenant_3"], 0)
        tree.insert("hello", "tenant_1", 1)  # 5 chars
        tree.insert("foobar", "tenant_1", 2)  # 6 chars
        tree.insert("helloworld", "tenant_2", 3)  # 10 chars
        tree.insert("test", "tenant_3", 4)  # 4 chars

        removed_chars = tree.remove_tenants(["tenant_1", "tenant_3"])

        # Check return value contains correct char counts
        assert removed_chars == {"tenant_1": 11, "tenant_3": 4}

        # Check tree state is correct
        assert "tenant_1" not in tree.tenant_to_char_count
        assert "tenant_3" not in tree.tenant_to_char_count
        assert "tenant_2" in tree.tenant_to_char_count
        assert tree.tenant_to_char_count == {"tenant_2": 10}

        # Check nodes are correctly maintained
        assert (
            "h" in tree.root.edge_label_to_child
        )  # hello node still exists for tenant_2
        assert "t" not in tree.root.edge_label_to_child  # test node removed
        assert "f" not in tree.root.edge_label_to_child  # foobar node removed

        # Check LRU structure
        assert set(tree.tenant_to_lru_tail.keys()) == {"tenant_2"}
        tenant_2_lru_texts = get_lru_texts_from_tree(tree, "tenant_2")
        assert tenant_2_lru_texts == ["", "world", "hello"]

    def test_remove_tenants_with_nonexistent(self, tree: PrefixTree) -> None:
        """Test remove_tenants with a mix of existing and non-existent tenants."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("hello", "tenant_1", 1)
        tree.insert("world", "tenant_2", 2)

        removed_chars = tree.remove_tenants(["tenant_1", "nonexistent", "alsonotfound"])

        # Check return value
        assert removed_chars == {"tenant_1": 5, "nonexistent": 0, "alsonotfound": 0}

        # Check tree state
        assert "tenant_1" not in tree.tenant_to_char_count
        assert tree.tenant_to_char_count == {"tenant_2": 5}
        assert "h" not in tree.root.edge_label_to_child  # hello node removed
        assert "w" in tree.root.edge_label_to_child  # world node still exists


class TestPrefixTreeEviction:
    def test_eviction_non_existent_tenant(self, tree: PrefixTree) -> None:
        """Test evict_tenant_by_lru for a non-existent tenant returns 0."""
        assert tree.evict_tenant_by_lru("nonexistent_tenant", 5) == 0

    def test_eviction_exact_min_remove_size_single_node(self, tree: PrefixTree) -> None:
        """Test evicting exactly min_remove_size characters from a single oldest node."""
        tree.add_tenants(["tenant_1"], 0)
        tree.insert("a", "tenant_1", 1)  # Oldest (1 char)
        tree.insert("bb", "tenant_1", 2)
        tree.insert("ccc", "tenant_1", 3)
        assert get_lru_texts_from_tree(tree, "tenant_1") == ["", "ccc", "bb", "a"]

        evicted_count = tree.evict_tenant_by_lru("tenant_1", 1)  # Evict "a"
        assert evicted_count == 1
        assert tree.tenant_to_char_count == {"tenant_1": 5}  # 6 - 1
        assert get_lru_texts_from_tree(tree, "tenant_1") == ["", "ccc", "bb"]

    def test_eviction_exceed_min_remove_size_single_node(
        self, tree: PrefixTree
    ) -> None:
        """Test evicting more than min_remove_size characters from a single oldest node."""
        tree.add_tenants(["tenant_1"], 0)
        tree.insert("aaa", "tenant_1", 1)  # Oldest (2 chars)
        tree.insert("bb", "tenant_1", 2)
        tree.insert("c", "tenant_1", 3)
        assert get_lru_texts_from_tree(tree, "tenant_1") == ["", "c", "bb", "aaa"]

        evicted_count = tree.evict_tenant_by_lru("tenant_1", 1)  # Evict "aaa"
        assert evicted_count == 3
        assert tree.tenant_to_char_count == {"tenant_1": 3}  # 6 - 3
        assert get_lru_texts_from_tree(tree, "tenant_1") == ["", "c", "bb"]

    def test_eviction_multiple_nodes(self, tree: PrefixTree) -> None:
        """Test evicting multiple oldest nodes to meet min_remove_size."""
        tree.add_tenants(["tenant_1"], 0)
        tree.insert("a", "tenant_1", 1)  # Oldest (1 char)
        tree.insert("bb", "tenant_1", 2)  # Next oldest (2 chars)
        tree.insert("ccc", "tenant_1", 3)
        assert get_lru_texts_from_tree(tree, "tenant_1") == ["", "ccc", "bb", "a"]

        evicted_count = tree.evict_tenant_by_lru("tenant_1", 2)  # Evict "a" and "b"
        assert evicted_count == 3  # 1 ("a") + 2 ("b")
        assert tree.tenant_to_char_count["tenant_1"] == 3  # 6 - 3
        assert get_lru_texts_from_tree(tree, "tenant_1") == ["", "ccc"]

    def test_eviction_same_timestamps(self, tree: PrefixTree) -> None:
        """Test evicting more than min_remove_size if multiple nodes share the oldest timestamp."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("helloworld", "tenant_1", 1)
        tree.insert("hellothere", "tenant_2", 2)
        assert get_lru_texts_from_tree(tree, "tenant_1") == ["", "hello", "world"]
        assert get_lru_texts_from_tree(tree, "tenant_2") == ["", "there", "hello"]

        # Should remove both "hello" and "world" because they have the same timestamp
        evicted_count = tree.evict_tenant_by_lru("tenant_1", 1)  # Request 1 char
        assert evicted_count == 10  # Removes "hello" and "world"
        assert tree.tenant_to_char_count == {"tenant_1": 0, "tenant_2": 10}
        assert get_lru_texts_from_tree(tree, "tenant_1") == [""]
        assert get_lru_texts_from_tree(tree, "tenant_2") == ["", "there", "hello"]

    def test_eviction_insufficient_chars_evicts_all(self, tree: PrefixTree) -> None:
        """Test evicting when min_remove_size is larger than available; evicts all."""
        tree.add_tenants(["tenant_1"], 0)
        tree.insert("xyz", "tenant_1", 1)  # 3 chars available
        evicted_count = tree.evict_tenant_by_lru("tenant_1", 10)
        assert evicted_count == 3
        assert tree.tenant_to_char_count == {"tenant_1": 0}
        assert get_lru_texts_from_tree(tree, "tenant_1") == [""]


class TestPrefixTreeGetSmallestTenants:
    """Tests for the get_smallest_tenants method."""

    def test_get_smallest_tenants(self, tree: PrefixTree) -> None:
        """Test get_smallest_tenants identifies the tenant with the fewest characters."""
        tree.add_tenants(["tenant_1", "tenant_2", "tenant_3"], 0)
        tree.insert("aaaa", "tenant_1", 1)  # 4 chars
        tree.insert("bb", "tenant_2", 2)  # 2 chars
        tree.insert("c", "tenant_3", 3)  # 1 char
        smallest_tenants = tree.get_smallest_tenants()
        assert smallest_tenants == ["tenant_3"]

    def test_get_smallest_tenants_empty_tree(self, tree: PrefixTree) -> None:
        """Test get_smallest_tenants on an empty tree returns None."""
        assert tree.get_smallest_tenants() is None

    def test_get_smallest_tenants_after_update(self, tree: PrefixTree) -> None:
        """Test get_smallest_tenants after removing the current smallest tenant."""
        tree.add_tenants(["tenant_1", "tenant_2", "tenant_3"], 0)
        tree.insert("aaaa", "tenant_1", 1)
        tree.insert("bb", "tenant_2", 2)
        tree.insert("c", "tenant_3", 3)
        tree.remove_tenants(["tenant_3"])  # Remove "c" (1 char)
        smallest_tenants = tree.get_smallest_tenants()
        assert smallest_tenants == ["tenant_2"]  # "bb" (2 chars) is now smallest

    def test_get_smallest_tenants_with_ties(self, tree: PrefixTree) -> None:
        """Test get_smallest_tenants when multiple tenants have the same minimum count."""
        tree.add_tenants(["tenant_1", "tenant_2", "tenant_3"], 0)
        tree.insert("aa", "tenant_1", 1)  # 2 chars
        tree.insert("bb", "tenant_2", 2)  # 2 chars
        tree.insert("cccc", "tenant_3", 3)  # 4 chars
        smallest_tenants = tree.get_smallest_tenants()
        assert set(smallest_tenants) == {"tenant_1", "tenant_2"}


class TestPrefixTreeComprehensive:
    """Comprehensive tests for the PrefixTree"""

    def test_tree_structure_multiple_insertions(self, tree: PrefixTree) -> None:
        """Test tree structure after multiple insertions."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("helloworld", "tenant_1", 1)
        tree.insert("hellothere", "tenant_2", 2)
        tree.insert("hellothomas", "tenant_2", 3)

        # Access tree directly
        root: Node = tree.root

        # Test tree structure - validate each node
        # Root node
        assert root.text == ""
        assert root.parent is None
        assert root.tenant_to_last_access_time == {"tenant_1": 1, "tenant_2": 3}
        assert set(root.edge_label_to_child.keys()) == {"h"}

        # Hello node
        hello_node: Node = root.edge_label_to_child["h"]
        assert hello_node.text == "hello"
        assert hello_node.parent.text == ""
        assert hello_node.tenant_to_last_access_time == {"tenant_1": 1, "tenant_2": 3}
        assert set(hello_node.edge_label_to_child.keys()) == {"w", "t"}

        # World node
        world_node: Node = hello_node.edge_label_to_child["w"]
        assert world_node.text == "world"
        assert world_node.parent.text == "hello"
        assert world_node.tenant_to_last_access_time == {"tenant_1": 1}
        assert set(world_node.edge_label_to_child.keys()) == set()

        # Th node
        th_node: Node = hello_node.edge_label_to_child["t"]
        assert th_node.text == "th"
        assert th_node.parent.text == "hello"
        assert th_node.tenant_to_last_access_time == {"tenant_2": 3}
        assert set(th_node.edge_label_to_child.keys()) == {"e", "o"}

        # Ere node
        ere_node: Node = th_node.edge_label_to_child["e"]
        assert ere_node.text == "ere"
        assert ere_node.parent.text == "th"
        assert ere_node.tenant_to_last_access_time == {"tenant_2": 2}
        assert set(ere_node.edge_label_to_child.keys()) == set()

        # Omas node
        omas_node: Node = th_node.edge_label_to_child["o"]
        assert omas_node.text == "omas"
        assert omas_node.parent.text == "th"
        assert omas_node.tenant_to_last_access_time == {"tenant_2": 3}
        assert set(omas_node.edge_label_to_child.keys()) == set()

    def test_multiple_evictions_maintains_lru_order(self, tree: PrefixTree) -> None:
        """Test multiple evictions maintain LRU order."""
        tree.add_tenants(["tenant_1", "tenant_2"], 0)
        tree.insert("helloworld", "tenant_1", 1)
        tree.insert("hellothere", "tenant_2", 2)
        tree.insert("hellothomas", "tenant_2", 3)
        assert tree.tenant_to_char_count == {"tenant_1": 10, "tenant_2": 14}
        assert get_lru_texts_from_tree(tree, "tenant_1") == ["", "hello", "world"]
        assert get_lru_texts_from_tree(tree, "tenant_2") == [
            "",
            "omas",
            "th",
            "hello",
            "ere",
        ]

        # Eviction 1 (tenant_1): min_remove_size=1. "hello" and "world" removed.
        evicted_1 = tree.evict_tenant_by_lru("tenant_1", 1)
        assert evicted_1 == 10
        assert tree.tenant_to_char_count == {"tenant_1": 0, "tenant_2": 14}
        assert get_lru_texts_from_tree(tree, "tenant_1") == [""]
        assert get_lru_texts_from_tree(tree, "tenant_2") == [
            "",
            "omas",
            "th",
            "hello",
            "ere",
        ]  # T2 unchanged

        # Eviction 2 (tenant_2): min_remove_size=1. "ere" is oldest timestamp, removed.
        evicted_2 = tree.evict_tenant_by_lru("tenant_2", 1)
        assert evicted_2 == 3  # "ere" is 3 chars
        assert tree.tenant_to_char_count == {"tenant_1": 0, "tenant_2": 11}  # 14 - 3
        assert get_lru_texts_from_tree(tree, "tenant_2") == ["", "omas", "th", "hello"]

        # Eviction 3 (tenant_2): min_remove_size=1. "omas"(ts3), "th"(ts3), "hello"(ts3) removed.
        evicted_3 = tree.evict_tenant_by_lru("tenant_2", 1)
        assert evicted_3 == 11  # 4+2+5 chars
        assert tree.tenant_to_char_count == {"tenant_1": 0, "tenant_2": 0}
        assert get_lru_texts_from_tree(tree, "tenant_2") == [""]


@pytest.mark.asyncio
class TestPrefixTreeActorComprehensive:
    """Comprehensive tests for the PrefixTreeActor"""

    async def test_tree_structure_multiple_insertions_actor(
        self, tree_actor: PrefixTreeActor
    ) -> None:
        # Add tenants and insert strings in specified order
        ray.get(tree_actor.add_tenants.remote(["tenant_1", "tenant_2"], 0))
        ray.get(tree_actor.insert.remote("helloworld", "tenant_1", 1))
        ray.get(tree_actor.insert.remote("hellothere", "tenant_2", 2))
        ray.get(tree_actor.insert.remote("hellothomas", "tenant_2", 3))
        assert await get_lru_texts_from_tree_actor(tree_actor, "tenant_1") == [
            "",
            "hello",
            "world",
        ]

        # Access tree directly
        root: Node = ray.get(tree_actor.getattr.remote("root"))

        # Test tree structure - validate each node
        # Root node
        assert root.text == ""
        assert root.parent is None
        assert root.tenant_to_last_access_time == {"tenant_1": 1, "tenant_2": 3}
        assert set(root.edge_label_to_child.keys()) == {"h"}

        # Hello node
        hello_node: Node = root.edge_label_to_child["h"]
        assert hello_node.text == "hello"
        assert hello_node.parent.text == ""
        assert hello_node.tenant_to_last_access_time == {"tenant_1": 1, "tenant_2": 3}
        assert set(hello_node.edge_label_to_child.keys()) == {"w", "t"}

        # World node
        world_node: Node = hello_node.edge_label_to_child["w"]
        assert world_node.text == "world"
        assert world_node.parent.text == "hello"
        assert world_node.tenant_to_last_access_time == {"tenant_1": 1}
        assert set(world_node.edge_label_to_child.keys()) == set()

        # Th node
        th_node: Node = hello_node.edge_label_to_child["t"]
        assert th_node.text == "th"
        assert th_node.parent.text == "hello"
        assert th_node.tenant_to_last_access_time == {"tenant_2": 3}
        assert set(th_node.edge_label_to_child.keys()) == {"e", "o"}

        # Ere node
        ere_node: Node = th_node.edge_label_to_child["e"]
        assert ere_node.text == "ere"
        assert ere_node.parent.text == "th"
        assert ere_node.tenant_to_last_access_time == {"tenant_2": 2}
        assert set(ere_node.edge_label_to_child.keys()) == set()

        # Omas node
        omas_node: Node = th_node.edge_label_to_child["o"]
        assert omas_node.text == "omas"
        assert omas_node.parent.text == "th"
        assert omas_node.tenant_to_last_access_time == {"tenant_2": 3}
        assert set(omas_node.edge_label_to_child.keys()) == set()

    async def test_multiple_evictions_maintains_lru_order_actor(
        self, tree_actor: PrefixTreeActor
    ) -> None:
        """Test multiple evictions maintain LRU order."""
        # Add tenants and insert test data
        ray.get(tree_actor.add_tenants.remote(["tenant_1", "tenant_2"], 0))
        ray.get(tree_actor.insert.remote("helloworld", "tenant_1", 1))
        ray.get(tree_actor.insert.remote("hellothere", "tenant_2", 2))
        ray.get(tree_actor.insert.remote("hellothomas", "tenant_2", 3))
        assert ray.get(tree_actor.getattr.remote("tenant_to_char_count")) == {
            "tenant_1": 10,
            "tenant_2": 14,
        }
        assert await get_lru_texts_from_tree_actor(tree_actor, "tenant_1") == [
            "",
            "hello",
            "world",
        ]
        assert await get_lru_texts_from_tree_actor(tree_actor, "tenant_2") == [
            "",
            "omas",
            "th",
            "hello",
            "ere",
        ]

        # Eviction 1 (tenant_1): min_remove_size=1. "hello" and "world" removed.
        evicted_1 = ray.get(tree_actor.evict_tenant_by_lru.remote("tenant_1", 1))
        assert evicted_1 == 10
        assert ray.get(tree_actor.getattr.remote("tenant_to_char_count")) == {
            "tenant_1": 0,
            "tenant_2": 14,
        }
        assert await get_lru_texts_from_tree_actor(tree_actor, "tenant_1") == [""]
        assert await get_lru_texts_from_tree_actor(tree_actor, "tenant_2") == [
            "",
            "omas",
            "th",
            "hello",
            "ere",
        ]  # T2 unchanged

        # Eviction 2 (tenant_2): min_remove_size=1. "ere" is oldest timestamp, removed.
        evicted_2 = ray.get(tree_actor.evict_tenant_by_lru.remote("tenant_2", 1))
        assert evicted_2 == 3  # "ere" is 3 chars
        assert ray.get(tree_actor.getattr.remote("tenant_to_char_count")) == {
            "tenant_1": 0,
            "tenant_2": 11,
        }  # 14 - 3
        assert await get_lru_texts_from_tree_actor(tree_actor, "tenant_2") == [
            "",
            "omas",
            "th",
            "hello",
        ]

        # Eviction 3 (tenant_2): min_remove_size=1. "omas"(ts3), "th"(ts3), "hello"(ts3) removed.
        evicted_3 = ray.get(tree_actor.evict_tenant_by_lru.remote("tenant_2", 1))
        assert evicted_3 == 11  # 4+2+5 chars
        assert ray.get(tree_actor.getattr.remote("tenant_to_char_count")) == {
            "tenant_1": 0,
            "tenant_2": 0,
        }
        assert await get_lru_texts_from_tree_actor(tree_actor, "tenant_2") == [""]


@pytest.mark.asyncio
class TestPrefixTreeActorEvictionLoop:
    """Tests for the automatic eviction loop in PrefixTreeActor"""

    async def test_eviction_loop_triggers_automatically(
        self, tree_actor: PrefixTreeActor
    ) -> None:
        """Test that the eviction loop automatically evicts data when threshold is exceeded."""
        # Set up eviction parameters
        eviction_threshold = 10  # Low threshold for testing
        eviction_target = 8  # Target to evict down to
        interval_secs = 0.1  # Short interval for testing

        # Start the eviction loop
        ray.get(
            tree_actor.start_eviction_loop.remote(
                eviction_threshold, eviction_target, interval_secs
            )
        )

        # Add tenant and insert data over the threshold
        ray.get(tree_actor.add_tenants.remote(["tenant_1"], 0))
        ray.get(tree_actor.insert.remote("hello", "tenant_1", 1))  # 5 chars
        ray.get(
            tree_actor.insert.remote("excess", "tenant_1", 2)
        )  # 6 more chars, total: 11

        # Verify initial count
        assert ray.get(tree_actor.getattr.remote("tenant_to_char_count")) == {
            "tenant_1": 11
        }

        # Wait for eviction loop to run (interval + small buffer)
        await asyncio.sleep(interval_secs + 0.2)

        # Verify data was automatically evicted down to target (8 chars)
        # The eviction should have removed 5 chars, so we should be at 6, which is <= 8
        char_count = ray.get(tree_actor.getattr.remote("tenant_to_char_count"))
        assert char_count["tenant_1"] == 6

    async def test_eviction_loop_multiple_tenants(
        self, tree_actor: PrefixTreeActor
    ) -> None:
        """Test that eviction loop evicts from each tenant that exceeds the threshold."""
        # Set up eviction parameters
        eviction_threshold = 10
        eviction_target = 8
        interval_secs = 0.1

        # Start the eviction loop
        ray.get(
            tree_actor.start_eviction_loop.remote(
                eviction_threshold, eviction_target, interval_secs
            )
        )

        # Add two tenants with data over threshold
        ray.get(tree_actor.add_tenants.remote(["tenant_1", "tenant_2"], 0))
        ray.get(tree_actor.insert.remote("hello", "tenant_1", 1))  # 5 chars
        ray.get(
            tree_actor.insert.remote("excess", "tenant_1", 2)
        )  # 6 more chars, total: 11
        ray.get(tree_actor.insert.remote("bigstring", "tenant_2", 3))  # 9 chars
        ray.get(
            tree_actor.insert.remote("more", "tenant_2", 4)
        )  # 4 more chars, total: 13

        # Verify initial counts
        initial_count = ray.get(tree_actor.getattr.remote("tenant_to_char_count"))
        assert initial_count["tenant_1"] == 11
        assert initial_count["tenant_2"] == 13

        # Wait for eviction loop to run
        await asyncio.sleep(interval_secs + 0.2)

        # Verify both tenants were evicted to target
        char_count = ray.get(tree_actor.getattr.remote("tenant_to_char_count"))

        # Tenant 1 should have "hello" evicted, so 11 - 5 = 6
        assert char_count["tenant_1"] == 6
        # Tenant 2 should have "bigstring" evicted, so 13 - 9 = 4
        assert char_count["tenant_2"] == 4

    async def test_eviction_loop_respects_threshold(
        self, tree_actor: PrefixTreeActor
    ) -> None:
        """Test that eviction loop only evicts tenants that exceed the threshold."""
        # Set up eviction parameters
        eviction_threshold = 10
        eviction_target = 8
        interval_secs = 0.1

        # Start the eviction loop
        ray.get(
            tree_actor.start_eviction_loop.remote(
                eviction_threshold, eviction_target, interval_secs
            )
        )

        # Add two tenants - one over threshold, one under
        ray.get(tree_actor.add_tenants.remote(["over_tenant", "under_tenant"], 0))
        ray.get(tree_actor.insert.remote("hello", "over_tenant", 1))  # 5 chars
        ray.get(
            tree_actor.insert.remote("excess", "over_tenant", 2)
        )  # 6 more chars, total: 11
        ray.get(tree_actor.insert.remote("small", "under_tenant", 3))  # 5 chars

        # Verify initial counts
        initial_count = ray.get(tree_actor.getattr.remote("tenant_to_char_count"))
        assert initial_count["over_tenant"] == 11
        assert initial_count["under_tenant"] == 5

        # Wait for eviction loop to run
        await asyncio.sleep(interval_secs + 0.2)

        # Verify only the tenant over threshold was evicted
        char_count = ray.get(tree_actor.getattr.remote("tenant_to_char_count"))
        # Tenant 1 should have "hello" evicted, so 11 - 5 = 6
        assert char_count["over_tenant"] == 6
        # Tenant 2 should be unchanged
        assert char_count["under_tenant"] == 5

    async def test_eviction_loop_can_be_started_multiple_times(
        self, tree_actor: PrefixTreeActor
    ) -> None:
        """Test that only the first call to start_eviction_loop starts a new loop."""
        # Call start_eviction_loop multiple times
        eviction_task_1 = ray.get(tree_actor.start_eviction_loop.remote(10, 8, 0.1))
        eviction_task_2 = ray.get(tree_actor.start_eviction_loop.remote(10, 0, 0.1))
        assert eviction_task_1 and not eviction_task_2

        # Add tenant and insert data over the threshold
        ray.get(tree_actor.add_tenants.remote(["tenant_1"], 0))
        ray.get(tree_actor.insert.remote("hello", "tenant_1", 1))  # 5 chars
        ray.get(
            tree_actor.insert.remote("excess", "tenant_1", 2)
        )  # 6 more chars, total: 11

        # Wait for eviction loop to run
        await asyncio.sleep(0.3)

        # Verify the first eviction_target_chars is respected.
        # Should evict "hello" to bring the char count down from 11 to 6.

        char_count = ray.get(tree_actor.getattr.remote("tenant_to_char_count"))
        assert char_count["tenant_1"] == 6


if __name__ == "__main__":
    import sys

    exit_code = pytest.main(["-v", __file__])
    sys.exit(exit_code)
