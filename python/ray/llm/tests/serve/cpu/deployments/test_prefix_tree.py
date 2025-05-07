import pytest
import ray
from typing import Set, List, Dict, Optional

from ray.llm._internal.serve.replica_scheduler.prefix_aware.prefix_tree import (
    PrefixTree,
    PrefixTreeActor,
    Node,
)


# Fixtures
@pytest.fixture
def tree() -> PrefixTree:
    """Create a fresh PrefixTree instance for each test."""
    return PrefixTree()


@pytest.fixture(scope="module")
def tree_actor():
    """Create a fresh PrefixTreeActor instance for each test."""
    tree_actor = PrefixTreeActor.remote()
    return tree_actor


# PrefixTreeActor tests
@pytest.mark.asyncio
async def test_tree_actor(tree_actor) -> None:
    """Test the PrefixTreeActor."""
    # 1. Test tree structure and LRU heap ordering
    tree_actor._reset.remote()

    # Insert strings in specified order
    tree_actor.insert.remote("helloworld", "tenant_1", 1)  # time 1 for tenant_1
    tree_actor.insert.remote("hellothere", "tenant_2", 2)  # time 2 for tenant_2
    tree_actor.insert.remote("hellothomas", "tenant_2", 3)  # time 3 for tenant_2

    # Access tree directly
    tree_rep: Dict = ray.get(tree_actor._to_dict.remote())
    root: Node = tree_rep["root"]

    # Test tree structure - validate each node
    # Root node
    assert root.text == ""
    assert root.tenant_to_last_access_time == {"tenant_1": 1, "tenant_2": 3}
    assert "h" in root.edge_label_to_child

    # Hello node
    hello_node: Node = root.edge_label_to_child["h"]
    assert hello_node.text == "hello"
    assert hello_node.tenant_to_last_access_time == {"tenant_1": 1, "tenant_2": 3}
    assert "w" in hello_node.edge_label_to_child
    assert "t" in hello_node.edge_label_to_child

    # World node
    world_node: Node = hello_node.edge_label_to_child["w"]
    assert world_node.text == "world"
    assert world_node.tenant_to_last_access_time == {"tenant_1": 1}
    assert len(world_node.edge_label_to_child) == 0

    # Th node
    th_node: Node = hello_node.edge_label_to_child["t"]
    assert th_node.text == "th"
    assert th_node.tenant_to_last_access_time == {"tenant_2": 3}
    assert "e" in th_node.edge_label_to_child
    assert "o" in th_node.edge_label_to_child

    # Ere node
    ere_node: Node = th_node.edge_label_to_child["e"]
    assert ere_node.text == "ere"
    assert ere_node.tenant_to_last_access_time == {"tenant_2": 2}
    assert len(ere_node.edge_label_to_child) == 0

    # Omas node
    omas_node: Node = th_node.edge_label_to_child["o"]
    assert omas_node.text == "omas"
    assert omas_node.tenant_to_last_access_time == {"tenant_2": 3}
    assert len(omas_node.edge_label_to_child) == 0

    # Test PrefixTree instance variables
    # Using tenant_to_nodes instead of tenants
    assert set(tree_rep["tenant_to_nodes"].keys()) == {"tenant_1", "tenant_2"}

    # Test tenant_to_nodes (check by text)
    tenant1_nodes_texts: Set[str] = {
        node.text for node in tree_rep["tenant_to_nodes"]["tenant_1"]
    }
    assert tenant1_nodes_texts == {"", "hello", "world"}

    tenant2_nodes_texts: Set[str] = {
        node.text for node in tree_rep["tenant_to_nodes"]["tenant_2"]
    }
    assert tenant2_nodes_texts == {"", "hello", "th", "ere", "omas"}

    # Test tenant_to_char_count
    # Before evictions
    assert (
        tree_rep["tenant_to_char_count"]["tenant_1"] == 10
    )  # root(0) + hello(5) + world(5) = 10
    assert (
        tree_rep["tenant_to_char_count"]["tenant_2"] == 14
    )  # root(0) + hello(5) + th(2) + ere(3) + omas(4) = 14

    # After evicting tenant_1 with min_remove_size=1
    # Should remove both "hello" and "world" nodes (10 chars) since they have the same timestamp
    evicted_count = ray.get(tree_actor.evict_tenant_by_lru.remote("tenant_1", 1))
    assert evicted_count == 10  # All 10 chars removed, not just 1
    tree_rep = ray.get(tree_actor._to_dict.remote())
    assert tree_rep["tenant_to_char_count"]["tenant_1"] == 0

    # After evicting tenant_2 with min_remove_size=1
    # Should remove "ere" node (3 chars) since it has the oldest timestamp (2)
    evicted_count = ray.get(tree_actor.evict_tenant_by_lru.remote("tenant_2", 1))
    assert evicted_count == 3  # All 3 chars from "ere" removed

    tree_rep = ray.get(tree_actor._to_dict.remote())
    assert tree_rep["tenant_to_char_count"]["tenant_2"] == 11  # 14 - 3 = 11

    # After evicting tenant_2 again with min_remove_size=1
    # Should remove "hello", "th", and "omas" nodes (11 chars) since they all have timestamp 3
    evicted_count = ray.get(tree_actor.evict_tenant_by_lru.remote("tenant_2", 1))
    assert evicted_count == 11  # All 11 remaining chars removed

    tree_rep = ray.get(tree_actor._to_dict.remote())
    assert tree_rep["tenant_to_char_count"]["tenant_2"] == 0


# PrefixTree tests
def test__add_tenant(tree: PrefixTree) -> None:
    """Test adding tenants to the tree via the private _add_tenant method."""
    # 1. Test basic tenant addition
    tree._reset()
    tree._add_tenant("tenant_1")
    assert "tenant_1" in tree.tenant_to_nodes
    assert tree.tenant_to_char_count["tenant_1"] == 0
    assert tree.tenant_to_nodes["tenant_1"] == set()

    # 2. Test adding duplicate tenant logs warning but doesn't raise error
    tree._reset()
    tree._add_tenant("tenant_1")
    # This should be a no-op
    tree._add_tenant("tenant_1")
    # Verify the tenant still exists
    assert "tenant_1" in tree.tenant_to_nodes


def test_insert(tree: PrefixTree) -> None:
    """Test the insert functionality of PrefixTree."""
    # 1. Test basic insertion
    tree._reset()
    # No need to call add_tenant first - insert will do it automatically
    tree.insert("hello", "tenant_1", 1)
    matched_text, matched_tenants = tree.prefix_match("hello")
    assert matched_text == "hello" and matched_tenants == ["tenant_1"]
    assert tree.tenant_to_char_count["tenant_1"] == 5
    assert len(tree.tenant_to_nodes["tenant_1"]) == 2

    # 2. Test duplicate insertion doesn't double count
    tree._reset()
    tree.insert("foo", "tenant_1", 1)
    tree.insert("foo", "tenant_1", 1)  # duplicate
    tree.insert("bar", "tenant_2", 2)
    assert (
        tree.tenant_to_char_count["tenant_1"] == 3
        and tree.tenant_to_char_count["tenant_2"] == 3
    )

    # 3. Test node splitting on partial match
    tree._reset()
    tree.insert("helloworld", "tenant_1", 1)
    tree.insert("hellothere", "tenant_2", 2)

    root: Node = tree.root
    h_node: Optional[Node] = root.edge_label_to_child.get("h")
    assert h_node is not None and h_node.text == "hello"
    assert h_node.edge_label_to_child.get("w").text == "world"
    assert h_node.edge_label_to_child.get("t").text == "there"

    # 4. Test that inserting a longer prompt with shared prefix doesn't create empty text nodes
    tree._reset()
    tree.insert("hello", "tenant_1", 1)
    tree.insert("helloworld", "tenant_2", 2)

    root = tree.root

    # Check that only the root has empty text by directly traversing the tree
    # Starting from root, collect all nodes with empty text
    empty_text_nodes: List[Node] = []
    nodes_to_check: List[Node] = [root]

    while nodes_to_check:
        node: Node = nodes_to_check.pop()
        if node.text == "":
            empty_text_nodes.append(node)
        # Add all children to check
        nodes_to_check.extend(node.edge_label_to_child.values())

    # There should be exactly one empty text node (the root)
    assert len(empty_text_nodes) == 1 and root in empty_text_nodes

    # Verify tree structure
    h_node = root.edge_label_to_child.get("h")
    assert h_node is not None and h_node.text == "hello"
    assert (
        "tenant_1" in h_node.tenant_to_last_access_time
        and "tenant_2" in h_node.tenant_to_last_access_time
    )

    # Verify "world" node belongs only to tenant 2
    world_node: Optional[Node] = h_node.edge_label_to_child.get("w")
    assert world_node is not None and world_node.text == "world"
    assert (
        "tenant_2" in world_node.tenant_to_last_access_time
        and "tenant_1" not in world_node.tenant_to_last_access_time
    )

    # Verify the only child of h_node is "w"
    assert len(h_node.edge_label_to_child) == 1


def test_prefix_match(tree: PrefixTree) -> None:
    """Test the prefix_match functionality of PrefixTree."""
    # 1. Test no match
    tree._reset()
    matched_text, matched_tenants = tree.prefix_match("hello")
    assert matched_text == "" and matched_tenants is None

    # 2. Test match with non-existing prefix returns empty string and all tenants
    tree._reset()
    tree.insert("hello", "tenant_1", 1)
    tree.insert("hellothere", "tenant_2", 2)
    matched_text, matched_tenants = tree.prefix_match("foobar")
    assert matched_text == "" and matched_tenants == ["tenant_1", "tenant_2"]

    # 3. Test exact match
    tree._reset()
    tree.insert("hello", "tenant_1", 1)
    matched_text, matched_tenants = tree.prefix_match("hello")
    assert matched_text == "hello" and matched_tenants == ["tenant_1"]

    # 4. Test partial match
    tree._reset()
    tree.insert("apple", "tenant_1", 1)
    tree.insert("apricot", "tenant_2", 2)
    matched_text, matched_tenants = tree.prefix_match("application")
    assert matched_text == "appl" and matched_tenants == ["tenant_1"]

    # 5. Test match by tenant
    tree._reset()
    tree.insert("apple", "tenant_1", 1)
    tree.insert("apricot", "tenant_2", 2)
    matched_text, matched_tenants = tree.prefix_match("application", ["tenant_2"])
    assert matched_text == "ap" and matched_tenants == ["tenant_2"]

    # 6. Test match by non-existent tenant
    tree._reset()
    tree.insert("apple", "tenant_1", 1)
    tree.insert("apricot", "tenant_2", 2)
    matched_text, matched_tenants = tree.prefix_match("application", ["tenant_3"])
    assert matched_text == "" and matched_tenants is None

    # 7. Test shared prefix matching with branches
    tree._reset()
    tree.insert("helloworld", "tenant_1", 1)
    tree.insert("hellothere", "tenant_2", 2)

    matched_text, matched_tenants = tree.prefix_match("helloworld")
    assert matched_text == "helloworld" and matched_tenants == ["tenant_1"]

    matched_text, matched_tenants = tree.prefix_match("hellothereworld")
    assert matched_text == "hellothere" and matched_tenants == ["tenant_2"]


def test__remove_tenant_single_node(tree: PrefixTree) -> None:
    """Test removing a single node for a tenant."""
    # 1. Test removing a single node
    tree._reset()
    tree.insert("hello", "tenant_1", 1)
    h_node: Node = tree.insert("hello", "tenant_1", 1)

    removed: int = tree._remove_tenant_single_node("tenant_1", h_node)
    assert removed == 5
    assert tree.tenant_to_char_count["tenant_1"] == 0
    assert (
        len(tree.tenant_to_nodes["tenant_1"]) == 1
        and tree.root in tree.tenant_to_nodes["tenant_1"]
    )

    # 2. Test removing node for non-existent tenant is idempotent
    tree._reset()
    tree.insert("hello", "tenant_1", 1)
    root: Node = tree.root
    h_node: Optional[Node] = root.edge_label_to_child.get("h")

    # Should not raise error, just return 0
    removed = tree._remove_tenant_single_node("nonexistent_tenant", h_node)
    assert removed == 0

    # 3. Test removing node that doesn't belong to tenant is idempotent
    tree._reset()
    tree.insert("hello", "tenant_1", 1)
    tree.insert("world", "tenant_2", 2)

    root = tree.root
    h_node = root.edge_label_to_child.get("h")

    # Should not raise error, just return 0
    removed = tree._remove_tenant_single_node("tenant_2", h_node)
    assert removed == 0


def test_remove_tenant(tree: PrefixTree) -> None:
    """Test removing a tenant from the tree."""
    # 1. Test basic tenant removal
    tree._reset()
    tree.insert("hello", "tenant_1", 1)
    removed: int = tree.remove_tenant("tenant_1")
    assert removed == 5
    assert (
        "tenant_1" not in tree.tenant_to_nodes
        and "tenant_1" not in tree.tenant_to_char_count
    )

    # 2. Test removing tenant with multiple nodes
    tree._reset()
    tree.insert("cat", "tenant_1", 1)
    tree.insert("dog", "tenant_1", 2)
    removed = tree.remove_tenant("tenant_1")
    assert removed == len("cat") + len("dog")

    # 3. Test removing non-existent tenant is idempotent (logs warning, returns 0)
    tree._reset()
    # Should not raise error, just return 0
    removed = tree.remove_tenant("nonexistent_tenant")
    assert removed == 0

    # 4. Test tree structure after removing tenant
    tree._reset()
    tree.insert("hello", "tenant_1", 1)
    tree.insert("hello", "tenant_2", 2)

    # Remove tenant_1, verify tenant_2 still works
    tree.remove_tenant("tenant_1")
    assert "tenant_1" not in tree.tenant_to_nodes and "tenant_2" in tree.tenant_to_nodes

    matched_text, matched_tenants = tree.prefix_match("hello")
    assert matched_text == "hello" and matched_tenants == ["tenant_2"]

    # 5. Test removing the last tenant from a node removes the node
    tree._reset()
    tree.insert("helloworld", "tenant_1", 1)
    tree.insert("hellothere", "tenant_2", 2)

    # Remove tenant_1
    tree.remove_tenant("tenant_1")

    root: Node = tree.root
    # 'h' node should only have one child now ('t' from hellothere)
    assert "h" in root.edge_label_to_child
    assert "t" in root.edge_label_to_child["h"].edge_label_to_child
    assert len(root.edge_label_to_child["h"].edge_label_to_child) == 1


def test_evict_tenant_by_lru(tree: PrefixTree) -> None:
    """Test the evict_tenant_by_lru functionality of PrefixTree."""

    # 1. Remove exactly min_remove_size characters
    tree._reset()
    tree.insert("a", "tenant_1", 1)
    tree.insert("bb", "tenant_1", 2)
    tree.insert("ccc", "tenant_1", 3)

    # Before eviction
    char_count_before: int = tree.tenant_to_char_count["tenant_1"]
    assert (
        len(tree.tenant_to_nodes["tenant_1"]) == 4
        and tree.tenant_to_char_count["tenant_1"] == 6
    )

    # During eviction
    min_remove_size: int = 1
    evicted_count: int = tree.evict_tenant_by_lru("tenant_1", min_remove_size)

    # After eviction
    char_count_after: int = tree.tenant_to_char_count["tenant_1"]
    assert evicted_count == min_remove_size
    assert char_count_before - char_count_after == evicted_count
    assert (
        len(tree.tenant_to_nodes["tenant_1"]) == 3
        and tree.tenant_to_char_count["tenant_1"] == 5
    )

    # 2. Remove more than min_remove_size characters
    tree._reset()
    tree.insert("a", "tenant_1", 1)
    tree.insert("bb", "tenant_1", 2)
    tree.insert("ccc", "tenant_1", 3)

    # Before eviction
    char_count_before = tree.tenant_to_char_count["tenant_1"]
    assert (
        len(tree.tenant_to_nodes["tenant_1"]) == 4
        and tree.tenant_to_char_count["tenant_1"] == 6
    )

    # During eviction
    min_remove_size = 2
    evicted_count = tree.evict_tenant_by_lru("tenant_1", min_remove_size)

    # After eviction
    char_count_after = tree.tenant_to_char_count["tenant_1"]
    assert evicted_count != min_remove_size and evicted_count == 3
    assert char_count_before - char_count_after == evicted_count
    assert (
        len(tree.tenant_to_nodes["tenant_1"]) == 2
        and tree.tenant_to_char_count["tenant_1"] == 3
    )

    # 3. Test eviction of non-existent tenant is idempotent
    tree._reset()
    # Should not raise error, just return 0
    evicted_count = tree.evict_tenant_by_lru("nonexistent_tenant", 5)
    assert evicted_count == 0

    # 4. Test eviction of tenant with insufficient characters is idempotent
    tree._reset()
    tree.insert("xyz", "tenant_1", 1)
    # Should not raise error, should evict all available characters
    evicted_count = tree.evict_tenant_by_lru("tenant_1", 4)
    assert evicted_count == 3  # "xyz" has 3 characters

    # 5. Test eviction of all tenant data
    tree._reset()
    tree.insert("xyz", "tenant_1", 1)

    total_size: int = tree.tenant_to_char_count["tenant_1"]
    evicted_count = tree.evict_tenant_by_lru("tenant_1", total_size)
    assert evicted_count == total_size
    # "tenant_1" should still be in tenant_to_nodes
    assert "tenant_1" in tree.tenant_to_nodes

    # 6. Test tree structure and LRU eviction
    tree._reset()

    # Insert strings in specified order
    tree.insert("helloworld", "tenant_1", 1)  # time 1 for tenant_1
    tree.insert("hellothere", "tenant_2", 2)  # time 2 for tenant_2
    tree.insert("hellothomas", "tenant_2", 3)  # time 3 for tenant_2

    # Access tree directly
    root: Node = tree.root

    # Test tree structure - validate each node
    # Root node
    assert root.text == "" and root.tenant_to_last_access_time == {
        "tenant_1": 1,
        "tenant_2": 3,
    }
    assert "h" in root.edge_label_to_child

    # Hello node
    hello_node: Node = root.edge_label_to_child["h"]
    assert hello_node.text == "hello" and hello_node.tenant_to_last_access_time == {
        "tenant_1": 1,
        "tenant_2": 3,
    }
    assert (
        "w" in hello_node.edge_label_to_child and "t" in hello_node.edge_label_to_child
    )

    # World node
    world_node: Node = hello_node.edge_label_to_child["w"]
    assert world_node.text == "world" and world_node.tenant_to_last_access_time == {
        "tenant_1": 1
    }
    assert len(world_node.edge_label_to_child) == 0

    # Th node
    th_node: Node = hello_node.edge_label_to_child["t"]
    assert th_node.text == "th" and th_node.tenant_to_last_access_time == {
        "tenant_2": 3
    }
    assert "e" in th_node.edge_label_to_child and "o" in th_node.edge_label_to_child

    # Ere node
    ere_node: Node = th_node.edge_label_to_child["e"]
    assert ere_node.text == "ere" and ere_node.tenant_to_last_access_time == {
        "tenant_2": 2
    }
    assert len(ere_node.edge_label_to_child) == 0

    # Omas node
    omas_node: Node = th_node.edge_label_to_child["o"]
    assert omas_node.text == "omas" and omas_node.tenant_to_last_access_time == {
        "tenant_2": 3
    }
    assert len(omas_node.edge_label_to_child) == 0

    # Test PrefixTree instance variables
    assert set(tree.tenant_to_nodes.keys()) == {"tenant_1", "tenant_2"}

    # Test tenant_to_nodes (check by text)
    tenant1_nodes_texts: Set[str] = {
        node.text for node in tree.tenant_to_nodes["tenant_1"]
    }
    assert tenant1_nodes_texts == {"", "hello", "world"}

    tenant2_nodes_texts: Set[str] = {
        node.text for node in tree.tenant_to_nodes["tenant_2"]
    }
    assert tenant2_nodes_texts == {"", "hello", "th", "ere", "omas"}

    # Test tenant_to_char_count
    # Before evictions
    assert (
        tree.tenant_to_char_count["tenant_1"] == 10
        and tree.tenant_to_char_count["tenant_2"] == 14
    )

    # After evicting tenant_1 with min_remove_size=1
    # Should remove both "hello" and "world" nodes (10 chars) since they have the same timestamp
    evicted_count = tree.evict_tenant_by_lru("tenant_1", 1)
    assert evicted_count == 10 and tree.tenant_to_char_count["tenant_1"] == 0

    # After evicting tenant_2 with min_remove_size=1
    # Should remove "ere" node (3 chars) since it has the oldest timestamp (2)
    evicted_count = tree.evict_tenant_by_lru("tenant_2", 1)
    assert (
        evicted_count == 3 and tree.tenant_to_char_count["tenant_2"] == 11
    )  # 14 - 3 = 11

    # After evicting tenant_2 again with min_remove_size=1
    # Should remove "hello", "th", and "omas" nodes (11 chars) since they all have timestamp 3
    evicted_count = tree.evict_tenant_by_lru("tenant_2", 1)
    assert evicted_count == 11 and tree.tenant_to_char_count["tenant_2"] == 0


def test_get_smallest_tenant(tree: PrefixTree) -> None:
    """Test the get_smallest_tenant functionality of PrefixTree."""
    # 1. Test with empty tree
    tree._reset()
    smallest: Optional[str] = tree.get_smallest_tenant()
    assert smallest is None

    # 2. Test with multiple tenants of different sizes
    tree._reset()
    tree.insert("aaaa", "tenant_1", 1)
    tree.insert("bb", "tenant_2", 2)
    tree.insert("c", "tenant_3", 3)

    smallest = tree.get_smallest_tenant()
    assert smallest == "tenant_3"

    # 3. Test after removing the smallest tenant
    tree._reset()
    tree.insert("aaaa", "tenant_1", 1)
    tree.insert("bb", "tenant_2", 2)
    tree.insert("c", "tenant_3", 3)
    tree.remove_tenant("tenant_3")
    smallest = tree.get_smallest_tenant()
    assert smallest == "tenant_2"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
