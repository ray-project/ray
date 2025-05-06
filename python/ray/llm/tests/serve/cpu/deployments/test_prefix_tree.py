import pytest
import time
import ray
from ray import serve
import heapq

from ray.llm._internal.serve.replica_scheduler.prefix_aware.prefix_tree import (
    PrefixTree,
)


@pytest.fixture(scope="module", autouse=True)
def serve_instance():
    # Start Ray and Serve once per test module
    ray.init(ignore_reinit_error=True)
    serve.start(detached=True)
    yield
    serve.shutdown()
    ray.shutdown()


@pytest.mark.asyncio
async def test_add_tenant():
    """Test adding tenants to the tree via the private _add_tenant method."""
    tree = serve.run(PrefixTree.bind())

    # 1. Test basic tenant addition
    await tree.reset.remote()
    await tree._add_tenant.remote("tenant_1")
    tree_rep = await tree.to_dict.remote()
    assert "tenant_1" in tree_rep["tenants"]
    assert tree_rep["tenant_char_count"]["tenant_1"] == 0
    assert tree_rep["tenant_nodes"]["tenant_1"] == set()

    # 2. Test adding duplicate tenant logs warning but doesn't raise error
    await tree.reset.remote()
    await tree._add_tenant.remote("tenant_1")
    # This should not raise an error now
    await tree._add_tenant.remote("tenant_1")
    # Verify the tenant still exists
    tree_rep = await tree.to_dict.remote()
    assert "tenant_1" in tree_rep["tenants"]


@pytest.mark.asyncio
async def test_insert():
    """Test the insert functionality of PrefixTree."""
    tree = serve.run(PrefixTree.bind())

    # 1. Test basic insertion
    await tree.reset.remote()
    # No need to call add_tenant first - insert will do it automatically
    await tree.insert.remote("hello", "tenant_1", int(time.time() * 1000))
    matched_text, tenants = await tree.prefix_match.remote("hello")
    assert matched_text == "hello"
    assert tenants == ["tenant_1"]

    tree_rep = await tree.to_dict.remote()
    assert tree_rep["tenant_char_count"]["tenant_1"] == 5
    assert len(tree_rep["tenant_nodes"]["tenant_1"]) == 2

    # 2. Test duplicate insertion doesn't double count
    await tree.reset.remote()
    # Insert automatically adds tenants
    await tree.insert.remote("foo", "tenant_1", int(time.time() * 1000))
    await tree.insert.remote("foo", "tenant_1", int(time.time() * 1000))  # duplicate
    await tree.insert.remote("bar", "tenant_2", int(time.time() * 1000))

    tree_rep = await tree.to_dict.remote()
    assert tree_rep["tenant_char_count"]["tenant_1"] == 3
    assert tree_rep["tenant_char_count"]["tenant_2"] == 3

    # 3. Test node splitting on partial match
    await tree.reset.remote()
    await tree.insert.remote("helloworld", "tenant_1", int(time.time() * 1000))
    await tree.insert.remote("hellothere", "tenant_2", int(time.time() * 1000))

    tree_rep = await tree.to_dict.remote()
    root = tree_rep["root"]
    h_node = root.children.get("h")
    assert h_node is not None
    assert h_node.text == "hello"
    assert h_node.children.get("w").text == "world"
    assert h_node.children.get("t").text == "there"

    # 4. Test inserting for non-existent tenant automatically adds the tenant
    await tree.reset.remote()
    # This should not raise an error now
    await tree.insert.remote("hello", "nonexistent_tenant", int(time.time() * 1000))

    # Verify the tenant was added
    tree_rep = await tree.to_dict.remote()
    assert "nonexistent_tenant" in tree_rep["tenants"]
    assert tree_rep["tenant_char_count"]["nonexistent_tenant"] == 5


@pytest.mark.asyncio
async def test_prefix_match():
    """Test the prefix_match functionality of PrefixTree."""
    tree = serve.run(PrefixTree.bind())

    # # 1. Test no match
    # await tree.reset.remote()
    # matched_text, tenants = await tree.prefix_match.remote("hello")
    # assert matched_text == ""
    # assert tenants is None

    # 2. Test match with non-existing prefix returns empty string and all tenants
    await tree.reset.remote()
    await tree.insert.remote("hello", "tenant_1", int(time.time() * 1000))
    await tree.insert.remote("hellothere", "tenant_2", int(time.time() * 1000))
    matched_text, tenants = await tree.prefix_match.remote("foobar")
    assert matched_text == ""
    assert len(tenants) == 2
    assert "tenant_1" in tenants
    assert "tenant_2" in tenants

    # 3. Test exact match
    await tree.reset.remote()
    await tree.insert.remote("hello", "tenant_1", int(time.time() * 1000))
    matched_text, tenants = await tree.prefix_match.remote("hello")
    assert matched_text == "hello"
    assert tenants == ["tenant_1"]

    # 4. Test partial match
    await tree.reset.remote()
    await tree.insert.remote("apple", "tenant_1", int(time.time() * 1000))
    await tree.insert.remote("apricot", "tenant_2", int(time.time() * 1000))
    text, tenants = await tree.prefix_match.remote("application")
    assert text == "appl"
    assert tenants == ["tenant_1"]

    # 5. Test match by tenant
    await tree.reset.remote()
    await tree.insert.remote("apple", "tenant_1", int(time.time() * 1000))
    await tree.insert.remote("apricot", "tenant_2", int(time.time() * 1000))
    text, tenants = await tree.prefix_match.remote("application", ["tenant_2"])
    assert text == "ap"
    assert tenants == ["tenant_2"]

    # 6. Test match by non-existent tenant
    await tree.reset.remote()
    await tree.insert.remote("apple", "tenant_1", int(time.time() * 1000))
    await tree.insert.remote("apricot", "tenant_2", int(time.time() * 1000))
    text, tenants = await tree.prefix_match.remote("application", ["tenant_3"])
    assert text == ""
    assert tenants is None

    # 7. Test shared prefix matching with branches
    await tree.reset.remote()
    await tree.insert.remote("helloworld", "tenant_1", int(time.time() * 1000))
    await tree.insert.remote("hellothere", "tenant_2", int(time.time() * 1000))
    text_a, tenants_a = await tree.prefix_match.remote("helloworld")
    text_b, tenants_b = await tree.prefix_match.remote("hellothereworld")
    assert text_a == "helloworld"
    assert tenants_a == ["tenant_1"]
    assert text_b == "hellothere"
    assert tenants_b == ["tenant_2"]


@pytest.mark.asyncio
async def test_remove_tenant():
    """Test removing a tenant from the tree."""
    tree = serve.run(PrefixTree.bind())

    # 1. Test basic tenant removal
    await tree.reset.remote()
    await tree.insert.remote("hello", "tenant_1", int(time.time() * 1000))
    removed = await tree.remove_tenant.remote("tenant_1")
    assert removed == 5

    tree_rep = await tree.to_dict.remote()
    assert "tenant_1" not in tree_rep["tenants"]
    assert "tenant_1" not in tree_rep["tenant_char_count"]
    assert "tenant_1" not in tree_rep["tenant_nodes"]

    # 2. Test removing tenant with multiple nodes
    await tree.reset.remote()
    await tree.insert.remote("cat", "tenant_1", int(time.time() * 1000))
    await tree.insert.remote("dog", "tenant_1", int(time.time() * 1000))
    removed = await tree.remove_tenant.remote("tenant_1")
    assert removed == len("cat") + len("dog")

    # 3. Test removing non-existent tenant raises ValueError
    await tree.reset.remote()
    with pytest.raises(ValueError):
        await tree.remove_tenant.remote("nonexistent_tenant")

    # 4. Test tree structure after removing tenant
    await tree.reset.remote()
    await tree.insert.remote("hello", "tenant_1", int(time.time() * 1000))
    await tree.insert.remote("hello", "tenant_2", int(time.time() * 1000))

    # Remove tenant_1, verify tenant_2 still works
    await tree.remove_tenant.remote("tenant_1")

    tree_rep = await tree.to_dict.remote()
    assert "tenant_1" not in tree_rep["tenants"]
    assert "tenant_2" in tree_rep["tenants"]

    matched_text, tenants = await tree.prefix_match.remote("hello")
    assert matched_text == "hello"
    assert tenants == ["tenant_2"]

    # 5. Test removing the last tenant from a node removes the node
    await tree.reset.remote()
    await tree.insert.remote("unique1", "tenant_1", int(time.time() * 1000))
    await tree.insert.remote("unique2", "tenant_2", int(time.time() * 1000))

    # Remove tenant_1
    await tree.remove_tenant.remote("tenant_1")

    tree_rep = await tree.to_dict.remote()
    root = tree_rep["root"]
    # 'u' node should only have one child now ('2' from unique2)
    assert "u" in root.children
    assert "2" in root.children["u"].children  # '2' from unique2
    assert len(root.children["u"].children) == 1


@pytest.mark.asyncio
async def test__remove_tenant_single_node():
    """Test removing a single node for a tenant."""
    tree = serve.run(PrefixTree.bind())

    # # 1. Test removing a single node
    # TEST FAILS: Ray creates new node instances when making remote calls?
    # The node from insert.remote() is not identity-equal to the one in tenant_nodes

    # await tree.reset.remote()
    # await tree.insert.remote("hello", "tenant_1", int(time.time() * 1000))
    # h_node = await tree.insert.remote("hello", "tenant_1", int(time.time() * 1000))

    # removed = await tree._remove_tenant_single_node.remote("tenant_1", h_node)
    # assert removed == 5

    # tree_rep = await tree.to_dict.remote()
    # assert tree_rep["tenant_char_count"]["tenant_1"] == 0
    # assert tree_rep["tenant_nodes"]["tenant_1"] == set()

    # 2. Test removing node for non-existent tenant raises ValueError
    await tree.reset.remote()
    await tree.insert.remote("hello", "tenant_1", int(time.time() * 1000))

    tree_rep = await tree.to_dict.remote()
    root = tree_rep["root"]
    h_node = root.children.get("h")

    with pytest.raises(ValueError):
        await tree._remove_tenant_single_node.remote("nonexistent_tenant", h_node)

    # 3. Test removing node that doesn't belong to tenant raises ValueError
    await tree.reset.remote()
    await tree.insert.remote("hello", "tenant_1", int(time.time() * 1000))
    await tree.insert.remote("world", "tenant_2", int(time.time() * 1000))

    tree_rep = await tree.to_dict.remote()
    root = tree_rep["root"]
    h_node = root.children.get("h")

    with pytest.raises(ValueError):
        await tree._remove_tenant_single_node.remote("tenant_2", h_node)


@pytest.mark.asyncio
async def test_evict_tenant_by_lru():
    """Test the evict_tenant_by_lru functionality of PrefixTree."""
    tree = serve.run(PrefixTree.bind())

    # 1. Test eviction with LRU ordering
    await tree.reset.remote()
    current_time = int(time.time() * 1000)
    await tree.insert.remote("a", "tenant_1", current_time)
    time.sleep(0.001)
    current_time = int(time.time() * 1000)
    await tree.insert.remote("bb", "tenant_1", current_time)
    time.sleep(0.001)
    current_time = int(time.time() * 1000)
    await tree.insert.remote("ccc", "tenant_1", current_time)

    tree_rep = await tree.to_dict.remote()
    before = tree_rep["tenant_char_count"]["tenant_1"]

    evicted = await tree.evict_tenant_by_lru.remote("tenant_1", 2)

    tree_rep = await tree.to_dict.remote()
    after = tree_rep["tenant_char_count"]["tenant_1"]

    assert evicted == 3
    assert before - after == evicted
    assert "tenant_1" in tree_rep["tenants"]

    # 2. Test eviction of non-existent tenant raises ValueError
    await tree.reset.remote()
    with pytest.raises(ValueError):
        await tree.evict_tenant_by_lru.remote("nonexistent_tenant", 5)

    # 3. Test eviction of tenant with insufficient characters raises ValueError
    await tree.reset.remote()
    await tree.insert.remote("xyz", "tenant_2", int(time.time() * 1000))
    with pytest.raises(ValueError):
        await tree.evict_tenant_by_lru.remote("tenant_2", 4)

    # 4. Test eviction of all tenant data
    await tree.reset.remote()
    await tree.insert.remote("xyz", "tenant_2", int(time.time() * 1000))

    tree_rep = await tree.to_dict.remote()
    total_size = tree_rep["tenant_char_count"]["tenant_2"]

    evicted = await tree.evict_tenant_by_lru.remote("tenant_2", total_size)
    assert evicted == total_size

    tree_rep = await tree.to_dict.remote()
    assert "tenant_2" in tree_rep["tenants"]

    # 5. Test tree structure and LRU heap ordering
    await tree.reset.remote()
    
    # Insert strings in specified order
    await tree.insert.remote("helloworld", "tenant_1", 1)  # time 1 for tenant_1
    await tree.insert.remote("hellothere", "tenant_2", 2)  # time 2 for tenant_2
    await tree.insert.remote("hellothomas", "tenant_2", 3)  # time 3 for tenant_2
    
    # Get tree representation for testing
    tree_rep = await tree.to_dict.remote()
    root = tree_rep["root"]
    
    # Test tree structure - validate each node
    # Root node
    assert root.text == ""
    assert root.tenant_last_access_time == {"tenant_1": 1, "tenant_2": 3}
    assert "h" in root.children
    
    # Hello node
    hello_node = root.children["h"]
    assert hello_node.text == "hello"
    assert hello_node.tenant_last_access_time == {"tenant_1": 1, "tenant_2": 3}
    assert "w" in hello_node.children
    assert "t" in hello_node.children
    
    # World node
    world_node = hello_node.children["w"]
    assert world_node.text == "world"
    assert world_node.tenant_last_access_time == {"tenant_1": 1}
    assert len(world_node.children) == 0
    
    # Th node
    th_node = hello_node.children["t"]
    assert th_node.text == "th"
    assert th_node.tenant_last_access_time == {"tenant_2": 3}
    assert "e" in th_node.children
    assert "o" in th_node.children
    
    # Ere node
    ere_node = th_node.children["e"]
    assert ere_node.text == "ere"
    assert ere_node.tenant_last_access_time == {"tenant_2": 2}
    assert len(ere_node.children) == 0
    
    # Omas node
    omas_node = th_node.children["o"]
    assert omas_node.text == "omas"
    assert omas_node.tenant_last_access_time == {"tenant_2": 3}
    assert len(omas_node.children) == 0
    
    # Test PrefixTree instance variables
    assert tree_rep["tenants"] == {"tenant_1", "tenant_2"}
    
    # Test tenant_char_count
    assert tree_rep["tenant_char_count"]["tenant_1"] == 10  # root(0) + hello(5) + world(5) = 10
    assert tree_rep["tenant_char_count"]["tenant_2"] == 14  # root(0) + hello(5) + th(2) + ere(3) + omas(4) = 14
    
    # Test tenant_nodes (check by text)
    tenant1_nodes_texts = {node.text for node in tree_rep["tenant_nodes"]["tenant_1"]}
    assert tenant1_nodes_texts == {"", "hello", "world"}
    
    tenant2_nodes_texts = {node.text for node in tree_rep["tenant_nodes"]["tenant_2"]}
    assert tenant2_nodes_texts == {"", "hello", "th", "ere", "omas"}
    
    # Test tenant_nodes_sorted - validate heap ordering
    assert heapq.heappop(tree_rep["tenant_nodes_sorted"]["tenant_1"]).node.tenant_last_access_time["tenant_1"] == 1
    assert heapq.heappop(tree_rep["tenant_nodes_sorted"]["tenant_1"]).node.tenant_last_access_time["tenant_1"] == 1
    assert heapq.heappop(tree_rep["tenant_nodes_sorted"]["tenant_2"]).node.tenant_last_access_time["tenant_2"] == 2
    assert heapq.heappop(tree_rep["tenant_nodes_sorted"]["tenant_2"]).node.tenant_last_access_time["tenant_2"] == 3


@pytest.mark.asyncio
async def test_get_smallest_tenant():
    """Test the get_smallest_tenant functionality of PrefixTree."""
    tree = serve.run(PrefixTree.bind())

    # 1. Test with empty tree
    await tree.reset.remote()
    smallest = await tree.get_smallest_tenant.remote()
    assert smallest is None

    # 2. Test with multiple tenants of different sizes
    await tree.reset.remote()
    current_time = int(time.time() * 1000)
    await tree.insert.remote("aaaa", "tenant_1", current_time)
    await tree.insert.remote("bb", "tenant_2", current_time)
    await tree.insert.remote("c", "tenant_3", current_time)

    smallest = await tree.get_smallest_tenant.remote()
    assert smallest == "tenant_3"

    # 3. Test after removing the smallest tenant
    await tree.reset.remote()
    current_time = int(time.time() * 1000)
    await tree.insert.remote("aaaa", "tenant_1", current_time)
    await tree.insert.remote("bb", "tenant_2", current_time)
    await tree.insert.remote("c", "tenant_3", current_time)
    await tree.remove_tenant.remote("tenant_3")
    smallest = await tree.get_smallest_tenant.remote()
    assert smallest == "tenant_2"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
