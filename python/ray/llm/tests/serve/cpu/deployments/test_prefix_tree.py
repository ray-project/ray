import pytest
import time
import ray
from ray import serve

from ray.llm._internal.serve.deployments.routers.prefix_tree import PrefixTree


@pytest.fixture(scope="module", autouse=True)
def serve_instance():
    # Start Ray and Serve once per test module
    ray.init(ignore_reinit_error=True)
    serve.start(detached=True)
    yield
    serve.shutdown()
    ray.shutdown()


@pytest.mark.asyncio
async def test_insert_and_basic_match():
    # Deploy a clean PrefixTree
    tree = serve.run(PrefixTree.bind())

    # Insert and match exact string
    await tree.insert.remote("hello", "tenant-A")
    matched_text, tenants = await tree.prefix_match.remote("hello")
    assert matched_text == "hello"
    assert tenants == ["tenant-A"]


@pytest.mark.asyncio
async def test_tree_splits_nodes_on_partial_match():
    tree = serve.run(PrefixTree.bind())
    await tree.insert.remote("helloworld", "A")
    await tree.insert.remote("hellothere", "B")

    # After inserting both, the root should have one child "h"
    root = await tree.get_root.remote()
    h_node = root.children.get("h")
    assert h_node is not None


@pytest.mark.asyncio
async def test_no_match():
    tree = serve.run(PrefixTree.bind())
    matched_text, tenants = await tree.prefix_match.remote("hello")
    assert matched_text == ""
    assert tenants is None


@pytest.mark.asyncio
async def test_duplicate_insertion_no_double_count():
    tree = serve.run(PrefixTree.bind())

    await tree.insert.remote("foo", "T1")
    await tree.insert.remote("foo", "T1")  # duplicate

    counts = await tree.get_tenant_char_count.remote()
    # Should count 'foo' only once
    assert counts.get("T1", 0) == 3


@pytest.mark.asyncio
async def test_shared_prefix_splitting_and_branching():
    tree = serve.run(PrefixTree.bind())

    await tree.insert.remote("helloworld", "A")
    await tree.insert.remote("hellothere", "B")

    text_a, tenants_a = await tree.prefix_match.remote("helloworld")
    text_b, tenants_b = await tree.prefix_match.remote("hellothere")

    assert text_a == "helloworld"
    assert tenants_a == ["A"]
    assert text_b == "hellothere"
    assert tenants_b == ["B"]


@pytest.mark.asyncio
async def test_prefix_match_partial_and_filter():
    tree = serve.run(PrefixTree.bind())

    await tree.insert.remote("apple", "X")
    await tree.insert.remote("apricot", "Y")

    # Partial match for 'application' -> 'appl'
    text, tenants = await tree.prefix_match.remote("application")
    assert text == "appl"
    assert tenants == ["X"]

    # Filter by available_tenants=['X'] on 'apricot'
    text_fx, tenants_fx = await tree.prefix_match.remote("apricot", ["X"])
    assert text_fx == "ap"
    assert tenants_fx == ["X"]

    # Filter by non-existent tenant yields no tenants
    text_fz, tenants_fz = await tree.prefix_match.remote("apricot", ["Z"])
    assert text_fz == ""
    assert tenants_fz is None


@pytest.mark.asyncio
async def test_remove_and_get_smallest_and_evict():
    tree = serve.run(PrefixTree.bind())

    # Test removal and char count
    await tree.insert.remote("cat", "T1")
    await tree.insert.remote("dog", "T1")

    counts = await tree.get_tenant_char_count.remote()
    assert counts.get("T1") == len("cat") + len("dog")

    # Remove entire tenant
    removed = await tree.remove_tenant_entirely.remote("T1")
    assert removed == len("cat") + len("dog")

    counts_after = await tree.get_tenant_char_count.remote()
    assert "T1" not in counts_after

    # Test eviction LRU behavior
    await tree.insert.remote("a", "T2")
    time.sleep(0.001)
    await tree.insert.remote("bb", "T2")
    time.sleep(0.001)
    await tree.insert.remote("ccc", "T2")

    before = (await tree.get_tenant_char_count.remote())["T2"]
    evicted = await tree.evict_tenant.remote("T2", 2)
    after = (await tree.get_tenant_char_count.remote())["T2"]

    assert evicted >= 2
    assert before - after == evicted


@pytest.mark.asyncio
async def test_get_smallest_tenant():
    tree = serve.run(PrefixTree.bind())
    await tree.insert.remote("aaaa", "A")
    await tree.insert.remote("bb", "B")
    smallest = await tree.get_smallest_tenant.remote()
    assert smallest == "B"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
