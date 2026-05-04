# coding: utf-8

import logging
import sys
import time

import numpy as np
import pytest

from ray._private.test_utils import client_test_enabled
from ray._private.worker import _wait_and_fetch, _wait_generators_bulk
from ray.exceptions import ObjectRefStreamEndOfStreamError

if client_test_enabled():
    from ray.util.client import ray
else:
    import ray
    import ray.util.state

logger = logging.getLogger(__name__)


def test_wait(ray_start_regular):
    @ray.remote
    def f(delay):
        time.sleep(delay)
        return

    object_refs = [f.remote(0), f.remote(0), f.remote(0), f.remote(0)]
    ready_ids, remaining_ids = ray.wait(object_refs)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 3
    ready_ids, remaining_ids = ray.wait(object_refs, num_returns=4)
    assert set(ready_ids) == set(object_refs)
    assert remaining_ids == []

    object_refs = [f.remote(0), f.remote(5)]
    ready_ids, remaining_ids = ray.wait(object_refs, timeout=0.5, num_returns=2)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 1

    # Verify that calling wait with duplicate object refs throws an
    # exception.
    x = ray.put(1)
    with pytest.raises(Exception):
        ray.wait([x, x])

    # Make sure it is possible to call wait with an empty list.
    ready_ids, remaining_ids = ray.wait([])
    assert ready_ids == []
    assert remaining_ids == []

    # Test semantics of num_returns with no timeout.
    obj_refs = [ray.put(i) for i in range(10)]
    (found, rest) = ray.wait(obj_refs, num_returns=2)
    assert len(found) == 2
    assert len(rest) == 8

    # Verify that incorrect usage raises a TypeError.
    x = ray.put(1)
    with pytest.raises(TypeError):
        ray.wait(x)
    with pytest.raises(TypeError):
        ray.wait(1)
    with pytest.raises(TypeError):
        ray.wait([1])


def test_wait_timing(ray_start_2_cpus):
    @ray.remote
    def f():
        time.sleep(1)

    future = f.remote()

    start = time.time()
    ready, not_ready = ray.wait([future], timeout=0.2)
    assert 0.2 < time.time() - start < 0.3
    assert len(ready) == 0
    assert len(not_ready) == 1


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test_wait_always_fetch_local(monkeypatch, ray_start_cluster):
    monkeypatch.setenv("RAY_scheduler_report_pinned_bytes_only", "false")
    cluster = ray_start_cluster
    head_node = cluster.add_node(num_cpus=0, object_store_memory=300e6)
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(num_cpus=1, object_store_memory=300e6)

    @ray.remote(num_cpus=1)
    def return_large_object():
        # 100mb so will spill on worker, but not once on head
        return np.zeros(100 * 1024 * 1024, dtype=np.uint8)

    @ray.remote(num_cpus=0)
    def small_local_task():
        return 1

    put_on_head = {ray._raylet.RAY_NODE_ID_KEY: head_node.node_id}
    put_on_worker = {ray._raylet.RAY_NODE_ID_KEY: worker_node.node_id}
    x = small_local_task.options(label_selector=put_on_head).remote()
    y = return_large_object.options(label_selector=put_on_worker).remote()
    z = return_large_object.options(label_selector=put_on_worker).remote()

    # will return when tasks are done
    ray.wait([x, y, z], num_returns=3, fetch_local=False)
    assert (
        ray._private.state.available_resources_per_node()[head_node.node_id][
            "object_store_memory"
        ]
        > 250e6
    )

    # x should be immediately available locally, start fetching y and z
    ray.wait([x, y, z], num_returns=1, fetch_local=True)
    assert (
        ray._private.state.available_resources_per_node()[head_node.node_id][
            "object_store_memory"
        ]
        > 250e6
    )

    time.sleep(5)
    # y, z should be pulled here
    assert (
        ray._private.state.available_resources_per_node()[head_node.node_id][
            "object_store_memory"
        ]
        < 150e6
    )


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk_fetch_local(monkeypatch, ray_start_cluster):
    monkeypatch.setenv("RAY_scheduler_report_pinned_bytes_only", "false")
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, object_store_memory=500e6)
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(num_cpus=2, object_store_memory=500e6)

    @ray.remote(num_cpus=1)
    def gen_large_objects():
        # 100mb so the object is stored in plasma.
        yield np.zeros(100 * 1024 * 1024, dtype=np.uint8)
        yield np.ones(100 * 1024 * 1024, dtype=np.uint8)

    put_on_worker = {ray._raylet.RAY_NODE_ID_KEY: worker_node.node_id}
    gen1 = gen_large_objects.options(label_selector=put_on_worker).remote()
    gen2 = gen_large_objects.options(label_selector=put_on_worker).remote()

    ready = _wait_generators_bulk(
        [(gen1, [True, False]), (gen2, [False, True])],
        num_return=2,
        timeout=10,
    )
    assert len(ready) == 2
    assert [gen for gen, _ in ready] == [gen1, gen2]
    assert all(len(refs) == 2 for _, refs in ready)

    assert np.all(ray.get(ready[0][1][0], timeout=0) == 0)
    assert np.all(ray.get(ready[1][1][1], timeout=0) == 1)


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_and_fetch_empty(ray_start_regular):
    ready, unready = _wait_and_fetch([])
    assert ready == []
    assert unready == []


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_and_fetch_num_returns_and_partition(ray_start_regular):
    refs = [ray.put(i) for i in range(5)]
    index = {r: i for i, r in enumerate(refs)}
    pairs = [(r, i % 2 == 0) for i, r in enumerate(refs)]

    for num_returns in (1, 3, 5):
        ready, unready = _wait_and_fetch(pairs, num_returns=num_returns)
        assert len(ready) == num_returns
        assert len(unready) == 5 - num_returns
        unready_keys = {k for k, _ in unready}
        assert set(ready) | unready_keys == set(refs)
        assert set(ready) & unready_keys == set()
        assert [index[k] for k in ready] == sorted(index[k] for k in ready)
        for k, fl in unready:
            assert fl is (index[k] % 2 == 0)


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_and_fetch_timeout_returns_partial_ready(ray_start_regular):
    @ray.remote
    def slow():
        time.sleep(5)

    fast_ref = ray.put(0)
    slow_ref = slow.remote()
    # List order: fast is ready immediately; we never get both within the timeout.
    ready, unready = _wait_and_fetch(
        [(fast_ref, True), (slow_ref, True)],
        num_returns=2,
        timeout=0.25,
    )
    assert ready == [fast_ref]
    assert unready == [(slow_ref, True)]


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_and_fetch_validation(ray_start_regular):
    x = ray.put(1)
    with pytest.raises(TypeError):
        _wait_and_fetch({})  # dict is not accepted
    with pytest.raises(TypeError):
        _wait_and_fetch([x])
    with pytest.raises(TypeError):
        _wait_and_fetch([(x, "x")])
    with pytest.raises(ValueError):
        _wait_and_fetch([(x, True)], num_returns=2)


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk(ray_start_regular):
    @ray.remote
    def gen(base, delays):
        for i, delay in enumerate(delays):
            time.sleep(delay)
            yield base + i

    gen1 = gen.remote(10, [0, 0, 0])
    gen2 = gen.remote(20, [0, 5])

    ready = _wait_generators_bulk(
        [(gen1, [True, False]), (gen2, [False, True])],
        num_return=1,
        timeout=2,
    )

    assert len(ready) == 1
    ready_gen, refs = ready[0]
    assert ready_gen is gen1
    assert ray.get(refs) == [10, 11]

    # The returned refs are consumed from the stream.
    assert ray.get(next(gen1)) == 12


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk_timeout(ray_start_regular):
    @ray.remote
    def slow_gen():
        time.sleep(3)
        yield 1

    gen = slow_gen.remote()
    assert _wait_generators_bulk([(gen, [False])], timeout=0.01) == []
    ready = _wait_generators_bulk([(gen, [False])], timeout=10)
    assert len(ready) == 1
    ready_gen, refs = ready[0]
    assert ready_gen is gen
    assert ray.get(refs) == [1]


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk_validation(ray_start_regular):
    @ray.remote
    def slow_gen():
        time.sleep(5)
        yield 1

    gen = slow_gen.remote()

    with pytest.raises(TypeError):
        _wait_generators_bulk({})
    with pytest.raises(TypeError):
        _wait_generators_bulk([(ray.put(1), [False])])
    with pytest.raises(TypeError):
        _wait_generators_bulk([(gen, False)])
    with pytest.raises(ValueError):
        _wait_generators_bulk([(gen, [])])
    with pytest.raises(ValueError):
        _wait_generators_bulk([(gen, [False])], num_return=2)


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk_after_eof(ray_start_regular):
    @ray.remote
    def empty_gen():
        if False:
            yield 1

    gen = empty_gen.remote()
    ray.get(gen._generator_ref)

    ready = _wait_generators_bulk([(gen, [True, True, True])], timeout=1)
    assert len(ready) == 1
    ready_gen, refs = ready[0]
    assert ready_gen is gen
    assert len(refs) == 3
    assert len(set(refs)) == 3
    for ref in refs:
        with pytest.raises(ObjectRefStreamEndOfStreamError):
            ray.get(ref)


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk_after_partial_eof(ray_start_regular):
    @ray.remote
    def one_item_gen():
        yield 1

    gen = one_item_gen.remote()
    ray.get(gen._generator_ref)

    ready = _wait_generators_bulk([(gen, [False, False, False])], timeout=1)
    assert len(ready) == 1
    ready_gen, refs = ready[0]
    assert ready_gen is gen
    assert len(refs) == 3
    assert len(set(refs)) == 3
    assert ray.get(refs[0]) == 1
    for ref in refs[1:]:
        with pytest.raises(ObjectRefStreamEndOfStreamError):
            ray.get(ref)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
