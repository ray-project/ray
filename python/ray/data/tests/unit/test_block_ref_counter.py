import threading
from typing import Callable, Dict

import pytest

import ray
from ray.data._internal.execution.block_ref_counter import BlockRefCounter


def _ref(uid: int) -> "ray.ObjectRef":
    """Real ObjectRef with a deterministic distinct 28-byte ID, no Ray cluster needed."""
    return ray.ObjectRef(uid.to_bytes(28, "big"))


class FakeAddObjectOutOfScopeCallback:
    """Test double for CoreWorker.add_object_out_of_scope_callback.

    Records each registered callback keyed by the block's object-ID bytes so a
    test can fire it explicitly. Set registered=False to simulate an object
    that is already out of scope at registration time.
    """

    def __init__(self, registered: bool = True):
        self._registered = registered
        self._callbacks: Dict[bytes, Callable[[bytes], None]] = {}

    def __call__(
        self, object_ref: "ray.ObjectRef", callback: Callable[[bytes], None]
    ) -> bool:
        if self._registered:
            self._callbacks[object_ref.binary()] = callback
        return self._registered

    def fire(self, object_ref: "ray.ObjectRef") -> None:
        id_binary = object_ref.binary()
        self._callbacks[id_binary](id_binary)


class TestBlockRefCounterAccounting:
    def test_single_block_produced_and_released(self):
        add_cb = FakeAddObjectOutOfScopeCallback()
        counter = BlockRefCounter(add_object_out_of_scope_callback=add_cb)
        ref = _ref(1)

        counter.on_block_produced(ref, 1, "op_a")
        assert counter.get_object_store_memory_usage("op_a") == 1

        add_cb.fire(ref)
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_multiple_blocks_same_producer(self):
        add_cb = FakeAddObjectOutOfScopeCallback()
        counter = BlockRefCounter(add_object_out_of_scope_callback=add_cb)
        ref1, ref2 = _ref(1), _ref(2)

        counter.on_block_produced(ref1, 1, "op_a")
        counter.on_block_produced(ref2, 1, "op_a")
        assert counter.get_object_store_memory_usage("op_a") == 2

        add_cb.fire(ref1)
        assert counter.get_object_store_memory_usage("op_a") == 1
        add_cb.fire(ref2)
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_multiple_producers_isolated(self):
        add_cb = FakeAddObjectOutOfScopeCallback()
        counter = BlockRefCounter(add_object_out_of_scope_callback=add_cb)
        ref1, ref2 = _ref(1), _ref(2)

        counter.on_block_produced(ref1, 1, "op_a")
        counter.on_block_produced(ref2, 1, "op_b")
        assert counter.get_object_store_memory_usage("op_a") == 1
        assert counter.get_object_store_memory_usage("op_b") == 1

        add_cb.fire(ref1)
        assert counter.get_object_store_memory_usage("op_a") == 0
        assert counter.get_object_store_memory_usage("op_b") == 1

    def test_duplicate_registration_is_noop(self):
        """on_block_produced is idempotent: a duplicate ref is silently ignored.

        This matters when an AllToAllOperator forwards an input ref unchanged;
        the ref was already registered by the upstream producer."""
        add_cb = FakeAddObjectOutOfScopeCallback()
        counter = BlockRefCounter(add_object_out_of_scope_callback=add_cb)
        ref = _ref(1)

        counter.on_block_produced(ref, 1, "op_a")
        counter.on_block_produced(ref, 1, "op_a")
        assert counter.get_object_store_memory_usage("op_a") == 1

        add_cb.fire(ref)
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_register_when_object_already_out_of_scope(self):
        """If registration reports the object is already gone, the increment is
        undone immediately so the producer nets to zero."""
        add_cb = FakeAddObjectOutOfScopeCallback(registered=False)
        counter = BlockRefCounter(add_object_out_of_scope_callback=add_cb)

        counter.on_block_produced(_ref(1), 1, "op_a")
        assert counter.get_object_store_memory_usage("op_a") == 0


class TestBlockRefCounterClear:
    def test_clear_resets_usage(self):
        add_cb = FakeAddObjectOutOfScopeCallback()
        counter = BlockRefCounter(add_object_out_of_scope_callback=add_cb)
        counter.on_block_produced(_ref(1), 1, "op_a")
        assert counter.get_object_store_memory_usage("op_a") == 1

        counter.clear()
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_stale_callback_after_clear_is_noop(self):
        """A stale callback firing after clear() must not touch accounting
        recorded after the reset."""
        add_cb = FakeAddObjectOutOfScopeCallback()
        counter = BlockRefCounter(add_object_out_of_scope_callback=add_cb)
        stale_ref = _ref(1)
        counter.on_block_produced(stale_ref, 1, "op_a")

        counter.clear()

        counter.on_block_produced(_ref(2), 1, "op_a")
        assert counter.get_object_store_memory_usage("op_a") == 1

        add_cb.fire(stale_ref)
        assert counter.get_object_store_memory_usage("op_a") == 1


class TestBlockRefCounterThreadSafety:
    def test_concurrent_callbacks_dont_corrupt_state(self):
        """Many threads firing callbacks at once must not corrupt the count."""
        add_cb = FakeAddObjectOutOfScopeCallback()
        counter = BlockRefCounter(add_object_out_of_scope_callback=add_cb)
        producer_id = "op_concurrent"
        n = 50
        refs = [_ref(i) for i in range(n)]
        for ref in refs:
            counter.on_block_produced(ref, 1, producer_id)
        assert counter.get_object_store_memory_usage(producer_id) == n

        threads = [threading.Thread(target=add_cb.fire, args=(ref,)) for ref in refs]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert counter.get_object_store_memory_usage(producer_id) == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
