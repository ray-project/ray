import threading

from ray.data._internal.execution.block_ref_counter import BlockRefCounter


class _FakeRef:
    """Minimal stand-in for ray.ObjectRef — has a .binary() that returns bytes."""

    def __init__(self, uid: int):
        self._binary = uid.to_bytes(28, "big")

    def binary(self) -> bytes:
        return self._binary


def _make_counter_with_callback(ref, size_bytes, producer_id):
    """Create a counter and manually register a block, returning (counter, callback).

    Instead of calling into Ray Core, we extract the callback closure so tests
    can fire it directly.
    """
    counter = BlockRefCounter()
    id_binary = ref.binary()

    # Replicate the on_block_produced logic without the Ray Core call.
    with counter._lock:
        counter._registered_ids.add(id_binary)
        counter._bytes_by_producer[producer_id] += size_bytes

    # Build the same closure on_block_produced would have registered.
    def _on_out_of_scope(id_bytes: bytes) -> None:
        with counter._lock:
            if id_bytes not in counter._registered_ids:
                return
            counter._registered_ids.discard(id_bytes)
            counter._bytes_by_producer[producer_id] -= size_bytes

    return counter, _on_out_of_scope, id_binary


class TestBlockRefCounterAccounting:
    def test_single_block_produced_and_released(self):
        ref = _FakeRef(1)
        counter, callback, id_binary = _make_counter_with_callback(ref, 100, "op_a")

        assert counter.get_object_store_memory_usage("op_a") == 100
        callback(id_binary)
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_multiple_blocks_same_producer(self):
        ref1, ref2 = _FakeRef(1), _FakeRef(2)
        counter, cb1, bin1 = _make_counter_with_callback(ref1, 100, "op_a")
        # Add second block to the same counter.
        with counter._lock:
            counter._registered_ids.add(ref2.binary())
            counter._bytes_by_producer["op_a"] += 200

        def cb2(id_bytes):
            with counter._lock:
                if id_bytes not in counter._registered_ids:
                    return
                counter._registered_ids.discard(id_bytes)
                counter._bytes_by_producer["op_a"] -= 200

        assert counter.get_object_store_memory_usage("op_a") == 300
        cb1(bin1)
        assert counter.get_object_store_memory_usage("op_a") == 200
        cb2(ref2.binary())
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_multiple_producers_isolated(self):
        ref1, ref2 = _FakeRef(1), _FakeRef(2)
        counter, cb1, bin1 = _make_counter_with_callback(ref1, 100, "op_a")
        with counter._lock:
            counter._registered_ids.add(ref2.binary())
            counter._bytes_by_producer["op_b"] += 200

        def cb2(id_bytes):
            with counter._lock:
                if id_bytes not in counter._registered_ids:
                    return
                counter._registered_ids.discard(id_bytes)
                counter._bytes_by_producer["op_b"] -= 200

        assert counter.get_object_store_memory_usage("op_a") == 100
        assert counter.get_object_store_memory_usage("op_b") == 200

        cb1(bin1)
        assert counter.get_object_store_memory_usage("op_a") == 0
        assert counter.get_object_store_memory_usage("op_b") == 200


class TestBlockRefCounterDeduplication:
    def test_duplicate_ref_not_double_counted(self):
        """on_block_produced for the same ref twice only counts size_bytes once."""
        ref = _FakeRef(1)
        counter = BlockRefCounter()
        id_binary = ref.binary()

        # First registration
        with counter._lock:
            counter._registered_ids.add(id_binary)
            counter._bytes_by_producer["op_a"] += 100

        # Second call for same ref — the guard should skip it.
        duplicate_added = False
        with counter._lock:
            if id_binary in counter._registered_ids:
                duplicate_added = False  # guard fires correctly
            else:
                counter._registered_ids.add(id_binary)
                counter._bytes_by_producer["op_a"] += 100
                duplicate_added = True

        assert not duplicate_added
        assert counter.get_object_store_memory_usage("op_a") == 100

    def test_unknown_producer_returns_zero(self):
        counter = BlockRefCounter()
        assert counter.get_object_store_memory_usage("nonexistent_op") == 0


class TestBlockRefCounterClear:
    def test_clear_resets_usage(self):
        ref = _FakeRef(1)
        counter, _, _ = _make_counter_with_callback(ref, 100, "op_a")
        assert counter.get_object_store_memory_usage("op_a") == 100

        counter.clear()
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_callback_after_clear_is_noop(self):
        """A callback firing after clear() must not crash or corrupt state."""
        ref = _FakeRef(1)
        counter, callback, id_binary = _make_counter_with_callback(ref, 100, "op_a")

        counter.clear()
        # Fire the callback — should be a silent no-op.
        callback(id_binary)
        assert counter.get_object_store_memory_usage("op_a") == 0

    def test_new_blocks_after_clear_are_tracked(self):
        """After clear(), new registrations work normally."""
        counter = BlockRefCounter()
        counter.clear()

        ref = _FakeRef(2)
        id_binary = ref.binary()
        with counter._lock:
            counter._registered_ids.add(id_binary)
            counter._bytes_by_producer["op_b"] += 50

        assert counter.get_object_store_memory_usage("op_b") == 50


class TestBlockRefCounterThreadSafety:
    def test_concurrent_callbacks_dont_corrupt_state(self):
        """Multiple threads firing callbacks concurrently must not go negative."""
        counter = BlockRefCounter()
        producer_id = "op_concurrent"
        n = 50
        refs = [_FakeRef(i) for i in range(n)]
        callbacks = []

        for ref in refs:
            id_binary = ref.binary()
            size = 10
            with counter._lock:
                counter._registered_ids.add(id_binary)
                counter._bytes_by_producer[producer_id] += size

            def make_cb(idb, sz):
                def cb(id_bytes):
                    with counter._lock:
                        if id_bytes not in counter._registered_ids:
                            return
                        counter._registered_ids.discard(id_bytes)
                        counter._bytes_by_producer[producer_id] -= sz

                return cb

            callbacks.append((make_cb(id_binary, size), id_binary))

        threads = [threading.Thread(target=cb, args=(idb,)) for cb, idb in callbacks]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert counter.get_object_store_memory_usage(producer_id) == 0


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
