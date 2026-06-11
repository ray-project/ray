# These tests require a live Ray cluster because the callback path goes through
# the C++ ReferenceCounter and the dedicated object_freed_callback_service_ thread.

import threading
import time

import pytest

import ray
import ray._private.worker as worker_module


@pytest.fixture(scope="module")
def ray_instance():
    ray.init(num_cpus=1)
    yield
    ray.shutdown()


def _core_worker():
    return worker_module.global_worker.core_worker


class TestAddObjectOutOfScopeCallback:
    """Integration tests against a live Ray cluster."""

    def test_callback_fires_when_ref_dropped(self, ray_instance):
        """Callback fires with the correct ObjectRef once the last reference is dropped."""
        received_id = []
        done = threading.Event()

        ref = ray.put(42)
        expected_id = ref
        registered = _core_worker().add_object_out_of_scope_callback(
            ref, lambda r: (received_id.append(r), done.set())
        )
        assert registered, "Expected registration to succeed"

        del ref
        assert done.wait(timeout=5), "Callback did not fire within 5 s"
        assert received_id[0] == expected_id

    def test_returns_false_for_already_out_of_scope(self, ray_instance):
        """Returns False when the object is already out of scope; callback never fires."""
        ref = ray.put(99)
        # Force the RC entry gone while keeping the Python ObjectRef alive.
        ray.internal.free([ref])

        # Poll with a no-op until the RC entry is gone (ray.internal.free is async).
        deadline = time.monotonic() + 5
        registered = True
        while time.monotonic() < deadline:
            registered = _core_worker().add_object_out_of_scope_callback(
                ref, lambda _: None
            )
            if not registered:
                break
            time.sleep(0.01)
        assert not registered, "Object never became out of scope within 5 s"

        # Now that we know the object is OOS, attempt registration with a real
        # callback — must return False — then use a sentinel to drain the callback
        # thread before asserting fired is still unset.
        fired = threading.Event()
        registered2 = _core_worker().add_object_out_of_scope_callback(
            ref, lambda _: fired.set()
        )
        assert not registered2, "Second registration on OOS object must return False"

        sentinel_done = threading.Event()
        sentinel = ray.put("sentinel")
        _core_worker().add_object_out_of_scope_callback(
            sentinel, lambda _: sentinel_done.set()
        )
        del sentinel
        assert sentinel_done.wait(timeout=5)
        assert not fired.is_set(), "Callback must not fire for an already-freed object"

    def test_callback_runs_on_non_driver_thread(self, ray_instance):
        """Callback must run on the dedicated callback thread, not the driver thread."""
        driver_thread_id = threading.get_ident()
        callback_thread_ids = []
        done = threading.Event()

        ref = ray.put("thread_check")

        def cb(_):
            callback_thread_ids.append(threading.get_ident())
            done.set()

        _core_worker().add_object_out_of_scope_callback(ref, cb)
        del ref
        assert done.wait(timeout=5)
        assert (
            callback_thread_ids[0] != driver_thread_id
        ), "Callback must not run on the driver thread"

    def test_multiple_callbacks_all_fire(self, ray_instance):
        """Multiple independent callbacks on the same object all fire."""
        n = 3
        counter = [0]
        lock = threading.Lock()
        done = threading.Event()

        ref = ray.put("multi")

        def make_cb():
            def cb(_):
                with lock:
                    counter[0] += 1
                    if counter[0] == n:
                        done.set()

            return cb

        for _ in range(n):
            _core_worker().add_object_out_of_scope_callback(ref, make_cb())

        del ref
        assert done.wait(timeout=5), f"Only {counter[0]}/{n} callbacks fired"

    def test_ray_internal_free_triggers_callback(self, ray_instance):
        """`ray.internal.free` should trigger the callback."""
        fired = threading.Event()

        ref = ray.put("free_me")
        registered = _core_worker().add_object_out_of_scope_callback(
            ref, lambda _: fired.set()
        )
        assert registered

        ray.internal.free([ref])
        assert fired.wait(timeout=5), "Callback did not fire after ray.internal.free"

    def test_callback_exception_does_not_crash(self, ray_instance):
        """A Python exception inside the callback must not propagate to C++."""
        second_fired = threading.Event()

        ref = ray.put("exc_test")

        def bad_cb(_):
            raise RuntimeError("intentional error in callback")

        _core_worker().add_object_out_of_scope_callback(ref, bad_cb)
        _core_worker().add_object_out_of_scope_callback(
            ref, lambda _: second_fired.set()
        )

        del ref
        assert second_fired.wait(
            timeout=5
        ), "Exception in one callback must not prevent subsequent callbacks"

    def test_callback_fires_exactly_once(self, ray_instance):
        """The callback fires exactly once."""
        count = [0]
        lock = threading.Lock()
        done = threading.Event()

        ref = ray.put("once")

        def cb(_):
            with lock:
                count[0] += 1
            done.set()

        _core_worker().add_object_out_of_scope_callback(ref, cb)
        del ref
        assert done.wait(timeout=5)

        # Drain the callback thread via a sentinel: any spurious second fire would
        # arrive before the sentinel, so if count is still 1 after this, we're clean.
        sentinel_done = threading.Event()
        sentinel = ray.put("sentinel")
        _core_worker().add_object_out_of_scope_callback(
            sentinel, lambda _: sentinel_done.set()
        )
        del sentinel
        assert sentinel_done.wait(timeout=5)
        assert count[0] == 1, f"Expected exactly 1 callback, got {count[0]}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
