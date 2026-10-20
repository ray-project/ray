import threading

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
        """Callback fires with the correct object ID bytes once the last reference is
        dropped."""
        received_id = []
        done = threading.Event()

        ref = ray.put(42)
        expected_binary = ref.binary()
        registered = _core_worker().add_object_out_of_scope_callback(
            ref, lambda id_bytes: (received_id.append(id_bytes), done.set())
        )
        assert registered, "Expected registration to succeed"

        del ref  # drops the last Python reference — callback must now fire
        assert done.wait(timeout=5), "Callback did not fire within 5 s"
        assert received_id[0] == expected_binary

    def test_many_callbacks_all_fire(self, ray_instance):
        """At scale, every registered callback fires exactly once after its ref is
        dropped. Guards the dedicated callback thread against dropped or
        duplicated notifications under load."""
        n = 2000
        remaining = [n]
        lock = threading.Lock()
        all_fired = threading.Event()
        fire_counts = {}

        def on_freed(id_bytes):
            with lock:
                fire_counts[id_bytes] = fire_counts.get(id_bytes, 0) + 1
                remaining[0] -= 1
                if remaining[0] == 0:
                    all_fired.set()

        refs = [ray.put(i) for i in range(n)]
        expected = {r.binary() for r in refs}
        assert all(
            _core_worker().add_object_out_of_scope_callback(ref, on_freed)
            for ref in refs
        )
        del refs  # drop every last reference at once
        assert all_fired.wait(
            timeout=30
        ), f"Only {n - remaining[0]}/{n} callbacks fired within 30s"
        with lock:
            assert set(fire_counts) == expected, "Unexpected or missing object IDs"
            assert all(c == 1 for c in fire_counts.values()), "A callback fired twice"

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
