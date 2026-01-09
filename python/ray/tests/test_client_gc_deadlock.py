"""
Test for Ray Client GC-related deadlock issue.

This test reproduces the deadlock that occurs when ClientObjectRef.__del__
is called during GC while DataClient.lock is held.
"""

import gc
import logging
import sys

import pytest

from ray.util.client.common import INT32_MAX
from ray.util.client.dataclient import DataClient
from ray.util.client.ray_client_helpers import ray_start_client_server

logger = logging.getLogger(__name__)


def _next_id_with_gc(self) -> int:
    """
    Modified _next_id that triggers GC while holding the lock.

    This reproduces the deadlock scenario where GC triggers
    ClientObjectRef.__del__, which tries to acquire the same lock.
    """
    assert self.lock.locked()
    self._req_id += 1

    # Force GC while holding the lock - this can trigger deadlock
    gc.collect()

    if self._req_id > INT32_MAX:
        self._req_id = 1
    assert self._req_id != 0

    logger.info(f"req_id={self._req_id}")
    return self._req_id


@pytest.mark.timeout(30)  # 30 second timeout - test should fail if it times out
def test_client_gc_deadlock(ray_start_regular):
    """
    Test that GC during DataClient lock acquisition doesn't cause deadlock.

    This test:
    1. Sets aggressive GC thresholds
    2. Creates objects with circular references (require GC to collect)
    3. Forces GC while DataClient.lock is held
    4. Verifies the test completes without hanging (within 30 seconds)

    If this test times out (30s), it means a deadlock occurred and the test FAILS.

    Note: This test is expected to fail (timeout) when run against the unfixed
    Ray Client code. After the fix (RLock + gc.disable or deferred release),
    it should pass quickly (< 5 seconds).

    Related: https://github.com/ray-project/ray/issues/59643
    """
    import threading
    import time

    start_time = time.time()
    timeout_occurred = threading.Event()

    def timeout_handler():
        timeout_occurred.set()
        pytest.fail("Test timed out after 30 seconds - deadlock detected!")

    # Set up our own timeout to ensure test fails
    timer = threading.Timer(25.0, timeout_handler)
    timer.daemon = True
    timer.start()

    with ray_start_client_server() as ray_client:
        # Set aggressive GC thresholds
        old_thresholds = gc.get_threshold()
        gc.set_threshold(1, 1, 1)

        try:

            @ray_client.remote
            def foo(x):
                return x

            # Monkey-patch _next_id to trigger GC
            original_next_id = DataClient._next_id
            DataClient._next_id = _next_id_with_gc

            try:
                # Create objects with circular references
                class SelfRef:
                    def __init__(self, future):
                        self.future = future
                        self.self_ref = self  # Circular reference

                results = []
                loop_time = 2

                for i in range(loop_time):
                    future = foo.remote(i)
                    self_ref = SelfRef(future)

                    # Get result
                    results.append(ray_client.get(future))

                    # Delete references to trigger GC
                    del self_ref
                    del future

                # Verify results
                assert results == list(range(loop_time))

                elapsed = time.time() - start_time
                print(f"Test completed successfully in {elapsed:.2f} seconds")
                print(f"Results: {results}")

            finally:
                # Restore original _next_id
                DataClient._next_id = original_next_id

        finally:
            # Restore GC thresholds
            gc.set_threshold(*old_thresholds)

    # Cancel the timeout timer
    timer.cancel()

    # If we reach here, test passed (no deadlock)
    elapsed = time.time() - start_time
    assert not timeout_occurred.is_set(), "Test timed out - deadlock detected"
    print(f"✓ Test passed in {elapsed:.2f}s - no deadlock detected")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "-s"]))
