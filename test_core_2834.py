"""
Regression test for CHECK failure in HandleTaskExecutionResult.

Bug: A double ray.cancel() could crash the worker with:
    Check failed: objects_valid 1 return objects expected, 1 returned.
    Object at idx 0 was not stored.

The trigger sequence was:
  1. Task is running
  2. First ray.cancel() --> KeyboardInterrupt interrupts the task
  3. execute_task re-raises KeyboardInterrupt
  4. execute_task_with_cancellation_handler catches it, calls store_task_errors
  5. Second ray.cancel() --> KeyboardInterrupt during store_task_errors
     (current_task_id was still set, so kill_main_task sent the interrupt)
  6. KeyboardInterrupt escaped all handlers in task_execution_handler
  7. Cython silently returned default CRayStatus (OK)
  8. HandleTaskExecutionResult found status=OK but return_objects[0]=nullptr
  9. CHECK failure

Fix (in _raylet.pyx):
  - Layer 1: Clear current_task_id at the start of the except KeyboardInterrupt
    handler in execute_task_with_cancellation_handler, preventing kill_main_task
    from sending a second _thread.interrupt_main() during error storage.
  - Layer 2: Added except BaseException handler in task_execution_handler as a
    safety net, returning CRayStatus.UnexpectedSystemExit instead of letting
    Cython fall through to CRayStatus.OK().

USAGE:
  Run this script with the fix applied:
      python test_core_2834.py

  Expected: [driver] Got TaskCancelledError (expected if fix is in place)

REPRODUCING THE ORIGINAL BUG (requires reverting the fix):
  1. Revert the two changes in _raylet.pyx (remove the early
     current_task_id = None and the except BaseException handler).
  2. Add a sleep to widen the error-storage window. In _raylet.pyx,
     inside store_task_errors, add "import time; time.sleep(3)"
     right before the store_task_outputs call (around line 1021).
  3. Rebuild Ray: cd python && python setup.py build_ext --inplace
  4. Run this script. The worker should crash with the CHECK failure.
"""

import time

import ray


@ray.remote(num_returns=1)
def long_running_task():
    """A task that uses a busy-wait loop so KeyboardInterrupt is delivered
    promptly."""
    import time as _time

    end = _time.monotonic() + 120
    while _time.monotonic() < end:
        _time.sleep(0.05)
    return 42


def main():
    ray.init(num_cpus=1)
    try:
        ref = long_running_task.remote()

        # Wait for task to start executing on the worker
        time.sleep(2)

        print("[driver] Sending first cancel (interrupts the task)...")
        ray.cancel(ref)

        # The first cancel delivers KeyboardInterrupt, which propagates to
        # execute_task_with_cancellation_handler's except handler, which calls
        # store_task_errors. Wait briefly then send a second cancel. With the
        # fix, the second cancel is harmless (current_task_id is already cleared).
        # Without the fix, this second interrupt would escape and crash the worker.
        time.sleep(1)

        print("[driver] Sending second cancel (interrupts error storage)...")
        ray.cancel(ref)

        # Wait for the worker to process
        time.sleep(5)

        try:
            result = ray.get(ref, timeout=30)
            print(f"[driver] Unexpected success: {result}")
        except ray.exceptions.TaskCancelledError:
            print("[driver] Got TaskCancelledError (expected if fix is in place)")
        except ray.exceptions.WorkerCrashedError as e:
            print(f"[driver] Worker crashed (CHECK failure triggered): {e}")
        except Exception as e:
            print(f"[driver] Got exception: {type(e).__name__}: {e}")

    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
