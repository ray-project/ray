"""
Reproduction for GitHub Issue #60910:
[Core] Actor tasks hang when head-of-line task's dependency resolution fails

More careful timing approach:
1. Submit Task A that depends on an object from a slow remote task
2. Wait long enough for Task B's dependency resolution to complete
3. Then kill the worker running the slow task to make Task A's dep fail
4. Task B should be stuck since its SendPendingTasks already returned nothing
   and MarkDependencyFailed doesn't call SendPendingTasks

The key insight: after Task B's dep resolution succeeds and calls
SendPendingTasks (which returns nothing because Task A at seq=0 is unresolved),
no further SendPendingTasks will be called for Task B.
"""
import ray
import time
import os
import signal

ray.init()

@ray.remote(max_concurrency=1)  # Sequential execution (default)
class SeqActor:
    def __init__(self):
        self.count = 0

    def do_work(self, dep=None):
        self.count += 1
        return self.count

@ray.remote
def never_finish():
    """Task that blocks forever - its return ref will never resolve."""
    import time
    time.sleep(3600)
    return "never"

# Create the sequential actor
actor = SeqActor.remote()

# Start a task that will never complete - gives us an unresolvable ref
print("Starting a blocking remote task...")
blocking_ref = never_finish.remote()

# Small sleep to ensure the task is actually running
time.sleep(1)

# Get the PID of the worker running the blocking task
# so we can kill it to trigger dependency resolution failure

# Submit Task A: depends on blocking_ref (unresolved)
print("Submitting Task A (depends on unresolved blocking_ref)...")
ref_a = actor.do_work.remote(dep=blocking_ref)

# Wait for Task B's dependency resolution to complete BEFORE Task A's dep fails
time.sleep(1)

# Submit Task B: no dependencies
print("Submitting Task B (no dependencies)...")
ref_b = actor.do_work.remote()

# Wait for Task B's dep resolution callback to fire (should be instant)
# and then try SendPendingTasks (which will fail because Task A at head is unresolved)
time.sleep(2)

# Now cancel the blocking task to trigger Task A's dependency failure
print("Cancelling the blocking task to trigger dependency failure...")
ray.cancel(blocking_ref, force=True)

# Wait for the cancellation to propagate
time.sleep(3)

# Now try to get Task B - if bug is present, it's stuck
print("\nWaiting for Task B (timeout=15s)...")
print("If bug present: Task B hangs (head-of-line blocking)")
try:
    result_b = ray.get(ref_b, timeout=15)
    print(f"Task B completed: {result_b}")
    print("Bug NOT reproduced with this approach")
except ray.exceptions.GetTimeoutError:
    print("BUG REPRODUCED: Task B timed out due to head-of-line blocking!")
    print("After Task A's dependency was cancelled, MarkDependencyFailed was called")
    print("but SendPendingTasks was NOT called, leaving Task B permanently stuck.")
except Exception as e:
    print(f"Task B error: {type(e).__name__}: {e}")

# Check Task A
print("\nChecking Task A...")
try:
    result_a = ray.get(ref_a, timeout=5)
    print(f"Task A completed: {result_a}")
except Exception as e:
    print(f"Task A error (expected): {type(e).__name__}")

ray.shutdown()
