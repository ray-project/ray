"""
Reproduction for GitHub Issue #60910:
[Core] Actor tasks hang when head-of-line task's dependency resolution fails

The bug: In the dependency resolution callback in actor_task_submitter.cc,
when a dependency fails, MarkDependencyFailed is called but SendPendingTasks
is NOT called. For sequential actors, this means if the head-of-line task's
dependency fails, all subsequent tasks are stuck.

Key timing: Task B's dependency must resolve BEFORE Task A's dependency fails.
Then when Task A's dep fails, SendPendingTasks is not called, leaving Task B stuck.

To reproduce: We need an object ref whose owner will die AFTER we submit the
task but BEFORE dependency resolution completes, and ensure task B resolves first.
"""
import ray
import time
import os
import signal

ray.init()

@ray.remote(max_concurrency=1)  # Sequential actor (default)
class SequentialWorker:
    def __init__(self):
        self.count = 0

    def work(self, _dep=None):
        self.count += 1
        return self.count

    def get_count(self):
        return self.count

@ray.remote
class ObjectCreator:
    """Creates an object ref but delays the actual computation."""
    def slow_create(self, delay_s=5):
        time.sleep(delay_s)
        return "data"

    def get_pid(self):
        return os.getpid()


worker = SequentialWorker.remote()

# Create an actor whose object will become unresolvable
creator = ObjectCreator.options(max_restarts=0).remote()
creator_pid = ray.get(creator.get_pid.remote())

# Start a slow computation - the ref won't be ready for 5 seconds
slow_ref = creator.slow_create.remote(5)

# Submit task A to worker: depends on slow_ref (not yet resolved)
print("Submitting Task A (depends on slow unresolved object)...")
ref_a = worker.work.remote(_dep=slow_ref)

# Brief sleep to ensure Task A is queued first
time.sleep(0.1)

# Submit task B: no dependencies (will resolve instantly)
print("Submitting Task B (no dependencies)...")
ref_b = worker.work.remote()

# Brief sleep to let Task B's dependency resolution complete
# (it has no deps, so resolves immediately)
time.sleep(0.5)

# Now kill the creator actor - this makes slow_ref unresolvable
# The dependency resolution for Task A will fail
print(f"Killing creator (pid={creator_pid}) to make Task A's dependency fail...")
os.kill(creator_pid, signal.SIGKILL)

# Wait for the failure to propagate
time.sleep(2)

# Now try to get Task B's result
# If the bug is present, Task B is stuck behind the failed Task A
print("\nWaiting for Task B (timeout=15s)...")
print("If bug is present, Task B hangs despite having no dependencies.")
try:
    result_b = ray.get(ref_b, timeout=15)
    print(f"Task B completed with result: {result_b}")
    print("Bug NOT reproduced (Task B was not blocked)")
except ray.exceptions.GetTimeoutError:
    print("BUG REPRODUCED: Task B timed out!")
    print("Task B has no dependencies but is stuck because Task A's failed")
    print("dependency didn't trigger SendPendingTasks in the sequential queue.")
except Exception as e:
    print(f"Task B error: {type(e).__name__}: {e}")

# Check Task A
print("\nChecking Task A (expected to fail)...")
try:
    result_a = ray.get(ref_a, timeout=5)
    print(f"Task A completed with result: {result_a}")
except Exception as e:
    print(f"Task A error (expected): {type(e).__name__}")

ray.shutdown()
