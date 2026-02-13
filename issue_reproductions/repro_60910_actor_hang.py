"""
Reproduction for GitHub Issue #60910:
[Core] Actor tasks hang when head-of-line task's dependency resolution fails

When an actor task at the head of the submit queue has its dependency
resolution fail (e.g., the object it depends on becomes unreachable),
subsequent tasks that have already resolved their dependencies get stuck
and never execute.

This affects sequential (ordered) actor task submission.
"""
import ray
import time
import sys
import os

ray.init()

@ray.remote
class Counter:
    def __init__(self):
        self.count = 0

    def increment(self, _dep=None):
        self.count += 1
        return self.count

@ray.remote
class Holder:
    """Actor that creates an object ref, then dies so the ref becomes unresolvable."""
    def create_object(self):
        return "data"

    def exit(self):
        os._exit(0)  # Force exit so the object becomes unreachable

# Create a counter actor (sequential by default)
counter = Counter.remote()

# Create a holder actor that will die
holder = Holder.remote()

# Get an object ref from the holder
obj_ref = holder.create_object.remote()

# Kill the holder - this makes obj_ref potentially unresolvable
# if the object hasn't been fetched yet
ray.kill(holder)
time.sleep(1)

# Now submit tasks to the counter:
# Task 1: depends on obj_ref (which may fail to resolve since holder is dead)
# Task 2: no dependencies (should complete quickly)
# Task 3: no dependencies (should complete quickly)

print("Submitting task 1 (depends on dead actor's object)...")
ref1 = counter.increment.remote(_dep=obj_ref)

print("Submitting task 2 (no dependencies)...")
ref2 = counter.increment.remote()

print("Submitting task 3 (no dependencies)...")
ref3 = counter.increment.remote()

# Try to get results with timeout
# If bug is present, ref2 and ref3 will hang even though they have no dependencies
print("Waiting for task 2 (should complete quickly if no head-of-line blocking)...")
try:
    result2 = ray.get(ref2, timeout=10)
    print(f"Task 2 completed with result: {result2}")
except ray.exceptions.GetTimeoutError:
    print("BUG REPRODUCED: Task 2 timed out - head-of-line blocking!")
    print("Task 2 has no dependencies but is stuck behind task 1")
except Exception as e:
    print(f"Task 2 error: {type(e).__name__}: {e}")

print("Waiting for task 3...")
try:
    result3 = ray.get(ref3, timeout=10)
    print(f"Task 3 completed with result: {result3}")
except ray.exceptions.GetTimeoutError:
    print("BUG REPRODUCED: Task 3 timed out - head-of-line blocking!")
except Exception as e:
    print(f"Task 3 error: {type(e).__name__}: {e}")

# Check task 1
print("Checking task 1 (expected to fail or timeout)...")
try:
    result1 = ray.get(ref1, timeout=10)
    print(f"Task 1 completed with result: {result1}")
except Exception as e:
    print(f"Task 1 error (expected): {type(e).__name__}: {e}")

ray.shutdown()
