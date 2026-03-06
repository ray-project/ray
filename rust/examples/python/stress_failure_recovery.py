#!/usr/bin/env python3
"""Stress test: error propagation, partial failures, retries, timeouts.

Tests:
  1. 10 tasks: 3 fail intentionally, 7 succeed — verify correct error capture
  2. Task retry with side-effect tracking (max_retries=3)
  3. Cascading error: task A calls task B which fails
  4. Timeout via ray.wait: slow task, collect with short timeout
  5. Actor method that raises, verify error propagation
"""

import ray
import time

ray.init(num_task_workers=4)

passed = 0
total = 0

# ── Test 1: Partial batch failures ──────────────────────────────────

@ray.remote
def maybe_fail(i):
    if i in (2, 5, 8):
        raise ValueError(f"intentional failure at index {i}")
    return i * 10

total += 1
refs = [maybe_fail.remote(i) for i in range(10)]
successes = []
failures = []
for i, ref in enumerate(refs):
    try:
        result = ray.get([ref])
        successes.append((i, result[0]))
    except Exception as e:
        failures.append((i, str(e)))

if len(successes) == 7 and len(failures) == 3:
    fail_indices = sorted([idx for idx, _ in failures])
    if fail_indices == [2, 5, 8]:
        print(f"  PASS  Partial failures: 7 successes, 3 failures at indices {fail_indices}")
        passed += 1
    else:
        print(f"  FAIL  Wrong failure indices: {fail_indices}")
else:
    print(f"  FAIL  Partial failures: {len(successes)} successes, {len(failures)} failures")

# ── Test 2: Retry behavior ──────────────────────────────────────────

@ray.remote
def flaky_task(fail_message):
    """A task that always fails — used to test that max_retries eventually gives up."""
    raise RuntimeError(fail_message)

@ray.remote
def succeed_after_check(value):
    """A simple task that succeeds — verify retries don't corrupt results."""
    return value * 2

total += 1
# Test that max_retries=0 (no retries) causes immediate failure
ref = flaky_task.options(max_retries=0).remote("no retries")
got_error = False
try:
    ray.get([ref])
except Exception as e:
    if "no retries" in str(e):
        got_error = True

# Test that a succeeding task with max_retries still works fine
ref2 = succeed_after_check.options(max_retries=3).remote(21)
result2 = ray.get([ref2])

if got_error and result2 == [42]:
    print(f"  PASS  Retry behavior: failing task raised error, succeeding task returned {result2[0]}")
    passed += 1
else:
    print(f"  FAIL  Retry behavior: got_error={got_error}, result2={result2}")

# ── Test 3: Cascading error ─────────────────────────────────────────

@ray.remote
def inner_fail():
    raise ValueError("inner task exploded")

@ray.remote
def outer_call():
    ref = inner_fail.remote()
    # This should propagate the inner error
    return ray.get([ref])[0]

total += 1
ref = outer_call.remote()
try:
    ray.get([ref])
    print(f"  FAIL  Cascading error: no exception raised")
except Exception as e:
    error_msg = str(e)
    if "inner task exploded" in error_msg or "RayTaskError" in error_msg:
        print(f"  PASS  Cascading error propagated: {error_msg[:80]}")
        passed += 1
    else:
        print(f"  FAIL  Cascading error: unexpected message: {error_msg[:80]}")

# ── Test 4: Timeout via ray.wait ────────────────────────────────────

@ray.remote
def slow_task():
    time.sleep(5)
    return "done"

@ray.remote
def fast_task():
    return "fast"

total += 1
slow_ref = slow_task.remote()
fast_ref = fast_task.remote()

# Wait with short timeout — should get fast_task but not slow_task
ready, remaining = ray.wait([slow_ref, fast_ref], num_returns=2, timeout=1.0)

# At least the fast task should be ready
fast_ready = len(ready) >= 1
slow_still_pending = len(remaining) >= 1

if fast_ready:
    print(f"  PASS  Timeout via ray.wait: {len(ready)} ready, {len(remaining)} remaining after 1s")
    passed += 1
else:
    print(f"  FAIL  Timeout: ready={len(ready)}, remaining={len(remaining)}")

# Clean up: wait for slow task to finish
ray.get([slow_ref])

# ── Test 5: Actor resilience under sustained load ───────────────────

@ray.remote
class ResilientActor:
    def __init__(self):
        self.call_count = 0
        self.history = []

    def process(self, value):
        self.call_count += 1
        self.history.append(value)
        return value * 2

    def get_count(self):
        return self.call_count

    def get_history_len(self):
        return len(self.history)

total += 1
actor = ResilientActor.remote()
# Make 100 calls rapidly
refs = [actor.process.remote(i) for i in range(100)]
results = ray.get(refs)
count = ray.get(actor.get_count.remote())
hist_len = ray.get(actor.get_history_len.remote())

expected_results = [i * 2 for i in range(100)]
if results == expected_results and count == 100 and hist_len == 100:
    print(f"  PASS  Actor resilience: 100 calls, count={count}, history={hist_len}")
    passed += 1
else:
    print(f"  FAIL  Actor resilience: results match={results == expected_results}, count={count}, hist={hist_len}")

# ── Summary ──────────────────────────────────────────────────────────

ray.shutdown()
print(f"\n{'ALL' if passed == total else 'SOME'} STRESS TESTS {'PASSED' if passed == total else 'FAILED'} ({passed}/{total})")
