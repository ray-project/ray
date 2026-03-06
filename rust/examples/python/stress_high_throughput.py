#!/usr/bin/env python3
"""Stress test: high-throughput task submission and actor method calls.

Tests:
  1. Submit 5,000 tasks to 4-worker pool, verify all results
  2. Batch ray.wait collection in waves of 100
  3. 500 rapid actor method calls, verify sequential consistency
"""

import time
import ray

ray.init(num_task_workers=4)

passed = 0
total = 0


# ── Helper ────────────────────────────────────────────────────────────

def fib(n):
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


EXPECTED_FIBS = {i: fib(i % 20) for i in range(5000)}

# ── Test 1: 5,000 task submissions ────────────────────────────────────

@ray.remote
def compute_fib(i):
    a, b = 0, 1
    for _ in range(i % 20):
        a, b = b, a + b
    return (i, a)

total += 1
t0 = time.time()
refs = [compute_fib.remote(i) for i in range(5000)]
results = ray.get(refs)
elapsed = time.time() - t0

errors = []
for idx, val in results:
    if val != EXPECTED_FIBS[idx]:
        errors.append((idx, val, EXPECTED_FIBS[idx]))

if len(results) == 5000 and len(errors) == 0:
    throughput = 5000 / elapsed
    print(f"  PASS  5,000 tasks: all correct, {throughput:.0f} tasks/sec ({elapsed:.2f}s)")
    passed += 1
else:
    print(f"  FAIL  5,000 tasks: {len(results)} results, {len(errors)} errors")

# ── Test 2: ray.wait batch collection ─────────────────────────────────

@ray.remote
def square(x):
    return x * x

total += 1
refs = [square.remote(i) for i in range(1000)]
collected = []
remaining = list(refs)
waves = 0
while remaining:
    batch_size = min(100, len(remaining))
    ready, remaining = ray.wait(remaining, num_returns=batch_size, timeout=10.0)
    collected.extend(ray.get(ready))
    waves += 1

expected = sorted([i * i for i in range(1000)])
actual = sorted(collected)
if actual == expected:
    print(f"  PASS  ray.wait batch collection: 1,000 results in {waves} waves")
    passed += 1
else:
    print(f"  FAIL  ray.wait batch: expected {len(expected)} results, got {len(actual)}")

# ── Test 3: 500 rapid actor method calls ──────────────────────────────

@ray.remote
class Counter:
    def __init__(self):
        self.n = 0
    def incr(self):
        self.n += 1
        return self.n

total += 1
c = Counter.remote()
refs = [c.incr.remote() for _ in range(500)]
results = ray.get(refs)
# Since actor methods are sequential, results should be 1..500 in order.
expected_seq = list(range(1, 501))
if results == expected_seq:
    print(f"  PASS  500 actor method calls: sequential consistency verified (final={results[-1]})")
    passed += 1
else:
    # Check at least the final value
    final = ray.get(refs[-1])
    print(f"  FAIL  500 actor calls: expected [1..500], got final={final}, len={len(results)}")

# ── Test 4: Multiple actors under load ────────────────────────────────

@ray.remote
class Accumulator:
    def __init__(self):
        self.total = 0
    def add(self, v):
        self.total += v
        return self.total

total += 1
actors = [Accumulator.remote() for _ in range(4)]
all_refs = []
for i in range(200):
    actor = actors[i % 4]
    all_refs.append(actor.add.remote(i))

results = ray.get(all_refs)
# Each actor gets every 4th number: actor0 gets 0,4,8,...; actor1 gets 1,5,9,...
actor_expected = [0] * 4
for i in range(200):
    actor_expected[i % 4] += i
# The last result for each actor should be the cumulative sum
# Get final values
finals = [ray.get(actors[j].add.remote(0)) for j in range(4)]
if finals == actor_expected:
    print(f"  PASS  4 actors x 50 calls each: totals={finals}")
    passed += 1
else:
    print(f"  FAIL  4 actors: expected {actor_expected}, got {finals}")

# ── Summary ───────────────────────────────────────────────────────────

ray.shutdown()
print(f"\n{'ALL' if passed == total else 'SOME'} STRESS TESTS {'PASSED' if passed == total else 'FAILED'} ({passed}/{total})")
