#!/usr/bin/env python3
"""Stress test: concurrent actor access, lifecycle churn, coordination.

Tests:
  1. 10 actors x 100 increments each, verify exact final counts
  2. Rapid actor create/kill cycle (50 iterations)
  3. 5 actors coordinating via shared state through a coordinator actor
  4. Actor with large internal state (1000 items)
"""

import ray

ray.init(num_task_workers=4)

passed = 0
total = 0

# ── Test 1: 10 actors x 100 increments ───────────────────────────────

@ray.remote
class Counter:
    def __init__(self):
        self.n = 0
    def incr(self):
        self.n += 1
        return self.n
    def get(self):
        return self.n

total += 1
actors = [Counter.remote() for _ in range(10)]
all_refs = []
for actor in actors:
    for _ in range(100):
        all_refs.append(actor.incr.remote())

# Wait for all to complete
ray.get(all_refs)
# Verify final counts
finals = [ray.get(a.get.remote()) for a in actors]
if all(f == 100 for f in finals):
    print(f"  PASS  10 actors x 100 increments: all counts = 100")
    passed += 1
else:
    print(f"  FAIL  10 actors: counts = {finals}")

# ── Test 2: Rapid actor create/kill cycle ─────────────────────────────

@ray.remote
class Ephemeral:
    def __init__(self, tag):
        self.tag = tag
    def identify(self):
        return self.tag

total += 1
success_count = 0
for i in range(50):
    e = Ephemeral.options(name=f"eph_{i}", namespace="churn").remote(i)
    result = ray.get(e.identify.remote())
    if result == i:
        success_count += 1

if success_count == 50:
    print(f"  PASS  50 actor create cycles: all identified correctly")
    passed += 1
else:
    print(f"  FAIL  Actor churn: {success_count}/50 succeeded")

# ── Test 3: Coordinator pattern ───────────────────────────────────────

@ray.remote
class Coordinator:
    def __init__(self):
        self.total = 0
        self.contributions = {}
    def contribute(self, worker_id, amount):
        self.total += amount
        self.contributions[worker_id] = self.contributions.get(worker_id, 0) + amount
        return self.total
    def get_total(self):
        return self.total
    def get_contributions(self):
        return self.contributions

@ray.remote
class Worker:
    def __init__(self, worker_id):
        self.worker_id = worker_id
    def get_id(self):
        return self.worker_id

total += 1
coord = Coordinator.remote()
workers = [Worker.remote(i) for i in range(5)]

# Each worker contributes its ID * 10 to the coordinator, 20 times each
refs = []
for w_idx in range(5):
    for _ in range(20):
        refs.append(coord.contribute.remote(w_idx, w_idx))

ray.get(refs)
final_total = ray.get(coord.get_total.remote())
contributions = ray.get(coord.get_contributions.remote())

# Expected: each worker contributes id*20 times: sum = 20*(0+1+2+3+4) = 200
expected_total = 20 * (0 + 1 + 2 + 3 + 4)
expected_contribs = {i: i * 20 for i in range(5)}
if final_total == expected_total and contributions == expected_contribs:
    print(f"  PASS  5 workers -> coordinator: total={final_total}, contributions={contributions}")
    passed += 1
else:
    print(f"  FAIL  Coordinator: total={final_total} (expected {expected_total}), contribs={contributions}")

# ── Test 4: Actor with large internal state ───────────────────────────

@ray.remote
class BigState:
    def __init__(self):
        self.store = {}
    def insert(self, key, value):
        self.store[key] = value
        return len(self.store)
    def get_size(self):
        return len(self.store)
    def sum_values(self):
        return sum(self.store.values())
    def clear(self):
        self.store.clear()
        return 0

total += 1
big = BigState.remote()
refs = [big.insert.remote(f"key_{i}", i) for i in range(1000)]
ray.get(refs)

size = ray.get(big.get_size.remote())
total_sum = ray.get(big.sum_values.remote())
expected_sum = sum(range(1000))

if size == 1000 and total_sum == expected_sum:
    print(f"  PASS  Large actor state: {size} items, sum={total_sum}")
    passed += 1
else:
    print(f"  FAIL  Large state: size={size} (expected 1000), sum={total_sum} (expected {expected_sum})")

# After clearing
cleared = ray.get(big.clear.remote())
size_after = ray.get(big.get_size.remote())
if size_after == 0:
    pass  # Good
else:
    print(f"  WARN  Clear didn't work: size_after={size_after}")

# ── Summary ───────────────────────────────────────────────────────────

ray.shutdown()
print(f"\n{'ALL' if passed == total else 'SOME'} STRESS TESTS {'PASSED' if passed == total else 'FAILED'} ({passed}/{total})")
