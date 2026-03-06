#!/usr/bin/env python3
"""Stress test: deep task chains, fan-out/fan-in, actor-task interaction, map-reduce.

Tests:
  1. Chain of 100 tasks, each passing accumulated value to the next
  2. Fan-out 50 child tasks from one parent, fan-in aggregate
  3. Actor method that spawns 10 remote tasks and aggregates results
  4. Map-reduce: 20 map tasks -> 4 reduce tasks -> 1 final aggregation
"""

import ray

ray.init(num_task_workers=4)

passed = 0
total = 0

# ── Test 1: Chain of 100 tasks ──────────────────────────────────────

@ray.remote
def chain_step(value, step):
    return value + step

total += 1
# Build a chain: each task depends on the previous one's result
ref = chain_step.remote(0, 0)
for i in range(1, 100):
    val = ray.get(ref)
    ref = chain_step.remote(val, i)

final = ray.get(ref)
expected = sum(range(100))  # 0 + 1 + 2 + ... + 99 = 4950
if final == expected:
    print(f"  PASS  Chain of 100 tasks: final={final}")
    passed += 1
else:
    print(f"  FAIL  Chain of 100: got {final}, expected {expected}")

# ── Test 2: Fan-out / fan-in ────────────────────────────────────────

@ray.remote
def compute_square(x):
    return x * x

@ray.remote
def aggregate(values):
    return sum(values)

total += 1
# Fan-out: spawn 50 tasks
child_refs = [compute_square.remote(i) for i in range(50)]
child_results = ray.get(child_refs)
# Fan-in: aggregate all results in one task
agg_ref = aggregate.remote(child_results)
agg_result = ray.get(agg_ref)

expected_agg = sum(i * i for i in range(50))
if agg_result == expected_agg:
    print(f"  PASS  Fan-out/fan-in 50 tasks: sum of squares={agg_result}")
    passed += 1
else:
    print(f"  FAIL  Fan-out/fan-in: got {agg_result}, expected {expected_agg}")

# ── Test 3: Actor spawning remote tasks ─────────────────────────────

@ray.remote
def double(x):
    return x * 2

@ray.remote
class TaskSpawner:
    def __init__(self):
        self.results = []

    def spawn_and_collect(self, n):
        """Spawn n remote tasks, collect results, return aggregate."""
        refs = [double.remote(i) for i in range(n)]
        results = ray.get(refs)
        self.results = results
        return sum(results)

    def get_results(self):
        return self.results

total += 1
spawner = TaskSpawner.remote()
total_sum = ray.get(spawner.spawn_and_collect.remote(10))
stored_results = ray.get(spawner.get_results.remote())

expected_sum = sum(i * 2 for i in range(10))
expected_results = [i * 2 for i in range(10)]
if total_sum == expected_sum and stored_results == expected_results:
    print(f"  PASS  Actor spawning 10 tasks: sum={total_sum}, results={stored_results}")
    passed += 1
else:
    print(f"  FAIL  Actor tasks: sum={total_sum} (expected {expected_sum}), results={stored_results}")

# ── Test 4: Map-reduce ──────────────────────────────────────────────

@ray.remote
def map_task(partition_id, data):
    """Map phase: compute word counts for a partition."""
    counts = {}
    for word in data:
        counts[word] = counts.get(word, 0) + 1
    return (partition_id, counts)

@ray.remote
def reduce_task(reducer_id, map_results):
    """Reduce phase: merge word counts from multiple mappers."""
    merged = {}
    for _, counts in map_results:
        for word, count in counts.items():
            merged[word] = merged.get(word, 0) + count
    return (reducer_id, merged)

total += 1
# Generate 20 partitions of data
words = ["alpha", "beta", "gamma", "delta"]
partitions = []
for i in range(20):
    # Each partition has some words based on its index
    partition = [words[j % len(words)] for j in range(i, i + 10)]
    partitions.append(partition)

# Map phase: 20 map tasks
map_refs = [map_task.remote(i, partitions[i]) for i in range(20)]
map_results = ray.get(map_refs)

# Reduce phase: 4 reducers, each gets 5 map results
reduce_refs = []
for r in range(4):
    chunk = map_results[r * 5:(r + 1) * 5]
    reduce_refs.append(reduce_task.remote(r, chunk))
reduce_results = ray.get(reduce_refs)

# Final aggregation
final_counts = {}
for _, counts in reduce_results:
    for word, count in counts.items():
        final_counts[word] = final_counts.get(word, 0) + count

# Compute expected counts directly
expected_counts = {}
for partition in partitions:
    for word in partition:
        expected_counts[word] = expected_counts.get(word, 0) + 1

if final_counts == expected_counts:
    print(f"  PASS  Map-reduce 20->4->1: counts={final_counts}")
    passed += 1
else:
    print(f"  FAIL  Map-reduce: got {final_counts}, expected {expected_counts}")

# ── Test 5: Diamond dependency ──────────────────────────────────────

@ray.remote
def add(a, b):
    return a + b

@ray.remote
def multiply(a, b):
    return a * b

total += 1
# Diamond: base -> (left, right) -> join
base_val = 10
left = ray.get(add.remote(base_val, 5))       # 15
right = ray.get(multiply.remote(base_val, 3))  # 30
join = ray.get(add.remote(left, right))         # 45

if join == 45:
    print(f"  PASS  Diamond dependency: {base_val} -> ({left}, {right}) -> {join}")
    passed += 1
else:
    print(f"  FAIL  Diamond: expected 45, got {join}")

# ── Summary ──────────────────────────────────────────────────────────

ray.shutdown()
print(f"\n{'ALL' if passed == total else 'SOME'} STRESS TESTS {'PASSED' if passed == total else 'FAILED'} ({passed}/{total})")
