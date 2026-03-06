#!/usr/bin/env python3
"""Stress test: object store lifecycle, put/get cycles, reference management.

Tests:
  1. Put 500 objects, verify all retrievable
  2. Put/get cycle 1,000 times (same pattern) — no corruption
  3. Overlapping object references — no interference
  4. Actor state accumulation and clearing
  5. Large batch put then sequential get
"""

import ray
import hashlib

ray.init(num_task_workers=4)

passed = 0
total = 0

# ── Test 1: Put 500 objects, verify all ─────────────────────────────

total += 1
refs = []
expected = []
for i in range(500):
    data = f"object_{i}_data"
    refs.append(ray.put(data))
    expected.append(data)

results = ray.get(refs)
if results == expected:
    print(f"  PASS  500 objects put/get: all verified")
    passed += 1
else:
    mismatches = sum(1 for r, e in zip(results, expected) if r != e)
    print(f"  FAIL  500 objects: {mismatches} mismatches")

# ── Test 2: 1,000 put/get cycles ───────────────────────────────────

total += 1
all_ok = True
for i in range(1000):
    value = (i, f"cycle_{i}", [i * 2, i * 3])
    ref = ray.put(value)
    result = ray.get(ref)
    if result != value:
        all_ok = False
        print(f"  FAIL  Cycle {i}: got {result}, expected {value}")
        break

if all_ok:
    print(f"  PASS  1,000 put/get cycles: all correct")
    passed += 1

# ── Test 3: Overlapping references ──────────────────────────────────

total += 1
# Put multiple objects, hold all refs, then get in different order
ref_a = ray.put("alpha")
ref_b = ray.put("beta")
ref_c = ray.put("gamma")
ref_d = ray.put("delta")

# Get in reverse order
d = ray.get(ref_d)
c = ray.get(ref_c)
b = ray.get(ref_b)
a = ray.get(ref_a)

# Get again (refs should still be valid)
a2 = ray.get(ref_a)
d2 = ray.get(ref_d)

if (a, b, c, d) == ("alpha", "beta", "gamma", "delta") and a2 == "alpha" and d2 == "delta":
    print(f"  PASS  Overlapping references: reverse order + re-get all correct")
    passed += 1
else:
    print(f"  FAIL  Overlapping refs: a={a}, b={b}, c={c}, d={d}, a2={a2}, d2={d2}")

# ── Test 4: Actor state accumulation and clearing ───────────────────

@ray.remote
class StateAccumulator:
    def __init__(self):
        self.items = {}
    def add(self, key, value):
        self.items[key] = value
        return len(self.items)
    def get_size(self):
        return len(self.items)
    def get_item(self, key):
        return self.items.get(key)
    def clear(self):
        self.items.clear()
        return 0
    def bulk_add(self, pairs):
        for k, v in pairs:
            self.items[k] = v
        return len(self.items)

total += 1
acc = StateAccumulator.remote()

# Add 500 items
refs = [acc.add.remote(f"key_{i}", i * 100) for i in range(500)]
ray.get(refs)

size = ray.get(acc.get_size.remote())
# Spot check some values
v0 = ray.get(acc.get_item.remote("key_0"))
v99 = ray.get(acc.get_item.remote("key_99"))
v499 = ray.get(acc.get_item.remote("key_499"))

if size == 500 and v0 == 0 and v99 == 9900 and v499 == 49900:
    # Now clear and verify
    ray.get(acc.clear.remote())
    size_after = ray.get(acc.get_size.remote())
    if size_after == 0:
        # Add more after clearing
        ray.get(acc.add.remote("new_key", 42))
        final_size = ray.get(acc.get_size.remote())
        if final_size == 1:
            print(f"  PASS  Actor state: 500 items, cleared, re-added, size={final_size}")
            passed += 1
        else:
            print(f"  FAIL  Actor state after re-add: size={final_size}")
    else:
        print(f"  FAIL  Actor clear: size_after={size_after}")
else:
    print(f"  FAIL  Actor state: size={size}, v0={v0}, v99={v99}, v499={v499}")

# ── Test 5: Large batch put then sequential get ─────────────────────

total += 1
# Put 200 byte arrays of varying sizes
batch_refs = []
batch_expected = []
for i in range(200):
    size = (i + 1) * 100  # 100 to 20,000 bytes
    data = bytes([i % 256]) * size
    batch_refs.append(ray.put(data))
    batch_expected.append(data)

# Get one at a time in random-ish order (backwards)
all_match = True
for i in range(199, -1, -1):
    result = ray.get(batch_refs[i])
    if result != batch_expected[i]:
        all_match = False
        print(f"  FAIL  Batch item {i}: size mismatch {len(result)} vs {len(batch_expected[i])}")
        break

if all_match:
    # Also verify batch get
    all_results = ray.get(batch_refs)
    if all_results == batch_expected:
        print(f"  PASS  200 varying-size objects: sequential and batch get verified")
        passed += 1
    else:
        print(f"  FAIL  Batch get mismatch")

# ── Test 6: Object store with complex Python types ──────────────────

total += 1
complex_objects = [
    {"nested": {"key": [1, 2, 3]}, "flag": True},
    (1, "two", 3.0, None),
    [{"a": 1}, {"b": 2}, {"c": 3}],
    {"set_as_list": sorted([1, 2, 3, 4, 5])},
    "simple string",
    42,
    3.14159,
    True,
    None,
    b"raw bytes",
]

refs = [ray.put(obj) for obj in complex_objects]
results = ray.get(refs)

if results == complex_objects:
    print(f"  PASS  Complex types: {len(complex_objects)} different types round-tripped")
    passed += 1
else:
    for i, (r, e) in enumerate(zip(results, complex_objects)):
        if r != e:
            print(f"  FAIL  Complex type {i}: {type(e).__name__} mismatch: {r} vs {e}")

# ── Summary ──────────────────────────────────────────────────────────

ray.shutdown()
print(f"\n{'ALL' if passed == total else 'SOME'} STRESS TESTS {'PASSED' if passed == total else 'FAILED'} ({passed}/{total})")
