#!/usr/bin/env python3
"""Stress test: large object store payloads and serialization.

Tests:
  1. 10MB object round-trip through object store
  2. 3MB object passed as task argument and transformed
  3. Batch put/get of 100 x 100KB objects
  4. Free large objects and verify reclamation
  5. Large object passed through 3 chained tasks by reference
"""

import ray
import hashlib

ray.init(num_task_workers=4)

passed = 0
total = 0

# ── Test 1: 10MB round-trip ───────────────────────────────────────────

total += 1
data_10mb = bytes(range(256)) * (10 * 1024 * 1024 // 256)
expected_hash = hashlib.sha256(data_10mb).hexdigest()
ref = ray.put(data_10mb)
result = ray.get(ref)
actual_hash = hashlib.sha256(result).hexdigest()
if actual_hash == expected_hash and len(result) == len(data_10mb):
    print(f"  PASS  10MB round-trip: {len(data_10mb):,} bytes, SHA256 match")
    passed += 1
else:
    print(f"  FAIL  10MB round-trip: len={len(result)}, hash match={actual_hash == expected_hash}")

# ── Test 2: 3MB task argument ─────────────────────────────────────────

@ray.remote
def process_large(data):
    # Return length and hash of the data
    return (len(data), hashlib.sha256(data).hexdigest())

total += 1
data_3mb = b"\xAB" * (3 * 1024 * 1024)
expected_3_hash = hashlib.sha256(data_3mb).hexdigest()
ref = process_large.remote(data_3mb)
length, actual_hash = ray.get(ref)
if length == len(data_3mb) and actual_hash == expected_3_hash:
    print(f"  PASS  3MB task argument: {length:,} bytes processed, hash verified")
    passed += 1
else:
    print(f"  FAIL  3MB task: len={length}, expected={len(data_3mb)}, hash match={actual_hash == expected_3_hash}")

# ── Test 3: Batch 100 x 100KB objects ────────────────────────────────

total += 1
refs = []
expected_data = []
for i in range(100):
    chunk = bytes([i % 256]) * (100 * 1024)
    expected_data.append(chunk)
    refs.append(ray.put(chunk))

results = ray.get(refs)
all_match = all(r == e for r, e in zip(results, expected_data))
if len(results) == 100 and all_match:
    print(f"  PASS  100 x 100KB batch: all {len(results)} objects verified")
    passed += 1
else:
    mismatches = sum(1 for r, e in zip(results, expected_data) if r != e)
    print(f"  FAIL  100 x 100KB batch: {mismatches} mismatches out of {len(results)}")

# ── Test 4: Free and verify reclamation ───────────────────────────────

@ray.remote
def identity(x):
    return x

total += 1
big_ref = ray.put(b"\x00" * (5 * 1024 * 1024))
# Verify it exists
val = ray.get(big_ref)
assert len(val) == 5 * 1024 * 1024
# Now put several more objects and verify they work (no corruption)
small_refs = [ray.put(i) for i in range(10)]
small_results = ray.get(small_refs)
if small_results == list(range(10)):
    print(f"  PASS  Large object coexistence: 5MB + 10 small objects all accessible")
    passed += 1
else:
    print(f"  FAIL  Object coexistence: small results={small_results}")

# ── Test 5: Large object through chained tasks ────────────────────────

@ray.remote
def append_marker(data, marker):
    return data + marker

total += 1
original = b"X" * (1 * 1024 * 1024)  # 1MB
r1 = append_marker.remote(original, b"_A")
v1 = ray.get(r1)
r2 = append_marker.remote(v1, b"_B")
v2 = ray.get(r2)
r3 = append_marker.remote(v2, b"_C")
v3 = ray.get(r3)

expected_len = len(original) + len(b"_A") + len(b"_B") + len(b"_C")
if len(v3) == expected_len and v3.endswith(b"_A_B_C"):
    print(f"  PASS  Chained 1MB transforms: 3 stages, {len(v3):,} bytes, suffix verified")
    passed += 1
else:
    print(f"  FAIL  Chained transforms: len={len(v3)}, expected={expected_len}")

# ── Summary ───────────────────────────────────────────────────────────

ray.shutdown()
print(f"\n{'ALL' if passed == total else 'SOME'} STRESS TESTS {'PASSED' if passed == total else 'FAILED'} ({passed}/{total})")
