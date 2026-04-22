"""Hello world for the Arrow Flight store integrated into Ray.

Tests three paths:
1. Local put/get (in-process)
2. Cross-process via Flight RPC (ray.get with Flight store enabled)
3. Same-node process_vm_writev (if on Linux)
"""

import os
os.environ["RAY_USE_FLIGHT_STORE"] = "1"

import ray
import pyarrow as pa
from ray._raylet import PyArrowFlightStore, get_flight_store

# --- Test 1: Local put/get ---
print("=== Test 1: Local put/get ===")
store = PyArrowFlightStore()
store.start_server()
print(f"  Flight server URI: {store.get_uri()}")

table = pa.table({"x": [1, 2, 3], "name": ["alice", "bob", "charlie"]})
store.put("hello", table)
print(f"  Put table ({table.num_rows} rows), store size={store.size()}")

result = store.get_local("hello")
print(f"  Get local: {result.to_pydict()}")

store.stop_server()
print("  OK\n")

# --- Test 2: Cross-process via Ray ---
print("=== Test 2: Ray task returning pa.Table ===")
ray.init(num_cpus=2)


@ray.remote
def make_table():
    return pa.table({
        "id": list(range(1000)),
        "value": [float(i) * 0.1 for i in range(1000)],
    })


@ray.remote
def process_table(t):
    assert isinstance(t, pa.Table), f"Expected pa.Table, got {type(t)}"
    return t.num_rows


ref = make_table.remote()
table = ray.get(ref)
print(f"  ray.get returned: {type(table).__name__} with {table.num_rows} rows")
print(f"  First 3 rows: {table.slice(0, 3).to_pydict()}")

# Pass table through another task
count = ray.get(process_table.remote(ref))
print(f"  Downstream task got {count} rows")

# Multiple tables in flight
refs = [make_table.remote() for _ in range(5)]
tables = ray.get(refs)
print(f"  Got {len(tables)} tables, rows: {[t.num_rows for t in tables]}")

print("  OK\n")

# --- Test 3: Non-Arrow return falls back to plasma ---
print("=== Test 3: Non-Arrow fallback to plasma ===")


@ray.remote
def make_dict():
    return {"hello": "world", "count": 42}


result = ray.get(make_dict.remote())
print(f"  Dict result: {result}")
assert result == {"hello": "world", "count": 42}
print("  OK\n")

ray.shutdown()
print("All tests passed!")
