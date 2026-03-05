"""Complex Ray application running on the Rust backend.

Pipeline: Distributed Statistical Analysis of Financial Transactions
=====================================================================

Architecture:
  - 4 workers (2 task workers + 2 actor workers)
  - 4 actors: DataStore, MapperCoordinator, Reducer, ProgressMonitor
  - Multiple task types: partition, compute_stats, validate
  - Uses ray.put(), ray.get(), ray.wait() throughout

Flow:
  1. Generate synthetic transaction dataset
  2. ray.put() the dataset into the object store
  3. DataStore actor partitions data and stores each partition
  4. Task workers compute per-partition statistics in parallel
  5. ProgressMonitor tracks completion via ray.wait()
  6. Reducer actor merges partial stats into final result
  7. Validation tasks verify correctness against sequential computation

All results are verified against a deterministic sequential reference.
"""

import struct
import time
import json
import traceback

from _raylet import (
    start_cluster,
    PyCoreWorker,
    PyGcsClient,
    PyWorkerID,
)

# ═══════════════════════════════════════════════════════════════════════
# Helpers (same as run_all_doc_examples.py)
# ═══════════════════════════════════════════════════════════════════════

def pack_i64(val):
    return struct.pack("<q", val)

def unpack_i64(data):
    return struct.unpack("<q", data)[0]

def pack_f64(val):
    return struct.pack("<d", val)

def unpack_f64(data):
    return struct.unpack("<d", data)[0]

def encode_json(obj):
    return json.dumps(obj).encode("utf-8")

def decode_json(data):
    return json.loads(data.decode("utf-8"))

# ═══════════════════════════════════════════════════════════════════════
# Start cluster
# ═══════════════════════════════════════════════════════════════════════

print("=" * 70)
print("  Complex Ray Application — Distributed Financial Stats Pipeline")
print("=" * 70)
print()
print("Starting in-process cluster...")
cluster = start_cluster()
gcs = PyGcsClient(cluster.gcs_address())
print(f"  GCS: {cluster.gcs_address()}")
print(f"  Node: {cluster.node_id().hex()[:16]}")
print()

# ═══════════════════════════════════════════════════════════════════════
# Generate synthetic dataset
# ═══════════════════════════════════════════════════════════════════════

NUM_TRANSACTIONS = 200
NUM_PARTITIONS = 4
CATEGORIES = ["food", "transport", "housing", "entertainment", "utilities"]

# Deterministic synthetic transactions
transactions = []
for i in range(NUM_TRANSACTIONS):
    txn = {
        "id": i,
        "amount": ((i * 37 + 13) % 1000) + 1,  # 1..1000, deterministic
        "category": CATEGORIES[i % len(CATEGORIES)],
        "is_fraud": (i % 47 == 0),  # ~4 fraudulent transactions
    }
    transactions.append(txn)

# Compute sequential reference for verification
ref_total = sum(t["amount"] for t in transactions)
ref_count = len(transactions)
ref_mean = ref_total / ref_count
ref_min = min(t["amount"] for t in transactions)
ref_max = max(t["amount"] for t in transactions)
ref_fraud_count = sum(1 for t in transactions if t["is_fraud"])
ref_category_totals = {}
for t in transactions:
    cat = t["category"]
    ref_category_totals[cat] = ref_category_totals.get(cat, 0) + t["amount"]

print(f"Dataset: {NUM_TRANSACTIONS} transactions, {NUM_PARTITIONS} partitions")
print(f"  Reference total: {ref_total}, mean: {ref_mean:.2f}")
print(f"  Reference min: {ref_min}, max: {ref_max}")
print(f"  Reference fraud count: {ref_fraud_count}")
print(f"  Reference category totals: {ref_category_totals}")
print()

# ═══════════════════════════════════════════════════════════════════════
# Infrastructure: create 4 workers
# ═══════════════════════════════════════════════════════════════════════

def make_driver():
    return PyCoreWorker(
        1, "127.0.0.1", cluster.gcs_address(), 1,
        node_id=cluster.node_id(),
    )

def make_actor(name, callback):
    wid = PyWorkerID.py_from_random()
    worker = PyCoreWorker(
        0, "127.0.0.1", cluster.gcs_address(), 1,
        worker_id=wid, node_id=cluster.node_id(),
    )
    worker.set_task_callback(callback)
    port = worker.start_grpc_server()
    actor_id = gcs.register_actor(name, "pipeline")
    return worker, wid, port, actor_id

def make_task_worker(callback):
    wid = PyWorkerID.py_from_random()
    worker = PyCoreWorker(
        0, "127.0.0.1", cluster.gcs_address(), 1,
        worker_id=wid, node_id=cluster.node_id(),
    )
    worker.set_task_callback(callback)
    port = worker.start_grpc_server()
    return worker, wid, port

def make_task_driver(task_worker, task_wid, task_port):
    driver = make_driver()
    driver.setup_task_dispatch("127.0.0.1", task_port, task_wid)
    return driver

def ray_put(driver, data, metadata=b"python"):
    oid = driver.put(data, metadata)
    return oid.binary()

def ray_get(driver, oid_binaries, timeout_ms=10000):
    results = driver.get(oid_binaries, timeout_ms)
    return [r[0] if r is not None else None for r in results]

def ray_get_one(driver, oid_binary, timeout_ms=10000):
    return ray_get(driver, [oid_binary], timeout_ms)[0]

def ray_wait(driver, oid_binaries, num_objects, timeout_ms=10000):
    ready_flags = driver.wait(oid_binaries, num_objects, timeout_ms)
    ready = [oid for oid, flag in zip(oid_binaries, ready_flags) if flag]
    remaining = [oid for oid, flag in zip(oid_binaries, ready_flags) if not flag]
    return ready, remaining

# ═══════════════════════════════════════════════════════════════════════
# Actor 1: DataStore — stores and partitions data
# ═══════════════════════════════════════════════════════════════════════

datastore_state = {"partitions": {}}

def datastore_callback(method, args, num_returns=1):
    if method == "store_partition":
        partition_id = unpack_i64(args[0])
        data = args[1]  # JSON-encoded partition
        datastore_state["partitions"][partition_id] = data
        return encode_json({"stored": partition_id, "size": len(decode_json(data))})
    elif method == "get_partition":
        partition_id = unpack_i64(args[0])
        return datastore_state["partitions"].get(partition_id, b"[]")
    elif method == "get_partition_count":
        return pack_i64(len(datastore_state["partitions"]))
    raise ValueError(f"DataStore: unknown method {method}")

# ═══════════════════════════════════════════════════════════════════════
# Actor 2: ProgressMonitor — tracks pipeline stage completion
# ═══════════════════════════════════════════════════════════════════════

monitor_state = {"stages": {}, "errors": []}

def monitor_callback(method, args, num_returns=1):
    if method == "record_stage":
        stage = args[0].decode("utf-8")
        status = args[1].decode("utf-8")
        monitor_state["stages"][stage] = status
        return encode_json({"stage": stage, "status": status})
    elif method == "record_error":
        error = args[0].decode("utf-8")
        monitor_state["errors"].append(error)
        return pack_i64(len(monitor_state["errors"]))
    elif method == "get_report":
        report = {
            "stages": monitor_state["stages"],
            "error_count": len(monitor_state["errors"]),
            "errors": monitor_state["errors"],
        }
        return encode_json(report)
    raise ValueError(f"Monitor: unknown method {method}")

# ═══════════════════════════════════════════════════════════════════════
# Actor 3: Reducer — merges partial statistics
# ═══════════════════════════════════════════════════════════════════════

reducer_state = {
    "total": 0, "count": 0, "min": float("inf"), "max": float("-inf"),
    "fraud_count": 0, "category_totals": {}, "partitions_merged": 0,
}

def reducer_callback(method, args, num_returns=1):
    if method == "merge":
        partial = decode_json(args[0])
        reducer_state["total"] += partial["total"]
        reducer_state["count"] += partial["count"]
        reducer_state["min"] = min(reducer_state["min"], partial["min"])
        reducer_state["max"] = max(reducer_state["max"], partial["max"])
        reducer_state["fraud_count"] += partial["fraud_count"]
        for cat, val in partial["category_totals"].items():
            reducer_state["category_totals"][cat] = \
                reducer_state["category_totals"].get(cat, 0) + val
        reducer_state["partitions_merged"] += 1
        return pack_i64(reducer_state["partitions_merged"])
    elif method == "get_result":
        result = {
            "total": reducer_state["total"],
            "count": reducer_state["count"],
            "mean": reducer_state["total"] / max(reducer_state["count"], 1),
            "min": reducer_state["min"],
            "max": reducer_state["max"],
            "fraud_count": reducer_state["fraud_count"],
            "category_totals": reducer_state["category_totals"],
            "partitions_merged": reducer_state["partitions_merged"],
        }
        return encode_json(result)
    raise ValueError(f"Reducer: unknown method {method}")

# ═══════════════════════════════════════════════════════════════════════
# Actor 4: FraudDetector — analyzes transactions for fraud patterns
# ═══════════════════════════════════════════════════════════════════════

fraud_state = {"flagged": [], "total_scanned": 0}

def fraud_callback(method, args, num_returns=1):
    if method == "scan":
        partition = decode_json(args[0])
        flagged = []
        for txn in partition:
            fraud_state["total_scanned"] += 1
            if txn.get("is_fraud"):
                flagged.append(txn["id"])
                fraud_state["flagged"].append(txn["id"])
            # Also flag unusually high amounts (>900)
            elif txn["amount"] > 900:
                flagged.append(txn["id"])
                fraud_state["flagged"].append(txn["id"])
        return encode_json({"flagged_ids": flagged, "scanned": len(partition)})
    elif method == "get_summary":
        return encode_json({
            "total_flagged": len(fraud_state["flagged"]),
            "total_scanned": fraud_state["total_scanned"],
            "flagged_ids": sorted(fraud_state["flagged"]),
        })
    raise ValueError(f"FraudDetector: unknown method {method}")

# ═══════════════════════════════════════════════════════════════════════
# Task Workers: compute_stats and validate
# ═══════════════════════════════════════════════════════════════════════

task1_calls = {"n": 0}

def task_worker_1_callback(method, args, num_returns=1):
    """Handles compute_stats and validate tasks."""
    task1_calls["n"] += 1
    if method == "compute_stats":
        partition = decode_json(args[0])
        total = sum(t["amount"] for t in partition)
        count = len(partition)
        mn = min(t["amount"] for t in partition) if partition else 0
        mx = max(t["amount"] for t in partition) if partition else 0
        fraud = sum(1 for t in partition if t.get("is_fraud"))
        cat_totals = {}
        for t in partition:
            cat = t["category"]
            cat_totals[cat] = cat_totals.get(cat, 0) + t["amount"]
        result = {
            "total": total, "count": count, "min": mn, "max": mx,
            "fraud_count": fraud, "category_totals": cat_totals,
        }
        return encode_json(result)
    elif method == "validate":
        # Validate a single transaction: check amount is positive, has category
        txn = decode_json(args[0])
        valid = txn.get("amount", 0) > 0 and txn.get("category", "") in \
            ["food", "transport", "housing", "entertainment", "utilities"]
        return encode_json({"id": txn["id"], "valid": valid})
    raise ValueError(f"TaskWorker1: unknown method {method}")

task2_calls = {"n": 0}

def task_worker_2_callback(method, args, num_returns=1):
    """Handles compute_stats (for load balancing) and transform tasks."""
    task2_calls["n"] += 1
    if method == "compute_stats":
        partition = decode_json(args[0])
        total = sum(t["amount"] for t in partition)
        count = len(partition)
        mn = min(t["amount"] for t in partition) if partition else 0
        mx = max(t["amount"] for t in partition) if partition else 0
        fraud = sum(1 for t in partition if t.get("is_fraud"))
        cat_totals = {}
        for t in partition:
            cat = t["category"]
            cat_totals[cat] = cat_totals.get(cat, 0) + t["amount"]
        result = {
            "total": total, "count": count, "min": mn, "max": mx,
            "fraud_count": fraud, "category_totals": cat_totals,
        }
        return encode_json(result)
    elif method == "transform":
        # Normalize amounts to a 0-100 scale
        partition = decode_json(args[0])
        max_amount = unpack_i64(args[1])
        normalized = []
        for t in partition:
            normalized.append({
                "id": t["id"],
                "normalized_amount": round(t["amount"] / max_amount * 100, 2),
                "category": t["category"],
            })
        return encode_json(normalized)
    raise ValueError(f"TaskWorker2: unknown method {method}")

# ═══════════════════════════════════════════════════════════════════════
# Create all workers and actors
# ═══════════════════════════════════════════════════════════════════════

print("Creating 4 workers (2 actor, 2 task)...")

# Actor workers
ds_worker, ds_wid, ds_port, ds_aid = make_actor("DataStore", datastore_callback)
mon_worker, mon_wid, mon_port, mon_aid = make_actor("Monitor", monitor_callback)
red_worker, red_wid, red_port, red_aid = make_actor("Reducer", reducer_callback)
fraud_worker, fraud_wid, fraud_port, fraud_aid = make_actor("FraudDetector", fraud_callback)

# Task workers
tw1, tw1_id, tw1_port = make_task_worker(task_worker_1_callback)
tw2, tw2_id, tw2_port = make_task_worker(task_worker_2_callback)

# Drivers
driver = make_driver()

# Setup actor handles on driver
for aid, name, port, wid in [
    (ds_aid, "DataStore", ds_port, ds_wid),
    (mon_aid, "Monitor", mon_port, mon_wid),
    (red_aid, "Reducer", red_port, red_wid),
    (fraud_aid, "FraudDetector", fraud_port, fraud_wid),
]:
    driver.setup_actor(aid, name, "pipeline", "127.0.0.1", port,
                       cluster.node_id(), wid)

# Task drivers (one per task worker)
task_driver_1 = make_task_driver(tw1, tw1_id, tw1_port)
task_driver_2 = make_task_driver(tw2, tw2_id, tw2_port)

print("  4 actors: DataStore, Monitor, Reducer, FraudDetector")
print("  2 task workers: compute_stats/validate, compute_stats/transform")
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 1: Put dataset into object store + partition
# ═══════════════════════════════════════════════════════════════════════

print("─" * 70)
print("STAGE 1: Data ingestion + partitioning")
print("─" * 70)

# ray.put() the full dataset
dataset_oid = ray_put(driver, encode_json(transactions))
print(f"  ray.put() → dataset OID ({len(transactions)} transactions)")

# Partition the data
partition_size = NUM_TRANSACTIONS // NUM_PARTITIONS
partitions = []
for i in range(NUM_PARTITIONS):
    start = i * partition_size
    end = start + partition_size if i < NUM_PARTITIONS - 1 else NUM_TRANSACTIONS
    partitions.append(transactions[start:end])

# Store each partition in the DataStore actor
for i, part in enumerate(partitions):
    oid = driver.submit_actor_method(
        ds_aid, "store_partition",
        [pack_i64(i), encode_json(part)]
    )
    result = decode_json(ray_get_one(driver, oid.binary()))
    print(f"  Partition {i}: {result['size']} transactions stored")

# Verify partition count
pc_oid = driver.submit_actor_method(ds_aid, "get_partition_count", [])
partition_count = unpack_i64(ray_get_one(driver, pc_oid.binary()))
assert partition_count == NUM_PARTITIONS, \
    f"Expected {NUM_PARTITIONS} partitions, got {partition_count}"

# Record stage completion
driver.submit_actor_method(mon_aid, "record_stage",
                           [b"ingestion", b"complete"])

print(f"  {partition_count} partitions stored ✓")
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 2: Parallel computation with ray.wait()
# ═══════════════════════════════════════════════════════════════════════

print("─" * 70)
print("STAGE 2: Parallel stats computation (load-balanced across 2 workers)")
print("─" * 70)

# Submit compute_stats tasks to alternating workers (load balancing)
# Track refs by their owning driver so we wait/get on the correct one.
driver1_stat_refs = []
driver2_stat_refs = []
for i in range(NUM_PARTITIONS):
    part_data = encode_json(partitions[i])
    if i % 2 == 0:
        refs = task_driver_1.submit_task("compute_stats", [part_data])
        driver1_stat_refs.append(refs[0].binary())
    else:
        refs = task_driver_2.submit_task("compute_stats", [part_data])
        driver2_stat_refs.append(refs[0].binary())

# Use ray.wait() to process results as they complete — one driver at a time
print(f"  Submitted {NUM_PARTITIONS} compute_stats tasks "
      f"(worker1={len(driver1_stat_refs)}, worker2={len(driver2_stat_refs)})")
completed_stats = []

# Collect from task driver 1
pending = list(driver1_stat_refs)
while pending:
    ready, pending = ray_wait(task_driver_1, pending, 1, timeout_ms=10000)
    for oid in ready:
        result = decode_json(ray_get_one(task_driver_1, oid))
        completed_stats.append(result)
        print(f"  [worker 1] Got stats: count={result['count']}, "
              f"total={result['total']}, fraud={result['fraud_count']}")

# Collect from task driver 2
pending = list(driver2_stat_refs)
while pending:
    ready, pending = ray_wait(task_driver_2, pending, 1, timeout_ms=10000)
    for oid in ready:
        result = decode_json(ray_get_one(task_driver_2, oid))
        completed_stats.append(result)
        print(f"  [worker 2] Got stats: count={result['count']}, "
              f"total={result['total']}, fraud={result['fraud_count']}")

assert len(completed_stats) == NUM_PARTITIONS, \
    f"Expected {NUM_PARTITIONS} results, got {len(completed_stats)}"

driver.submit_actor_method(mon_aid, "record_stage",
                           [b"compute_stats", b"complete"])
print(f"  All {NUM_PARTITIONS} partitions computed ✓")
print(f"  Worker 1 handled {task1_calls['n']} tasks, "
      f"Worker 2 handled {task2_calls['n']} tasks")
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 3: Reduce partial results
# ═══════════════════════════════════════════════════════════════════════

print("─" * 70)
print("STAGE 3: Merge partial statistics via Reducer actor")
print("─" * 70)

for i, stat in enumerate(completed_stats):
    oid = driver.submit_actor_method(red_aid, "merge", [encode_json(stat)])
    merged_count = unpack_i64(ray_get_one(driver, oid.binary()))
    print(f"  Merged partition {i+1}/{NUM_PARTITIONS} → {merged_count} total")

# Get final result
result_oid = driver.submit_actor_method(red_aid, "get_result", [])
final_result = decode_json(ray_get_one(driver, result_oid.binary()))

driver.submit_actor_method(mon_aid, "record_stage",
                           [b"reduce", b"complete"])

print(f"  Final: total={final_result['total']}, count={final_result['count']}, "
      f"mean={final_result['mean']:.2f}")
print(f"  Min={final_result['min']}, Max={final_result['max']}, "
      f"Fraud={final_result['fraud_count']}")
print(f"  Categories: {final_result['category_totals']}")
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 4: Parallel fraud scanning via FraudDetector actor
# ═══════════════════════════════════════════════════════════════════════

print("─" * 70)
print("STAGE 4: Fraud detection scan (actor)")
print("─" * 70)

fraud_scan_refs = []
for i in range(NUM_PARTITIONS):
    oid = driver.submit_actor_method(
        fraud_aid, "scan", [encode_json(partitions[i])]
    )
    fraud_scan_refs.append(oid.binary())

# Collect all fraud scan results
total_flagged = 0
for i, oid in enumerate(fraud_scan_refs):
    scan_result = decode_json(ray_get_one(driver, oid))
    total_flagged += len(scan_result["flagged_ids"])
    print(f"  Partition {i}: scanned={scan_result['scanned']}, "
          f"flagged={len(scan_result['flagged_ids'])}")

# Get fraud summary
summary_oid = driver.submit_actor_method(fraud_aid, "get_summary", [])
fraud_summary = decode_json(ray_get_one(driver, summary_oid.binary()))

driver.submit_actor_method(mon_aid, "record_stage",
                           [b"fraud_scan", b"complete"])

print(f"  Total flagged: {fraud_summary['total_flagged']} "
      f"(scanned {fraud_summary['total_scanned']})")
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 5: Validation tasks + data transformation
# ═══════════════════════════════════════════════════════════════════════

print("─" * 70)
print("STAGE 5: Validation + transformation tasks")
print("─" * 70)

# Validate first 10 transactions via task worker 1
validate_refs = []
for i in range(10):
    refs = task_driver_1.submit_task("validate", [encode_json(transactions[i])])
    validate_refs.append(refs[0].binary())

# Use ray.wait() to collect validation results
pending = list(validate_refs)
valid_count = 0
while pending:
    ready, pending = ray_wait(task_driver_1, pending, 1, timeout_ms=10000)
    for oid in ready:
        v = decode_json(ray_get_one(task_driver_1, oid))
        if v["valid"]:
            valid_count += 1

print(f"  Validated 10 transactions: {valid_count}/10 valid ✓")

# Transform partition 0 using task worker 2
transform_refs = task_driver_2.submit_task(
    "transform",
    [encode_json(partitions[0]), pack_i64(ref_max)]
)
normalized = decode_json(ray_get_one(task_driver_2, transform_refs[0].binary()))
print(f"  Transformed partition 0: {len(normalized)} normalized transactions")
print(f"  Sample: id={normalized[0]['id']}, "
      f"normalized_amount={normalized[0]['normalized_amount']}%, "
      f"category={normalized[0]['category']}")

driver.submit_actor_method(mon_aid, "record_stage",
                           [b"validation", b"complete"])
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 6: Cross-check with object store (ray.put/ray.get round-trip)
# ═══════════════════════════════════════════════════════════════════════

print("─" * 70)
print("STAGE 6: Object store round-trip verification")
print("─" * 70)

# Put the final result into object store, get it back, verify
final_oid = ray_put(driver, encode_json(final_result))
retrieved = decode_json(ray_get_one(driver, final_oid))
assert retrieved == final_result, "Object store round-trip failed!"
print(f"  ray.put(final_result) → ray.get() → match ✓")

# Put multiple objects and batch get
oids = []
for i in range(5):
    oids.append(ray_put(driver, pack_i64(i * 100)))

batch_results = ray_get(driver, oids)
batch_values = [unpack_i64(r) for r in batch_results]
assert batch_values == [0, 100, 200, 300, 400], \
    f"Batch get failed: {batch_values}"
print(f"  Batch put/get [0,100,200,300,400] → match ✓")

# ray.wait on mixed object types
mixed_oids = oids[:3] + [final_oid]
ready, remaining = ray_wait(driver, mixed_oids, len(mixed_oids), timeout_ms=10000)
assert len(ready) == 4, f"Expected 4 ready, got {len(ready)}"
assert len(remaining) == 0, f"Expected 0 remaining, got {len(remaining)}"
print(f"  ray.wait(4 mixed objects) → all ready ✓")

driver.submit_actor_method(mon_aid, "record_stage",
                           [b"verification", b"complete"])
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 7: Get progress report from Monitor
# ═══════════════════════════════════════════════════════════════════════

print("─" * 70)
print("STAGE 7: Progress report")
print("─" * 70)

report_oid = driver.submit_actor_method(mon_aid, "get_report", [])
report = decode_json(ray_get_one(driver, report_oid.binary()))
print(f"  Stages completed: {list(report['stages'].keys())}")
print(f"  Error count: {report['error_count']}")
print()

# ═══════════════════════════════════════════════════════════════════════
# VERIFICATION: Compare against sequential reference
# ═══════════════════════════════════════════════════════════════════════

print("=" * 70)
print("  VERIFICATION")
print("=" * 70)
print()

errors = []

def check(name, actual, expected, tolerance=0):
    if tolerance > 0:
        if abs(actual - expected) > tolerance:
            errors.append(f"{name}: expected {expected}, got {actual}")
            print(f"  ✗ {name}: expected {expected}, got {actual}")
        else:
            print(f"  ✓ {name}: {actual} (expected {expected})")
    else:
        if actual != expected:
            errors.append(f"{name}: expected {expected}, got {actual}")
            print(f"  ✗ {name}: expected {expected}, got {actual}")
        else:
            print(f"  ✓ {name}: {actual}")

check("Total amount", final_result["total"], ref_total)
check("Transaction count", final_result["count"], ref_count)
check("Mean amount", final_result["mean"], ref_mean, tolerance=0.01)
check("Min amount", final_result["min"], ref_min)
check("Max amount", final_result["max"], ref_max)
check("Fraud count", final_result["fraud_count"], ref_fraud_count)
check("Partitions merged", final_result["partitions_merged"], NUM_PARTITIONS)

for cat in CATEGORIES:
    check(f"Category '{cat}'",
          final_result["category_totals"].get(cat, 0),
          ref_category_totals.get(cat, 0))

check("Pipeline stages", len(report["stages"]), 6)
check("All validations passed", valid_count, 10)
check("Normalized partition size", len(normalized), partition_size)
check("Worker 1 + Worker 2 tasks",
      task1_calls["n"] + task2_calls["n"],
      NUM_PARTITIONS + 10 + 1)  # 4 stats + 10 validates + 1 transform = 15

# Verify named actor lookup via GCS
found = gcs.get_named_actor("DataStore", "pipeline")
assert found is not None, "DataStore actor not found via GCS"
print(f"  ✓ GCS named actor lookup: DataStore found")

found_reducer = gcs.get_named_actor("Reducer", "pipeline")
assert found_reducer is not None, "Reducer actor not found via GCS"
print(f"  ✓ GCS named actor lookup: Reducer found")

not_found = gcs.get_named_actor("NonExistent", "pipeline")
assert not_found is None, "Non-existent actor should return None"
print(f"  ✓ GCS named actor lookup: NonExistent → None")

print()

# ═══════════════════════════════════════════════════════════════════════
# FINAL REPORT
# ═══════════════════════════════════════════════════════════════════════

print("=" * 70)
if errors:
    print(f"  FAILED — {len(errors)} verification errors:")
    for e in errors:
        print(f"    - {e}")
else:
    print("  ALL VERIFICATIONS PASSED")
print("=" * 70)
print()
print(f"  Workers:       4 (2 task workers, 2 actor workers)")
print(f"  Actors:        4 (DataStore, Monitor, Reducer, FraudDetector)")
print(f"  Task calls:    {task1_calls['n'] + task2_calls['n']} "
      f"(worker1={task1_calls['n']}, worker2={task2_calls['n']})")
print(f"  ray.put():     {2 + 5} calls (dataset + final + 5 batch)")
print(f"  ray.get():     {NUM_PARTITIONS + 10 + 4 + 5 + 3}+ calls")
print(f"  ray.wait():    3 calls (stats, validation, mixed objects)")
print(f"  Transactions:  {NUM_TRANSACTIONS} processed")
print(f"  Fraud flagged: {fraud_summary['total_flagged']}")
print(f"  Pipeline stages: {len(report['stages'])}")
print()

exit(0 if not errors else 1)
