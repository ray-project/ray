"""Complex Ray application running on the Rust backend.

Pipeline: Distributed Statistical Analysis of Financial Transactions
=====================================================================

Uses ``import ray`` and ``@ray.remote`` decorators — the standard Ray
Python API backed by the Rust ``_raylet`` extension module.

Architecture:
  - 4 task workers (shared pool for all @ray.remote functions)
  - 4 actors: DataStore, ProgressMonitor, Reducer, FraudDetector
  - 3 remote functions: compute_stats, validate, transform
  - 7 pipeline stages with ray.put(), ray.get(), ray.wait()
  - All results verified against a deterministic sequential reference
"""

import ray

# ═══════════════════════════════════════════════════════════════════════
# Initialise cluster
# ═══════════════════════════════════════════════════════════════════════

print("=" * 70)
print("  Complex Ray Application — Distributed Financial Stats Pipeline")
print("  Using import ray + @ray.remote on the Rust backend")
print("=" * 70)
print()

ray.init(num_task_workers=4)
print(f"  ray.is_initialized() = {ray.is_initialized()}")
print(f"  Task workers: {len(ray._runtime._task_pool)}")
print()

# ═══════════════════════════════════════════════════════════════════════
# Remote functions
# ═══════════════════════════════════════════════════════════════════════


@ray.remote
def compute_stats(partition):
    """Compute per-partition statistics."""
    total = sum(t["amount"] for t in partition)
    count = len(partition)
    mn = min(t["amount"] for t in partition) if partition else 0
    mx = max(t["amount"] for t in partition) if partition else 0
    fraud = sum(1 for t in partition if t.get("is_fraud"))
    cat_totals = {}
    for t in partition:
        cat_totals[t["category"]] = cat_totals.get(t["category"], 0) + t["amount"]
    return {
        "total": total, "count": count, "min": mn, "max": mx,
        "fraud_count": fraud, "category_totals": cat_totals,
    }


@ray.remote
def validate(txn):
    """Validate a single transaction."""
    categories = ["food", "transport", "housing", "entertainment", "utilities"]
    valid = txn.get("amount", 0) > 0 and txn.get("category", "") in categories
    return {"id": txn["id"], "valid": valid}


@ray.remote
def transform(partition, max_amount):
    """Normalize amounts to a 0-100 scale."""
    return [
        {
            "id": t["id"],
            "normalized_amount": round(t["amount"] / max_amount * 100, 2),
            "category": t["category"],
        }
        for t in partition
    ]


# ═══════════════════════════════════════════════════════════════════════
# Actor classes
# ═══════════════════════════════════════════════════════════════════════


@ray.remote
class DataStore:
    def __init__(self):
        self.partitions = {}

    def store_partition(self, partition_id, data):
        self.partitions[partition_id] = data
        return {"stored": partition_id, "size": len(data)}

    def get_partition(self, partition_id):
        return self.partitions.get(partition_id, [])

    def get_partition_count(self):
        return len(self.partitions)


@ray.remote
class ProgressMonitor:
    def __init__(self):
        self.stages = {}
        self.errors = []

    def record_stage(self, stage, status):
        self.stages[stage] = status
        return {"stage": stage, "status": status}

    def record_error(self, error):
        self.errors.append(error)
        return len(self.errors)

    def get_report(self):
        return {
            "stages": self.stages,
            "error_count": len(self.errors),
            "errors": self.errors,
        }


@ray.remote
class Reducer:
    def __init__(self):
        self.total = 0
        self.count = 0
        self.min_val = float("inf")
        self.max_val = float("-inf")
        self.fraud_count = 0
        self.category_totals = {}
        self.partitions_merged = 0

    def merge(self, partial):
        self.total += partial["total"]
        self.count += partial["count"]
        self.min_val = min(self.min_val, partial["min"])
        self.max_val = max(self.max_val, partial["max"])
        self.fraud_count += partial["fraud_count"]
        for cat, val in partial["category_totals"].items():
            self.category_totals[cat] = self.category_totals.get(cat, 0) + val
        self.partitions_merged += 1
        return self.partitions_merged

    def get_result(self):
        return {
            "total": self.total,
            "count": self.count,
            "mean": self.total / max(self.count, 1),
            "min": self.min_val,
            "max": self.max_val,
            "fraud_count": self.fraud_count,
            "category_totals": self.category_totals,
            "partitions_merged": self.partitions_merged,
        }


@ray.remote
class FraudDetector:
    def __init__(self):
        self.flagged = []
        self.total_scanned = 0

    def scan(self, partition):
        flagged = []
        for txn in partition:
            self.total_scanned += 1
            if txn.get("is_fraud") or txn["amount"] > 900:
                flagged.append(txn["id"])
                self.flagged.append(txn["id"])
        return {"flagged_ids": flagged, "scanned": len(partition)}

    def get_summary(self):
        return {
            "total_flagged": len(self.flagged),
            "total_scanned": self.total_scanned,
            "flagged_ids": sorted(self.flagged),
        }


# ═══════════════════════════════════════════════════════════════════════
# Dataset
# ═══════════════════════════════════════════════════════════════════════

NUM_TRANSACTIONS = 200
NUM_PARTITIONS = 4
CATEGORIES = ["food", "transport", "housing", "entertainment", "utilities"]

transactions = []
for i in range(NUM_TRANSACTIONS):
    transactions.append({
        "id": i,
        "amount": ((i * 37 + 13) % 1000) + 1,
        "category": CATEGORIES[i % len(CATEGORIES)],
        "is_fraud": (i % 47 == 0),
    })

# Sequential reference
ref_total = sum(t["amount"] for t in transactions)
ref_count = len(transactions)
ref_mean = ref_total / ref_count
ref_min = min(t["amount"] for t in transactions)
ref_max = max(t["amount"] for t in transactions)
ref_fraud_count = sum(1 for t in transactions if t["is_fraud"])
ref_category_totals = {}
for t in transactions:
    ref_category_totals[t["category"]] = (
        ref_category_totals.get(t["category"], 0) + t["amount"]
    )

partition_size = NUM_TRANSACTIONS // NUM_PARTITIONS
partitions = []
for i in range(NUM_PARTITIONS):
    start = i * partition_size
    end = start + partition_size if i < NUM_PARTITIONS - 1 else NUM_TRANSACTIONS
    partitions.append(transactions[start:end])

print(f"Dataset: {NUM_TRANSACTIONS} transactions, {NUM_PARTITIONS} partitions")
print(f"  Reference: total={ref_total}, mean={ref_mean:.2f}, "
      f"min={ref_min}, max={ref_max}")
print(f"  Fraud: {ref_fraud_count}, Categories: {ref_category_totals}")
print()

# ═══════════════════════════════════════════════════════════════════════
# Create actors
# ═══════════════════════════════════════════════════════════════════════

ds = DataStore.remote()
monitor = ProgressMonitor.remote()
reducer = Reducer.remote()
fraud_detector = FraudDetector.remote()

print("  4 actors: DataStore, ProgressMonitor, Reducer, FraudDetector")
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 1: Data ingestion + partitioning
# ═══════════════════════════════════════════════════════════════════════

print("-" * 70)
print("STAGE 1: Data ingestion + partitioning")
print("-" * 70)

dataset_ref = ray.put(transactions)
print(f"  ray.put() -> dataset ({NUM_TRANSACTIONS} transactions)")

for i, part in enumerate(partitions):
    result = ray.get(ds.store_partition.remote(i, part))
    print(f"  Partition {i}: {result['size']} transactions stored")

count = ray.get(ds.get_partition_count.remote())
assert count == NUM_PARTITIONS
ray.get(monitor.record_stage.remote("ingestion", "complete"))
print(f"  {count} partitions stored")
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 2: Parallel stats with ray.wait()
# ═══════════════════════════════════════════════════════════════════════

print("-" * 70)
print("STAGE 2: Parallel stats computation (4 workers, ray.wait)")
print("-" * 70)

stat_refs = [compute_stats.remote(part) for part in partitions]
print(f"  Submitted {NUM_PARTITIONS} compute_stats tasks")

completed_stats = []
pending = list(stat_refs)
wave = 0
while pending:
    ready, pending = ray.wait(pending, num_returns=1)
    wave += 1
    result = ray.get(ready[0])
    completed_stats.append(result)
    print(f"  [wave {wave}] count={result['count']}, total={result['total']}, "
          f"fraud={result['fraud_count']}")

assert len(completed_stats) == NUM_PARTITIONS
ray.get(monitor.record_stage.remote("compute_stats", "complete"))
print(f"  All {NUM_PARTITIONS} partitions computed")
print(f"  Worker load: {ray._runtime._task_call_counts}")
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 3: Reduce partial results
# ═══════════════════════════════════════════════════════════════════════

print("-" * 70)
print("STAGE 3: Merge partial statistics via Reducer actor")
print("-" * 70)

for i, stat in enumerate(completed_stats):
    merged = ray.get(reducer.merge.remote(stat))
    print(f"  Merged partition {i+1}/{NUM_PARTITIONS} -> {merged} total")

final_result = ray.get(reducer.get_result.remote())
ray.get(monitor.record_stage.remote("reduce", "complete"))

print(f"  Final: total={final_result['total']}, count={final_result['count']}, "
      f"mean={final_result['mean']:.2f}")
print(f"  Min={final_result['min']}, Max={final_result['max']}, "
      f"Fraud={final_result['fraud_count']}")
print(f"  Categories: {final_result['category_totals']}")
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 4: Fraud detection scan
# ═══════════════════════════════════════════════════════════════════════

print("-" * 70)
print("STAGE 4: Fraud detection scan (FraudDetector actor)")
print("-" * 70)

scan_refs = [fraud_detector.scan.remote(part) for part in partitions]
for i, ref in enumerate(scan_refs):
    result = ray.get(ref)
    print(f"  Partition {i}: scanned={result['scanned']}, "
          f"flagged={len(result['flagged_ids'])}")

fraud_summary = ray.get(fraud_detector.get_summary.remote())
ray.get(monitor.record_stage.remote("fraud_scan", "complete"))
print(f"  Total flagged: {fraud_summary['total_flagged']} "
      f"(scanned {fraud_summary['total_scanned']})")
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 5: Validation + transformation tasks
# ═══════════════════════════════════════════════════════════════════════

print("-" * 70)
print("STAGE 5: Validation + transformation tasks")
print("-" * 70)

validate_refs = [validate.remote(transactions[i]) for i in range(10)]
pending = list(validate_refs)
valid_count = 0
while pending:
    ready, pending = ray.wait(pending, num_returns=1)
    v = ray.get(ready[0])
    if v["valid"]:
        valid_count += 1

print(f"  Validated 10 transactions: {valid_count}/10 valid")

normalized = ray.get(transform.remote(partitions[0], ref_max))
print(f"  Transformed partition 0: {len(normalized)} normalized transactions")
print(f"  Sample: id={normalized[0]['id']}, "
      f"normalized={normalized[0]['normalized_amount']}%, "
      f"category={normalized[0]['category']}")

ray.get(monitor.record_stage.remote("validation", "complete"))
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 6: Object store round-trip verification
# ═══════════════════════════════════════════════════════════════════════

print("-" * 70)
print("STAGE 6: Object store round-trip verification")
print("-" * 70)

final_ref = ray.put(final_result)
retrieved = ray.get(final_ref)
assert retrieved == final_result
print("  ray.put(final_result) -> ray.get() -> match")

batch_refs = [ray.put(i * 100) for i in range(5)]
batch_vals = ray.get(batch_refs)
assert batch_vals == [0, 100, 200, 300, 400]
print("  Batch put/get [0,100,200,300,400] -> match")

mixed_refs = batch_refs[:3] + [final_ref]
ready, remaining = ray.wait(mixed_refs, num_returns=len(mixed_refs))
assert len(ready) == 4 and len(remaining) == 0
print("  ray.wait(4 mixed objects) -> all ready")

ray.get(monitor.record_stage.remote("verification", "complete"))
print()

# ═══════════════════════════════════════════════════════════════════════
# STAGE 7: Progress report
# ═══════════════════════════════════════════════════════════════════════

print("-" * 70)
print("STAGE 7: Progress report")
print("-" * 70)

report = ray.get(monitor.get_report.remote())
print(f"  Stages: {list(report['stages'].keys())}")
print(f"  Errors: {report['error_count']}")
print()

# ═══════════════════════════════════════════════════════════════════════
# VERIFICATION
# ═══════════════════════════════════════════════════════════════════════

print("=" * 70)
print("  VERIFICATION")
print("=" * 70)
print()

errors = []


def check(name, actual, expected, tol=0):
    ok = abs(actual - expected) <= tol if tol else actual == expected
    if not ok:
        errors.append(f"{name}: expected {expected}, got {actual}")
        print(f"  FAIL {name}: expected {expected}, got {actual}")
    else:
        print(f"  OK   {name}: {actual}")


check("Total amount", final_result["total"], ref_total)
check("Transaction count", final_result["count"], ref_count)
check("Mean amount", final_result["mean"], ref_mean, tol=0.01)
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

total_tasks = sum(ray._runtime._task_call_counts)
check("Total task calls", total_tasks, NUM_PARTITIONS + 10 + 1)

# GCS named actor lookups via ray.get_actor
ds_handle = ray.get_actor("DataStore_1")
print(f"  OK   ray.get_actor('DataStore_1') -> {ds_handle}")

reducer_handle = ray.get_actor("Reducer_3")
print(f"  OK   ray.get_actor('Reducer_3') -> {reducer_handle}")

try:
    ray.get_actor("Ghost")
    errors.append("Ghost actor should not exist")
    print("  FAIL ray.get_actor('Ghost') did not raise")
except ValueError:
    print("  OK   ray.get_actor('Ghost') -> ValueError")

print()
print("=" * 70)
if errors:
    print(f"  FAILED -- {len(errors)} verification errors:")
    for e in errors:
        print(f"    - {e}")
else:
    print("  ALL VERIFICATIONS PASSED")
print("=" * 70)
print()
print(f"  Task workers:  {len(ray._runtime._task_pool)} (shared pool)")
print(f"  Actors:        4 (DataStore, ProgressMonitor, Reducer, FraudDetector)")
print(f"  Task calls:    {total_tasks} (per-worker: {ray._runtime._task_call_counts})")
print(f"  ray.put():     7 calls")
print(f"  ray.get():     30+ calls")
print(f"  ray.wait():    3 calls (stats, validation, batch objects)")
print(f"  Transactions:  {NUM_TRANSACTIONS}")
print(f"  Fraud flagged: {fraud_summary['total_flagged']}")
print()

ray.shutdown()
exit(0 if not errors else 1)
