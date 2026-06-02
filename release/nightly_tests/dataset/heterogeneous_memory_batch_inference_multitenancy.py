"""
Heterogeneous Memory Batch Inference — Multitenancy Variant

Runs two copies of `heterogeneous_memory_batch_inference` concurrently on a
single Ray cluster, each pinned to its own labeled subcluster via Ray Data
`label_selector`. Reports two properties of the subcluster scheduling
work:

1. Isolation: wall-clock time for the two tenants running concurrently is
   close to the time for a single tenant running alone on its half of the
   cluster. Departure from that indicates cross-subcluster interference.
2. Placement: no task from a labeled pipeline ran on the wrong subcluster's
   nodes, and no operator task escaped to the unlabeled head node.

Cluster (heterogeneous_memory_compute_multitenancy.yaml):
  - 1 head node, unlabeled.
  - 10 CPU + 2 GPU workers per tenant, each pool labeled
    `subcluster: tenant_a` or `subcluster: tenant_b`.

Sequence:
  1. Solo baseline run as tenant_a -> T_single
  2. Concurrent run as tenant_a + tenant_b (two threads) -> per-tenant
     wall times; multi wall is the max of the two.
  3. Print solo vs. multi timing and overhead ratio.
  4. Sweep `list_tasks` against `list_nodes`, print any placement
     mismatches.
  5. After metrics have been written, raise if either runtime or
     placement failed.
"""

import argparse
import os
import threading
import time
from typing import Dict, List

import heterogeneous_memory_batch_inference as hmbi
from benchmark import Benchmark, benchmark_py_modules
from heterogeneous_memory_batch_inference import build_and_run_pipeline

import ray
from ray.util.state import list_nodes, list_tasks


SUBCLUSTERS = ("tenant_a", "tenant_b")

# Max acceptable concurrent-vs-solo overhead before we flag a regression.
MAX_OVERHEAD = 0.15

# Operator-name prefixes set by Ray Data on tasks for this pipeline:
#   ray.data.range -> "Read*", map_batches(...) -> "MapBatches(...)",
#   write_datasink(...) -> "Write*". Exhaustive for this pipeline; extend
#   if the pipeline ever adds shuffles / aggregates / repartitions.
DATA_TASK_NAME_PREFIXES = ("MapBatches(", "Read", "Write")

# Upper bound on tasks fetched via list_tasks. We print the scanned count
# so the operator can spot truncation.
LIST_TASKS_LIMIT = 200_000


def run_pipeline(subcluster: str, args: argparse.Namespace) -> None:
    build_and_run_pipeline(
        num_rows=args.num_rows,
        gen_batch_size=args.gen_batch_size,
        cpu_batch_size=args.cpu_batch_size,
        gpu_batch_size=args.gpu_batch_size,
        gpu_concurrency=args.gpu_concurrency,
        subcluster=subcluster,
    )


def run_solo(args: argparse.Namespace) -> float:
    start = time.perf_counter()
    run_pipeline("tenant_a", args)
    return time.perf_counter() - start


def run_concurrent(args: argparse.Namespace) -> Dict[str, float]:
    """Run both tenants on threads concurrently; return per-tenant wall-time."""
    per_tenant: Dict[str, float] = {}

    def _run(sc: str) -> None:
        t0 = time.perf_counter()
        run_pipeline(sc, args)
        per_tenant[sc] = time.perf_counter() - t0

    threads = [
        threading.Thread(target=_run, args=(sc,), name=f"pipeline-{sc}")
        for sc in SUBCLUSTERS
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return per_tenant


def verify_placement() -> dict:
    """Sweep the state API; print any subcluster leaks or head escapes.

    Collects mismatch details into the returned dict so the caller can
    raise after metrics have been written.
    """
    nodes = list_nodes(detail=True, limit=1000)
    node_subcluster = {n.node_id: (n.labels or {}).get("subcluster") for n in nodes}

    bad_on_labeled = []  # task on labeled node with mismatching selector
    bad_on_head = []  # dataset-shaped task that escaped to head / unlabeled node
    tasks_on_labeled = 0
    tasks_on_head = 0
    total_tasks = 0

    for t in list_tasks(detail=True, limit=LIST_TASKS_LIMIT):
        total_tasks += 1
        node_sc = node_subcluster.get(t.node_id)
        if node_sc in SUBCLUSTERS:
            tasks_on_labeled += 1
            want = (t.label_selector or {}).get("subcluster")
            if want != node_sc:
                bad_on_labeled.append(
                    (t.task_id, t.name, t.func_or_class_name, want, node_sc)
                )
        else:
            tasks_on_head += 1
            if t.name and any(t.name.startswith(p) for p in DATA_TASK_NAME_PREFIXES):
                bad_on_head.append((t.task_id, t.name))

    print(
        f"Placement: scanned {total_tasks} tasks "
        f"(limit={LIST_TASKS_LIMIT}; if equal, results may be truncated). "
        f"{tasks_on_labeled} on labeled nodes, {tasks_on_head} on head/unlabeled."
    )

    if bad_on_labeled:
        print(
            f"Subcluster mismatches on labeled nodes " f"(total {len(bad_on_labeled)}):"
        )
        for row in bad_on_labeled:
            print(f"  {row}")
    else:
        print("No subcluster mismatches on labeled nodes.")

    if bad_on_head:
        print(
            f"Dataset tasks escaped to head/unlabeled nodes "
            f"(total {len(bad_on_head)}):"
        )
        for row in bad_on_head:
            print(f"  {row}")
    else:
        print("No dataset tasks escaped to head/unlabeled nodes.")

    return {
        "tasks_scanned": total_tasks,
        "tasks_on_labeled_nodes": tasks_on_labeled,
        "tasks_on_head_or_unlabeled": tasks_on_head,
        "subcluster_mismatches": len(bad_on_labeled),
        "head_escapes": len(bad_on_head),
        "list_tasks_limit": LIST_TASKS_LIMIT,
    }


def main(args: argparse.Namespace) -> dict:
    """Returns the benchmark dict. Errors are recorded under "_errors" so
    metrics can be written first; the caller raises after write_result."""
    errors: List[str] = []

    solo_time = run_solo(args)
    per_tenant = run_concurrent(args)
    multi_time = max(per_tenant.values()) if per_tenant else float("nan")
    overhead_ratio = multi_time / solo_time if solo_time > 0 else float("inf")
    per_tenant_str = ", ".join(
        f"{sc}={per_tenant.get(sc, float('nan')):.2f}s" for sc in SUBCLUSTERS
    )
    print(
        f"Solo: {solo_time:.2f}s; Multi: {multi_time:.2f}s "
        f"[{per_tenant_str}]; overhead ratio: {overhead_ratio:.3f}"
    )
    if overhead_ratio > 1 + MAX_OVERHEAD:
        errors.append(
            f"Multi-tenant overhead {overhead_ratio:.3f}x exceeds budget "
            f"{1 + MAX_OVERHEAD:.3f}x (solo={solo_time:.2f}s, "
            f"multi={multi_time:.2f}s)."
        )

    placement_metrics = verify_placement()
    if placement_metrics["subcluster_mismatches"]:
        errors.append(
            f"{placement_metrics['subcluster_mismatches']} task(s) ran on a "
            "node with the wrong subcluster label."
        )
    if placement_metrics["head_escapes"]:
        errors.append(
            f"{placement_metrics['head_escapes']} dataset task(s) escaped "
            "to head/unlabeled nodes."
        )

    result = {
        "solo_runtime_s": solo_time,
        "multi_runtime_s": multi_time,
        "overhead_ratio": overhead_ratio,
        "num_rows": int(args.num_rows),
        "gpu_concurrency": int(args.gpu_concurrency),
    }
    for sc in SUBCLUSTERS:
        if sc in per_tenant:
            result[f"multi_runtime_s_{sc}"] = per_tenant[sc]
    result.update(placement_metrics)
    if errors:
        result["_errors"] = errors
    return result


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Heterogeneous memory batch inference multitenancy benchmark"
    )
    p.add_argument("--num-rows", type=int, default=400_000)
    p.add_argument("--gen-batch-size", type=int, default=1024)
    p.add_argument("--cpu-batch-size", type=int, default=1024)
    p.add_argument("--gpu-batch-size", type=int, default=256)
    p.add_argument("--gpu-concurrency", type=int, default=8)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    name = "heterogeneous-memory-batch-inference-multitenancy"
    # Ship both benchmark.py and heterogeneous_memory_batch_inference.py to
    # workers: when this script is run as ``__main__``, the UDF classes
    # (FakeGPUInference, etc.) live in the imported
    # ``heterogeneous_memory_batch_inference`` module, so cloudpickle
    # serializes them by reference and each worker needs to import the
    # module to deserialize.
    ray.init(
        runtime_env={
            "py_modules": benchmark_py_modules() + [os.path.abspath(hmbi.__file__)],
        }
    )
    benchmark = Benchmark()
    benchmark.run_fn(name, main, args)
    benchmark.write_result()

    # Raise after metrics have been written so the dashboard still records
    # the failed run (matches the sort_benchmark.py "print then raise"
    # convention).
    errors = benchmark.result.get(name, {}).get("_errors", [])
    if errors:
        raise AssertionError("; ".join(errors))
