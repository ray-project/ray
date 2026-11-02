"""Release test: label-based operator scheduling in the actor-only backend.

Runs one synthetic pipeline whose middle stage is pinned to labeled nodes via a
per-operator ``label_selector`` and verifies that the constraint-aware sizer
places it correctly under contention from unconstrained operators.

Cluster (constraint_scheduling_labels_compute.yaml): 1 unlabeled head +
2 nodes labeled ``ray-pool: pool_a`` + 2 ``pool_b`` + 4 ``common``, all
m5.2xlarge (8 CPUs each; 9 nodes, 72 CPUs).

Pipeline:
    range(N) -> GenStage (~1 MB/row, 1 CPU, autoscaling)
             -> LabeledStage (label-selected, per-case shape, the bottleneck)
             -> [HogStage: 5 CPUs, autoscaling (per-case min, 9) -- hog cases]
             -> NullDatasink

The hog cases are ordering gates built on CPU quantization: the unconstrained
hog can scale to one 5-CPU actor on EVERY node, and any node hosting one has
only 3 free CPUs left -- too few for a 4-CPU labeled actor. If the sizer did
not grant and place the labeled operator first, the hog would camp the labeled
nodes and the labeled actors (eligible nowhere else) would pend forever; the
hog sits downstream, so it would never get input either: a permanent deadlock,
caught by the release timeout. Completing at all is the core assertion; the
placement sweep and the max-alive check make it exact.

Every operator's min_size is satisfiable when scheduling behaves correctly:
the hog's min is set per case to the number of nodes guaranteed to stay free
of labeled actors (derived from the live cluster), so it genuinely demands the
rest of the cluster without ever being structurally unmet. These tests stay
valid if an unmet min_size later becomes a hard failure instead of a hang.

The autoscaling case instead lets the sizer scale pools into contention: the
unconstrained gen stage backfills the labeled nodes' spare capacity, and
growing the (bottleneck) labeled operator to its max requires the starved-op
path -- favored-node nomination, victim drain, then constrained placement --
to work end to end.

Cases (--case):
    pool                 flagship ordering gate: selector {ray-pool: pool_a},
                         4 CPUs x 4 fixed actors = all 16 pool_a CPUs
    node                 pin to ONE node: selector {ray.io/node-id: <picked>},
                         2 CPUs x 4 fixed actors = that node's 8 CPUs
    multi_pool           selector {ray-pool: in(pool_a,pool_b)}, 4 CPUs x
                         fixed (8, 8) = all 32 CPUs of both pools
    negation             selector {ray-pool: !pool_a}; asserts only that no
                         labeled actor lands on pool_a
    autoscale_contention selector {ray-pool: pool_a}, 4 CPUs x autoscaling
                         (1, 4); gen backfills pool_a first, the sizer must
                         reclaim it
"""

import argparse
import os
import time
from typing import Dict, List

import constraint_scheduling_common as common
from benchmark import Benchmark, benchmark_py_modules
from constraint_scheduling_common import (
    POOL_LABEL_KEY,
    GenStage,
    HogStage,
    LabeledStage,
    MaxAliveActorPoller,
    NullDatasink,
    pick_pool_node,
    pool_nodes,
    verify_stage_placement,
)

import ray

NODE_ID_LABEL_KEY = "ray.io/node-id"

# Cluster shape (must match constraint_scheduling_labels_compute.yaml).
NUM_NODES = 9  # 1 head + 8 workers, 8 CPUs each

GEN_CONCURRENCY = (1, 32)
HOG_NUM_CPUS = 5
# The hog autoscales up to one 5-CPU actor per node; its min is set per case
# (see build_case).
HOG_MAX = NUM_NODES

# Explicit per-actor memory for every synthetic stage (they sleep and pass
# ~32 MB batches through; 1 GiB is generous). Keeps memory a non-binding
# resource on 4 GB/vCPU instances so the CPU-quantization math holds.
STAGE_MEMORY = 1 * 1024**3

# The Write sink is pinned to the common pool in EVERY case. The sink is busy
# for the whole run and the sizer never evicts a busy unconstrained actor, so
# a single 1-CPU Write actor landing on a labeled node (e.g. by backfilling a
# slot vacated by transient allocation churn) would wedge an exact-fit labeled
# pool one actor short of its target forever. Confining the sink means
# labeled-node capacity can only be temporarily held by drainable operators
# (gen), which the starved-op reclaim path recovers. These tests gate
# constraint-aware placement, not the non-evictable-busy-actor behavior.
WRITE_SELECTOR = {POOL_LABEL_KEY: "common"}


def _num_hog_capable_nodes() -> int:
    """Alive nodes that can host one 5-CPU hog actor.

    Counted from the cluster's actual CPU resources rather than assumed from
    the compute config, so an unschedulable (or CPU-reserved) head node lowers
    the hog's min instead of stranding it below min_size forever.
    """
    return sum(
        1
        for n in ray.nodes()
        if n["Alive"] and n["Resources"].get("CPU", 0) >= HOG_NUM_CPUS
    )


def build_case(args: argparse.Namespace) -> Dict:
    """Resolve the per-case labeled-op shape and placement expectations."""
    if args.case == "pool":
        return dict(
            selector={POOL_LABEL_KEY: "pool_a"},
            num_cpus=4,
            concurrency=(4, 4),
            expected_max_alive=4,
            with_hog=True,
            # The labeled op fully occupies the 2 pool_a nodes; every other
            # hog-capable node fits one 5-CPU hog actor.
            hog_concurrency=(_num_hog_capable_nodes() - 2, HOG_MAX),
            is_allowed=lambda node_id, labels: labels.get(POOL_LABEL_KEY) == "pool_a",
            allowed_desc="pool_a nodes",
            require_nodes=pool_nodes("pool_a"),
        )
    if args.case == "node":
        picked = pick_pool_node("pool_a")
        print(f"Pinning LabeledStage to node {picked}")
        return dict(
            selector={NODE_ID_LABEL_KEY: picked},
            num_cpus=2,
            concurrency=(4, 4),
            expected_max_alive=4,
            with_hog=True,
            # The labeled op fully occupies only the picked node; every other
            # hog-capable node fits one hog actor.
            hog_concurrency=(_num_hog_capable_nodes() - 1, HOG_MAX),
            is_allowed=lambda node_id, labels: node_id == picked,
            allowed_desc=f"node {picked}",
            require_nodes=[picked],
        )
    if args.case == "multi_pool":
        return dict(
            selector={POOL_LABEL_KEY: "in(pool_a,pool_b)"},
            num_cpus=4,
            # Fixed at exactly both pools' capacity (8 x 4 CPUs = 32): all 8
            # actors belong to the initial min wave, created before the
            # scheduling loop ramps unconstrained operators (and bootstrap
            # waits for them to be ALIVE), so perfect 2-per-node packing is
            # deterministic. With autoscaling concurrency the labeled op
            # grows late, and a busy unconstrained 1-CPU actor camping a pool
            # node's remainder is never evicted, so the exact-fit last actor
            # could never place.
            concurrency=(8, 8),
            expected_max_alive=8,
            with_hog=False,
            is_allowed=lambda node_id, labels: labels.get(POOL_LABEL_KEY)
            in ("pool_a", "pool_b"),
            allowed_desc="pool_a or pool_b nodes",
            require_nodes=pool_nodes("pool_a") + pool_nodes("pool_b"),
        )
    if args.case == "negation":
        return dict(
            selector={POOL_LABEL_KEY: "!pool_a"},
            num_cpus=4,
            concurrency=(6, 6),
            expected_max_alive=6,
            with_hog=True,
            # Only pool_a (2 nodes) is guaranteed free of labeled actors: if
            # the sizer spreads the 6 labeled actors 1-per-node across the 6
            # eligible worker nodes, each keeps only 4 free CPUs -- too few
            # for a 5-CPU hog actor. A higher hog min could be structurally
            # unsatisfiable, so min 2 here.
            hog_concurrency=(2, HOG_MAX),
            # Only assert "not on pool_a": whether nodes lacking the label key
            # (the head) match "!pool_a" is up to Ray core's matcher, so the
            # test does not encode an assumption about it.
            is_allowed=lambda node_id, labels: labels.get(POOL_LABEL_KEY) != "pool_a",
            allowed_desc="any node not labeled pool_a",
            require_nodes=[],
        )
    if args.case == "autoscale_contention":
        return dict(
            selector={POOL_LABEL_KEY: "pool_a"},
            num_cpus=4,
            concurrency=(1, 4),
            expected_max_alive=4,
            with_hog=False,
            # Reclaim-path test: the labeled op starts small, gen backfills
            # pool_a's spare CPUs, and growing the labeled op to its exact-fit
            # max requires starved-node nomination + victim drain. gen is
            # drainable (it downscales once its input runs dry), so the
            # reclaim is guaranteed to have victims; the sink is pinned to the
            # common pool (see WRITE_SELECTOR).
            is_allowed=lambda node_id, labels: labels.get(POOL_LABEL_KEY) == "pool_a",
            allowed_desc="pool_a nodes",
            require_nodes=pool_nodes("pool_a"),
        )
    raise ValueError(f"Unknown case {args.case!r}")


def run_pipeline(args: argparse.Namespace, case: Dict) -> None:
    ds = ray.data.range(args.num_rows)
    ds = ds.map_batches(
        GenStage,
        batch_size=args.batch_size,
        concurrency=GEN_CONCURRENCY,
        memory=STAGE_MEMORY,
    )
    ds = ds.map_batches(
        LabeledStage,
        batch_size=args.batch_size,
        num_cpus=case["num_cpus"],
        concurrency=case["concurrency"],
        memory=STAGE_MEMORY,
        # Extra kwargs flow into the operator's ray_remote_args.
        label_selector=case["selector"],
    )
    if case["with_hog"]:
        ds = ds.map_batches(
            HogStage,
            batch_size=args.batch_size,
            num_cpus=HOG_NUM_CPUS,
            concurrency=case["hog_concurrency"],
            memory=STAGE_MEMORY,
        )
    ds.write_datasink(
        NullDatasink(),
        ray_remote_args={"label_selector": WRITE_SELECTOR},
    )
    print(f"\n===== ds.stats() [case={args.case}] =====\n{ds.stats()}\n")


def main(args: argparse.Namespace) -> Dict:
    case = build_case(args)
    print(
        f"Case {args.case!r}: selector={case['selector']}, "
        f"num_cpus={case['num_cpus']}, concurrency={case['concurrency']}"
    )

    stages = [LabeledStage.__name__]
    if case["with_hog"]:
        stages.append(HogStage.__name__)

    start = time.perf_counter()
    with MaxAliveActorPoller(stages) as poller:
        run_pipeline(args, case)
    runtime_s = time.perf_counter() - start

    errors: List[str] = []

    labeled_max_alive = poller.max_alive[LabeledStage.__name__]
    if labeled_max_alive != case["expected_max_alive"]:
        errors.append(
            f"LabeledStage max simultaneous ALIVE actors was "
            f"{labeled_max_alive}, expected {case['expected_max_alive']}"
        )

    labeled_max_requested = poller.max_requested[LabeledStage.__name__]
    if labeled_max_requested > case["expected_max_alive"]:
        errors.append(
            f"LabeledStage max simultaneous ALIVE + PENDING actors was "
            f"{labeled_max_requested}, expected <= {case['expected_max_alive']}"
        )

    placement = verify_stage_placement(
        LabeledStage.__name__,
        case["is_allowed"],
        case["allowed_desc"],
        require_nodes=case["require_nodes"],
    )
    errors.extend(placement.pop("errors"))

    result = {
        "case": args.case,
        "runtime_s": runtime_s,
        "rows_per_s": args.num_rows / runtime_s,
        "num_rows": args.num_rows,
        "labeled_max_alive": labeled_max_alive,
        "labeled_max_requested": labeled_max_requested,
        # Informational only: how far past its min the hog ramps depends on
        # what capacity the other ops leave.
        "hog_max_alive": poller.max_alive.get(HogStage.__name__, 0),
    }
    result.update({f"labeled_{k}": v for k, v in placement.items()})
    if errors:
        result["_errors"] = errors
    return result


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--case",
        required=True,
        choices=["pool", "node", "multi_pool", "negation", "autoscale_contention"],
    )
    p.add_argument("--num-rows", type=int, default=25_000)
    p.add_argument("--batch-size", type=int, default=32)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    # Ship benchmark.py + the common module to workers: the UDF classes live in
    # constraint_scheduling_common, which workers must import to deserialize.
    ray.init(
        runtime_env={
            "py_modules": benchmark_py_modules() + [os.path.abspath(common.__file__)],
        }
    )
    name = f"constraint-label-scheduling-{args.case}"
    benchmark = Benchmark()
    benchmark.run_fn(name, main, args)
    benchmark.write_result()

    # Raise after metrics have been written so the dashboard still records the
    # failed run.
    errors = benchmark.result.get(name, {}).get("_errors", [])
    if errors:
        raise AssertionError("; ".join(errors))
