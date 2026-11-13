"""Release test: placement-group scheduling in the actor-only backend.

A GPU inference operator schedules into a static placement group under the
constraint-aware sizer. PG operators are "self-placed": the sizer sizes them by
bundle capacity (not its per-node free map) and delegates bundle placement to
Ray core.

Cluster (constraint_scheduling_pg_compute.yaml): 1 head + 4 CPU workers +
2 fake-GPU workers (m5.2xlarge with logical GPU:4 on top of their 8 CPUs ->
8 GPUs total, no real GPUs needed).

The test creates a 6-bundle {"GPU": 1, "CPU": 1} placement group and gives the
inference operator autoscaling concurrency (1, 8), so the sizer ramps the pool
under the PG cap. The cap (6) is strictly below both the cluster's GPUs (8)
and the operator's max concurrency (8):

    range(N) -> GenStage (~1 MB/row, 1 CPU) -> CpuStage (1 CPU, autoscaling)
             -> FakeGPUInfer (1 GPU + 1 CPU, concurrency=(1, 8), PG-scheduled)
             -> NullDatasink

The inference actor reserves BOTH a GPU and a CPU (the common real-world shape
for GPU inference); a PG actor's resources must fit inside a bundle, so each
bundle carries {"GPU": 1, "CPU": 1}.

Asserts:
  - max simultaneous ALIVE inference actors == 6: the pool is capped at the
    PG's bundle capacity even though more GPUs and concurrency headroom exist;
  - max simultaneous ALIVE + PENDING inference actors <= 6: the sizer never
    requests an actor the PG cannot host (it would pend, invisible to ALIVE);
  - every inference actor ran inside the PG (ActorState.placement_group_id)
    and therefore on a GPU node;
  - the pipeline completes (a PG-sizing bug that pends actors would deadlock
    into the release timeout).

The per-actor-PG path (a ray_remote_args_fn assigning a fresh PG per actor,
the vLLM "ray" backend pattern) is covered end to end by the llm_batch_vllm
release test, not here.
"""

import argparse
import os
import time
from typing import Dict, List

import constraint_scheduling_common as common
from benchmark import Benchmark, benchmark_py_modules
from constraint_scheduling_common import (
    FakeGPUInfer,
    GenStage,
    MaxAliveActorPoller,
    NullDatasink,
    stage_actors,
    verify_stage_placement,
)

import ray
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

NUM_PG_BUNDLES = 6
INFER_CONCURRENCY = (1, 8)
PG_READY_TIMEOUT_S = 300

# Explicit per-actor memory for the CPU stages (they sleep and pass ~32 MB
# batches; 1 GiB is generous). Without it the sizer's default memory
# reservation makes memory the binding resource on 4 GB/vCPU instances. The
# PG-placed inference op is sized by bundles, so it needs none.
STAGE_MEMORY = 1 * 1024**3


class CpuStage:
    """Light CPU preprocessing (callable class so tuple concurrency is valid)."""

    def __call__(self, batch):
        time.sleep(0.5)
        batch["processed"] = [1] * len(batch["data"])
        return batch


def run_pipeline(args: argparse.Namespace, pg) -> None:
    ds = ray.data.range(args.num_rows)
    ds = ds.map_batches(
        GenStage,
        batch_size=args.batch_size,
        concurrency=(1, 16),
        memory=STAGE_MEMORY,
    )
    ds = ds.map_batches(
        CpuStage,
        batch_size=args.batch_size,
        concurrency=(1, 16),
        memory=STAGE_MEMORY,
    )
    ds = ds.map_batches(
        FakeGPUInfer,
        batch_size=args.batch_size,
        num_cpus=1,
        num_gpus=1,
        concurrency=INFER_CONCURRENCY,
        # Extra kwargs flow into the operator's ray_remote_args.
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg),
    )
    ds.write_datasink(NullDatasink())
    print(f"\n===== ds.stats() =====\n{ds.stats()}\n")


def main(args: argparse.Namespace) -> Dict:
    # Create and await the PG before building the Dataset: the operator parses
    # its placement constraint from ray_remote_args once. Each bundle reserves
    # the full per-actor shape (1 GPU + 1 CPU).
    pg = ray.util.placement_group(
        [{"GPU": 1, "CPU": 1}] * NUM_PG_BUNDLES, strategy="PACK"
    )
    ray.get(pg.ready(), timeout=PG_READY_TIMEOUT_S)
    print(
        f"Placement group ready: {pg.id.hex()} "
        f"({NUM_PG_BUNDLES}x GPU:1+CPU:1, PACK)"
    )

    start = time.perf_counter()
    with MaxAliveActorPoller([FakeGPUInfer.__name__]) as poller:
        run_pipeline(args, pg)
    runtime_s = time.perf_counter() - start

    errors: List[str] = []

    infer_max_alive = poller.max_alive[FakeGPUInfer.__name__]
    if infer_max_alive != NUM_PG_BUNDLES:
        errors.append(
            f"FakeGPUInfer max simultaneous ALIVE actors was {infer_max_alive}, "
            f"expected the PG bundle capacity {NUM_PG_BUNDLES} (max concurrency "
            f"{INFER_CONCURRENCY[1]}, cluster GPUs 8)"
        )

    infer_max_requested = poller.max_requested[FakeGPUInfer.__name__]
    if infer_max_requested > NUM_PG_BUNDLES:
        errors.append(
            f"FakeGPUInfer max simultaneous ALIVE + PENDING actors was "
            f"{infer_max_requested}, expected <= the PG bundle capacity "
            f"{NUM_PG_BUNDLES}: the sizer requested actors that cannot fit"
        )

    outside_pg = [
        (a.actor_id, a.placement_group_id)
        for a in stage_actors(FakeGPUInfer.__name__)
        if a.placement_group_id != pg.id.hex()
    ]
    if outside_pg:
        errors.append(
            f"{len(outside_pg)} FakeGPUInfer actor(s) ran outside placement "
            f"group {pg.id.hex()}: {outside_pg}"
        )

    gpu_nodes = _gpu_node_ids()
    placement = verify_stage_placement(
        FakeGPUInfer.__name__,
        lambda node_id, labels: gpu_nodes.get(node_id, False),
        "GPU nodes",
    )
    errors.extend(placement.pop("errors"))

    result = {
        "runtime_s": runtime_s,
        "rows_per_s": args.num_rows / runtime_s,
        "num_rows": args.num_rows,
        "num_pg_bundles": NUM_PG_BUNDLES,
        "infer_max_alive": infer_max_alive,
        "infer_max_requested": infer_max_requested,
        "num_actors_outside_pg": len(outside_pg),
    }
    result.update({f"infer_{k}": v for k, v in placement.items()})
    if errors:
        result["_errors"] = errors
    return result


def _gpu_node_ids() -> Dict[str, bool]:
    return {
        n["NodeID"]: n["Resources"].get("GPU", 0) > 0 for n in ray.nodes() if n["Alive"]
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--num-rows", type=int, default=30_000)
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
    name = "constraint-pg-scheduling"
    benchmark = Benchmark()
    benchmark.run_fn(name, main, args)
    benchmark.write_result()

    # Raise after metrics have been written so the dashboard still records the
    # failed run.
    errors = benchmark.result.get(name, {}).get("_errors", [])
    if errors:
        raise AssertionError("; ".join(errors))
