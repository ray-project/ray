import argparse
import functools
import os
import statistics
import time
from typing import Any, Dict, List, Optional, Tuple

import numpy
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import ray
from ray._private.test_utils import EC2InstanceTerminatorWithGracePeriod
from ray.data.context import DataContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from benchmark import Benchmark


USE_CORE_ACTOR_POOL_ENV_VAR = "RAY_DATA_USE_CORE_ACTOR_POOL"
MODEL_SIZE = 1024**3
NODE_RECOVERY_TIMEOUT_S = 20 * 60
NODE_RECOVERY_POLL_INTERVAL_S = 5


class TransientBenchmarkError(RuntimeError):
    pass


class SingleRunError(RuntimeError):
    def __init__(self, run_result: Dict[str, Any]):
        super().__init__(run_result["error"])
        self.run_result = run_result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Map benchmark")
    parser.add_argument(
        "--api", choices=["map", "map_batches", "flat_map"], required=True
    )
    parser.add_argument(
        "--sf", choices=["1", "10", "100", "1000", "10000"], default="1"
    )
    parser.add_argument(
        "--batch-format",
        choices=["numpy", "pandas", "pyarrow"],
        help=(
            "Batch format to use with 'map_batches'. This argument is ignored for "
            "'map' and 'flat_map'.",
        ),
    )
    parser.add_argument(
        "--compute",
        choices=["tasks", "actors"],
        help=(
            "Compute strategy to use with 'map_batches'. This argument is ignored for "
            "'map' and 'flat_map'.",
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="Batch size to use with 'map_batches'.",
    )
    parser.add_argument(
        "--map-batches-sleep-ms",
        type=int,
        default=50,
        help=(
            "Sleep time in milliseconds for each map_batches call. This is useful to "
            "simulate complex computation."
        ),
    )
    parser.add_argument(
        "--repeat-inputs",
        type=int,
        default=1,
        help=(
            "Number of times to repeat the input data. This is useful to make the "
            "job run longer."
        ),
    )
    parser.add_argument(
        "--repeat-map-batches",
        choices=["once", "repeat"],
        default="once",
        help=(
            "Whether to repeat map_batches. If 'once', the map_batches will run once. "
            "If 'repeat', the map_batches will run twice, with the second run using the "
            "output of the first run as input."
        ),
    )
    parser.add_argument(
        "--concurrency",
        default=[1, 1024],
        nargs=2,
        type=int,
        help="Concurrency to use with 'map_batches'.",
    )
    parser.add_argument(
        "--fail-actor-indices",
        type=str,
        default="",
        help=(
            "Comma-separated actor indices to inject transient failures into. "
            "E.g., '0,2' means only actor #0 and #2 will fail."
        ),
    )
    parser.add_argument(
        "--fail-at-call",
        type=int,
        default=50,
        help=(
            "Per-actor call count at which selected actors raise a transient error."
        ),
    )
    parser.add_argument(
        "--kill-actor-indices",
        type=str,
        default="",
        help=(
            "Comma-separated actor indices to kill via ray.kill(). "
            "E.g., '0,2,5' kills actors #0, #2, #5."
        ),
    )
    parser.add_argument(
        "--kill-after-tasks",
        type=int,
        default=100,
        help="Kill the specified actors after this many total tasks are dispatched.",
    )
    parser.add_argument(
        "--kill-node",
        action="store_true",
        default=False,
        help=(
            "Kill a worker node during pipeline execution to trigger reconstruction."
        ),
    )
    parser.add_argument(
        "--kill-node-after-tasks",
        type=int,
        default=100,
        help=(
            "Kill the node after this many total tasks are dispatched. "
            "Uses the same global task counter as --kill-after-tasks for determinism."
        ),
    )
    parser.add_argument(
        "--pipeline-stages",
        type=int,
        default=1,
        help=(
            "Number of chained map_batches(actors) stages. "
            "Stages > 1 inserts a random_shuffle between each stage to force "
            "materialization. Tests cascading reconstruction across multiple pools."
        ),
    )
    parser.add_argument(
        "--num-runs",
        type=int,
        default=1,
        help=(
            "Number of times to repeat the benchmark. Reports mean/std across runs."
        ),
    )
    args = parser.parse_args()
    validate_args(args)
    return args


# Centralized actor that orchestrates fault injection across pipeline stages.
# Tracks actors per stage and triggers faults (transient errors, actor kills,
# node kills) based on configurable thresholds.
@ray.remote(num_cpus=0)
class FaultInjectionCoordinator:
    def __init__(
        self,
        *,
        target_stage_idx: int,
        fail_actor_indices: List[int],
        fail_at_call: int,
        kill_actor_indices: List[int],
        kill_after_tasks: int,
        kill_node: bool,
        kill_node_after_tasks: int,
        head_node_id: str,
        node_killer: Optional[ray.actor.ActorHandle],
    ):
        self._target_stage_idx = target_stage_idx
        self._fail_actor_indices = set(fail_actor_indices)
        self._fail_at_call = fail_at_call
        self._kill_actor_indices = list(kill_actor_indices)
        self._kill_after_tasks = kill_after_tasks
        self._kill_node = kill_node
        self._kill_node_after_tasks = kill_node_after_tasks
        self._head_node_id = head_node_id
        self._node_killer = node_killer

        self._stage_task_counts: Dict[int, int] = {}
        self._stage_next_actor_index: Dict[int, int] = {}
        self._actor_index_by_id: Dict[int, Dict[str, int]] = {}
        self._actor_handles_by_index: Dict[int, Dict[int, ray.actor.ActorHandle]] = {}
        self._actor_nodes_by_index: Dict[int, Dict[int, str]] = {}

        self._failed_actor_indices: List[int] = []
        self._killed_actor_indices: List[int] = []
        self._actor_kill_triggered = False
        self._node_kill_triggered = False
        self._killed_node_id: Optional[str] = None

    def register_actor(
        self,
        stage_idx: int,
        actor_id: str,
        actor_handle: ray.actor.ActorHandle,
        node_id: str,
    ) -> int:
        index_by_id = self._actor_index_by_id.setdefault(stage_idx, {})
        if actor_id in index_by_id:
            actor_index = index_by_id[actor_id]
        else:
            actor_index = self._stage_next_actor_index.get(stage_idx, 0)
            self._stage_next_actor_index[stage_idx] = actor_index + 1
            index_by_id[actor_id] = actor_index

        self._actor_handles_by_index.setdefault(stage_idx, {})[actor_index] = actor_handle
        self._actor_nodes_by_index.setdefault(stage_idx, {})[actor_index] = node_id
        return actor_index

    def before_call(
        self, stage_idx: int, actor_id: str, local_call_count: int
    ) -> Dict[str, Any]:
        """Called by each actor before processing a batch. Increments the
        per-stage task counter and checks whether to trigger a fault.

        Note: this per-batch RPC only runs for fault scenarios. Perf tests
        use IncrementBatch with no coordinator, so there is no overhead."""
        actor_index = self._actor_index_by_id.setdefault(stage_idx, {}).get(actor_id)
        action = {
            "raise_transient": False,
            "actor_index": actor_index,
            "stage_task_count": self._stage_task_counts.get(stage_idx, 0),
        }
        if stage_idx != self._target_stage_idx:
            return action

        stage_task_count = self._stage_task_counts.get(stage_idx, 0) + 1
        self._stage_task_counts[stage_idx] = stage_task_count
        action["stage_task_count"] = stage_task_count

        if (
            actor_index is not None
            and actor_index in self._fail_actor_indices
            and local_call_count == self._fail_at_call
            and actor_index not in self._failed_actor_indices
        ):
            self._failed_actor_indices.append(actor_index)
            action["raise_transient"] = True

        if (
            self._kill_actor_indices
            and not self._actor_kill_triggered
            and stage_task_count >= self._kill_after_tasks
            and self._kill_selected_actors(stage_idx)
        ):
            self._actor_kill_triggered = True

        if (
            self._kill_node
            and not self._node_kill_triggered
            and stage_task_count >= self._kill_node_after_tasks
            and self._kill_target_node(stage_idx)
        ):
            self._node_kill_triggered = True

        return action

    def summary(self) -> Dict[str, Any]:
        return {
            "target_stage_idx": self._target_stage_idx,
            "stage_task_counts": {
                str(stage_idx): task_count
                for stage_idx, task_count in self._stage_task_counts.items()
            },
            "registered_actor_indices": {
                str(stage_idx): sorted(actor_map.keys())
                for stage_idx, actor_map in self._actor_handles_by_index.items()
            },
            "transient_failures_injected": sorted(self._failed_actor_indices),
            "actor_kill_triggered": self._actor_kill_triggered,
            "killed_actor_indices": sorted(self._killed_actor_indices),
            "node_kill_triggered": self._node_kill_triggered,
            "killed_node_id": self._killed_node_id,
        }

    def _kill_selected_actors(self, stage_idx: int) -> bool:
        actor_handles = self._actor_handles_by_index.get(stage_idx, {})
        missing_indices = [
            actor_index
            for actor_index in self._kill_actor_indices
            if actor_index not in actor_handles
        ]
        if missing_indices:
            return False

        for actor_index in self._kill_actor_indices:
            ray.kill(actor_handles[actor_index], no_restart=False)
            self._killed_actor_indices.append(actor_index)
        return True

    def _kill_target_node(self, stage_idx: int) -> bool:
        actor_nodes = self._actor_nodes_by_index.get(stage_idx, {})
        candidate_indices = sorted(
            actor_index
            for actor_index, node_id in actor_nodes.items()
            if node_id != self._head_node_id
        )
        if not candidate_indices:
            return False

        target_actor_index = candidate_indices[0]
        target_node_id = actor_nodes[target_actor_index]
        target_node_ip = self._resolve_node_ip(target_node_id)
        if target_node_ip is None or self._node_killer is None:
            return False

        ray.get(self._node_killer._kill_resource.remote(target_node_id, target_node_ip, None))
        self._killed_node_id = target_node_id
        return True

    def _resolve_node_ip(self, node_id: str) -> Optional[str]:
        for node in ray.nodes():
            if node["NodeID"] == node_id and node["Alive"]:
                return node["NodeManagerAddress"]
        return None


def main(args: argparse.Namespace) -> None:
    benchmark = Benchmark()
    partial_result = initialize_result(args)

    try:
        if args.kill_node:
            ensure_ray_initialized()
            expected_worker_nodes = get_alive_worker_node_count()
        else:
            expected_worker_nodes = None

        for run_index in range(args.num_runs):
            try:
                run_result = run_single_benchmark(args, run_index)
            except SingleRunError as exc:
                partial_result["runs"].append(exc.run_result)
                partial_result["error"] = exc.run_result["error"]
                partial_result["num_runs_completed"] = len(
                    [run for run in partial_result["runs"] if run["status"] == "ok"]
                )
                finalize_result(partial_result)
                benchmark.result["main"] = partial_result
                benchmark.write_result()
                raise

            partial_result["runs"].append(run_result)
            partial_result["time_runs"].append(run_result["time"])
            partial_result["num_runs_completed"] = len(partial_result["time_runs"])

            if args.kill_node and run_index < args.num_runs - 1:
                wait_for_worker_node_recovery(expected_worker_nodes)

        finalize_result(partial_result)
        benchmark.result["main"] = partial_result
        benchmark.write_result()
    except Exception:
        if "main" not in benchmark.result:
            finalize_result(partial_result)
            benchmark.result["main"] = partial_result
            benchmark.write_result()
        raise


def validate_args(args: argparse.Namespace) -> None:
    args.fail_actor_index_list = parse_actor_indices(args.fail_actor_indices)
    args.kill_actor_index_list = parse_actor_indices(args.kill_actor_indices)

    if args.num_runs < 1:
        raise ValueError("--num-runs must be >= 1")
    if args.pipeline_stages < 1:
        raise ValueError("--pipeline-stages must be >= 1")
    if args.fail_at_call < 1:
        raise ValueError("--fail-at-call must be >= 1")
    if args.kill_after_tasks < 1:
        raise ValueError("--kill-after-tasks must be >= 1")
    if args.kill_node_after_tasks < 1:
        raise ValueError("--kill-node-after-tasks must be >= 1")

    if args.pipeline_stages > 1 and args.repeat_map_batches == "repeat":
        raise ValueError(
            "--pipeline-stages > 1 can't be combined with --repeat-map-batches repeat"
        )

    if args.pipeline_stages > 1 and not (
        args.api == "map_batches" and args.compute == "actors"
    ):
        raise ValueError("--pipeline-stages > 1 requires --api map_batches --compute actors")

    fault_modes = [
        bool(args.fail_actor_index_list),
        bool(args.kill_actor_index_list),
        args.kill_node,
    ]
    if sum(fault_modes) > 1:
        raise ValueError(
            "Use only one fault mode at a time: transient actor failures, actor kills, "
            "or node kills"
        )

    if requires_actor_fault_injection(args) and not (
        args.api == "map_batches" and args.compute == "actors"
    ):
        raise ValueError(
            "Fault injection requires --api map_batches --compute actors"
        )


def parse_actor_indices(value: str) -> List[int]:
    if not value:
        return []
    actor_indices = []
    for raw_value in value.split(","):
        raw_value = raw_value.strip()
        if not raw_value:
            continue
        actor_index = int(raw_value)
        if actor_index < 0:
            raise ValueError("Actor indices must be >= 0")
        actor_indices.append(actor_index)
    return sorted(set(actor_indices))


def requires_actor_fault_injection(args: argparse.Namespace) -> bool:
    return bool(
        args.fail_actor_index_list or args.kill_actor_index_list or args.kill_node
    )


def get_actor_stage_count(args: argparse.Namespace) -> int:
    if args.api != "map_batches" or args.compute != "actors":
        return 0
    if args.pipeline_stages > 1:
        return args.pipeline_stages
    if args.repeat_map_batches == "repeat":
        return 2
    return 1


def initialize_result(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        "time": 0.0,
        "time_stddev": 0.0,
        "time_runs": [],
        "num_runs": args.num_runs,
        "num_runs_completed": 0,
        "runs": [],
        "actor_pool_impl": get_actor_pool_impl(),
        "use_core_actor_pool": get_actor_pool_impl() == "core",
        "args": vars(args).copy(),
    }


def finalize_result(result: Dict[str, Any]) -> None:
    time_runs = result["time_runs"]
    if time_runs:
        result["time"] = statistics.mean(time_runs)
        result["time_stddev"] = (
            statistics.stdev(time_runs) if len(time_runs) > 1 else 0.0
        )
    else:
        result["time"] = 0.0
        result["time_stddev"] = 0.0


def get_actor_pool_impl() -> str:
    return "core" if os.environ.get(USE_CORE_ACTOR_POOL_ENV_VAR) == "1" else "data"


def run_single_benchmark(args: argparse.Namespace, run_index: int) -> Dict[str, Any]:
    run_result = {
        "run": run_index + 1,
        "status": "ok",
        "time": 0.0,
    }
    path = build_input_paths(args)

    use_actors = args.api == "map_batches" and args.compute == "actors"
    actor_stage_count = get_actor_stage_count(args)
    coordinator = None
    node_killer = None
    context = None
    original_actor_task_retry_on_errors = None

    try:
        if use_actors:
            # 1GB dummy model simulates real inference workloads where
            # models are passed to actors via the object store.
            dummy_model = numpy.zeros(MODEL_SIZE, dtype=numpy.int8)
            model_ref = ray.put(dummy_model)
            if requires_actor_fault_injection(args):
                coordinator, node_killer = create_fault_injection_helpers(
                    args, actor_stage_count
                )
        else:
            model_ref = None

        if use_actors and args.fail_actor_index_list:
            context = DataContext.get_current()
            original_actor_task_retry_on_errors = context.actor_task_retry_on_errors
            context.actor_task_retry_on_errors = [TransientBenchmarkError]

        start_time = time.perf_counter()
        ds = build_dataset(args, path, model_ref=model_ref, coordinator=coordinator)
        for _ in ds.iter_internal_ref_bundles():
            pass
        run_result["time"] = time.perf_counter() - start_time

        if coordinator is not None:
            run_result["fault_summary"] = ray.get(coordinator.summary.remote())
            validate_fault_summary(args, run_result["fault_summary"])
        return run_result
    except Exception as exc:
        run_result["status"] = "error"
        run_result["time"] = time.perf_counter() - start_time if "start_time" in locals() else 0.0
        run_result["error"] = format_exception(exc)
        if coordinator is not None:
            run_result["fault_summary"] = ray.get(coordinator.summary.remote())
        raise SingleRunError(run_result) from exc
    finally:
        if context is not None:
            context.actor_task_retry_on_errors = original_actor_task_retry_on_errors
        if coordinator is not None:
            ray.kill(coordinator, no_restart=True)
        if node_killer is not None:
            ray.kill(node_killer, no_restart=True)


def build_input_paths(args: argparse.Namespace) -> List[str]:
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{args.sf}/lineitem"
    return [path] * args.repeat_inputs


def build_dataset(
    args: argparse.Namespace,
    path: List[str],
    *,
    model_ref: Optional[ray.ObjectRef],
    coordinator: Optional[ray.actor.ActorHandle],
):
    ds = ray.data.read_parquet(path)

    if args.api == "map":
        ds = ds.map(increment_row)
    elif args.api == "flat_map":
        ds = ds.flat_map(flat_increment_row)
    elif args.api == "map_batches":
        if args.compute == "actors":
            actor_stage_count = get_actor_stage_count(args)
            for stage_idx in range(actor_stage_count):
                ds = apply_actor_stage(
                    ds,
                    args,
                    model_ref=model_ref,
                    coordinator=coordinator,
                    stage_idx=stage_idx,
                )
                if args.pipeline_stages > 1 and stage_idx < actor_stage_count - 1:
                    # Materialization barrier: forces stage outputs to fully
                    # materialize before the next stage, enabling cascading
                    # reconstruction testing across multiple actor pools.
                    ds = ds.random_shuffle()
        else:
            ds = apply_task_stage(ds, args)
            if args.repeat_map_batches == "repeat":
                ds = apply_task_stage(ds, args)

    ds = ds.map_batches(dummy_write)
    return ds


def apply_task_stage(ds, args: argparse.Namespace):
    return ds.map_batches(
        functools.partial(
            increment_batch,
            map_batches_sleep_ms=args.map_batches_sleep_ms,
        ),
        batch_format=args.batch_format,
        batch_size=args.batch_size,
    )


def apply_actor_stage(
    ds,
    args: argparse.Namespace,
    *,
    model_ref: ray.ObjectRef,
    coordinator: Optional[ray.actor.ActorHandle],
    stage_idx: int,
):
    # IncrementBatch for perf tests (no coordination overhead).
    # FaultInjectableIncrementBatch for fault tests (calls coordinator per batch).
    actor_cls = IncrementBatch
    constructor_args: List[Any] = [model_ref, args.map_batches_sleep_ms]
    if coordinator is not None:
        actor_cls = FaultInjectableIncrementBatch
        constructor_args.extend([coordinator, stage_idx])

    return ds.map_batches(
        actor_cls,
        fn_constructor_args=constructor_args,
        batch_format=args.batch_format,
        batch_size=args.batch_size,
        concurrency=tuple(args.concurrency),
    )


def create_fault_injection_helpers(
    args: argparse.Namespace, actor_stage_count: int
) -> Tuple[ray.actor.ActorHandle, Optional[ray.actor.ActorHandle]]:
    ensure_ray_initialized()
    head_node_id = ray.get_runtime_context().get_node_id()
    scheduling_strategy = NodeAffinitySchedulingStrategy(
        node_id=head_node_id, soft=False
    )

    node_killer = None
    if args.kill_node:
        node_killer = EC2InstanceTerminatorWithGracePeriod.options(
            scheduling_strategy=scheduling_strategy
        ).remote(head_node_id, max_to_kill=1)
        ray.get(node_killer.ready.remote())

    # Faults are injected only in the last stage; earlier stages run clean.
    coordinator = FaultInjectionCoordinator.options(
        scheduling_strategy=scheduling_strategy
    ).remote(
        target_stage_idx=actor_stage_count - 1,
        fail_actor_indices=args.fail_actor_index_list,
        fail_at_call=args.fail_at_call,
        kill_actor_indices=args.kill_actor_index_list,
        kill_after_tasks=args.kill_after_tasks,
        kill_node=args.kill_node,
        kill_node_after_tasks=args.kill_node_after_tasks,
        head_node_id=head_node_id,
        node_killer=node_killer,
    )
    return coordinator, node_killer


def ensure_ray_initialized() -> None:
    if not ray.is_initialized():
        ray.init()


def validate_fault_summary(args: argparse.Namespace, summary: Dict[str, Any]) -> None:
    if args.fail_actor_index_list:
        missing_actor_failures = sorted(
            set(args.fail_actor_index_list)
            - set(summary["transient_failures_injected"])
        )
        if missing_actor_failures:
            raise RuntimeError(
                f"Transient failures were not injected for actors: "
                f"{missing_actor_failures}"
            )

    if args.kill_actor_index_list and not summary["actor_kill_triggered"]:
        raise RuntimeError("Actor kill did not trigger during the run")

    if args.kill_node and not summary["node_kill_triggered"]:
        raise RuntimeError("Node kill did not trigger during the run")


def get_alive_worker_node_count() -> int:
    head_node_id = ray.get_runtime_context().get_node_id()
    return sum(
        1
        for node in ray.nodes()
        if node["Alive"] and node["NodeID"] != head_node_id
    )


def wait_for_worker_node_recovery(expected_worker_nodes: int) -> None:
    deadline = time.time() + NODE_RECOVERY_TIMEOUT_S
    while time.time() < deadline:
        if get_alive_worker_node_count() >= expected_worker_nodes:
            return
        time.sleep(NODE_RECOVERY_POLL_INTERVAL_S)

    raise TimeoutError(
        "Timed out waiting for the worker node count to recover after node kill"
    )


def dummy_write(batch):
    return {"num_rows": [len(batch["column00"])]}


def format_exception(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def increment_row(row):
    row["column00"] += 1
    return row


def flat_increment_row(row):
    row["column00"] += 1
    return [row]


def increment_batch(batch, map_batches_sleep_ms=0):
    if map_batches_sleep_ms > 0:
        time.sleep(map_batches_sleep_ms / 1000.0)

    if isinstance(batch, (dict, pd.DataFrame)):
        # Avoid modifying the column in-place (i.e., +=) because NumPy arrays are
        # read-only. See https://github.com/ray-project/ray/issues/369.
        batch["column00"] = batch["column00"] + 1
    elif isinstance(batch, pa.Table):
        column00_incremented = pc.add(batch["column00"], 1)
        batch = batch.set_column(
            batch.column_names.index("column00"), "column00", column00_incremented
        )
    else:
        assert False, f"Invalid batch format: {type(batch)}"
    return batch


class IncrementBatch:
    def __init__(self, model_ref, map_batches_sleep_ms=0):
        self.model = ray.get(model_ref)
        self.map_batches_sleep_ms = map_batches_sleep_ms

    def __call__(self, batch):
        return increment_batch(batch, self.map_batches_sleep_ms)


class FaultInjectableIncrementBatch:
    def __init__(
        self,
        model_ref,
        map_batches_sleep_ms: int,
        coordinator: ray.actor.ActorHandle,
        stage_idx: int,
    ):
        self.model = ray.get(model_ref)
        self.map_batches_sleep_ms = map_batches_sleep_ms
        self.coordinator = coordinator
        self.stage_idx = stage_idx
        self.local_call_count = 0

        runtime_context = ray.get_runtime_context()
        self.actor_id = runtime_context.get_actor_id()
        self.actor_index = ray.get(
            coordinator.register_actor.remote(
                stage_idx,
                self.actor_id,
                runtime_context.current_actor,
                runtime_context.get_node_id(),
            )
        )

    def __call__(self, batch):
        self.local_call_count += 1
        action = ray.get(
            self.coordinator.before_call.remote(
                self.stage_idx, self.actor_id, self.local_call_count
            )
        )
        if action["raise_transient"]:
            raise TransientBenchmarkError(
                f"Injected transient error in actor {self.actor_index} "
                f"at local call {self.local_call_count}"
            )
        return increment_batch(batch, self.map_batches_sleep_ms)


if __name__ == "__main__":
    args = parse_args()
    main(args)
