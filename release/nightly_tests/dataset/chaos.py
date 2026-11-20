"""Node chaos for Ray Data release tests.

A test script adds the flags with `add_chaos_args`, starts Ray with
`chaos_init_kwargs`, then calls `start_chaos` right before the pipeline runs and
`stop_chaos` at the end of the benchmarked function.
"""

import argparse
import asyncio
import random
import time
from typing import Any, Dict, Optional

import ray
import ray._private.services as services
from ray._common.network_utils import build_address
from ray._private.test_utils import (
    NodeKillerBase,
    _execute_command_on_node,
    _shutdown_raylet,
)
from ray.job_config import JobConfig
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

RAYLET_KILLER = "raylet"
EC2_GRACE_PERIOD_KILLER = "ec2_grace_period"


def add_chaos_args(
    parser: argparse.ArgumentParser,
    *,
    min_alive_workers: int = 1,
) -> None:
    parser.add_argument(
        "--chaos",
        action="store_true",
        help="Kill worker nodes while the pipeline runs.",
    )
    parser.add_argument(
        "--chaos-killer",
        choices=[RAYLET_KILLER, EC2_GRACE_PERIOD_KILLER],
        default=RAYLET_KILLER,
        help=(
            f"'{RAYLET_KILLER}' kills the raylet, so the node's objects are lost at "
            f"once. '{EC2_GRACE_PERIOD_KILLER}' drains the node, then stops Ray on "
            "it after a grace period."
        ),
    )
    parser.add_argument(
        "--chaos-kill-delay-s",
        type=float,
        default=60,
        help="Seconds to wait before the first kill.",
    )
    parser.add_argument(
        "--chaos-kill-interval-s",
        type=float,
        default=60,
        help="Seconds between kill rounds. Each round lands at a random point in it.",
    )
    parser.add_argument(
        "--chaos-kill-batch-size",
        type=int,
        default=2,
        help="Worker nodes killed together in each round.",
    )
    parser.add_argument(
        "--chaos-max-kills",
        type=int,
        default=None,
        help="Stop after this many nodes are killed. Unlimited by default.",
    )
    parser.add_argument(
        "--chaos-min-alive-workers",
        type=int,
        default=min_alive_workers,
        help=(
            "Never kill a node if fewer than this many worker nodes would stay "
            "alive. Chaos pauses until replacement nodes join."
        ),
    )
    parser.add_argument(
        "--lineage-reconstruction",
        action="store_true",
        help=(
            "Start the driver with "
            "JobConfig(_disable_job_level_lineage_reconstruction=True), so Ray Data "
            "lineage reconstruction rebuilds lost objects instead of Ray Core. With "
            f"--chaos and --chaos-killer={RAYLET_KILLER}, fail the run if lineage "
            "reconstruction never ran."
        ),
    )


def chaos_init_kwargs(args: argparse.Namespace) -> Dict[str, Any]:
    """Return the `ray.init` kwargs that `--lineage-reconstruction` needs."""
    if not args.lineage_reconstruction:
        return {}
    # Ray Data lineage reconstruction is gated on this job config. It also turns
    # Ray Core lineage reconstruction off for the job.
    return {"job_config": JobConfig(_disable_job_level_lineage_reconstruction=True)}


class _FlooredNodeKillerMixin:
    """Picks random worker nodes, but keeps at least `min_alive_workers` alive.

    `NodeKillerBase` only keeps one worker alive, so with unlimited kills a cluster
    that replaces nodes slowly would shrink to a single node. The floor bounds how
    much capacity chaos can take at once. Kills resume as replacement nodes join.
    """

    def __init__(self, *args, min_alive_workers: int, **kwargs):
        super().__init__(*args, **kwargs)
        self._min_alive_workers = min_alive_workers

    async def _find_resources_to_kill(self):
        while self.is_running:
            # A killed node can still read as alive until the GCS notices, so the
            # killed set is excluded as well.
            alive_workers = [
                (n["NodeID"], n["NodeManagerAddress"], n["NodeManagerPort"])
                for n in ray.nodes()
                if n["Alive"] and n["NodeID"] != self.head_node_id
            ]
            alive_workers = [w for w in alive_workers if w not in self.killed]
            if len(alive_workers) - self.batch_size_to_kill >= self._min_alive_workers:
                return random.sample(alive_workers, self.batch_size_to_kill)
            await asyncio.sleep(5)
        return []

    def num_killed(self) -> int:
        return len(self.killed)


@ray.remote(num_cpus=0)
class FlooredRayletKiller(_FlooredNodeKillerMixin, NodeKillerBase):
    """Kills worker raylets, so the objects in their object stores are lost."""

    async def _kill_resource(self, node_id: str, ip: str, port: int) -> bool:
        if port is None:
            return False
        _shutdown_raylet(build_address(ip, port), graceful=False)
        return True


@ray.remote(num_cpus=0)
class FlooredEC2InstanceTerminatorWithGracePeriod(
    _FlooredNodeKillerMixin, NodeKillerBase
):
    """Drains worker nodes, then stops Ray on them after a grace period.

    Same kill as `EC2InstanceTerminatorWithGracePeriod` in
    `ray._private.test_utils`, with the alive-worker floor.
    """

    def __init__(self, *args, grace_period_s: float = 30, **kwargs):
        super().__init__(*args, **kwargs)
        self._grace_period_s = grace_period_s

    async def _kill_resource(self, node_id: str, ip: str, port: int) -> bool:
        if ip is None:
            return False
        self._drain_node(node_id)
        await asyncio.sleep(self._grace_period_s)
        # Anyscale extends the drain deadline if you shut down the instance
        # directly. Force-stopping Ray makes Anyscale terminate it shortly after
        # without updating the deadline.
        _execute_command_on_node("ray stop --force", ip)
        return True

    def _drain_node(self, node_id: str) -> None:
        # Imported lazily, or Ray can't serialize the class.
        from ray.core.generated import autoscaler_pb2

        gcs_client = ray._raylet.GcsClient(
            address=services.canonicalize_bootstrap_address_or_die(addr="auto")
        )
        deadline_timestamp_ms = (time.time_ns() // 1e6) + (self._grace_period_s * 1e3)
        is_accepted, _ = gcs_client.drain_node(
            node_id,
            autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
            "",
            deadline_timestamp_ms,
        )
        assert is_accepted, f"Drain request for node {node_id} was rejected"


_KILLER_CLASSES = {
    RAYLET_KILLER: FlooredRayletKiller,
    EC2_GRACE_PERIOD_KILLER: FlooredEC2InstanceTerminatorWithGracePeriod,
}


def start_chaos(args: argparse.Namespace):
    """Start killing nodes. Returns the killer actor, or None without `--chaos`."""
    if not args.chaos:
        return None
    assert ray.is_initialized()

    head_node_id = ray.get_runtime_context().get_node_id()
    resource_killer = (
        _KILLER_CLASSES[args.chaos_killer]
        .options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=head_node_id, soft=False
            )
        )
        .remote(
            head_node_id,
            kill_interval_s=args.chaos_kill_interval_s,
            kill_delay_s=args.chaos_kill_delay_s,
            max_to_kill=args.chaos_max_kills,
            batch_size_to_kill=args.chaos_kill_batch_size,
            min_alive_workers=args.chaos_min_alive_workers,
        )
    )
    ray.get(resource_killer.ready.remote())
    resource_killer.run.remote()
    return resource_killer


def stop_chaos(resource_killer, timeout_s: float = 300) -> None:
    """Stop killing nodes, then wait until the GCS marks every killed node dead.

    Call this inside the benchmarked function: `Benchmark.run_fn` queries every
    raylet for spill stats once that function returns, and the query fails on a
    node that was just killed but isn't marked dead yet.
    """
    if resource_killer is None:
        return
    ray.get(resource_killer.stop_run.remote())
    # Waits for the kill loop to exit, so a kill in flight during `stop_run` is
    # included.
    killed = {
        node_id for node_id, _, _ in ray.get(resource_killer.get_killed_nodes.remote())
    }
    deadline = time.monotonic() + timeout_s
    while True:
        still_alive = {n["NodeID"] for n in ray.nodes() if n["Alive"]} & killed
        if not still_alive:
            return
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"{len(still_alive)} killed nodes still read as alive after "
                f"{timeout_s}s: {sorted(still_alive)}"
            )
        time.sleep(5)


class LineageReconstructionCounter:
    """Counts Ray Data lineage reconstructions for this run.

    Wraps ``LineageTracker.register_task_failed``, which the executor calls once
    per detected loss, so the run can assert lineage reconstruction actually ran
    rather than passing because chaos never lost a needed object. The streaming
    executor runs in the driver process, so patching the class here observes its
    tracker.

    Counts only calls that return seed ids. An empty list means reconstruction of
    that lineage was already under way and nothing was resubmitted.
    """

    def __init__(self):
        from ray.data._internal.execution.lineage_tracker import LineageTracker

        self.reconstructions = 0
        original = LineageTracker.register_task_failed

        def counting_register_task_failed(tracker, *args, **kwargs):
            seed_task_ids, plan_id = original(tracker, *args, **kwargs)
            if seed_task_ids:
                self.reconstructions += 1
            return seed_task_ids, plan_id

        LineageTracker.register_task_failed = counting_register_task_failed


def install_lineage_reconstruction_counter(
    args: argparse.Namespace,
) -> Optional[LineageReconstructionCounter]:
    """Return a counter when the run should exercise lineage reconstruction."""
    if not (args.chaos and args.lineage_reconstruction):
        return None
    assert ray.data.DataContext.get_current().enable_ray_data_reconstruction, (
        "--lineage-reconstruction is set, but Ray Data lineage reconstruction is "
        "off. Check that the driver started with "
        "JobConfig(_disable_job_level_lineage_reconstruction=True)."
    )
    return LineageReconstructionCounter()


def record_chaos_results(
    result: Dict[str, Any],
    resource_killer,
    counter: Optional[LineageReconstructionCounter],
) -> None:
    """Add the chaos counts to a benchmark result before it's written."""
    if resource_killer is not None:
        nodes_killed = ray.get(resource_killer.num_killed.remote())
        print(f"Chaos killed {nodes_killed} worker nodes")
        result["nodes_killed"] = nodes_killed
    if counter is not None:
        result["lineage_reconstructions"] = counter.reconstructions


def check_chaos_results(
    args: argparse.Namespace,
    resource_killer,
    counter: Optional[LineageReconstructionCounter],
) -> None:
    """Fail a run that didn't exercise what it was meant to.

    Call after the result is written, so the counts are reported on failure.
    """
    if resource_killer is not None:
        assert (
            ray.get(resource_killer.num_killed.remote()) > 0
        ), "Chaos was enabled but no node was killed."
    # The run completing proves the dataset survived chaos; this asserts it
    # survived *because of* lineage reconstruction. A drained node may hand its
    # objects off before it stops, so only a raylet kill is sure to lose them.
    if counter is not None and args.chaos_killer == RAYLET_KILLER:
        assert counter.reconstructions > 0, (
            "Chaos was enabled but Ray Data lineage reconstruction never ran, so "
            "this run did not exercise lineage reconstruction."
        )
