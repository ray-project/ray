#!/usr/bin/env python
"""The health loop end to end on a laptop: no GPU, no NCCL, no cloud.

Starts a 4-node Ray cluster on this machine (``ray.cluster_utils.Cluster``) and
runs the real Ray Train controller against toy probes, one scenario per REP
path:

  no_config    no ``health_config``: the run is untouched
  faulty       a probe and an evaluator that raise: the run still finishes
  preflight    a pre-flight check rejects a node; workers avoid it
  diagnose     ``health.report()`` shows a stalled rank -> DIAGNOSE pushes a
               stack dump and a host check -> the next poll reads them ->
               REATTEMPT -> the run restarts and finishes
  evict        a node-keyed ``ClusterProbe`` reports a hot node -> EVICT -> the
               run restarts on the other nodes and finishes

Setup, once. The wheel supplies Ray Core; the symlink supplies this checkout's
``ray/train``:

    python -m venv ~/health-venv && source ~/health-venv/bin/activate
    pip install "ray[train] @ https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp312-cp312-macosx_12_0_arm64.whl"
    python python/ray/setup-dev.py -y --allow train

Then, from anywhere but ``/tmp`` (Ray's ``/tmp/ray`` shadows the package):

    python release/train_tests/health/local_e2e.py            # all scenarios
    python release/train_tests/health/local_e2e.py diagnose   # just one
"""
import sys
import tempfile
import time
import traceback
from pathlib import Path

import ray
import ray.train.health as health
from ray.cluster_utils import Cluster
from ray.train import FailureConfig, RunConfig, ScalingConfig, UserCallback
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer

NUM_WORKER_NODES = 3
NUM_WORKERS = 2
RECORDER = "health_local_e2e_recorder"
STORAGE = tempfile.mkdtemp(prefix="health_local_e2e_")


# ----------------------------------------------------------------------
# Shared plumbing
# ----------------------------------------------------------------------
@ray.remote(num_cpus=0)
class Recorder:
    """Where the train function and the controller tell the driver what happened."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.attempts, self.placements, self.decisions = {}, [], []

    def start_attempt(self, rank, node_id):
        self.attempts[rank] = self.attempts.get(rank, 0) + 1
        self.placements.append((time.time(), self.attempts[rank], rank, node_id))
        return self.attempts[rank]

    def decision(self, action, target_nodes):
        self.decisions.append((time.time(), action, target_nodes))

    def get(self):
        return self.placements, self.decisions


def _recorder():
    return ray.get_actor(RECORDER)


class RecordDecisions(UserCallback):
    def after_health_decision(self, run_context, health_decision):
        print(
            f"  [after_health_decision] {health_decision.action.name}: "
            f"{health_decision.reason}"
        )
        _recorder().decision.remote(
            health_decision.action.name, getattr(health_decision, "target_nodes", [])
        )


def train_func(config):
    import ray.train

    rank = ray.train.get_context().get_world_rank()
    node_id = ray.get_runtime_context().get_node_id()
    attempt = ray.get(_recorder().start_attempt.remote(rank, node_id))

    for step in range(config["steps"]):
        if config.get("stall_rank") == rank and attempt == 1 and step == 3:
            time.sleep(3600)  # alive but no longer making progress
        time.sleep(0.5)
        health.report({"step_time_s": 0.5}, step=step)


def fit(name, policies=None, max_failures=0, steps=16):
    trainer = DataParallelTrainer(
        train_func,
        train_loop_config={"steps": steps, **FIT_CONFIG.get(name, {})},
        scaling_config=ScalingConfig(
            num_workers=NUM_WORKERS, resources_per_worker={"CPU": 1, "slot": 1}
        ),
        run_config=RunConfig(
            name=name,
            storage_path=STORAGE,
            health_config=health.HealthConfig(policies=policies) if policies else None,
            callbacks=[RecordDecisions()],
            failure_config=FailureConfig(max_failures=max_failures),
        ),
    )
    trainer.fit()
    return ray.get(_recorder().get.remote())


FIT_CONFIG = {"diagnose": {"stall_rank": 1}}


# ----------------------------------------------------------------------
# Toy probes and evaluators
# ----------------------------------------------------------------------
class Explodes(health.ClusterProbe):
    interval_s = 0.5

    def poll(self, ctx):
        raise RuntimeError("probe bug")


class ExplodingEvaluator(health.Evaluator):
    def evaluate(self, state):
        raise RuntimeError("evaluator bug")


class RejectNode(health.OnDemandProbe):
    """Pre-flight check that fails on one chosen node."""

    def __init__(self, bad_node_id):
        self.bad_node_id = bad_node_id

    def poll(self, ctx):
        ok = ray.get_runtime_context().get_node_id() != self.bad_node_id
        return health.ProbeResult(passed=ok, detail="" if ok else "failed the screen")


class StackProbe(health.OnDemandProbe):
    """Every thread's Python stack, from inside the training worker."""

    scope = health.WORKER_SCOPE
    timeout_s = 10.0

    def poll(self, ctx):
        stacks = "\n".join(
            f"# thread {tid}\n{''.join(traceback.format_stack(frame))}"
            for tid, frame in sys._current_frames().items()
        )
        return health.ProbeResult(
            passed=True,
            detail=f"rank {ctx.rank} stack",
            artifacts=[ctx.upload("stack_traces", {f"rank_{ctx.rank}.log": stacks})],
        )


class HostProbe(health.OnDemandProbe):
    """A node-scoped check that runs outside the training process."""

    timeout_s = 10.0

    def poll(self, ctx):
        return health.ProbeResult(passed=True, detail=f"node {ctx.node_id[:8]} ok")


class StalledRank(health.Evaluator):
    """A rank whose reported step stops advancing: diagnose, then reattempt."""

    def __init__(self, stall_s=4.0):
        self._stall_s = stall_s
        self._last = {}

    def on_worker_group_start(self):
        self._last.clear()

    def evaluate(self, state):
        now = time.monotonic()
        stalled = []
        for rank, worker in state.workers.items():
            step, since = self._last.get(rank, (None, now))
            if worker.step != step:
                self._last[rank] = (worker.step, now)
            elif now - since >= self._stall_s:
                stalled.append(rank)
        if not stalled:
            return []
        reason = f"ranks {stalled} made no progress for {self._stall_s:.0f}s"
        if state.on_demand_probe_results(StackProbe):
            return [
                health.Reattempt(
                    cause=health.Cause.NO_PROGRESS, reason=f"{reason}; stacks captured"
                )
            ]
        return [
            health.Diagnose(
                cause=health.Cause.NO_PROGRESS,
                reason=reason,
                on_demand_probes=[StackProbe(), HostProbe()],
                target_ranks=stalled,
            )
        ]


class NodeTemps(health.ClusterProbe):
    """Node-keyed cluster probe: rank 0's node runs hot on the first attempt."""

    interval_s = 0.5

    def __init__(self):
        self.hot_node = None

    def poll(self, ctx):
        if self.hot_node is None and ctx.rank_to_node:
            self.hot_node = ctx.rank_to_node[0]
        return {
            n: health.ProbeResult(
                metrics={"temp_c": 95.0 if n == self.hot_node else 60.0}
            )
            for n in ctx.node_ids
        }


class EvictHot(health.Evaluator):
    def evaluate(self, state):
        hot = [
            n for n, r in state.results(NodeTemps).items() if r.metrics["temp_c"] > 90
        ]
        if not hot:
            return []
        return [
            health.Evict(
                cause=health.Cause.HARDWARE, reason="GPU over 90C", target_nodes=hot
            )
        ]


# ----------------------------------------------------------------------
# Scenarios. Each returns a list of failed checks.
# ----------------------------------------------------------------------
def _check(failures, ok, what):
    print(f"  [{'ok' if ok else 'FAIL'}] {what}")
    if not ok:
        failures.append(what)


def scenario_no_config(worker_nodes):
    failures = []
    placements, decisions = fit("no_config")
    _check(failures, len(placements) == NUM_WORKERS, "one attempt, all workers ran")
    _check(failures, not decisions, "no health decisions")
    return failures


def scenario_faulty(worker_nodes):
    failures = []
    policy = health.HealthPolicy(
        probe_creator=lambda: [Explodes()],
        evaluator_creator=lambda: [ExplodingEvaluator()],
    )
    placements, decisions = fit("faulty", [policy])
    _check(failures, len(placements) == NUM_WORKERS, "run finished in one attempt")
    _check(failures, not decisions, "no decisions from broken components")
    return failures


def scenario_preflight(worker_nodes):
    failures = []
    bad = worker_nodes[0]
    policy = health.HealthPolicy(
        probe_creator=lambda: [RejectNode(bad)], preflight=True
    )
    placements, decisions = fit("preflight", [policy])
    used = {node for _, _, _, node in placements}
    _check(
        failures,
        [(a, n) for _, a, n in decisions] == [("EVICT", [bad])],
        "pre-flight evicted that node",
    )
    _check(failures, bad not in used, "no worker scheduled on the rejected node")
    _check(failures, len(placements) == NUM_WORKERS, "run finished in one attempt")
    return failures


def scenario_diagnose(worker_nodes):
    failures = []
    policy = health.HealthPolicy(evaluator_creator=lambda: [StalledRank()])
    placements, decisions = fit("diagnose", [policy], max_failures=1, steps=40)
    actions = [a for _, a, _ in decisions]
    _check(failures, actions == ["DIAGNOSE", "REATTEMPT"], f"decisions {actions}")
    stack = Path(
        STORAGE, "diagnose", "health_diagnostics", "stack_traces", "rank_1.log"
    )
    text = stack.read_text() if stack.exists() else ""
    _check(
        failures, "time.sleep(3600)" in text, f"stack dump shows the stall ({stack})"
    )
    _check(
        failures, max(a for _, a, _, _ in placements) == 2, "restarted once, finished"
    )
    return failures


def scenario_evict(worker_nodes):
    failures = []
    policy = health.HealthPolicy(
        probe_creator=lambda: [NodeTemps()], evaluator_creator=lambda: [EvictHot()]
    )
    placements, decisions = fit("evict", [policy], max_failures=1)
    _check(failures, len(decisions) == 1, "one decision")
    evicted_at, action, evicted = decisions[0] if decisions else (0, None, [])
    after = [(rank, node) for t, _, rank, node in placements if t > evicted_at]
    _check(failures, action == "EVICT" and len(evicted) == 1, "EVICT of one node")
    _check(failures, len(after) == NUM_WORKERS, "all workers restarted after it")
    _check(
        failures,
        not set(evicted) & {node for _, node in after},
        "restarted workers avoided the evicted node",
    )
    return failures


SCENARIOS = {
    "no_config": scenario_no_config,
    "faulty": scenario_faulty,
    "preflight": scenario_preflight,
    "diagnose": scenario_diagnose,
    "evict": scenario_evict,
}


def main() -> int:
    chosen = sys.argv[1:] or list(SCENARIOS)
    unknown = set(chosen) - set(SCENARIOS)
    if unknown:
        print(f"unknown scenario(s) {sorted(unknown)}; pick from {list(SCENARIOS)}")
        return 2

    # Workers need a "slot", so they only land on the worker nodes, and one
    # worker node is always spare for pre-flight rejection and eviction.
    cluster = Cluster(initialize_head=True, head_node_args={"num_cpus": 2})
    worker_nodes = [
        cluster.add_node(num_cpus=1, resources={"slot": 1}).node_id
        for _ in range(NUM_WORKER_NODES)
    ]
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    recorder = Recorder.options(name=RECORDER).remote()
    print(f"storage: {STORAGE}")

    failed = {}
    try:
        for name in chosen:
            print(f"\n=== {name} ===")
            ray.get(recorder.reset.remote())
            started = time.monotonic()
            try:
                failures = SCENARIOS[name](worker_nodes)
            except Exception as e:  # noqa: BLE001
                failures = [f"raised {type(e).__name__}: {e}"]
                traceback.print_exc()
            print(
                f"  {'PASS' if not failures else 'FAIL'} "
                f"({time.monotonic() - started:.0f}s)"
            )
            if failures:
                failed[name] = failures
    finally:
        ray.shutdown()
        cluster.shutdown()

    print(f"\n{len(chosen) - len(failed)}/{len(chosen)} scenarios passed")
    for name, failures in failed.items():
        print(f"  {name}: {'; '.join(failures)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
