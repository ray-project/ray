import sys
import time
from pathlib import Path

import pytest

import ray
import ray.train
from ray.cluster_utils import Cluster
from ray.train.health import (
    Diagnose,
    Evaluator,
    Evict,
    HealthCheck,
    HealthConfig,
    NodeProbe,
    Noop,
    PeriodicHealthCheck,
    ProbeResult,
    Reattempt,
    WorkerProbe,
)
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2.api.callback import UserCallback
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.api.exceptions import HealthCheckError


@pytest.fixture
def cluster():
    cluster = Cluster(initialize_head=True, head_node_args=dict(num_cpus=0))
    for _ in range(2):
        cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    ray.init(
        address=cluster.address,
        runtime_env={
            "working_dir": str(Path(__file__).parent),
            "env_vars": {HEALTH_CHECK_INTERVAL_S_ENV_VAR: "0.5"},
        },
    )
    yield cluster
    ray.shutdown()
    cluster.shutdown()


@ray.remote(num_cpus=0)
class Scenario:
    """The flags the probes read, and what happened in the run."""

    def __init__(self):
        self.flags = {}
        self.events = []
        self.node_ids = []
        self.preflight_runs = 0

    def set(self, **flags):
        self.flags.update(flags)

    def get(self, flag):
        return self.flags.get(flag)

    def record(self, event):
        self.events.append(event)

    def get_events(self):
        return self.events

    def start_attempt(self, node_id):
        self.node_ids.append(node_id)
        return len(self.node_ids) - 1

    def get_node_ids(self):
        return self.node_ids

    def start_preflight(self):
        self.preflight_runs += 1
        return self.preflight_runs - 1


def _scenario():
    return ray.get_actor("scenario")


class FlagProbe(NodeProbe):
    """Reports an issue while its flag is set."""

    flag = None

    def poll(self) -> ProbeResult:
        issue = ray.get(_scenario().get.remote(self.flag))
        return ProbeResult(metrics={"issue": float(bool(issue))})


class ProbeA(FlagProbe):
    flag = "issue_a"


class ProbeB(FlagProbe):
    flag = "issue_b"


class ProbeC(FlagProbe):
    flag = "issue_c"


class DiagnosisProbe(NodeProbe):
    """Reports whether its node is the bad node."""

    def poll(self) -> ProbeResult:
        bad_node = ray.get(_scenario().get.remote("bad_node"))
        is_bad = bad_node == ray.get_runtime_context().get_node_id()
        return ProbeResult(metrics={"bad": float(is_bad)})


class RankProbe(WorkerProbe):
    """Answers on each worker it runs on."""

    def poll(self) -> ProbeResult:
        return ProbeResult()


class EvictBadNodes(Evaluator):
    def evaluate(self, state):
        results = state.probe_results.get("DiagnosisProbe", {})
        bad_nodes = [node for node, r in results.items() if r.metrics.get("bad")]
        decision = Noop()
        if bad_nodes:
            decision = Evict(reason="bad node", target_nodes=bad_nodes)
        ranks = sorted(state.probe_results.get("RankProbe", {}))
        event = ("diagnosis", type(decision).__name__, ranks)
        ray.get(_scenario().record.remote(event))
        return decision


class DiagnoseOnIssue(Evaluator):
    """Diagnoses when its probe, and the workers' health reports if
    ``report_metric`` is set, start to report an issue."""

    def __init__(self, probe_name, report_metric=None, requires_idle_workers=False):
        self._probe_name = probe_name
        self._report_metric = report_metric
        self._requires_idle_workers = requires_idle_workers
        self._had_issue = False

    def evaluate(self, state):
        results = state.probe_results.get(self._probe_name, {}).values()
        has_issue = any(r.metrics.get("issue") for r in results)
        if self._report_metric:
            reports = state.health_reports.values()
            has_issue &= any(r.metrics.get(self._report_metric) for r in reports)
        started = has_issue and not self._had_issue
        self._had_issue = has_issue
        if not started:
            return Noop()
        return Diagnose(
            reason=f"{self._probe_name} reports an issue",
            checks=[
                HealthCheck(
                    probe_creator=lambda: [DiagnosisProbe(), RankProbe()],
                    evaluator_creator=lambda: [EvictBadNodes()],
                    requires_idle_workers=self._requires_idle_workers,
                )
            ],
        )


class RecordHealthDecisions(UserCallback):
    def after_health_decision(self, run_context, health_decision):
        _scenario().record.remote(("decision", type(health_decision).__name__))


def _wait_for_event(scenario, event, timeout_s=60):
    deadline = time.monotonic() + timeout_s
    while event not in ray.get(scenario.get_events.remote()):
        if time.monotonic() > deadline:
            raise TimeoutError(f"No {event} in {timeout_s}s.")
        time.sleep(0.1)


def diagnose_train_fn():
    scenario = _scenario()
    node_id = ray.get_runtime_context().get_node_id()
    if ray.get(scenario.start_attempt.remote(node_id)) > 0:
        # The attempt after the eviction trains normally.
        ray.get(scenario.set.remote(issue_b=False))
        time.sleep(1)
        return

    time.sleep(1)
    # Probe A and the training step report an issue, and their diagnosis finds
    # nothing to act on.
    ray.train.health.report({"slow_step": 1.0})
    ray.get(scenario.set.remote(issue_a=True))
    _wait_for_event(scenario, ("diagnosis", "Noop", [0]))
    ray.get(scenario.set.remote(issue_a=False))
    ray.train.health.report({"slow_step": 0.0})
    time.sleep(1)
    # Probe B reports an issue, and its diagnosis finds this node bad.
    ray.get(scenario.set.remote(issue_b=True, bad_node=node_id))
    time.sleep(60)


def test_diagnose_keeps_training_or_evicts(cluster):
    scenario = Scenario.options(name="scenario").remote()
    trainer = DataParallelTrainer(
        diagnose_train_fn,
        scaling_config=ray.train.ScalingConfig(num_workers=1),
        run_config=ray.train.RunConfig(
            failure_config=ray.train.FailureConfig(max_failures=0),
            callbacks=[RecordHealthDecisions()],
            health_config=HealthConfig(
                inflight_checks=[
                    PeriodicHealthCheck(
                        probe_creator=lambda: [ProbeA(), ProbeB()],
                        evaluator_creator=lambda: [
                            DiagnoseOnIssue("ProbeA", report_metric="slow_step"),
                            DiagnoseOnIssue("ProbeB"),
                        ],
                        poll_interval_s=0.1,
                    )
                ]
            ),
        ),
    )
    trainer.fit()

    assert ray.get(scenario.get_events.remote()) == [
        ("decision", "Diagnose"),
        ("diagnosis", "Noop", [0]),
        ("decision", "Diagnose"),
        ("diagnosis", "Evict", [0]),
        ("decision", "Evict"),
    ]
    # The eviction restarted the worker group.
    assert len(ray.get(scenario.get_node_ids.remote())) == 2


def idle_workers_diagnosis_train_fn():
    scenario = _scenario()
    node_id = ray.get_runtime_context().get_node_id()
    attempt = ray.get(scenario.start_attempt.remote(node_id))
    ray.get(scenario.record.remote(("attempt", attempt)))
    if attempt == 0:
        # Probe C reports an issue, and its diagnosis requires idle workers.
        ray.get(scenario.set.remote(issue_c=True))
        time.sleep(60)


def test_a_diagnose_that_requires_idle_workers_runs_before_training_resumes(cluster):
    scenario = Scenario.options(name="scenario").remote()
    trainer = DataParallelTrainer(
        idle_workers_diagnosis_train_fn,
        scaling_config=ray.train.ScalingConfig(num_workers=1),
        run_config=ray.train.RunConfig(
            failure_config=ray.train.FailureConfig(max_failures=0),
            callbacks=[RecordHealthDecisions()],
            health_config=HealthConfig(
                inflight_checks=[
                    PeriodicHealthCheck(
                        probe_creator=lambda: [ProbeC()],
                        evaluator_creator=lambda: [
                            DiagnoseOnIssue("ProbeC", requires_idle_workers=True)
                        ],
                        poll_interval_s=0.1,
                    )
                ]
            ),
        ),
    )
    trainer.fit()

    assert ray.get(scenario.get_events.remote()) == [
        ("attempt", 0),
        ("decision", "Diagnose"),
        ("diagnosis", "Noop", [0]),
        ("attempt", 1),
    ]


class PreflightProbe(NodeProbe):
    """Reports which pre-flight run this is."""

    def poll(self) -> ProbeResult:
        run = ray.get(_scenario().start_preflight.remote())
        return ProbeResult(metrics={"run": float(run)})


class EvictOnTheFirstPreflight(Evaluator):
    def evaluate(self, state):
        results = state.probe_results.get("PreflightProbe", {})
        nodes = [node for node, r in results.items() if r.metrics.get("run") == 0]
        return Evict(reason="first pre-flight", target_nodes=nodes) if nodes else Noop()


def preflight_train_fn():
    node_id = ray.get_runtime_context().get_node_id()
    ray.get(_scenario().start_attempt.remote(node_id))


def test_preflight_checks_can_evict_before_training_starts(cluster):
    scenario = Scenario.options(name="scenario").remote()
    trainer = DataParallelTrainer(
        preflight_train_fn,
        scaling_config=ray.train.ScalingConfig(num_workers=1),
        run_config=ray.train.RunConfig(
            failure_config=ray.train.FailureConfig(max_failures=0),
            callbacks=[RecordHealthDecisions()],
            health_config=HealthConfig(
                preflight_checks=[
                    HealthCheck(
                        probe_creator=lambda: [PreflightProbe()],
                        evaluator_creator=lambda: [EvictOnTheFirstPreflight()],
                        requires_idle_workers=True,
                    )
                ]
            ),
        ),
    )
    trainer.fit()

    # The first attempt's pre-flight checks evicted its node before training
    # started, and the second attempt's found nothing to act on.
    assert ray.get(scenario.get_events.remote()) == [("decision", "Evict")]
    assert len(ray.get(scenario.get_node_ids.remote())) == 1


class AllReduceProbe(WorkerProbe):
    """All-reduces a one on every worker."""

    def poll(self) -> ProbeResult:
        import torch
        import torch.distributed as dist

        ones = torch.ones(1)
        dist.all_reduce(ones)
        return ProbeResult(metrics={"sum": ones.item()})


class RecordAllReduce(Evaluator):
    def evaluate(self, state):
        results = state.probe_results.get("AllReduceProbe", {}).values()
        sums = sorted(r.metrics.get("sum") for r in results)
        ray.get(_scenario().record.remote(("all-reduce", sums)))
        return Noop()


class SpeedProbe(NodeProbe):
    """Reports whether its node is the slow node."""

    def poll(self) -> ProbeResult:
        slow_node = ray.get(_scenario().get.remote("slow_node"))
        is_slow = slow_node == ray.get_runtime_context().get_node_id()
        return ProbeResult(metrics={"slow": float(is_slow)})


class TempProbe(NodeProbe):
    """Reports its node's temperature: the hot node is hotter."""

    def poll(self) -> ProbeResult:
        hot_node = ray.get(_scenario().get.remote("hot_node"))
        is_hot = hot_node == ray.get_runtime_context().get_node_id()
        return ProbeResult(metrics={"temp_c": 90.0 if is_hot else 60.0})


class ReattemptOnAHotterNode(Evaluator):
    def evaluate(self, state):
        temps = [r.metrics["temp_c"] for r in state.probe_results["TempProbe"].values()]
        if max(temps) - min(temps) > 10:
            return Reattempt(reason="a node is hotter than the others")
        return Noop()


class DiagnoseASlowNode(Evaluator):
    """Diagnoses the nodes' temperatures when a node starts to be slow."""

    def __init__(self):
        self._had_slow_node = False

    def evaluate(self, state):
        results = state.probe_results.get("SpeedProbe", {}).values()
        has_slow_node = any(r.metrics.get("slow") for r in results)
        started = has_slow_node and not self._had_slow_node
        self._had_slow_node = has_slow_node
        if not started:
            return Noop()
        return Diagnose(
            reason="a node is slower than the others",
            checks=[
                HealthCheck(
                    probe_creator=lambda: [TempProbe()],
                    evaluator_creator=lambda: [ReattemptOnAHotterNode()],
                )
            ],
        )


def slow_and_hot_train_fn():
    if ray.train.get_context().get_world_rank() == 0:
        node_id = ray.get_runtime_context().get_node_id()
        ray.get(_scenario().set.remote(slow_node=node_id, hot_node=node_id))
    time.sleep(60)


def test_preflight_all_reduce_then_a_diagnosis_that_reattempts(cluster):
    """Two workers on two nodes: the pre-flight all-reduce passes and training
    starts; a slow node is diagnosed as hotter, which reattempts, and with
    max_failures=0 the run fails."""
    scenario = Scenario.options(name="scenario").remote()
    trainer = TorchTrainer(
        slow_and_hot_train_fn,
        scaling_config=ray.train.ScalingConfig(num_workers=2),
        run_config=ray.train.RunConfig(
            failure_config=ray.train.FailureConfig(max_failures=0),
            callbacks=[RecordHealthDecisions()],
            health_config=HealthConfig(
                preflight_checks=[
                    HealthCheck(
                        probe_creator=lambda: [AllReduceProbe()],
                        evaluator_creator=lambda: [RecordAllReduce()],
                        requires_idle_workers=True,
                    )
                ],
                inflight_checks=[
                    PeriodicHealthCheck(
                        probe_creator=lambda: [SpeedProbe()],
                        evaluator_creator=lambda: [DiagnoseASlowNode()],
                        poll_interval_s=0.1,
                    )
                ],
            ),
        ),
    )
    with pytest.raises(HealthCheckError) as error:
        trainer.fit()

    assert isinstance(error.value.decision, Reattempt)
    assert ray.get(scenario.get_events.remote()) == [
        ("all-reduce", [2.0, 2.0]),
        ("decision", "Diagnose"),
        ("decision", "Reattempt"),
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
