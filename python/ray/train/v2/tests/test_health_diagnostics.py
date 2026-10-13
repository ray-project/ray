"""OnDemandRunner: where pushed probes run, and what happens when they fail."""
import sys

import pytest

from ray.train.health import (
    NODE_SCOPE,
    WORKER_SCOPE,
    Diagnose,
    OnDemandProbe,
    ProbeResult,
)
from ray.train.health._internal.on_demand import OnDemandRunner

# Two nodes, two ranks each.
RANK_TO_NODE = {0: "nA", 1: "nA", 2: "nB", 3: "nB"}


class WorkerCheck(OnDemandProbe):
    scope = WORKER_SCOPE

    def poll(self, ctx):
        artifacts = [ctx.upload("worker", {f"rank_{ctx.rank}.log": "x"})]
        return ProbeResult(detail=f"rank {ctx.rank}", artifacts=artifacts)


class NodeCheck(OnDemandProbe):
    scope = NODE_SCOPE

    def poll(self, ctx):
        return ProbeResult(detail=f"node {ctx.node_id}", passed=True)


class NeedsGpu(NodeCheck):
    stop_workers = True


def _direct(probe, contexts):
    return {ctx.entity_id: probe.poll(ctx) for ctx in contexts}


def _runner(**kwargs):
    uploads = {}

    def upload(tool, files):
        uploads.setdefault(tool, {}).update(files)
        return f"s3://run/health_diagnostics/{tool}"

    kwargs.setdefault("run_on_node", _direct)
    kwargs.setdefault("run_on_worker", _direct)
    return OnDemandRunner(upload=upload, **kwargs), uploads


# ----------------------------------------------------------------------
# Diagnose: targeting
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "probe, decision, expected",
    [
        (WorkerCheck(), Diagnose(target_ranks=[2, 3]), ["2", "3"]),
        # Two ranks on nB is one check on nB.
        (NodeCheck(), Diagnose(target_ranks=[2, 3]), ["nB"]),
        (NodeCheck(), Diagnose(target_nodes=["nA"]), ["nA"]),
        (WorkerCheck(), Diagnose(), ["0", "1", "2", "3"]),
        (NodeCheck(), Diagnose(), ["nA", "nB"]),
    ],
)
def test_diagnose_targets(probe, decision, expected):
    runner, _ = _runner()
    decision.on_demand_probes = [probe]
    results = runner.diagnose(decision, RANK_TO_NODE)
    assert sorted(results[probe.probe_name()]) == expected


def test_a_failed_or_missing_result_is_recorded_as_a_failure():
    def partial(probe, contexts):
        return {"2": RuntimeError("py-spy segfaulted")}

    runner, _ = _runner(run_on_worker=partial)
    decision = Diagnose(on_demand_probes=[WorkerCheck()], target_ranks=[2, 3])
    results = runner.diagnose(decision, RANK_TO_NODE)["WorkerCheck"]
    assert results["2"].passed is False and "segfaulted" in results["2"].detail
    assert results["3"].passed is False and "no result" in results["3"].detail


def test_a_raising_dispatch_fails_every_target():
    def boom(probe, contexts):
        raise RuntimeError("scheduling failed")

    runner, _ = _runner(run_on_node=boom)
    results = runner.diagnose(Diagnose(on_demand_probes=[NodeCheck()]), RANK_TO_NODE)
    assert all(r.passed is False for r in results["NodeCheck"].values())


def test_bulk_output_goes_to_storage():
    runner, uploads = _runner()
    decision = Diagnose(on_demand_probes=[WorkerCheck()], target_ranks=[2])
    result = runner.diagnose(decision, RANK_TO_NODE)["WorkerCheck"]["2"]
    assert "rank_2.log" in uploads["worker"]
    assert result.artifacts == ["s3://run/health_diagnostics/worker"]


def test_workers_pause_once_for_a_batch_that_needs_the_gpu():
    calls = []
    runner, _ = _runner(
        pause_workers=lambda: calls.append("pause"),
        resume_workers=lambda: calls.append("resume"),
    )
    decision = Diagnose(on_demand_probes=[NeedsGpu(), NodeCheck(), NeedsGpu()])
    runner.diagnose(decision, RANK_TO_NODE)
    assert calls == ["pause", "resume"]


# ----------------------------------------------------------------------
# Pre-flight
# ----------------------------------------------------------------------
def test_preflight_runs_node_probes_once_per_candidate():
    runner, _ = _runner(run_on_worker=None)
    results = runner.preflight([NodeCheck()], ["n0", "n1"])
    assert sorted(results["NodeCheck"]) == ["n0", "n1"]


def test_preflight_skips_worker_scoped_probes():
    runner, _ = _runner(run_on_worker=None)
    assert runner.preflight([WorkerCheck()], ["n0"]) == {}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
