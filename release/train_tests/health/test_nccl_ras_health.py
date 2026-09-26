"""Tests for the user-side NCCL RAS policy, against real GPU captures.

    pytest release/train_tests/health/test_nccl_ras_health.py

The fixtures in data/ came off a 4x A10G cluster. Every bug these tests pin was
one a hand-written fixture could not catch: a wrong nvidia-smi field name that
read zero ECC errors on every GPU, and RAS numbering ranks per communicator so
four different subgroups all looked like {0, 1}.
"""
import json
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import nccl_ras_health as nrh  # noqa: E402

from ray.train.health import (  # noqa: E402
    Action,
    Cause,
    ClusterContext,
    HealthPolicy,
    HealthState,
    ProbeResult,
    WorkerHealth,
)
from ray.train.health._internal.manager import HealthManager  # noqa: E402

DATA = Path(__file__).parent / "data"
CTX = ClusterContext(node_ids=[])


@pytest.fixture(scope="module")
def ras_json():
    return (DATA / "ncclras_a10g_mismatch.json").read_text()


@pytest.fixture(scope="module")
def smi_text():
    return (DATA / "nvidia-smi_a10g_healthy.txt").read_text()


def _probe_over(*raws):
    """A probe whose `ncclras` answers are the given strings, in order."""
    probe = nrh.NcclRasProbe()
    answers = list(raws)
    probe.query = lambda ctx: answers.pop(0) if answers else None
    return probe


def with_subgroups(ras_json, tp01=(50, 49), tp23=(50, 50)):
    """The real world communicator plus two TP subgroups over it.

    Subgroup ranks are communicator-local, exactly as RAS reports them.
    """
    raw = json.loads(ras_json)
    world = raw["communicators"][0]
    procs = [(r["host"], r["pid"]) for r in world["ranks"]]

    def subgroup(name, members, counts):
        return {
            "hash": name,
            "ranks": [
                {
                    "rank": i,
                    "host": procs[g][0],
                    "pid": procs[g][1],
                    "status": world["ranks"][g]["status"],
                    "collective_counts": {"AllReduce": counts[i]},
                }
                for i, g in enumerate(members)
            ],
        }

    raw["communicators"] = [
        world,
        subgroup("0xTP01", [0, 1], tp01),
        subgroup("0xTP23", [2, 3], tp23),
    ]
    return json.dumps(raw)


# ======================================================================
# Parsing real RAS output
# ======================================================================
def test_the_real_capture_is_the_injected_wedge(ras_json):
    comms = nrh.parse_ras_json(ras_json)
    (comm,) = comms.values()
    assert {r: c["AllReduce"] for r, c in comm.op_counts.items()} == {
        0: 101,
        1: 100,
        2: 101,
        3: 101,
    }
    assert all(comm.running.values())
    assert comm.mismatched


def test_ras_numbers_ranks_per_communicator_and_we_translate(ras_json):
    """The bug a real 4-rank run exposed: every subgroup reported {0, 1}."""
    raw = json.loads(with_subgroups(ras_json))
    assert [r["rank"] for r in raw["communicators"][2]["ranks"]] == [0, 1]

    comms = nrh.parse_ras_json(json.dumps(raw))
    assert sorted(comms["0xTP01"].op_counts) == [0, 1]
    assert sorted(comms["0xTP23"].op_counts) == [2, 3]  # was [0, 1] before
    assert comms["0xTP23"].ranks_are_global


def test_without_process_identity_ranks_are_not_claimed_global(ras_json):
    raw = json.loads(with_subgroups(ras_json))
    for comm in raw["communicators"][1:]:
        for rank in comm["ranks"]:
            del rank["pid"]
    comms = nrh.parse_ras_json(json.dumps(raw))
    assert not comms["0xTP23"].ranks_are_global


def test_unparseable_output_is_none():
    assert nrh.parse_ras_json("Connection refused") is None


# ======================================================================
# The probe
# ======================================================================
def test_no_delta_yet_means_no_opinion(ras_json):
    (result,) = _probe_over(ras_json).poll(CTX).values()
    assert result.passed is None and result.events == []


def test_mismatched_and_not_advancing_is_frozen(ras_json):
    probe = _probe_over(ras_json, ras_json)
    probe.poll(CTX)
    (result,) = probe.poll(CTX).values()
    assert result.events == ["frozen"] and result.passed is False
    assert result.metrics["ranks_are_global"] == 1.0
    assert sorted(result.devices) == ["0", "1", "2", "3"]


def test_the_same_skew_while_advancing_is_not_frozen(ras_json):
    later = ras_json.replace('"AllReduce": 101', '"AllReduce": 105')
    probe = _probe_over(ras_json, later)
    probe.poll(CTX)
    (result,) = probe.poll(CTX).values()
    assert result.events == [] and result.passed is True


def test_subgroups_are_reported_under_global_ranks(ras_json):
    doc = with_subgroups(ras_json)
    probe = _probe_over(doc, doc)
    probe.poll(CTX)
    results = probe.poll(CTX)
    assert sorted(int(r) for r in results["0xTP23"].devices) == [2, 3]
    assert results["0xTP01"].events == ["frozen"]  # 50 vs 49, not moving
    assert results["0xTP23"].events == []  # 50 vs 50, not mismatched


# ======================================================================
# nvidia-smi, against a real A10G report
# ======================================================================
def test_a_healthy_a10g_is_clean(smi_text):
    metrics, events = nrh.parse_nvidia_smi(smi_text)
    assert events == []
    assert metrics["max_temp_c"] == 39.0
    assert metrics["slowdown_temp_c"] == 95.0  # off the card, not hardcoded


@pytest.mark.parametrize(
    "key",
    ["DRAM Uncorrectable", "SRAM Uncorrectable SEC-DED", "SRAM Uncorrectable Parity"],
)
def test_every_uncorrectable_counter_counts(smi_text, key):
    injected = re.sub(rf"({re.escape(key)}\s+:\s+)0", r"\g<1>4", smi_text, count=1)
    assert injected != smi_text
    metrics, events = nrh.parse_nvidia_smi(injected)
    assert metrics["ecc_uncorrectable"] == 4.0 and "ecc_uncorrectable" in events


def test_lifetime_ecc_does_not_condemn_a_node_mid_run(smi_text):
    hits = [m.start() for m in re.finditer(r"DRAM Uncorrectable\s+:\s+0", smi_text)]
    head, tail = smi_text[: hits[1]], smi_text[hits[1] :]
    aggregate = head + re.sub(r"(DRAM Uncorrectable\s+:\s+)0", r"\g<1>7", tail, count=1)
    metrics, events = nrh.parse_nvidia_smi(aggregate)
    assert metrics["ecc_uncorrectable"] == 0.0
    assert metrics["ecc_uncorrectable_lifetime"] == 7.0
    assert events == []


def test_the_counters_section_does_not_false_positive(smi_text):
    assert re.search(r"SW Thermal Slowdown\s+:\s+0 us", smi_text)
    assert "thermal_throttle" not in nrh.parse_nvidia_smi(smi_text)[1]


def test_throttle_and_heat_are_seen(smi_text):
    hot = re.sub(
        r"(SW Thermal Slowdown\s+:\s+)Not Active", r"\g<1>Active", smi_text, count=1
    )
    hot = re.sub(r"(GPU Current Temp\s+:\s+)39 C", r"\g<1>96 C", hot)
    assert nrh.parse_nvidia_smi(hot)[1] == ["gpu_hot", "thermal_throttle"]


# ======================================================================
# NcclHangEvaluator: the merged detector's rule, then attribution
# ======================================================================
class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _state(comms, nodes_bad=None, reported=None, step=None, n=4):
    on_demand = {}
    if nodes_bad is not None:
        on_demand["NvidiaSmiProbe"] = {
            node: ProbeResult(
                passed=not bad, events=["ecc_uncorrectable"] if bad else []
            )
            for node, bad in nodes_bad.items()
        }
    reported = reported or {}
    return HealthState(
        workers={
            r: WorkerHealth(r, f"node{r}", 1.0, step=step, reported=reported.get(r, {}))
            for r in range(n)
        },
        entities={"NcclRasProbe": comms},
        on_demand_probes=on_demand,
    )


def _frozen(ranks, global_=True):
    return ProbeResult(
        metrics={"ranks_are_global": float(global_)},
        devices={str(r): {} for r in ranks},
        events=["frozen"],
        passed=False,
    )


def test_a_single_frozen_sample_is_not_a_hang():
    ev = nrh.NcclHangEvaluator(confirm_duration_s=20, clock=Clock())
    assert ev.evaluate(_state({"c": _frozen([0, 1, 2, 3])})) == []


def test_confirmation_is_wall_clock():
    clock = Clock()
    ev = nrh.NcclHangEvaluator(confirm_duration_s=20, clock=clock)
    ev.evaluate(_state({"c": _frozen([0, 1])}))
    clock.t += 19
    assert ev.evaluate(_state({"c": _frozen([0, 1])})) == []
    clock.t += 1
    assert ev.evaluate(_state({"c": _frozen([0, 1])}))[0].action is Action.REATTEMPT


def test_without_diagnostics_the_reason_does_not_claim_a_check():
    """Caught on a real run: it said "diagnostics found no hardware fault"."""
    ev = nrh.NcclHangEvaluator(confirm_duration_s=0, clock=Clock())
    reason = ev.evaluate(_state({"c": _frozen([0, 1])}))[0].reason
    assert "no diagnostics are configured" in reason


def test_diagnose_once_then_evict_on_bad_hardware():
    ev = nrh.NcclHangEvaluator(
        confirm_duration_s=0, diagnostics=nrh.hang_diagnostics(), clock=Clock()
    )
    first = ev.evaluate(_state({"c": _frozen([2, 3])}))[0]
    assert first.action is Action.DIAGNOSE
    assert first.target_ranks == [2, 3] and first.target_nodes == ["node2", "node3"]

    after = ev.evaluate(_state({"c": _frozen([2, 3])}, nodes_bad={"node3": True}))[0]
    assert after.action is Action.EVICT
    assert after.cause is Cause.HARDWARE and after.target_nodes == ["node3"]


def test_diagnose_once_then_reattempt_on_clean_hardware():
    """A hang alone never evicts: that would cordon a node for a code bug."""
    ev = nrh.NcclHangEvaluator(
        confirm_duration_s=0, diagnostics=nrh.hang_diagnostics(), clock=Clock()
    )
    ev.evaluate(_state({"c": _frozen([2, 3])}))
    after = ev.evaluate(
        _state({"c": _frozen([2, 3])}, nodes_bad={"node2": False, "node3": False})
    )[0]
    assert after.action is Action.REATTEMPT and after.cause is Cause.NO_PROGRESS


# ======================================================================
# CollectiveHangEvaluator: RAS joined against health.report()
# ======================================================================
def coords(rank):  # tp=2, pp=2 over 4 ranks
    return {"tp_rank": rank % 2, "pp_rank": rank // 2}


def test_parallelism_groups_from_reported_coordinates():
    groups = nrh.parallelism_groups({r: coords(r) for r in range(4)})
    assert groups[frozenset({0, 1})] == "tp_rank"
    assert groups[frozenset({0, 2})] == "pp_rank"


def _join_state(comm_ranks, step, step_time=2.0, global_=True):
    return _state(
        {"c": _frozen(comm_ranks, global_)},
        reported={r: {**coords(r), "step_time_s": step_time} for r in range(4)},
        step=step,
    )


def test_a_slow_step_shorter_than_the_jobs_threshold_does_not_fire():
    """A 30s checkpoint pause on a job whose steps take 10s is not a hang.

    RAS sees mismatch-and-no-progress for the whole pause; a fixed 20s confirm
    window would fire. The threshold here is 5 x 10s = 50s.
    """
    clock = Clock()
    ev = nrh.CollectiveHangEvaluator(min_stall_s=5, stall_factor=5, clock=clock)
    ev.evaluate(_join_state([0, 1], step=5, step_time=10.0))
    clock.t += 30
    assert ev.evaluate(_join_state([0, 1], step=5, step_time=10.0)) == []
    # The pause ends: the communicator advances, and its streak resets.
    assert ev.evaluate(_state({}, reported={}, step=6)) == []
    clock.t += 30
    assert ev.evaluate(_join_state([0, 1], step=6, step_time=10.0)) == []


def test_a_stall_past_the_threshold_fires_and_names_the_group():
    clock = Clock()
    ev = nrh.CollectiveHangEvaluator(min_stall_s=5, stall_factor=5, clock=clock)
    ev.evaluate(_join_state([0, 1], step=5, step_time=10.0))
    clock.t += 51
    decision = ev.evaluate(_join_state([0, 1], step=5, step_time=10.0))[0]
    assert decision.action is Action.REATTEMPT
    assert "TP group" in decision.reason and "50s" in decision.reason


def test_a_pp_group_is_named_too():
    clock = Clock()
    ev = nrh.CollectiveHangEvaluator(min_stall_s=10, clock=clock)
    ev.evaluate(_join_state([0, 2], step=5))
    clock.t += 11
    assert "PP group" in ev.evaluate(_join_state([0, 2], step=5))[0].reason


def test_local_ranks_are_never_matched_against_groups():
    """If translation failed, {0, 1} is not the TP group -- it is unknown."""
    clock = Clock()
    ev = nrh.CollectiveHangEvaluator(min_stall_s=10, clock=clock)
    ev.evaluate(_join_state([0, 2], step=5, global_=False))
    clock.t += 11
    decision = ev.evaluate(_join_state([0, 2], step=6, global_=False))[0]
    assert "unnamed group" in decision.reason


def test_the_threshold_scales_with_the_jobs_step_time():
    """Same freeze, same 60s: fires for a 2s-step job, not for a 40s-step one."""
    for step_time, fires in ((2.0, True), (40.0, False)):
        clock = Clock()
        ev = nrh.CollectiveHangEvaluator(min_stall_s=5, stall_factor=5, clock=clock)
        ev.evaluate(_join_state([0, 1], step=5, step_time=step_time))
        clock.t += 60
        got = ev.evaluate(_join_state([0, 1], step=5, step_time=step_time))
        assert bool(got) is fires, step_time


# ======================================================================
# Through the HealthManager, the way a run uses it
# ======================================================================
def test_the_policy_drives_the_manager_to_a_decision(ras_json):
    policy = nrh.nccl_ras_policy(confirm_duration_s=0, diagnose=False)
    assert isinstance(policy, HealthPolicy)
    manager = HealthManager([policy])
    (probe,) = manager.cluster_probes()
    assert [type(p) for p in manager.preflight_probes()] == [nrh.NcclRasReadyProbe]
    answers = [ras_json, ras_json]
    probe.query = lambda ctx: answers.pop(0) if answers else ras_json

    manager.run_cluster_probes(CTX)
    assert manager.poll_decision() is None  # first sample: no delta yet
    manager.run_cluster_probes(CTX)
    decision = manager.poll_decision()
    assert decision.action is Action.REATTEMPT
    assert decision.cause is Cause.NO_PROGRESS


# ======================================================================
# Pre-flight
# ======================================================================
@pytest.mark.parametrize(
    "version, expected",
    [(22809, (2, 28)), ((2, 25, 1), (2, 25)), ("NCCL RAS 2.28.9", (2, 28))],
)
def test_nccl_versions_parse(version, expected):
    assert nrh._major_minor(version) == expected


def test_an_old_ncclras_fails_preflight(monkeypatch):
    class Done:
        returncode, stdout = 0, "NCCL version 2.25.1"

    monkeypatch.setattr(nrh.subprocess, "run", lambda *a, **k: Done())
    monkeypatch.setattr(nrh, "_pyspy_can_attach", lambda: True)
    result = nrh.NcclRasReadyProbe().poll(None)
    assert result.passed is False
    assert "ncclras 2.25 is older than 2.28" in result.detail


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
