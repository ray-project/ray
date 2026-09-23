"""Run the parsers against output a real GPU actually produced.

Everything else in the health tests uses fixtures someone wrote. These use
captures from a 4x A10G cluster, which is the only way to catch the class of
bug where a field name is wrong and the check silently reports "clean".

That is not hypothetical: `_parse_nvidia_smi` originally looked for a counter
called `Uncorrectable`, which `nvidia-smi -q` does not have. It reported zero
ECC errors on every GPU ever made.
"""
import re
import sys
from pathlib import Path

import pytest

from ray.train.v2._internal.callbacks.nccl_ras import parse_ras_schema
from ray.train.v2._internal.execution.health.adapters.nccl_ras_diagnostics import (
    _parse_nvidia_smi,
)
from ray.train.v2._internal.execution.health.adapters.nccl_ras_policy import (
    NcclRasProbe,
)
from ray.train.v2._internal.execution.health.probe import ClusterContext

DATA = Path(__file__).parent / "data" / "health"
CTX = ClusterContext(node_ids=[])


@pytest.fixture(scope="module")
def ras_json():
    return (DATA / "ncclras_a10g_mismatch.json").read_text()


@pytest.fixture(scope="module")
def smi_text():
    return (DATA / "nvidia-smi_a10g_healthy.txt").read_text()


# ----------------------------------------------------------------------
# NCCL RAS
# ----------------------------------------------------------------------
def test_the_real_ras_schema_parses(ras_json):
    report = parse_ras_schema(ras_json)
    assert report is not None
    assert report.timestamp == "2026-09-23 11:55:37"
    assert len(report.comm_op_counts) == 1


def test_the_captured_job_is_the_wedge_we_injected(ras_json):
    """Rank 1 one op behind three peers, every rank still RUNNING."""
    report = parse_ras_schema(ras_json)
    comm = next(iter(report.comm_op_counts))

    counts = {r: c["AllReduce"] for r, c in report.comm_op_counts[comm].items()}
    assert counts == {0: 101, 1: 100, 2: 101, 3: 101}
    assert set(report.comm_rank_status[comm].values()) == {"RUNNING"}
    assert report.comm_op_skews[comm]["AllReduce"] == 1
    assert report.mismatched_comms == {comm}
    assert not report.healthy


def test_the_probe_calls_it_frozen_when_nothing_advances(ras_json):
    """Two identical samples: mismatched and no rank advanced any op."""
    report = parse_ras_schema(ras_json)
    reports = [report, report]
    probe = NcclRasProbe(lambda: reports.pop(0) if reports else None)

    first = probe.poll(CTX)
    comm = next(iter(first))
    assert first[comm].passed is None, "no delta yet, so no opinion"

    second = probe.poll(CTX)[comm]
    assert second.events == ["frozen"]
    assert second.passed is False
    assert second.metrics == {"mismatched": 1.0, "ops_advanced": 0.0, "ranks": 4.0}
    assert set(second.devices) == {"0", "1", "2", "3"}


def test_a_progressing_job_is_not_frozen(ras_json):
    """The same skew, but the counts move: transient, not a hang."""
    report = parse_ras_schema(ras_json)
    later = parse_ras_schema(ras_json.replace('"AllReduce": 101', '"AllReduce": 105'))
    reports = [report, later]
    probe = NcclRasProbe(lambda: reports.pop(0) if reports else None)

    probe.poll(CTX)
    second = probe.poll(CTX)[next(iter(report.comm_op_counts))]
    assert second.events == []
    assert second.passed is True


def test_the_text_report_names_the_culprit():
    """What a DIAGNOSE captures for a human to read."""
    text = (DATA / "ncclras_a10g_mismatch.txt").read_text()
    assert "MISMATCH" in text
    assert "Rank 1 has launched up to operation 100" in text
    assert "node 10.0.109.86" in text


# ----------------------------------------------------------------------
# nvidia-smi
# ----------------------------------------------------------------------
def test_a_healthy_a10g_reports_clean(smi_text):
    metrics, events = _parse_nvidia_smi(smi_text)
    assert events == []
    assert metrics["ecc_uncorrectable"] == 0.0
    assert metrics["max_temp_c"] == 39.0


def test_the_slowdown_threshold_comes_off_the_card_not_from_us(smi_text):
    """An A10G slows at 95C. A hardcoded 90 would be wrong for every other part."""
    metrics, _ = _parse_nvidia_smi(smi_text)
    assert metrics["slowdown_temp_c"] == 95.0


@pytest.mark.parametrize(
    "field",
    ["DRAM Uncorrectable", "SRAM Uncorrectable SEC-DED", "SRAM Uncorrectable Parity"],
)
def test_every_uncorrectable_counter_is_actually_counted(smi_text, field):
    """There is no field called plain `Uncorrectable`; assuming there was is
    why this check read zero on every real GPU."""
    injected = re.sub(rf"({re.escape(field)}\s+:\s+)0", r"\g<1>4", smi_text, count=1)
    assert injected != smi_text, f"{field} not found in the real report"

    metrics, events = _parse_nvidia_smi(injected)
    assert metrics["ecc_uncorrectable"] == 4.0
    assert "ecc_uncorrectable" in events


def test_lifetime_ecc_does_not_condemn_a_node_mid_run(smi_text):
    """`Aggregate` is the card's whole life; `Volatile` is this run.

    Summing them would make every GPU that has ever seen a bit flip look like
    it is failing right now.
    """
    occurrences = [
        m.start() for m in re.finditer(r"DRAM Uncorrectable\s+:\s+0", smi_text)
    ]
    assert len(occurrences) == 2, "expected a Volatile and an Aggregate block"

    head, tail = smi_text[: occurrences[1]], smi_text[occurrences[1] :]
    aggregate_only = head + re.sub(
        r"(DRAM Uncorrectable\s+:\s+)0", r"\g<1>7", tail, count=1
    )

    metrics, events = _parse_nvidia_smi(aggregate_only)
    assert metrics["ecc_uncorrectable"] == 0.0
    assert metrics["ecc_uncorrectable_lifetime"] == 7.0
    assert "ecc_uncorrectable" not in events


def test_throttling_and_heat_are_detected(smi_text):
    injected = smi_text.replace(
        "SW Thermal Slowdown                            : Not Active",
        "SW Thermal Slowdown                            : Active",
        1,
    )
    injected = re.sub(r"(GPU Current Temp\s+:\s+)39 C", r"\g<1>96 C", injected, count=1)

    metrics, events = _parse_nvidia_smi(injected)
    assert "thermal_throttle" in events
    assert "gpu_hot" in events  # 96 >= the card's own 95C slowdown point
    assert metrics["max_temp_c"] == 96.0


def test_the_counters_section_does_not_false_positive(smi_text):
    """`Clocks Event Reasons Counters` repeats the same key names with
    microsecond values. Matching on the key alone would fire on every GPU."""
    assert "SW Thermal Slowdown                            : 0 us" in smi_text
    _, events = _parse_nvidia_smi(smi_text)
    assert "thermal_throttle" not in events


def test_row_remap_failure_is_caught(smi_text):
    injected = smi_text.replace(
        "Remapping Failure Occurred                     : No",
        "Remapping Failure Occurred                     : Yes",
        1,
    )
    _, events = _parse_nvidia_smi(injected)
    assert "row_remap_failure" in events


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
