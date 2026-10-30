import sys
from typing import List

import pytest

from ray.core.generated.events_health_event_pb2 import (
    HealthCheckerDisabled,
    HealthDecision,
    HealthEvent,
    HealthFinding,
    NcclHangFinding,
)
from ray.train.v2._internal.health import event_exporter
from ray.train.v2._internal.health.event_exporter import (
    HEALTH_MANAGER_CHECKER_NAME,
    MAX_ARTIFACT_TEXT_BYTES,
    HealthEventExporter,
)

State = HealthFinding.State
Artifact = HealthFinding.HealthArtifact


@pytest.fixture
def emitted(monkeypatch) -> List[HealthEvent]:
    """Enables export and captures the HealthEvents sent to the recorder."""
    events = []
    monkeypatch.setattr(event_exporter, "is_python_ray_event_enabled", lambda _: True)
    monkeypatch.setattr(
        HealthEventExporter, "_emit_to_recorder", lambda self, e: events.append(e)
    )
    return events


def _nccl_hang(*comm_ids: str) -> NcclHangFinding:
    return NcclHangFinding(
        run_id="run-1",
        attempt_id="attempt-1",
        communicators=[
            NcclHangFinding.Communicator(
                communicator_id=c, state=NcclHangFinding.Communicator.SUSPECTED
            )
            for c in comm_ids
        ],
    )


def test_finding_lifecycle(emitted):
    exporter = HealthEventExporter()

    finding_id = exporter.emit_new_finding(
        "nccl_hang_detector",
        summary="1 communicator stalled",
        started_at=100.0,
        details=_nccl_hang("a"),
        artifacts=[Artifact(name="ras_text_report", text="report")],
        observed_at=101.0,
    )
    exporter.emit_updated_finding(
        finding_id, summary="2 communicators stalled", details=_nccl_hang("a", "b")
    )
    exporter.emit_updated_finding(finding_id, summary="still 2 stalled")
    exporter.emit_resolved_finding(finding_id, details=_nccl_hang())

    assert [e.finding.state for e in emitted] == [
        State.NEW,
        State.UPDATED,
        State.UPDATED,
        State.RESOLVED,
    ]
    assert [e.sequence for e in emitted] == [1, 2, 3, 4]
    assert all(e.checker_name == "nccl_hang_detector" for e in emitted)
    assert all(e.finding.finding_id == finding_id for e in emitted)
    assert emitted[0].observed_at.ToNanoseconds() == 101 * 10**9

    new, updated, kept, resolved = (e.finding for e in emitted)
    assert new.nccl_hang.run_id == "run-1"
    assert [a.name for a in new.artifacts] == ["ras_text_report"]
    # Artifacts are only on the event that produced them.
    assert not updated.artifacts
    # Every event is a full snapshot: omitted fields keep their latest values.
    assert all(f.started_at.ToNanoseconds() == 100 * 10**9 for f in (updated, kept))
    assert len(kept.nccl_hang.communicators) == 2
    assert resolved.summary == "still 2 stalled"
    assert resolved.nccl_hang.run_id == "run-1"
    assert not resolved.nccl_hang.communicators
    assert exporter.open_finding_ids() == []


def test_closed_finding_id_is_final(emitted):
    exporter = HealthEventExporter()

    finding_id = exporter.emit_new_finding("probe", summary="s", started_at=0.0)
    exporter.emit_stopped_finding(finding_id)
    exporter.emit_updated_finding(finding_id, summary="s2")
    exporter.emit_resolved_finding(finding_id)

    assert [e.finding.state for e in emitted] == [State.NEW, State.STOPPED]


def test_recurrence_is_a_new_finding(emitted):
    exporter = HealthEventExporter()

    finding_ids = []
    for _ in range(2):
        finding_id = exporter.emit_new_finding("probe", summary="s", started_at=0.0)
        exporter.emit_updated_finding(finding_id, summary="s2")
        exporter.emit_resolved_finding(finding_id)
        finding_ids.append(finding_id)

    assert finding_ids[0] != finding_ids[1]
    assert [(e.finding.finding_id, e.finding.state) for e in emitted] == [
        (finding_ids[0], State.NEW),
        (finding_ids[0], State.UPDATED),
        (finding_ids[0], State.RESOLVED),
        (finding_ids[1], State.NEW),
        (finding_ids[1], State.UPDATED),
        (finding_ids[1], State.RESOLVED),
    ]
    assert [e.sequence for e in emitted] == [1, 2, 3, 4, 5, 6]


def test_sequences_are_per_checker(emitted):
    exporter = HealthEventExporter()

    exporter.emit_new_finding("a", summary="s", started_at=0.0)
    exporter.emit_new_finding("b", summary="s", started_at=0.0)
    exporter.emit_new_finding("a", summary="s", started_at=0.0)
    exporter.emit_decision("REATTEMPT")

    assert [(e.checker_name, e.sequence) for e in emitted] == [
        ("a", 1),
        ("b", 1),
        ("a", 2),
        (HEALTH_MANAGER_CHECKER_NAME, 1),
    ]


def test_checker_disabled_stops_its_open_findings(emitted):
    exporter = HealthEventExporter()

    a1 = exporter.emit_new_finding("a", summary="s", started_at=0.0)
    b1 = exporter.emit_new_finding("b", summary="s", started_at=0.0)
    exporter.emit_checker_disabled(
        "a", HealthCheckerDisabled.Reason.DEPENDENCY_UNAVAILABLE, "ncclras missing"
    )
    # Dropped: the checker is disabled, and it's only reported once.
    assert exporter.emit_new_finding("a", summary="s", started_at=0.0) is None
    exporter.emit_checker_disabled("a", HealthCheckerDisabled.Reason.INTERNAL_ERROR)

    a_events = [e for e in emitted if e.checker_name == "a"]
    assert [e.WhichOneof("body") for e in a_events] == [
        "finding",
        "finding",
        "checker_disabled",
    ]
    assert a_events[1].finding.finding_id == a1
    assert a_events[1].finding.state == State.STOPPED
    assert a_events[2].checker_disabled.detail == "ncclras missing"
    assert [e.sequence for e in a_events] == [1, 2, 3]
    assert exporter.open_finding_ids() == [b1]


def test_stop_open_findings(emitted):
    exporter = HealthEventExporter()

    exporter.emit_new_finding("a", summary="s", started_at=0.0)
    b1 = exporter.emit_new_finding("b", summary="s", started_at=0.0)
    exporter.stop_all_open_findings("a")
    assert exporter.open_finding_ids() == [b1]

    exporter.stop_all_open_findings()
    assert exporter.open_finding_ids() == []
    assert [e.finding.state for e in emitted[2:]] == [State.STOPPED, State.STOPPED]


def test_emit_decision(emitted):
    exporter = HealthEventExporter()

    decision_id = exporter.emit_decision(
        "DIAGNOSE",
        reason="rank 3 slow",
        evaluators=["ThermalStraggler"],
        finding_ids=["f1"],
        target_node_ids=["ab" * 28],
        target_ranks=[3],
        on_demand_probes=["NcclCheck"],
    )
    assert exporter.emit_decision("NOOP") is None

    assert len(emitted) == 1
    decision = emitted[0].decision
    assert decision.decision_id == decision_id
    assert decision.action == HealthDecision.Action.DIAGNOSE
    assert list(decision.target_node_ids) == [bytes.fromhex("ab" * 28)]
    assert list(decision.target_ranks) == [3]
    assert list(decision.on_demand_probes) == ["NcclCheck"]


@pytest.mark.parametrize(
    "text, expected_bytes",
    [
        ("x" * 10, 10),
        ("x" * (MAX_ARTIFACT_TEXT_BYTES + 10), MAX_ARTIFACT_TEXT_BYTES),
        # A 3-byte character straddling the cap is dropped, not split.
        ("x" * (MAX_ARTIFACT_TEXT_BYTES - 1) + "€", MAX_ARTIFACT_TEXT_BYTES - 1),
    ],
)
def test_artifact_text_is_capped(emitted, text, expected_bytes):
    exporter = HealthEventExporter()

    exporter.emit_new_finding(
        "probe",
        summary="s",
        started_at=0.0,
        artifacts=[
            Artifact(name="dump", text=text),
            Artifact(name="dir", uri="s3://x"),
        ],
    )

    text_artifact, uri_artifact = emitted[0].finding.artifacts
    assert len(text_artifact.text.encode("utf-8")) == expected_bytes
    assert uri_artifact.uri == "s3://x"


@pytest.mark.parametrize(
    "call",
    [
        lambda ex: ex.emit_new_finding(
            "probe", summary="s", started_at=0.0, details={"bad": object()}
        ),
        lambda ex: ex.emit_decision("NOT_AN_ACTION"),
        lambda ex: ex.emit_decision("EVICT", target_node_ids=["not-hex"]),
    ],
)
def test_invalid_input_raises_without_side_effects(emitted, call):
    exporter = HealthEventExporter()

    with pytest.raises((ValueError, TypeError)):
        call(exporter)
    assert emitted == []
    assert exporter.open_finding_ids() == []


def test_export_errors_are_swallowed(monkeypatch):
    def failing_emit(self, event):
        raise RuntimeError("export pipeline down")

    monkeypatch.setattr(event_exporter, "is_python_ray_event_enabled", lambda _: True)
    monkeypatch.setattr(HealthEventExporter, "_emit_to_recorder", failing_emit)
    exporter = HealthEventExporter()

    finding_id = exporter.emit_new_finding("probe", summary="s", started_at=0.0)
    # Bookkeeping still advances so the next event is UPDATED.
    assert exporter.open_finding_ids() == [finding_id]


def test_disabled_by_default(monkeypatch):
    sent = []
    monkeypatch.setattr(
        HealthEventExporter, "_emit_to_recorder", lambda self, e: sent.append(e)
    )
    exporter = HealthEventExporter()

    finding_id = exporter.emit_new_finding("probe", summary="s", started_at=0.0)
    exporter.emit_resolved_finding(finding_id)
    assert sent == []
    assert exporter.open_finding_ids() == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
