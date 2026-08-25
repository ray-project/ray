"""Wire-format contract for the columnar codec.

The SCR1 magic is framed OUTSIDE zlib, so any ingestion path can detect the format
in O(1) from the bytes alone -- independently of how the producer chose to encode.
This makes a fleet mid-rollout (mixed columnar/cloudpickle senders) safe: the
controller routes on what each sender actually emitted.
"""
import pytest

from ray.serve._private import autoscaling_metrics_codec as codec
from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    DeploymentHandleSource,
    DeploymentID,
    HandleMetricReport,
    ReplicaID,
    ReplicaMetricReport,
    TimeStampedValue,
)
from ray.serve._private.utils import compress_metric_report, decompress_metric_report

DEP = DeploymentID("D", "default")


def _replica_report():
    return ReplicaMetricReport(
        replica_id=ReplicaID("r0", DEP),
        metrics={
            RUNNING_REQUESTS_KEY: [
                TimeStampedValue(1.0, 2.0),
                TimeStampedValue(2.0, 4.0),
            ]
        },
        timestamp=100.0,
    )


def _handle_report():
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id="h0",
        actor_id="a0",
        handle_source=DeploymentHandleSource.PROXY,
        queued_requests=[TimeStampedValue(1.0, 1.0)],
        metrics={RUNNING_REQUESTS_KEY: {"D#r0": [TimeStampedValue(1.0, 3.0)]}},
        timestamp=100.0,
    )


MAKERS = [_replica_report, _handle_report]


@pytest.mark.parametrize("make", MAKERS)
def test_columnar_frame_detected_in_o1(make):
    buf = codec.encode(make())
    assert buf[:4] == b"SCR1"  # magic visible without decompressing
    assert codec.is_columnar(buf) is True


@pytest.mark.parametrize("make", MAKERS)
def test_cloudpickle_frame_not_columnar(make):
    """The real cloudpickle producer wire must not be misread as columnar."""
    buf = compress_metric_report(make())
    assert codec.is_columnar(buf) is False
    # and it still round-trips through the cloudpickle consumer
    assert decompress_metric_report(buf).timestamp == make().timestamp


def test_short_buffers_not_columnar():
    for b in (b"", b"S", b"SCR", b"\x78\x9c"):
        assert codec.is_columnar(b) is False


@pytest.mark.parametrize("make", MAKERS)
def _handle_report_width(n):
    """Handle report whose running-requests metric spans n replica keys."""
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id="h0",
        actor_id="a0",
        handle_source=DeploymentHandleSource.PROXY,
        queued_requests=[TimeStampedValue(1.0, 1.0)],
        metrics={
            RUNNING_REQUESTS_KEY: {
                f"D#r{i}": [TimeStampedValue(1.0, 3.0)] for i in range(n)
            }
        },
        timestamp=100.0,
    )


def test_should_encode_columnar_width_gate(monkeypatch):
    """Producers choose columnar only for HANDLE reports wide enough to clear the
    RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS gate. Thin handle reports and replica
    reports stay on the Python-object path. (Columnar's array decode/merge only
    beats objects above the measured ~64-replica crossover.)"""
    monkeypatch.setattr(codec, "RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS", 64)

    # Gated on report width (distinct replica keys).
    assert codec.should_encode_columnar(_handle_report_width(63)) is False  # below gate
    assert codec.should_encode_columnar(_handle_report_width(64)) is True  # at gate
    assert codec.should_encode_columnar(_handle_report_width(128)) is True  # above gate
    # Replica reports are ALWAYS objects (columnar is handle-report-only).
    assert codec.should_encode_columnar(_replica_report()) is False


def test_should_encode_columnar_gate_threshold_tunable(monkeypatch):
    """The gate threshold is env-tunable; lowering it lets thinner reports go columnar."""
    monkeypatch.setattr(codec, "RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS", 8)
    assert codec.should_encode_columnar(_handle_report_width(7)) is False
    assert codec.should_encode_columnar(_handle_report_width(8)) is True


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
