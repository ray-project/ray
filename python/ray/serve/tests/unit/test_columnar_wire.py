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
        aggregated_metrics={RUNNING_REQUESTS_KEY: 3.0},
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
        aggregated_queued_requests=1.0,
        queued_requests=[TimeStampedValue(1.0, 1.0)],
        aggregated_metrics={RUNNING_REQUESTS_KEY: {"D#r0": 3.0}},
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
def test_reconstruct_through_wire(make):
    """The non-aggregate-mode fallback path: encode -> reconstruct -> same type."""
    rep = make()
    out = codec.reconstruct(codec.encode(rep))
    assert type(out) is type(rep)
    assert out.timestamp == rep.timestamp


def _handle_report_width(n):
    """Handle report whose running-requests metric spans n replica keys."""
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id="h0",
        actor_id="a0",
        handle_source=DeploymentHandleSource.PROXY,
        aggregated_queued_requests=1.0,
        queued_requests=[TimeStampedValue(1.0, 1.0)],
        aggregated_metrics={RUNNING_REQUESTS_KEY: {f"D#r{i}": 3.0 for i in range(n)}},
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
    # Producers only choose columnar where the controller aggregates raw
    # timeseries; pin aggregate mode on so the width gate is what is under test.
    monkeypatch.setattr(codec, "RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER", True)

    # Gated on report width (distinct replica keys).
    assert codec.should_encode_columnar(_handle_report_width(63)) is False  # below gate
    assert codec.should_encode_columnar(_handle_report_width(64)) is True  # at gate
    assert codec.should_encode_columnar(_handle_report_width(128)) is True  # above gate
    # Replica reports are ALWAYS objects (columnar is handle-report-only).
    assert codec.should_encode_columnar(_replica_report()) is False


def test_should_encode_columnar_gate_threshold_tunable(monkeypatch):
    """The gate threshold is env-tunable; lowering it lets thinner reports go columnar."""
    monkeypatch.setattr(codec, "RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS", 8)
    monkeypatch.setattr(codec, "RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER", True)
    assert codec.should_encode_columnar(_handle_report_width(7)) is False
    assert codec.should_encode_columnar(_handle_report_width(8)) is True


def test_reconstruct_omits_keys_absent_from_aggregated_metrics():
    """reconstruct() must be a faithful inverse of encode().

    _encode iterates the ``metrics`` dict and pads a NaN into the agg column for
    any entry that has no matching ``aggregated_metrics`` value (every entry needs
    a slot). reconstruct must NOT surface those padding NaNs as real
    ``aggregated_metrics`` keys -- otherwise a reconstructed report could carry
    extra NaN entries the original never had, and simple mode would sum a NaN on
    the running-requests key and poison the autoscaling signal. Live producers
    keep the two key sets aligned, so this guards the contract against a future
    producer that does not.
    """
    # Handle: a running-replica subkey present in metrics but missing from agg.
    handle = HandleMetricReport(
        deployment_id=DEP,
        handle_id="h0",
        actor_id="a0",
        handle_source=DeploymentHandleSource.PROXY,
        aggregated_queued_requests=1.0,
        queued_requests=[TimeStampedValue(1.0, 1.0)],
        aggregated_metrics={RUNNING_REQUESTS_KEY: {"D#r0": 5.0}},  # D#r1 omitted
        metrics={
            RUNNING_REQUESTS_KEY: {
                "D#r0": [TimeStampedValue(1.0, 3.0)],
                "D#r1": [TimeStampedValue(1.0, 4.0)],
            }
        },
        timestamp=100.0,
    )
    running = codec.reconstruct(codec.encode(handle)).aggregated_metrics[
        RUNNING_REQUESTS_KEY
    ]
    assert running == {"D#r0": 5.0}  # D#r1 dropped, NOT stored as NaN
    assert all(v == v for v in running.values())  # no NaN survived

    # Replica: a custom metric present in metrics but missing from agg.
    replica = ReplicaMetricReport(
        replica_id=ReplicaID("r0", DEP),
        aggregated_metrics={RUNNING_REQUESTS_KEY: 3.0},  # "gpu_util" omitted
        metrics={
            RUNNING_REQUESTS_KEY: [TimeStampedValue(1.0, 2.0)],
            "gpu_util": [TimeStampedValue(1.0, 0.7)],
        },
        timestamp=100.0,
    )
    agg = codec.reconstruct(codec.encode(replica)).aggregated_metrics
    assert agg == {RUNNING_REQUESTS_KEY: 3.0}  # gpu_util dropped, NOT stored as NaN
    assert all(v == v for v in agg.values())  # no NaN survived


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
