"""Wire-format contract for the columnar codec.

The SCR1 magic is framed OUTSIDE zlib, so any ingestion path can detect the format
in O(1) from the bytes alone -- independently of the RAY_SERVE_COLUMNAR_METRICS
producer flag. This makes a fleet mid-rollout (mixed columnar/cloudpickle senders)
safe: the controller routes on what each sender actually emitted.
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
