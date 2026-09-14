"""Columnar autoscaling metrics: the SCR1 wire format.

Covers framing and O(1) wire detection, the producer-side width gate, decode
rejection of malformed frames, and the fallback when numpy is unavailable.
"""

import sys

import numpy as np
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

# --------------------------------------------------------------------------
# shared fixtures
# --------------------------------------------------------------------------

DEP = DeploymentID("D", "default")

NOW = 1000.0


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


MAKERS = [_replica_report, _handle_report]


def _rich_handle_report():
    """2 metrics, ragged point counts, 3 replicas, queued series (round-trip case)."""
    metrics = {
        RUNNING_REQUESTS_KEY: {
            f"D#r{r}": [
                TimeStampedValue(1.0 + i, float((r + i) % 7)) for i in range(r % 4 + 1)
            ]
            for r in range(3)
        },
        "custom_load": {
            f"D#r{r}": [TimeStampedValue(2.0 + i, 0.5 * (r + i)) for i in range(2)]
            for r in range(3)
        },
    }
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id="h7",
        actor_id="act1",
        handle_source=DeploymentHandleSource.PROXY,
        queued_requests=[TimeStampedValue(9.0 + i, float(i)) for i in range(4)],
        metrics=metrics,
        timestamp=123.5,
    )


def _tampered_handle_frame(
    entries_rows, n_points, replica_keys, names, frame_type="handle"
):
    arrays = {
        "entries": np.array(entries_rows, dtype="<i8").reshape(-1, 4),
        "ts": np.arange(n_points, dtype="<f8"),
        "val": np.ones(n_points, dtype="<f8"),
        "q_ts": np.zeros(0, dtype="<f8"),
        "q_val": np.zeros(0, dtype="<f8"),
    }
    descriptors, blob = codec._pack(arrays)
    header = {
        "type": frame_type,
        "deployment": ["D", "default"],
        "handle_id": "h",
        "actor_id": "a",
        "handle_source": "PROXY",
        "timestamp": NOW,
        "metric_names": names,
        "replica_keys": replica_keys,
        "arrays": descriptors,
    }
    return codec._frame(header, blob)


# --------------------------------------------------------------------------
# codec: framing, wire detection, width gate, decode rejection, numpy fallback
# --------------------------------------------------------------------------


def test_columnar_frame_detected_in_o1():
    buf = codec.encode(_handle_report())
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


def test_decode_rejects_garbage_zlib():
    with pytest.raises(ValueError, match="corrupt"):
        codec.decode(b"SCR1" + b"this-is-not-zlib-data")


def test_decode_rejects_truncated_frame():
    buf = codec.encode(_rich_handle_report())
    with pytest.raises(ValueError):
        codec.decode(buf[: len(buf) // 2])


def test_decode_rejects_out_of_bounds_ragged_index():
    # entries row points past the point arrays -> must fail AT DECODE, not later
    # inside the control loop's merge.
    buf = _tampered_handle_frame(
        [[0, 0, 0, 99]], n_points=2, replica_keys=["k"], names=["m"]
    )
    with pytest.raises(ValueError, match="ragged index"):
        codec.decode(buf)


def test_decode_rejects_bad_metric_or_replica_index():
    buf = _tampered_handle_frame(
        [[5, 0, 0, 1]], n_points=2, replica_keys=["k"], names=["m"]
    )
    with pytest.raises(ValueError, match="ragged index"):
        codec.decode(buf)
    buf = _tampered_handle_frame(
        [[0, 3, 0, 1]], n_points=2, replica_keys=["k"], names=["m"]
    )
    with pytest.raises(ValueError, match="ragged index"):
        codec.decode(buf)


def test_should_encode_columnar_takes_every_handle_report():
    """Columnar is chosen by report type alone: handle reports carry every replica the
    handle routes to, replica reports carry one and stay on the Python-object path."""
    assert codec.should_encode_columnar(_handle_report_width(1)) is True
    assert codec.should_encode_columnar(_handle_report_width(128)) is True
    assert codec.should_encode_columnar(_replica_report()) is False


def test_encode_decode_round_trip_preserves_every_series():
    """The codec's own contract: metadata, the ragged per-replica series and the queued
    series all survive a round trip. TimeStampedValue declares value compare=False, so
    compare timestamps and values explicitly rather than the objects."""
    rep = _rich_handle_report()
    frame = codec.encode(rep)
    flat = codec.decode_handle_flat(frame)
    assert flat["handle_id"] == rep.handle_id
    assert flat["deployment_id"] == rep.deployment_id
    assert flat["actor_id"] == rep.actor_id
    assert flat["handle_source"] == rep.handle_source.value
    assert flat["timestamp"] == rep.timestamp
    assert [(p.timestamp, p.value) for p in rep.queued_requests] == list(
        zip(flat["q_ts"].tolist(), flat["q_val"].tolist())
    )
    names = codec.decode(frame)["header"]["metric_names"]
    assert names[flat["mi"]] == RUNNING_REQUESTS_KEY
    got = {}
    for metric_idx, key_idx, off, n in flat["entries"].tolist():
        got[(names[metric_idx], flat["replica_keys"][key_idx])] = list(
            zip(flat["ts"][off : off + n].tolist(), flat["val"][off : off + n].tolist())
        )
    assert got == {
        (metric, key): [(p.timestamp, p.value) for p in series]
        for metric, per_replica in rep.metrics.items()
        for key, series in per_replica.items()
    }


def test_decode_validates_every_frame_whatever_the_header_says():
    """The bounds check must not be skippable. It used to sit behind an "is this a handle
    frame" guard, so a tampered header type dodged it and the merge read past the array."""
    buf = _tampered_handle_frame(
        [[0, 0, 0, 99]], n_points=2, replica_keys=["k"], names=["m"], frame_type="nope"
    )
    with pytest.raises(ValueError, match="ragged index"):
        codec.decode(buf)


def test_decode_rejects_negative_ragged_index():
    """A negative index must fail rather than wrap: Python would resolve -1 to the last
    metric or replica key and silently mis-attribute the series."""
    for row in ([[-1, 0, 0, 1]], [[0, -1, 0, 1]], [[0, 0, -1, 1]], [[0, 0, 0, -1]]):
        buf = _tampered_handle_frame(row, n_points=2, replica_keys=["k"], names=["m"])
        with pytest.raises(ValueError, match="ragged index"):
            codec.decode(buf)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
