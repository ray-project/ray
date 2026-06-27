"""Columnar codec for the FULL HandleMetricReport / ReplicaMetricReport.

The reports are structs whose `metrics` field is a dict keyed by metric name, each
holding (for handles) a per-replica timeseries or (for replicas) a single timeseries
— i.e. a ragged "list per metric". This codec encodes the entire struct losslessly:

  - every timeseries POINT (the bulk, O(metrics*sources*points)) goes into two flat
    concatenated float64 arrays (`ts`, `val`) — decoded zero-copy via np.frombuffer,
    with NO per-point Python objects on the hot path;
  - a ragged index (`entries`) + aligned aggregated scalars (`agg`) go into int64/float64
    arrays (also zero-copy);
  - labels (metric names, replica keys, ids, enum, scalars) ride in a small JSON header.

`decode()` returns a native view (numpy arrays + label lists) for sum-decomposable
merging. `reconstruct()` rebuilds the original dataclasses (used only to prove
round-trip losslessness).
"""
import json
import struct
import zlib
from typing import Any, Dict, Union

import numpy as np

from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    DeploymentHandleSource,
    DeploymentID,
    HandleMetricReport,
    ReplicaID,
    ReplicaMetricReport,
    TimeStampedValue,
)

_MAGIC = b"SCR1"


def is_columnar(buf: bytes) -> bool:
    """Wire-detect: True iff ``buf`` is a columnar (SCR1) frame.

    The 4-byte magic is framed OUTSIDE the zlib stream, so any ingestion path can
    route on the wire format alone -- independently of the RAY_SERVE_COLUMNAR_METRICS
    producer flag. A fleet mid-rollout (mixed columnar/cloudpickle senders) is then
    handled correctly: the consumer reads whatever each sender actually emitted."""
    return len(buf) >= 4 and buf[:4] == _MAGIC


# ---------------------------------------------------------------------------
# array packing helpers (flat raw section, frombuffer-friendly)
# ---------------------------------------------------------------------------
def _pack(arrays: Dict[str, np.ndarray]):
    descriptors, chunks, off = {}, [], 0
    for name, arr in arrays.items():
        a = np.ascontiguousarray(arr)
        b = a.tobytes()
        descriptors[name] = {
            "dtype": a.dtype.str,
            "shape": list(a.shape),
            "off": off,
            "len": len(b),
        }
        chunks.append(b)
        off += len(b)
    return descriptors, b"".join(chunks)


def _view(descriptors, raw, base, name) -> np.ndarray:
    d = descriptors[name]
    n = 1
    for s in d["shape"]:
        n *= s
    arr = np.frombuffer(
        raw, dtype=np.dtype(d["dtype"]), count=n, offset=base + d["off"]
    )  # zero-copy
    return arr.reshape(d["shape"])


def _flatten_series(series_list):
    """Concatenate a list of TimeSeries into flat ts/val python lists + per-series lengths."""
    ts, val, lengths = [], [], []
    for series in series_list:
        lengths.append(len(series))
        for p in series:
            ts.append(p.timestamp)
            val.append(p.value)
    return ts, val, lengths


# ---------------------------------------------------------------------------
# encode
# ---------------------------------------------------------------------------
def encode(report: Union[HandleMetricReport, ReplicaMetricReport]) -> bytes:
    if isinstance(report, HandleMetricReport):
        return _encode_handle(report)
    return _encode_replica(report)


def _encode_handle(rep: HandleMetricReport) -> bytes:
    metric_names = list(rep.metrics.keys())
    mi = {m: i for i, m in enumerate(metric_names)}
    replica_keys, rk = [], {}
    entries, aggs, series_list = [], [], []
    for m in metric_names:
        for key, series in rep.metrics[m].items():
            if key not in rk:
                rk[key] = len(replica_keys)
                replica_keys.append(key)
            entries.append((mi[m], rk[key], 0, len(series)))  # offset filled below
            aggs.append(rep.aggregated_metrics.get(m, {}).get(key, float("nan")))
            series_list.append(series)
    ts, val, _ = _flatten_series(series_list)
    # fill offsets
    off = 0
    for i, (a, b, _o, n) in enumerate(entries):
        entries[i] = (a, b, off, n)
        off += n
    q_ts = [p.timestamp for p in rep.queued_requests]
    q_val = [p.value for p in rep.queued_requests]
    arrays = {
        "entries": (
            np.array(entries, dtype="<i8") if entries else np.zeros((0, 4), "<i8")
        ).reshape(-1, 4),
        "agg": np.array(aggs, dtype="<f8"),
        "ts": np.array(ts, dtype="<f8"),
        "val": np.array(val, dtype="<f8"),
        "q_ts": np.array(q_ts, dtype="<f8"),
        "q_val": np.array(q_val, dtype="<f8"),
    }
    descriptors, blob = _pack(arrays)
    header = {
        "type": "handle",
        "deployment": [rep.deployment_id.name, rep.deployment_id.app_name],
        "handle_id": rep.handle_id,
        "actor_id": rep.actor_id,
        "handle_source": rep.handle_source.value,
        "timestamp": rep.timestamp,
        "aggregated_queued_requests": rep.aggregated_queued_requests,
        "metric_names": metric_names,
        "replica_keys": replica_keys,
        "arrays": descriptors,
    }
    return _frame(header, blob)


def _encode_replica(rep: ReplicaMetricReport) -> bytes:
    metric_names = list(rep.metrics.keys())
    mi = {m: i for i, m in enumerate(metric_names)}
    entries, aggs, series_list = [], [], []
    for m in metric_names:
        series = rep.metrics[m]
        entries.append((mi[m], 0, len(series)))
        aggs.append(rep.aggregated_metrics.get(m, float("nan")))
        series_list.append(series)
    ts, val, _ = _flatten_series(series_list)
    off = 0
    for i, (a, _o, n) in enumerate(entries):
        entries[i] = (a, off, n)
        off += n
    arrays = {
        "entries": (
            np.array(entries, dtype="<i8") if entries else np.zeros((0, 3), "<i8")
        ).reshape(-1, 3),
        "agg": np.array(aggs, dtype="<f8"),
        "ts": np.array(ts, dtype="<f8"),
        "val": np.array(val, dtype="<f8"),
    }
    descriptors, blob = _pack(arrays)
    header = {
        "type": "replica",
        "replica_unique_id": rep.replica_id.unique_id,
        "deployment": [
            rep.replica_id.deployment_id.name,
            rep.replica_id.deployment_id.app_name,
        ],
        "timestamp": rep.timestamp,
        "metric_names": metric_names,
        "arrays": descriptors,
    }
    return _frame(header, blob)


def _frame(header: dict, blob: bytes) -> bytes:
    hb = json.dumps(header).encode()
    return _MAGIC + zlib.compress(struct.pack("<I", len(hb)) + hb + blob, level=9)


# ---------------------------------------------------------------------------
# decode (native view) + reconstruct (lossless round-trip)
# ---------------------------------------------------------------------------
def decode(buf: bytes) -> Dict[str, Any]:
    if buf[:4] != _MAGIC:
        raise ValueError("bad magic")
    raw = zlib.decompress(buf[4:])
    hlen = struct.unpack_from("<I", raw, 0)[0]
    header = json.loads(bytes(raw[4 : 4 + hlen]))
    base = 4 + hlen
    desc = header["arrays"]
    out = {"header": header}
    for name in desc:
        out[name] = _view(desc, raw, base, name)  # zero-copy float64/int64 views
    return out


def reconstruct(buf: bytes) -> Union[HandleMetricReport, ReplicaMetricReport]:
    d = decode(buf)
    h = d["header"]
    ts, val, entries, agg = d["ts"], d["val"], d["entries"], d["agg"]
    if h["type"] == "handle":
        metric_names, replica_keys = h["metric_names"], h["replica_keys"]
        metrics = {m: {} for m in metric_names}
        aggregated = {m: {} for m in metric_names}
        for i in range(entries.shape[0]):
            m_i, r_i, off, n = (int(x) for x in entries[i])
            m, key = metric_names[m_i], replica_keys[r_i]
            metrics[m][key] = [
                TimeStampedValue(float(ts[off + j]), float(val[off + j]))
                for j in range(n)
            ]
            aggregated[m][key] = float(agg[i])
        return HandleMetricReport(
            deployment_id=DeploymentID(h["deployment"][0], h["deployment"][1]),
            handle_id=h["handle_id"],
            actor_id=h["actor_id"],
            handle_source=DeploymentHandleSource(h["handle_source"]),
            aggregated_queued_requests=h["aggregated_queued_requests"],
            queued_requests=[
                TimeStampedValue(float(d["q_ts"][k]), float(d["q_val"][k]))
                for k in range(d["q_ts"].shape[0])
            ],
            aggregated_metrics=aggregated,
            metrics=metrics,
            timestamp=h["timestamp"],
        )
    else:
        metric_names = h["metric_names"]
        metrics, aggregated = {}, {}
        for i in range(entries.shape[0]):
            m_i, off, n = (int(x) for x in entries[i])
            m = metric_names[m_i]
            metrics[m] = [
                TimeStampedValue(float(ts[off + j]), float(val[off + j]))
                for j in range(n)
            ]
            aggregated[m] = float(agg[i])
        return ReplicaMetricReport(
            replica_id=ReplicaID(
                h["replica_unique_id"],
                DeploymentID(h["deployment"][0], h["deployment"][1]),
            ),
            aggregated_metrics=aggregated,
            metrics=metrics,
            timestamp=h["timestamp"],
        )


# ---------------------------------------------------------------------------
# round-trip self-test (TimeStampedValue.value is compare=False, so compare deeply)
# ---------------------------------------------------------------------------
def _series_eq(a, b):
    return len(a) == len(b) and all(
        x.timestamp == y.timestamp and x.value == y.value for x, y in zip(a, b)
    )


def _handle_eq(a: HandleMetricReport, b: HandleMetricReport) -> bool:
    if (
        a.deployment_id,
        a.handle_id,
        a.actor_id,
        a.handle_source,
        a.timestamp,
        a.aggregated_queued_requests,
    ) != (
        b.deployment_id,
        b.handle_id,
        b.actor_id,
        b.handle_source,
        b.timestamp,
        b.aggregated_queued_requests,
    ):
        return False
    if not _series_eq(a.queued_requests, b.queued_requests):
        return False
    if a.aggregated_metrics != b.aggregated_metrics:
        return False
    if set(a.metrics) != set(b.metrics):
        return False
    for m in a.metrics:
        if set(a.metrics[m]) != set(b.metrics[m]):
            return False
        for k in a.metrics[m]:
            if not _series_eq(a.metrics[m][k], b.metrics[m][k]):
                return False
    return True


def _replica_eq(a: ReplicaMetricReport, b: ReplicaMetricReport) -> bool:
    if (a.replica_id, a.timestamp, a.aggregated_metrics) != (
        b.replica_id,
        b.timestamp,
        b.aggregated_metrics,
    ):
        return False
    if set(a.metrics) != set(b.metrics):
        return False
    return all(_series_eq(a.metrics[m], b.metrics[m]) for m in a.metrics)


def decode_replica_running_requests(payload, metric_name=RUNNING_REQUESTS_KEY):
    """For a REPLICA columnar payload, return (replica_id, ts_arr, val_arr, timestamp)
    for the given metric -- zero-copy arrays, no per-point objects."""
    view = decode(payload)
    h = view["header"]
    dep = DeploymentID(h["deployment"][0], h["deployment"][1])
    replica_id = ReplicaID(h["replica_unique_id"], dep)
    ts_arr, val_arr = view["ts"][:0], view["val"][:0]
    names = h["metric_names"]
    if metric_name in names:
        mi = names.index(metric_name)
        for row in view["entries"]:
            if int(row[0]) == mi:
                off, n = int(row[1]), int(row[2])
                ts_arr, val_arr = view["ts"][off : off + n], view["val"][off : off + n]
                break
    return replica_id, ts_arr, val_arr, h["timestamp"]


def decode_handle_arrays(payload):
    """For a HANDLE columnar payload: metadata + per-replica running-requests arrays
    + queued arrays. Zero-copy; no per-point Python objects."""
    view = decode(payload)
    h = view["header"]
    ts, val, entries = view["ts"], view["val"], view["entries"]
    names = h["metric_names"]
    running = []
    rk = h["replica_keys"]
    if RUNNING_REQUESTS_KEY in names:
        mi = names.index(RUNNING_REQUESTS_KEY)
        for row in entries:
            if int(row[0]) == mi:
                off, n = int(row[2]), int(row[3])
                running.append((rk[int(row[1])], ts[off : off + n], val[off : off + n]))
    return {
        "handle_id": h["handle_id"],
        "deployment_id": DeploymentID(h["deployment"][0], h["deployment"][1]),
        "actor_id": h["actor_id"],
        "handle_source": h["handle_source"],
        "timestamp": h["timestamp"],
        "running": running,
        "queued": (view["q_ts"], view["q_val"]),
    }


def decode_handle_flat(payload):
    """Flat columnar handle view for the FUSED kernel: metadata + flat ts/val point
    arrays + the int64 entries index (rows [metric_idx, replica_key_idx, off, n]) +
    queued arrays. No per-replica Python objects -- the kernel slices in C."""
    view = decode(payload)
    h = view["header"]
    names = h["metric_names"]
    mi = names.index(RUNNING_REQUESTS_KEY) if RUNNING_REQUESTS_KEY in names else -1
    entries = np.ascontiguousarray(view["entries"], dtype=np.int64).reshape(-1, 4)
    return {
        "handle_id": h["handle_id"],
        "deployment_id": DeploymentID(h["deployment"][0], h["deployment"][1]),
        "actor_id": h["actor_id"],
        "handle_source": h["handle_source"],
        "timestamp": h["timestamp"],
        "ts": np.ascontiguousarray(view["ts"], dtype=np.float64),
        "val": np.ascontiguousarray(view["val"], dtype=np.float64),
        "entries": entries,
        "mi": mi,
        "replica_keys": h["replica_keys"],
        "q_ts": np.ascontiguousarray(view["q_ts"], dtype=np.float64),
        "q_val": np.ascontiguousarray(view["q_val"], dtype=np.float64),
    }


if __name__ == "__main__":
    RUNNING = "running_requests"
    dep = DeploymentID("D", "default")

    # Handle report: 2 metrics, ragged point counts, 3 replicas, queued series.
    metrics = {
        RUNNING: {
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
    agg = {
        RUNNING: {f"D#r{r}": float(r) for r in range(3)},
        "custom_load": {f"D#r{r}": 0.1 * r for r in range(3)},
    }
    hr = HandleMetricReport(
        deployment_id=dep,
        handle_id="h7",
        actor_id="act1",
        handle_source=DeploymentHandleSource.PROXY,
        aggregated_queued_requests=3.5,
        queued_requests=[TimeStampedValue(9.0 + i, float(i)) for i in range(4)],
        aggregated_metrics=agg,
        metrics=metrics,
        timestamp=123.5,
    )
    buf = encode(hr)
    assert _handle_eq(hr, reconstruct(buf)), "handle round-trip FAILED"
    print(f"handle round-trip OK  ({len(buf)} bytes, {len(metrics)} metrics, ragged)")

    # Replica report: 2 metrics.
    rr = ReplicaMetricReport(
        replica_id=ReplicaID("r42", dep),
        aggregated_metrics={RUNNING: 4.0, "custom_load": 1.25},
        metrics={
            RUNNING: [TimeStampedValue(1.0 + i, float(i)) for i in range(5)],
            "custom_load": [TimeStampedValue(2.0 + i, 0.5 * i) for i in range(3)],
        },
        timestamp=77.0,
    )
    buf2 = encode(rr)
    assert _replica_eq(rr, reconstruct(buf2)), "replica round-trip FAILED"
    print(f"replica round-trip OK ({len(buf2)} bytes, {len(rr.metrics)} metrics)")
    print("ALL ROUND-TRIP TESTS PASSED")
