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
round-trip losslessness). ("Columnar" here means no per-point Python objects;
decompression still allocates one buffer per frame.)
"""
from __future__ import annotations

import json
import struct
import zlib
from typing import Any, Dict, Union

try:
    import numpy as np
except ModuleNotFoundError:  # numpy is only needed on the columnar (opt-in) path;
    np = None  # serve-minimal installs lack it and never reach the columnar code.

from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    DeploymentHandleSource,
    DeploymentID,
    HandleMetricReport,
    ReplicaID,
    ReplicaMetricReport,
    TimeStampedValue,
)
from ray.serve._private.constants import (
    RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER,
    RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS,
    RAY_SERVE_ENABLE_DIRECT_INGRESS,
)

_MAGIC = b"SCR1"


def is_columnar(buf: bytes) -> bool:
    """Wire-detect: True iff ``buf`` is a columnar (SCR1) frame.

    The 4-byte magic is framed OUTSIDE the zlib stream, so any ingestion path can
    route on the wire format alone -- independently of how the producer chose to
    encode. A fleet mid-rollout (mixed columnar/cloudpickle senders) is then
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


def should_encode_columnar(
    report: Union[HandleMetricReport, ReplicaMetricReport]
) -> bool:
    """Whether a producer should serialize this report columnar (vs Python objects).

    Format is chosen by report TYPE and self-identifies on the wire (see is_columnar),
    so mixed columnar/object sources aggregate correctly at the controller:
      - HandleMetricReport (fat: many replicas x points) -> columnar when the report
        covers >= RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS replicas
        (decode + GIL-release win only above the measured ~64-replicas/handle crossover;
        below it the array machinery is pure overhead, so thin reports stay objects).
      - ReplicaMetricReport (one replica, small) -> ALWAYS Python objects: columnar's
        array merge over many small per-replica sources is slower in the controller's
        in-loop decision-time aggregation (measured), so columnar is never used for
        replica reports.

    Args:
        report: the metric report about to be serialized.

    Returns:
        True to encode columnar, False to use the Python-object path.
    """
    if np is None or not isinstance(report, HandleMetricReport):
        # numpy is optional (serve-minimal installs lack it); without it the producer
        # cannot build arrays, so always take the object path.
        return False
    if not (
        RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER or RAY_SERVE_ENABLE_DIRECT_INGRESS
    ):
        # In simple mode the controller never consumes arrays -- it would reconstruct()
        # every frame (object decode at pure overhead) -- so producers keep the object
        # path. The controller-side reconstruct fallback stays as the mismatch net.
        return False
    # Width gate: rationale at RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS (constants.py).
    widest = max(
        (len(series_by_replica) for series_by_replica in report.metrics.values()),
        default=0,
    )
    return widest >= RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS


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
    # level=1: metric reports are serialized on the producer hot path (per
    # replica/handle, every metrics interval), so minimize compression CPU;
    # the columnar float64 payload is already compact and the win is the
    # native decode, not the wire size.
    return _MAGIC + zlib.compress(struct.pack("<I", len(hb)) + hb + blob, level=1)


# ---------------------------------------------------------------------------
# decode (native view) + reconstruct (lossless round-trip)
# ---------------------------------------------------------------------------
def decode(buf: bytes) -> Dict[str, Any]:
    if buf[:4] != _MAGIC:
        raise ValueError("bad magic")
    try:
        raw = zlib.decompress(buf[4:])
    except zlib.error as e:
        raise ValueError(f"columnar frame corrupt: {e}") from e
    # Defensive length checks so a truncated/corrupt frame raises a clear
    # ValueError instead of a cryptic struct.error/JSONDecodeError inside the
    # controller's control loop.
    if len(raw) < 4:
        raise ValueError("columnar frame truncated: missing header length")
    hlen = struct.unpack_from("<I", raw, 0)[0]
    if 4 + hlen > len(raw):
        raise ValueError("columnar frame truncated: header exceeds payload")
    header = json.loads(bytes(raw[4 : 4 + hlen]))
    base = 4 + hlen
    desc = header["arrays"]
    out = {"header": header}
    for name in desc:
        out[name] = _view(desc, raw, base, name)  # zero-copy float64/int64 views
    if header.get("type") == "handle":
        # Sanity-check the ragged index HERE so a malformed frame fails as one
        # isolated ingest error instead of raising inside the control loop's merge
        # every tick until the entry ages out.
        if out["val"].size != out["ts"].size or out["q_ts"].size != out["q_val"].size:
            raise ValueError("columnar frame corrupt: array length mismatch")
        entries = out["entries"]
        if entries.size and (
            int(entries[:, 0].max()) >= len(header["metric_names"])
            or int(entries[:, 1].max()) >= len(header["replica_keys"])
            or int(entries[:, 2].min()) < 0
            or int(entries[:, 3].min()) < 0
            or int((entries[:, 2] + entries[:, 3]).max()) > out["ts"].size
            or entries.shape[0] != out["agg"].size
        ):
            raise ValueError("columnar frame corrupt: ragged index out of bounds")
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
            v = float(agg[i])
            # _encode pads a metrics entry that has no matching aggregated
            # value with NaN so every entry gets an agg slot; skip it here so
            # reconstruct stays a faithful inverse and never injects a key the
            # original aggregated_metrics lacked (a stray NaN on the running-
            # requests key would poison the simple-mode request sum).
            if v == v:  # NaN is the only value not equal to itself
                aggregated[m][key] = v
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
            v = float(agg[i])
            if v == v:  # skip NaN padding (see handle branch): faithful inverse
                aggregated[m] = v
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


def decode_replica_all_metrics(payload):
    """For a REPLICA columnar payload, return (replica_id, {metric_name: (ts, val)},
    timestamp) for ALL metrics -- zero-copy arrays, no per-point objects. Carries
    custom autoscaling metrics through the columnar path, not just running_requests."""
    view = decode(payload)
    h = view["header"]
    dep = DeploymentID(h["deployment"][0], h["deployment"][1])
    replica_id = ReplicaID(h["replica_unique_id"], dep)
    names = h["metric_names"]
    ts_all, val_all = view["ts"], view["val"]
    metric_arrays = {}
    for row in view["entries"]:
        mi, off, n = int(row[0]), int(row[1]), int(row[2])
        metric_arrays[names[mi]] = (ts_all[off : off + n], val_all[off : off + n])
    return replica_id, metric_arrays, h["timestamp"]


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
