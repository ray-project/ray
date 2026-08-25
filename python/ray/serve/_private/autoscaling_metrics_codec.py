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
import logging
import struct
import zlib
from typing import Any, Dict

try:
    import numpy as np
except ModuleNotFoundError:  # numpy is only needed on the columnar (opt-in) path;
    np = None  # serve-minimal installs lack it and never reach the columnar code.

from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    DeploymentID,
    HandleMetricReport,
)
from ray.serve._private.constants import (
    RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS,
    SERVE_LOGGER_NAME,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)

_MAGIC = b"SCR1"
# Warn at most once per process each (see should_encode_columnar / can_decode_columnar).
_WARNED_NUMPY_MISSING = False
_WARNED_NUMPY_UNDECODABLE = False


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
def encode(report: HandleMetricReport) -> bytes:
    return _encode_handle(report)


def should_encode_columnar(report: HandleMetricReport) -> bool:
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
    if not isinstance(report, HandleMetricReport):
        return False
    if np is None:
        # numpy is optional (serve-minimal installs lack it) and is not pulled in by
        # ray[serve], so warn once when a report is wide enough to have benefited --
        # otherwise the optimization is simply absent with no signal.
        _warn_columnar_unavailable_once(_widest_metric(report))
        return False
    # Width gate: rationale at RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS (constants.py).
    return _widest_metric(report) >= RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS


def can_decode_columnar() -> bool:
    """Whether this process can decode columnar frames. The producer's numpy is not
    the controller's: replicas carry per-deployment runtime_envs, so a frame can
    arrive here from a process that had numpy when this one does not."""
    return np is not None


def warn_columnar_undecodable_once() -> None:
    """Warn once per process; a dropped report would otherwise just undercount load."""
    global _WARNED_NUMPY_UNDECODABLE
    if _WARNED_NUMPY_UNDECODABLE:
        return
    _WARNED_NUMPY_UNDECODABLE = True
    logger.warning(
        "Dropping columnar autoscaling metrics: they were sent by a process that has "
        "numpy, but numpy is not installed here, so they cannot be decoded. Autoscaling "
        "will undercount load for the affected deployments until numpy is installed in "
        "this process's environment."
    )


def _widest_metric(report: HandleMetricReport) -> int:
    """Replica count of the report's widest metric, which is what the width gate reads."""
    return max(
        (len(series_by_replica) for series_by_replica in report.metrics.values()),
        default=0,
    )


def _warn_columnar_unavailable_once(widest: int) -> None:
    """Warn at most once per process, and only for reports the gate would have taken."""
    global _WARNED_NUMPY_MISSING
    if _WARNED_NUMPY_MISSING or widest < RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS:
        return
    _WARNED_NUMPY_MISSING = True
    logger.warning(
        f"numpy is not installed, so autoscaling metrics for this deployment "
        f"({widest} replicas per handle) are sent as Python objects rather than "
        f"columnar arrays. Installing numpy reduces controller ingest cost at this "
        f"scale; behaviour is otherwise unchanged."
    )


def _encode_handle(rep: HandleMetricReport) -> bytes:
    metric_names = list(rep.metrics.keys())
    mi = {m: i for i, m in enumerate(metric_names)}
    replica_keys, rk = [], {}
    entries, series_list = [], []
    for m in metric_names:
        for key, series in rep.metrics[m].items():
            if key not in rk:
                rk[key] = len(replica_keys)
                replica_keys.append(key)
            entries.append((mi[m], rk[key], 0, len(series)))  # offset filled below
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
        "metric_names": metric_names,
        "replica_keys": replica_keys,
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
        ):
            raise ValueError("columnar frame corrupt: ragged index out of bounds")
    return out


# ---------------------------------------------------------------------------
# round-trip self-test (TimeStampedValue.value is compare=False, so compare deeply)
# ---------------------------------------------------------------------------


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
