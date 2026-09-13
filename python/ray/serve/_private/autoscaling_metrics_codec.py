"""Columnar codec for HandleMetricReport, whose `metrics` field is a dict keyed by
metric name holding a per-replica timeseries, i.e. a ragged "list per metric".

Encodes the struct losslessly, splitting bulk from labels so the hot path stays native:
every timeseries POINT (the bulk, O(metrics*replicas*points)) goes into two flat
float64 arrays plus an int64 ragged index, read zero-copy via np.frombuffer with no
per-point Python objects, while the labels and scalars ride in a small JSON header.
"Columnar" here means no per-point Python objects; decompression still allocates one
buffer per frame.
"""
from __future__ import annotations

import json
import logging
import struct
import zlib
from typing import Any, Dict, List, TypedDict

import numpy as np

from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    DeploymentID,
    HandleMetricReport,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)

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
    """Concatenate a list of TimeSeries into flat ts/val python lists."""
    ts, val = [], []
    for series in series_list:
        for p in series:
            ts.append(p.timestamp)
            val.append(p.value)
    return ts, val


# ---------------------------------------------------------------------------
# encode
# ---------------------------------------------------------------------------
def encode(report: HandleMetricReport) -> bytes:
    return _encode_handle(report)


def should_encode_columnar(report: HandleMetricReport) -> bool:
    """Whether a producer should serialize this report columnar (vs Python objects).

    Handle reports carry every replica the handle routes to, so they are the ones worth
    encoding natively; replica reports carry one and stay on the object path. The format
    self-identifies on the wire (see is_columnar), so mixed senders decode correctly.

    Args:
        report: the metric report about to be serialized.

    Returns:
        True to encode columnar, False to use the Python-object path.
    """
    return isinstance(report, HandleMetricReport)


def _encode_handle(rep: HandleMetricReport) -> bytes:
    metric_names = list(rep.metrics.keys())
    mi = {m: i for i, m in enumerate(metric_names)}
    replica_keys: List[str] = []
    rk: Dict[str, int] = {}
    entries, series_list = [], []
    for m in metric_names:
        for key, series in rep.metrics[m].items():
            if key not in rk:
                rk[key] = len(replica_keys)
                replica_keys.append(key)
            entries.append((mi[m], rk[key], 0, len(series)))  # offset filled below
            series_list.append(series)
    ts, val = _flatten_series(series_list)
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
    # Pad so the arrays land 8-byte aligned: np.frombuffer tolerates an unaligned
    # offset, but unaligned SIMD access costs more on aarch64, which premerge targets.
    # json.loads ignores the trailing spaces.
    hb += b" " * ((4 - len(hb)) % 8)
    # level=1: metric reports are serialized on the producer hot path (per handle,
    # every metrics interval), so minimize compression CPU;
    # the columnar float64 payload is already compact and the win is the
    # native decode, not the wire size.
    return _MAGIC + zlib.compress(struct.pack("<I", len(hb)) + hb + blob, level=1)


# ---------------------------------------------------------------------------
# decode (native view)
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
    # Sanity-check the ragged index HERE so a malformed frame fails as one isolated
    # ingest error instead of raising inside the control loop's merge every tick until
    # the entry ages out. Unconditional: a header field must not be able to skip it.
    if out["val"].size != out["ts"].size or out["q_ts"].size != out["q_val"].size:
        raise ValueError("columnar frame corrupt: array length mismatch")
    entries = out["entries"]
    if entries.size and (
        int(entries.min()) < 0  # every column: a negative index would WRAP, not fail
        or int(entries[:, 0].max()) >= len(header["metric_names"])
        or int(entries[:, 1].max()) >= len(header["replica_keys"])
        or int((entries[:, 2] + entries[:, 3]).max()) > out["ts"].size
    ):
        raise ValueError("columnar frame corrupt: ragged index out of bounds")
    return out


# ---------------------------------------------------------------------------
# flat view for the merge kernel
# ---------------------------------------------------------------------------


class FlatHandleReport(TypedDict):
    """Decoded columnar handle report. `entries` rows are [metric_idx, replica_key_idx,
    off, n] into the flat ts/val arrays; `mi` is the index of RUNNING_REQUESTS_KEY in
    the report's metric names, or -1 when it carries none."""

    handle_id: str
    deployment_id: DeploymentID
    actor_id: str
    handle_source: str
    timestamp: float
    ts: np.ndarray
    val: np.ndarray
    entries: np.ndarray
    mi: int
    replica_keys: List[str]
    q_ts: np.ndarray
    q_val: np.ndarray


def decode_handle_flat(payload: bytes) -> FlatHandleReport:
    """Flat columnar handle view for the FUSED kernel: no per-replica Python objects,
    so the kernel slices in C."""
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
