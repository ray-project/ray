"""Utilities for Ray Data telemetry: operator naming / logical-op usage
recording, plus helpers for the cluster metric queries fired by the usage collector
in background threads.
"""

import json
import logging
import os
import threading
from typing import Dict, Optional

import requests

from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import Read, ReadFiles, Write

logger = logging.getLogger(__name__)

# The dictionary for the operator name and count.
_recorded_operators = dict()
_recorded_operators_lock = threading.Lock()

# Bounded timeout for the Prometheus counter HTTP queries.
_PROMETHEUS_QUERY_TIMEOUT_S = 0.25


def _prometheus_host() -> str:
    """Prometheus base URL, matching the dashboard's ``RAY_PROMETHEUS_HOST``."""
    return os.environ.get("RAY_PROMETHEUS_HOST", "http://localhost:9090")


def _prometheus_headers() -> Dict[str, str]:
    """Parse ``RAY_PROMETHEUS_HEADERS`` (a JSON dict or list of ``[key, value]``
    pairs). Returns an empty dict on any parse failure.
    """
    try:
        headers = json.loads(os.environ.get("RAY_PROMETHEUS_HEADERS", "{}"))
    except json.JSONDecodeError:
        logger.debug("Failed to parse RAY_PROMETHEUS_HEADERS as JSON", exc_info=True)
        return {}
    if isinstance(headers, list):
        return dict(headers)
    return headers or {}


def query_prometheus_counter(promql: str) -> Optional[int]:
    """Instant-query a cumulative Prometheus counter and return its cluster-wide
    integer value.

    Returns None on any failure (Prometheus unreachable, non-200, empty result),
    as usage collection is best effort
    """
    try:
        resp = requests.get(
            f"{_prometheus_host()}/api/v1/query",
            params={"query": promql},
            headers=_prometheus_headers(),
            timeout=_PROMETHEUS_QUERY_TIMEOUT_S,
        )
        if resp.status_code != 200:
            logger.debug(
                "Prometheus counter query %r returned %d", promql, resp.status_code
            )
            return None
        results = resp.json()["data"]["result"]
        if not results:
            logger.debug("Prometheus counter query %r returned empty result", promql)
            return None
        return int(sum(float(r["value"][1]) for r in results))
    except (requests.RequestException, ValueError, KeyError, IndexError, TypeError):
        # RequestException: unreachable/timeout/non-HTTP error; the rest:
        # unexpected/empty response shape. Best-effort — return None on any.
        logger.debug("Failed to query Prometheus counter %r", promql, exc_info=True)
        return None


def compute_delta(start: Optional[int], end: Optional[int]) -> Optional[int]:
    """Non-negative delta between two cumulative samples. Returns None if
    either sample is missing"""
    if start is None or end is None:
        return None
    return max(0, end - start)


def _is_builtin_cls(cls: type) -> bool:
    """Return True if ``cls`` is defined under the ``ray`` package.

    Used to gate which operator / datasource / datasink class names are safe
    to surface in telemetry. Anything outside ``ray.*`` is treated as
    user-defined and anonymized.
    """
    return (cls.__module__ or "").startswith("ray.")


def record_operators_usage(op: LogicalOperator):
    """Record logical operator usage with Ray telemetry."""
    ops_dict = dict()
    _collect_operators_to_dict(op, ops_dict)
    ops_json_str = ""
    with _recorded_operators_lock:
        for op_name, count in ops_dict.items():
            _recorded_operators.setdefault(op_name, 0)
            _recorded_operators[op_name] += count
        ops_json_str = json.dumps(_recorded_operators)

    record_extra_usage_tag(TagKey.DATA_LOGICAL_OPS, ops_json_str)


def anonymize_op_name(op: LogicalOperator) -> str:
    """Return an op name suitable for usage collection.

    Read/Write surface their datasource/datasink suffix (``ReadParquet``,
    ``WriteIceberg``) when the underlying class ships under ``ray.data.*``;
    user-defined datasources/datasinks collapse to ``ReadCustom`` /
    ``WriteCustom``. ``ReadFiles`` (the V2 file-read op) surfaces its
    format via ``datasource_name`` (e.g. ``ReadFilesParquetV2``) when the
    scanner class is built-in; user-defined scanners collapse to
    ``ReadFilesCustom``. All other built-in operators emit their class
    name (``Sort``, ``MapBatches``, ``Limit``, …); user-defined
    ``LogicalOperator`` subclasses collapse to ``Unknown``.
    """
    if isinstance(op, Read):
        if _is_builtin_cls(type(op.datasource)):
            return f"Read{op.datasource.get_name()}"
        return "ReadCustom"
    if isinstance(op, Write):
        sink = op.datasink_or_legacy_datasource
        if _is_builtin_cls(type(sink)):
            return f"Write{sink.get_name()}"
        return "WriteCustom"
    if isinstance(op, ReadFiles):
        # Gate on the scanner class — the string ``datasource_name`` field
        # could be set to anything by a user-defined V2 datasource, so it's
        # not safe to surface on its own.
        if _is_builtin_cls(type(op.scanner)):
            return f"ReadFiles{op.datasource_name}"
        return "ReadFilesCustom"
    cls = type(op)
    return cls.__name__ if _is_builtin_cls(cls) else "Unknown"


def _collect_operators_to_dict(op: LogicalOperator, ops_dict: Dict[str, int]):
    """Collect the logical operator name and count into `ops_dict`."""
    for child in op.input_dependencies:
        _collect_operators_to_dict(child, ops_dict)

    op_name = anonymize_op_name(op)
    ops_dict.setdefault(op_name, 0)
    ops_dict[op_name] += 1
