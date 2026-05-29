import json
import threading
from typing import Dict

from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import Read, ReadFiles, Write

# The dictionary for the operator name and count.
_recorded_operators = dict()
_recorded_operators_lock = threading.Lock()


def _is_builtin_cls(cls: type) -> bool:
    """Return True if ``cls`` is defined under the ``ray.data`` package.

    Used to gate which operator / datasource / datasink class names are safe
    to surface in telemetry. Anything outside ``ray.data.*`` is treated as
    user-defined and anonymized.
    """
    return (cls.__module__ or "").startswith("ray.data.")


def record_operators_usage(op: LogicalOperator):
    """Record logical operator usage with Ray telemetry."""
    ops_dict = dict()
    _collect_operators_to_dict(op, ops_dict)
    ops_json_str = ""
    with _recorded_operators_lock:
        for op, count in ops_dict.items():
            _recorded_operators.setdefault(op, 0)
            _recorded_operators[op] += count
        ops_json_str = json.dumps(_recorded_operators)

    record_extra_usage_tag(TagKey.DATA_LOGICAL_OPS, ops_json_str)


def anonymize_op_name(op: LogicalOperator) -> str:
    """Return an op name suitable for telemetry.

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
