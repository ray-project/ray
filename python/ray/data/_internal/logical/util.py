from typing import Dict
import json
import threading

from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.map_operator import Write


_recorded_operators = dict()
_recorded_operators_lock = threading.Lock()


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


def _collect_operators_to_dict(op: LogicalOperator, ops_dict: Dict[str, int]):
    """Collect the logical operator name and count into `ops_dict`."""
    for child in op.input_dependencies:
        _collect_operators_to_dict(child, ops_dict)

    op_name = op.name
    if isinstance(op, Read):
        ds_name = type(op._datasource).__name__
        op_name = f"Read{ds_name}"
    elif isinstance(op, Write):
        ds_name = type(op._datasource).__name__
        op_name = f"Write{ds_name}"

    ops_dict.setdefault(op_name, 0)
    ops_dict[op_name] += 1
