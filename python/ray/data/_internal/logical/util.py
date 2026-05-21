import json
import re
import threading
from typing import Dict

from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import AbstractUDFMap, Read, Write

# The dictionary for the operator name and count.
_recorded_operators = dict()
_recorded_operators_lock = threading.Lock()

# The white list of operator names allowed to be recorded.
_op_name_white_list = [
    # Read
    "ReadBigQuery",
    "ReadRange",
    "ReadMongo",
    "ReadParquet",
    "ReadParquetBulk",
    "ReadImage",
    "ReadJSON",
    "ReadCSV",
    "ReadText",
    "ReadNumpy",
    "ReadTFRecord",
    "ReadBinary",
    "ReadTorch",
    "ReadAvro",
    "ReadWebDataset",
    "ReadSQL",
    "ReadDatabricksUC",
    "ReadLance",
    "ReadHuggingFace",
    "ReadCustom",
    # From
    "FromArrow",
    "FromItems",
    "FromNumpy",
    "FromPandas",
    # Write
    "WriteBigQuery",
    "WriteParquet",
    "WriteJSON",
    "WriteCSV",
    "WriteTFRecord",
    "WriteNumpy",
    "WriteMongo",
    "WriteWebDataset",
    "WriteSQL",
    "WriteCustom",
    # Map
    "Map",
    "MapBatches",
    "Filter",
    "FlatMap",
    # All-to-all
    "RandomizeBlockOrder",
    "RandomShuffle",
    "Repartition",
    "Sort",
    "Aggregate",
    # N-ary
    "Zip",
    "Union",
]


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


def _anonymize_op_name(op: LogicalOperator) -> str:
    """Return an op name suitable for telemetry, anonymized against
    ``_op_name_white_list``. User-defined datasources/datasinks collapse to
    ``ReadCustom``/``WriteCustom``
    """
    if isinstance(op, Read):
        name = f"Read{op.datasource.get_name()}"
        return name if name in _op_name_white_list else "ReadCustom"
    if isinstance(op, Write):
        name = f"Write{op.datasink_or_legacy_datasource.get_name()}"
        return name if name in _op_name_white_list else "WriteCustom"
    name = op.name or ""
    if isinstance(op, AbstractUDFMap):
        # Remove the function name from the map operator name.
        # E.g., Map(<lambda>) -> Map
        name = re.sub(r"\(.*\)$", "", name)
    return name if name in _op_name_white_list else "Unknown"


def _collect_operators_to_dict(op: LogicalOperator, ops_dict: Dict[str, int]):
    """Collect the logical operator name and count into `ops_dict`."""
    for child in op.input_dependencies:
        _collect_operators_to_dict(child, ops_dict)

    op_name = _anonymize_op_name(op)
    ops_dict.setdefault(op_name, 0)
    ops_dict[op_name] += 1
