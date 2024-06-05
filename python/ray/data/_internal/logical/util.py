import json
import re
import threading
from typing import Dict

from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractUDFMap
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.write_operator import Write

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


def _collect_operators_to_dict(op: LogicalOperator, ops_dict: Dict[str, int]):
    """Collect the logical operator name and count into `ops_dict`."""
    for child in op.input_dependencies:
        _collect_operators_to_dict(child, ops_dict)

    op_name = op.name

    # Check read and write operator, and anonymize user-defined data source.
    if isinstance(op, Read):
        op_name = f"Read{op._datasource.get_name()}"
        if op_name not in _op_name_white_list:
            op_name = "ReadCustom"
    elif isinstance(op, Write):
        op_name = f"Write{op._datasink_or_legacy_datasource.get_name()}"
        if op_name not in _op_name_white_list:
            op_name = "WriteCustom"
    elif isinstance(op, AbstractUDFMap):
        # Remove the function name from the map operator name.
        # E.g., Map(<lambda>) -> Map
        op_name = re.sub("\\(.*\\)$", "", op_name)

    # Anonymize any operator name if not in white list.
    if op_name not in _op_name_white_list:
        op_name = "Unknown"

    ops_dict.setdefault(op_name, 0)
    ops_dict[op_name] += 1
