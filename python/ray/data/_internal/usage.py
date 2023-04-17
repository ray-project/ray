from typing import Dict, TYPE_CHECKING
import json
import threading

from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

if TYPE_CHECKING:
    from ray.data._internal.logical.interfaces import LogicalOperator

# Guards the below dicts.
_recording_lock = threading.Lock()
# The dictionary for the operator name and count.
_recorded_operators = dict()
# The dictionary for the block format name and count.
_recorded_block_formats = dict()

# The white list of operator names allowed to be recorded.
_op_name_white_list = [
    # Read
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
    "ReadCustom",
    # From
    "FromItems",
    "FromPandasRefs",
    "FromHuggingFace",
    "FromDask",
    "FromModin",
    "FromMars",
    "FromNumpyRefs",
    "FromArrowRefs",
    # Write
    "WriteParquet",
    "WriteJSON",
    "WriteCSV",
    "WriteTFRecord",
    "WriteNumpy",
    "WriteMongo",
    "WriteCustom",
    # Map
    "MapBatches",
    "MapRows",
    "Filter",
    "FlatMap",
    # All-to-all
    "RandomizeBlocks",
    "RandomShuffle",
    "Repartition",
    "Sort",
    "Aggregate",
    # N-ary
    "Zip",
]


def record_block_format_usage(block_format: str):
    with _recording_lock:
        _recorded_block_formats.setdefault(block_format, 0)
        _recorded_block_formats[block_format] += 1
        formats_json_str = json.dumps(_recorded_block_formats)

    record_extra_usage_tag(TagKey.DATA_BLOCK_FORMATS, formats_json_str)


def record_operators_usage(op: "LogicalOperator"):
    """Record logical operator usage with Ray telemetry."""
    ops_dict = dict()
    _collect_operators_to_dict(op, ops_dict)
    ops_json_str = ""
    with _recording_lock:
        for op, count in ops_dict.items():
            _recorded_operators.setdefault(op, 0)
            _recorded_operators[op] += count
        ops_json_str = json.dumps(_recorded_operators)

    record_extra_usage_tag(TagKey.DATA_LOGICAL_OPS, ops_json_str)


def _collect_operators_to_dict(op: "LogicalOperator", ops_dict: Dict[str, int]):
    """Collect the logical operator name and count into `ops_dict`."""
    from ray.data._internal.logical.operators.read_operator import Read
    from ray.data._internal.logical.operators.write_operator import Write

    for child in op.input_dependencies:
        _collect_operators_to_dict(child, ops_dict)

    op_name = op.name

    # Check read and write operator, and anonymize user-defined data source.
    if isinstance(op, Read):
        op_name = f"Read{op._datasource.get_name()}"
        if op_name not in _op_name_white_list:
            op_name = "ReadCustom"
    elif isinstance(op, Write):
        op_name = f"Write{op._datasource.get_name()}"
        if op_name not in _op_name_white_list:
            op_name = "WriteCustom"

    # Anonymize any operator name if not in white list.
    if op_name not in _op_name_white_list:
        op_name = "Unknown"

    ops_dict.setdefault(op_name, 0)
    ops_dict[op_name] += 1
