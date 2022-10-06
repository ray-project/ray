import functools
import os
from typing import List, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow

RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION = (
    "RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION"
)
RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION = (
    "RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION"
)


def _register_custom_datasets_serializers(serialization_context):
    try:
        import pyarrow as pa  # noqa: F401
    except ModuleNotFoundError:
        # No pyarrow installed so not using Arrow, so no need for custom serializers.
        return

    # Register all custom serializers required by Datasets.
    _register_arrow_data_serializer(serialization_context)
    _register_arrow_json_readoptions_serializer(serialization_context)
    _register_arrow_json_parseoptions_serializer(serialization_context)


# Register custom Arrow JSON ReadOptions serializer to workaround it not being picklable
# in Arrow < 8.0.0.
def _register_arrow_json_readoptions_serializer(serialization_context):
    if (
        os.environ.get(
            RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION,
            "0",
        )
        == "1"
    ):
        return

    import pyarrow.json as pajson

    serialization_context._register_cloudpickle_serializer(
        pajson.ReadOptions,
        custom_serializer=lambda opts: (opts.use_threads, opts.block_size),
        custom_deserializer=lambda args: pajson.ReadOptions(*args),
    )


def _register_arrow_json_parseoptions_serializer(serialization_context):
    if (
        os.environ.get(
            RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION,
            "0",
        )
        == "1"
    ):
        return

    import pyarrow.json as pajson

    serialization_context._register_cloudpickle_serializer(
        pajson.ParseOptions,
        custom_serializer=lambda opts: (
            opts.explicit_schema,
            opts.newlines_in_values,
            opts.unexpected_field_behavior,
        ),
        custom_deserializer=lambda args: pajson.ParseOptions(*args),
    )


# Register custom Arrow data serializer to work around zero-copy slice pickling bug.
# See https://issues.apache.org/jira/browse/ARROW-10739.
def _register_arrow_data_serializer(serialization_context):
    """Custom reducer for Arrow data that works around a zero-copy slicing pickling
    bug by using the Arrow IPC format for the underlying serialization.

    Background:
        Arrow has both array-level slicing and buffer-level slicing; both are zero-copy,
        but the former has a serialization bug where the entire buffer is serialized
        instead of just the slice, while the latter's serialization works as expected
        and only serializes the slice of the buffer. I.e., array-level slicing doesn't
        propagate the slice down to the buffer when serializing the array.

        All that these copy methods do is, at serialization time, take the array-level
        slicing and translate them to buffer-level slicing, so only the buffer slice is
        sent over the wire instead of the entire buffer.

    See https://issues.apache.org/jira/browse/ARROW-10739.
    """
    import pyarrow as pa

    if os.environ.get(RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION, "0") == "1":
        return

    # Register custom reducer for Arrow Arrays.
    array_types = _get_arrow_array_types()
    for array_type in array_types:
        serialization_context._register_cloudpickle_reducer(
            array_type, _arrow_array_reduce
        )
    # Register custom reducer for Arrow ChunkedArrays.
    serialization_context._register_cloudpickle_reducer(
        pa.ChunkedArray, _arrow_chunkedarray_reduce
    )
    # Register custom reducer for Arrow RecordBatches.
    serialization_context._register_cloudpickle_reducer(
        pa.RecordBatch, _arrow_recordbatch_reduce
    )
    # Register custom reducer for Arrow Tables.
    serialization_context._register_cloudpickle_reducer(pa.Table, _arrow_table_reduce)


def _get_arrow_array_types() -> List[type]:
    """Get all Arrow array types that we want to register a custom serializer for."""
    import pyarrow as pa
    from ray.data.extensions import ArrowTensorArray, ArrowVariableShapedTensorArray

    array_types = [
        pa.lib.NullArray,
        pa.lib.BooleanArray,
        pa.lib.UInt8Array,
        pa.lib.UInt16Array,
        pa.lib.UInt32Array,
        pa.lib.UInt64Array,
        pa.lib.Int8Array,
        pa.lib.Int16Array,
        pa.lib.Int32Array,
        pa.lib.Int64Array,
        pa.lib.Date32Array,
        pa.lib.Date64Array,
        pa.lib.TimestampArray,
        pa.lib.Time32Array,
        pa.lib.Time64Array,
        pa.lib.DurationArray,
        pa.lib.HalfFloatArray,
        pa.lib.FloatArray,
        pa.lib.DoubleArray,
        pa.lib.ListArray,
        pa.lib.LargeListArray,
        pa.lib.MapArray,
        pa.lib.FixedSizeListArray,
        pa.lib.UnionArray,
        pa.lib.BinaryArray,
        pa.lib.StringArray,
        pa.lib.LargeBinaryArray,
        pa.lib.LargeStringArray,
        pa.lib.DictionaryArray,
        pa.lib.FixedSizeBinaryArray,
        pa.lib.Decimal128Array,
        pa.lib.Decimal256Array,
        pa.lib.StructArray,
        pa.lib.ExtensionArray,
        ArrowTensorArray,
        ArrowVariableShapedTensorArray,
    ]
    try:
        array_types.append(pa.lib.MonthDayNanoIntervalArray)
    except AttributeError:
        # MonthDayNanoIntervalArray doesn't exist on older pyarrow versions.
        pass
    return array_types


def _arrow_array_reduce(a: "pyarrow.Array"):
    """Custom reducer for Arrow arrays that works around a zero-copy slicing pickling
    bug by using the Arrow IPC format for the underlying serialization.
    """
    import pyarrow as pa

    batch = pa.RecordBatch.from_arrays([a], [""])
    restore_recordbatch, serialized = _arrow_recordbatch_reduce(batch)

    return functools.partial(_restore_array, restore_recordbatch), serialized


def _restore_array(
    restore_recordbatch: Callable[[bytes], "pyarrow.RecordBatch"], buf: bytes
) -> "pyarrow.Array":
    """Restore a serialized Arrow Array."""
    return restore_recordbatch(buf).column(0)


def _arrow_chunkedarray_reduce(a: "pyarrow.ChunkedArray"):
    """Custom reducer for Arrow ChunkedArrays that works around a zero-copy slicing
    pickling bug by using the Arrow IPC format for the underlying serialization.
    """
    import pyarrow as pa

    table = pa.Table.from_arrays([a], names=[""])
    restore_table, serialized = _arrow_table_reduce(table)
    return functools.partial(_restore_chunkedarray, restore_table), serialized


def _restore_chunkedarray(
    restore_table: Callable[[bytes], "pyarrow.Table"], buf: bytes
) -> "pyarrow.ChunkedArray":
    """Restore a serialized Arrow ChunkedArray."""
    return restore_table(buf).column(0)


def _arrow_recordbatch_reduce(batch: "pyarrow.RecordBatch"):
    """Custom reducer for Arrow RecordBatch that works around a zero-copy slicing
    pickling bug by using the Arrow IPC format for the underlying serialization.
    """
    from pyarrow.ipc import RecordBatchStreamWriter
    from pyarrow.lib import BufferOutputStream

    output_stream = BufferOutputStream()
    with RecordBatchStreamWriter(output_stream, schema=batch.schema) as wr:
        wr.write_batch(batch)
    return _restore_recordbatch, (output_stream.getvalue(),)


def _restore_recordbatch(buf: bytes) -> "pyarrow.RecordBatch":
    """Restore a serialized Arrow RecordBatch."""
    from pyarrow.ipc import RecordBatchStreamReader

    with RecordBatchStreamReader(buf) as reader:
        return reader.read_next_batch()


def _arrow_table_reduce(table: "pyarrow.Table"):
    """Custom reducer for Arrow Table that works around a zero-copy slicing pickling
    bug by using the Arrow IPC format for the underlying serialization.
    """
    from pyarrow.ipc import RecordBatchStreamWriter
    from pyarrow.lib import BufferOutputStream

    output_stream = BufferOutputStream()
    with RecordBatchStreamWriter(output_stream, schema=table.schema) as wr:
        wr.write_table(table)
    return _restore_table, (output_stream.getvalue(),)


def _restore_table(buf: bytes) -> "pyarrow.Table":
    """Restore a serialized Arrow Table."""
    from pyarrow.ipc import RecordBatchStreamReader

    with RecordBatchStreamReader(buf) as reader:
        return reader.read_all()
