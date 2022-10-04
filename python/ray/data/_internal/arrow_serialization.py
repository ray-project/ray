import logging
import os
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow

RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION = (
    "RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION"
)
RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION = (
    "RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION"
)

logger = logging.getLogger(__name__)

# Whether we have already warned the user about bloated fallback serialization.
_serialization_fallback_set = set()

# Whether we're currently running in a test, either local or CI.
_in_test = None


def _is_in_test():
    global _in_test

    if _in_test is None:
        _in_test = any(
            env_var in os.environ
            # These environment variables are always set by pytest and Buildkite,
            # respectively.
            for env_var in ("PYTEST_CURRENT_TEST", "BUILDKITE")
        )
    return _in_test


def _register_custom_serializers():
    # Register all custom serializers required by Datasets.
    _register_arrow_data_serializer()
    _register_arrow_json_readoptions_serializer()
    _register_arrow_json_parseoptions_serializer()


# Register custom Arrow JSON ReadOptions serializer to workaround it not being picklable
# in Arrow < 8.0.0.
def _register_arrow_json_readoptions_serializer():
    import ray

    if (
        os.environ.get(
            RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION,
            "0",
        )
        == "1"
    ):
        import logging

        logger = logging.getLogger(__name__)
        logger.info("Disabling custom Arrow JSON ReadOptions serialization.")
        return

    try:
        import pyarrow.json as pajson
    except ModuleNotFoundError:
        return

    ray.util.register_serializer(
        pajson.ReadOptions,
        serializer=lambda opts: (opts.use_threads, opts.block_size),
        deserializer=lambda args: pajson.ReadOptions(*args),
    )


def _register_arrow_json_parseoptions_serializer():
    import ray

    if (
        os.environ.get(
            RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION,
            "0",
        )
        == "1"
    ):
        import logging

        logger = logging.getLogger(__name__)
        logger.info("Disabling custom Arrow JSON ParseOptions serialization.")
        return

    try:
        import pyarrow.json as pajson
    except ModuleNotFoundError:
        return

    ray.util.register_serializer(
        pajson.ParseOptions,
        serializer=lambda opts: (
            opts.explicit_schema,
            opts.newlines_in_values,
            opts.unexpected_field_behavior,
        ),
        deserializer=lambda args: pajson.ParseOptions(*args),
    )


# Register custom Arrow data serializer to work around zero-copy slice pickling bug.
# See https://issues.apache.org/jira/browse/ARROW-10739.
def _register_arrow_data_serializer():
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
    import ray
    import pyarrow as pa

    if os.environ.get(RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION) == "1":
        import logging

        logger = logging.getLogger(__name__)
        logger.info(
            "Disabling custom Arrow data serialization. This may result in bloated "
            "serialization of Arrow tables!"
        )
        return

    context = ray._private.worker.global_worker.get_serialization_context()
    # Register custom reducer for Arrow Arrays.
    array_types = _get_arrow_array_types()
    for array_type in array_types:
        context._register_cloudpickle_reducer(array_type, _arrow_array_reduce)
    # Register custom reducer for Arrow ChunkedArrays.
    context._register_cloudpickle_reducer(pa.ChunkedArray, _arrow_chunkedarray_reduce)
    # Register custom reducer for Arrow RecordBatches.
    context._register_cloudpickle_reducer(pa.RecordBatch, _arrow_recordbatch_reduce)
    # Register custom reducer for Arrow Tables.
    context._register_cloudpickle_reducer(pa.Table, _arrow_table_reduce)


def _get_arrow_array_types() -> List[type]:
    """Get all Arrow array types that we want to register a custom serializer for."""
    try:
        import pyarrow as pa
    except ModuleNotFoundError:
        # No pyarrow installed so not using Arrow, so no need for custom serializer.
        return []

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
    from pyarrow.ipc import RecordBatchStreamWriter
    from pyarrow.lib import RecordBatch, BufferOutputStream

    batch = RecordBatch.from_arrays([a], [""])
    output_stream = BufferOutputStream()
    with RecordBatchStreamWriter(output_stream, schema=batch.schema) as wr:
        wr.write_batch(batch)
    return _restore_array, (output_stream.getvalue(),)


def _restore_array(buf: bytes) -> "pyarrow.Array":
    from pyarrow.ipc import RecordBatchStreamReader

    with RecordBatchStreamReader(buf) as reader:
        return reader.read_next_batch().column(0)


def _arrow_chunkedarray_reduce(a: "pyarrow.ChunkedArray"):
    """Custom reducer for Arrow ChunkedArrays that works around a zero-copy slicing
    pickling bug by using the Arrow IPC format for the underlying serialization.
    """
    import pyarrow as pa
    from pyarrow.ipc import RecordBatchStreamWriter
    from pyarrow.lib import RecordBatch, BufferOutputStream

    # Convert chunked array to contiguous array.
    if a.num_chunks == 0:
        a = pa.array([], type=a.type)
    elif isinstance(a.type, pa.ExtensionType):
        chunk = a.chunk(0)
        a = type(chunk).from_storage(
            chunk.type, pa.concat_arrays([c.storage for c in a.chunks])
        )
    else:
        a = a.combine_chunks()

    batch = RecordBatch.from_arrays([a], [""])
    output_stream = BufferOutputStream()
    with RecordBatchStreamWriter(output_stream, schema=batch.schema) as wr:
        wr.write_batch(batch)
    return _restore_chunked_array, (output_stream.getvalue(),)


def _restore_chunked_array(buf: bytes) -> "pyarrow.ChunkedArray":
    import pyarrow as pa
    from pyarrow.ipc import RecordBatchStreamReader

    with RecordBatchStreamReader(buf) as reader:
        return pa.chunked_array([reader.read_next_batch().column(0)])


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
    from pyarrow.ipc import RecordBatchStreamReader

    with RecordBatchStreamReader(buf) as reader:
        return reader.read_all()
