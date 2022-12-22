import os
from typing import TYPE_CHECKING

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

        We work around this by registering a custom cloudpickle reducers for Arrow
        Tables that delegates serialization to the Arrow IPC format; thankfully, Arrow's
        IPC serialization has fixed this buffer truncation bug.

    See https://issues.apache.org/jira/browse/ARROW-10739.
    """
    import pyarrow as pa

    if os.environ.get(RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION, "0") == "1":
        return

    # Register custom reducer for Arrow Tables.
    serialization_context._register_cloudpickle_reducer(pa.Table, _arrow_table_reduce)


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
