from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Optional,
    Union,
    Iterable,
    Iterator,
    List,
)
import struct

import numpy as np

from ray.util.annotations import PublicAPI
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow
    import tensorflow as tf
    from tensorflow_metadata.proto.v0 import schema_pb2

TF_RECORD_VALUE_TYPE = Union[int, float, bytes]


@PublicAPI(stability="alpha")
class TFRecordDatasource(FileBasedDatasource):
    """TFRecord datasource, for reading and writing TFRecord files."""

    _FILE_EXTENSION = "tfrecords"

    def _read_stream(
        self, f: "pyarrow.NativeFile", path: str, **reader_args
    ) -> Iterator[Block]:
        import tensorflow as tf

        tf_schema: Optional["schema_pb2.Schema"] = reader_args.get("tf_schema", None)
        batch_size = reader_args.get("batch_size", 2048)
        compression = reader_args.get("arrow_open_stream_args", {}).get(
            "compression", None
        )

        if tf_schema:
            try:
                yield from self._fast_read_stream(
                    path, tf_schema, batch_size, compression
                )
                return
            except ModuleNotFoundError as e:
                print(f"Failed to import tfx_bsl; falling back to slow read: {e}")
            except tf.errors.DataLossError as e:
                print(
                    f"Error when batch parsing TFRecords file with provided schema; "
                    f"falling back to slow read: {e}"
                )
        yield from self._slow_read_stream(f, path, tf_schema, batch_size)

    def _fast_read_stream(
        self,
        path: str,
        tf_schema: "schema_pb2.Schema",
        batch_size: int = 2048,
        compression: Optional[str] = None,
    ):
        try:
            from tfx_bsl.cc.tfx_bsl_extension.coders import ExamplesToRecordBatchDecoder
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "To use CustomTFRecordDatasource, please install "
                "tfx-bsl package with `pip install tfx-bsl`."
            )
        import pyarrow as pa
        import tensorflow as tf

        if compression:
            compression = compression.upper()

        decoder = ExamplesToRecordBatchDecoder(tf_schema.SerializeToString())
        for record in tf.data.TFRecordDataset(path, compression_type=compression).batch(
            batch_size
        ):
            yield pa.Table.from_batches([decoder.DecodeBatch(record.numpy())])

    def _slow_read_stream(
        self,
        f: "pyarrow.NativeFile",
        path: str,
        tf_schema: Optional["schema_pb2.Schema"],
        batch_size=2048,
    ):
        from google.protobuf.message import DecodeError
        import pyarrow as pa
        import tensorflow as tf

        batched_example_dicts = []
        all_column_names = set()

        def _unwrap_single_value_list_columns(batched_example_dicts):
            # For a given list of dicts (representing a batch of examples),
            # If a column contains at LEAST one single-value list,
            # we unwrap the list to use the single value (or None in
            # the case of an empty list).
            col_is_all_single_value_lists = {
                col_name: True for col_name in all_column_names
            }
            col_has_single_value_list = {
                col_name: False for col_name in all_column_names
            }
            for dct in batched_example_dicts:
                for col_name, col_val in dct.items():
                    if isinstance(col_val, list) and len(col_val) > 1:
                        col_is_all_single_value_lists[col_name] = False
                    if isinstance(col_val, list) and len(col_val) == 1:
                        col_has_single_value_list[col_name] = True
            columns_to_unwrap = [
                c
                for c in col_is_all_single_value_lists
                if col_is_all_single_value_lists[c] and col_has_single_value_list[c]
            ]

            for col_unwrap in columns_to_unwrap:
                for idx, dct in enumerate(batched_example_dicts):
                    if col_unwrap in batched_example_dicts[idx]:
                        if len(batched_example_dicts[idx][col_unwrap]) == 1:
                            # If value is a single-element list, unwrap the list
                            batched_example_dicts[idx][col_unwrap] = dct[col_unwrap][0]
                        else:
                            # If value is an empty list, unwrap to `None`
                            batched_example_dicts[idx][col_unwrap] = None
            return batched_example_dicts

        for record in _read_records(f, path):
            example = tf.train.Example()
            try:
                example.ParseFromString(record)
            except DecodeError as e:
                raise ValueError(
                    "`TFRecordDatasource` failed to parse `tf.train.Example` "
                    f"record in '{path}'. This error can occur if your TFRecord "
                    f"file contains a message type other than `tf.train.Example`: {e}"
                )
            record_dict = _convert_example_to_dict(example, tf_schema)
            for feature_name in record_dict:
                all_column_names.add(feature_name)
            batched_example_dicts.append(record_dict)
            if len(batched_example_dicts) >= batch_size:
                # If a schema is specified, skip single-value list unwrapping
                if not tf_schema:
                    batched_example_dicts = _unwrap_single_value_list_columns(
                        batched_example_dicts
                    )
                yield pa.Table.from_pylist(batched_example_dicts)
                batched_example_dicts = []

        # Yield the remaining records, skipping single-value list unwrapping if
        # a schema is specified.
        if not tf_schema:
            batched_example_dicts = _unwrap_single_value_list_columns(
                batched_example_dicts
            )
        yield pa.Table.from_pylist(batched_example_dicts)

    def _write_block(
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor,
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        tf_schema: Optional["schema_pb2.Schema"] = None,
        **writer_args,
    ) -> None:
        _check_import(self, module="crc32c", package="crc32c")

        arrow_table = block.to_arrow()

        # It seems like TFRecords are typically row-based,
        # https://www.tensorflow.org/tutorials/load_data/tfrecord#writing_a_tfrecord_file_2
        # so we must iterate through the rows of the block,
        # serialize to tf.train.Example proto, and write to file.

        examples = _convert_arrow_table_to_examples(arrow_table, tf_schema)

        # Write each example to the arrow file in the TFRecord format.
        for example in examples:
            _write_record(f, example)


def _convert_example_to_dict(
    example: "tf.train.Example",
    tf_schema: Optional["schema_pb2.Schema"],
) -> Dict[str, Union[List[TF_RECORD_VALUE_TYPE], TF_RECORD_VALUE_TYPE]]:
    record = {}
    schema_dict = {}
    # Convert user-specified schema into dict for convenient mapping
    if tf_schema is not None:
        for schema_feature in tf_schema.feature:
            schema_dict[schema_feature.name] = schema_feature.type

    for feature_name, feature in example.features.feature.items():
        if tf_schema is not None and feature_name not in schema_dict:
            raise ValueError(
                f"Found extra unexpected feature {feature_name} "
                f"not in specified schema: {tf_schema}"
            )
        schema_feature_type = schema_dict.get(feature_name)
        record[feature_name] = _get_feature_value(feature, schema_feature_type)
    return record


def _convert_arrow_table_to_examples(
    arrow_table: "pyarrow.Table",
    tf_schema: Optional["schema_pb2.Schema"] = None,
) -> Iterable["tf.train.Example"]:
    import tensorflow as tf

    schema_dict = {}
    # Convert user-specified schema into dict for convenient mapping
    if tf_schema is not None:
        for schema_feature in tf_schema.feature:
            schema_dict[schema_feature.name] = schema_feature.type

    # Serialize each row[i] of the block to a tf.train.Example and yield it.
    for i in range(arrow_table.num_rows):
        # First, convert row[i] to a dictionary.
        features: Dict[str, "tf.train.Feature"] = {}
        for name in arrow_table.column_names:
            if tf_schema is not None and name not in schema_dict:
                raise ValueError(
                    f"Found extra unexpected feature {name} "
                    f"not in specified schema: {tf_schema}"
                )
            schema_feature_type = schema_dict.get(name)
            features[name] = _value_to_feature(
                arrow_table[name][i],
                schema_feature_type,
            )

        # Convert the dictionary to an Example proto.
        proto = tf.train.Example(features=tf.train.Features(feature=features))

        yield proto


def _get_single_true_type(dct) -> str:
    """Utility function for getting the single key which has a `True` value in
    a dict. Used to filter a dict of `{field_type: is_valid}` to get
    the field type from a schema or data source."""
    filtered_types = iter([_type for _type in dct if dct[_type]])
    # In the case where there are no keys with a `True` value, return `None`
    return next(filtered_types, None)


def _get_feature_value(
    feature: "tf.train.Feature",
    schema_feature_type: Optional["schema_pb2.FeatureType"] = None,
) -> Union[List[TF_RECORD_VALUE_TYPE], TF_RECORD_VALUE_TYPE]:
    underlying_feature_type = {
        "bytes": feature.HasField("bytes_list"),
        "float": feature.HasField("float_list"),
        "int": feature.HasField("int64_list"),
    }
    # At most one of `bytes_list`, `float_list`, and `int64_list`
    # should contain values. If none contain data, this indicates
    # an empty feature value.
    assert sum(bool(value) for value in underlying_feature_type.values()) <= 1
    if schema_feature_type is not None:
        try:
            from tensorflow_metadata.proto.v0 import schema_pb2
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "To use TensorFlow schemas, please install "
                "the tensorflow-metadata package."
            )
        # If a schema is specified, compare to the underlying type
        specified_feature_type = {
            "bytes": schema_feature_type == schema_pb2.FeatureType.BYTES,
            "float": schema_feature_type == schema_pb2.FeatureType.FLOAT,
            "int": schema_feature_type == schema_pb2.FeatureType.INT,
        }
        und_type = _get_single_true_type(underlying_feature_type)
        spec_type = _get_single_true_type(specified_feature_type)
        if und_type is not None and und_type != spec_type:
            raise ValueError(
                "Schema field type mismatch during read: specified type is "
                f"{spec_type}, but underlying type is {und_type}",
            )
        # Override the underlying value type with the type in the user-specified schema.
        underlying_feature_type = specified_feature_type
    if underlying_feature_type["bytes"]:
        value = feature.bytes_list.value
    elif underlying_feature_type["float"]:
        value = feature.float_list.value
    elif underlying_feature_type["int"]:
        value = feature.int64_list.value
    else:
        value = []
    value = list(value)
    return value


def _value_to_feature(
    value: Union["pyarrow.Scalar", "pyarrow.Array"],
    schema_feature_type: Optional["schema_pb2.FeatureType"] = None,
) -> "tf.train.Feature":
    import tensorflow as tf
    import pyarrow as pa

    if isinstance(value, pa.ListScalar):
        # Use the underlying type of the ListScalar's value in
        # determining the output feature's data type.
        value_type = value.type.value_type
        value = value.as_py()
    else:
        value_type = value.type
        value = value.as_py()
        if value is None:
            value = []
        else:
            value = [value]

    underlying_value_type = {
        "bytes": pa.types.is_binary(value_type),
        "float": pa.types.is_floating(value_type),
        "int": pa.types.is_integer(value_type),
    }
    assert sum(bool(value) for value in underlying_value_type.values()) <= 1

    if schema_feature_type is not None:
        try:
            from tensorflow_metadata.proto.v0 import schema_pb2
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "To use TensorFlow schemas, please install "
                "the tensorflow-metadata package."
            )
        specified_feature_type = {
            "bytes": schema_feature_type == schema_pb2.FeatureType.BYTES,
            "float": schema_feature_type == schema_pb2.FeatureType.FLOAT,
            "int": schema_feature_type == schema_pb2.FeatureType.INT,
        }

        und_type = _get_single_true_type(underlying_value_type)
        spec_type = _get_single_true_type(specified_feature_type)
        if und_type is not None and und_type != spec_type:
            raise ValueError(
                "Schema field type mismatch during write: specified type is "
                f"{spec_type}, but underlying type is {und_type}",
            )
        # Override the underlying value type with the type in the user-specified schema.
        underlying_value_type = specified_feature_type

    if underlying_value_type["int"]:
        return tf.train.Feature(int64_list=tf.train.Int64List(value=value))
    if underlying_value_type["float"]:
        return tf.train.Feature(float_list=tf.train.FloatList(value=value))
    if underlying_value_type["bytes"]:
        return tf.train.Feature(bytes_list=tf.train.BytesList(value=value))
    if pa.types.is_null(value_type):
        raise ValueError(
            "Unable to infer type from partially missing column. "
            "Try setting read parallelism = 1, or use an input data source which "
            "explicitly specifies the schema."
        )
    raise ValueError(
        f"Value is of type {value_type}, "
        "which we cannot convert to a supported tf.train.Feature storage type "
        "(bytes, float, or int)."
    )


# Adapted from https://github.com/vahidk/tfrecord/blob/74b2d24a838081356d993ec0e147eaf59ccd4c84/tfrecord/reader.py#L16-L96  # noqa: E501
#
# MIT License
#
# Copyright (c) 2020 Vahid Kazemi
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
def _read_records(
    file: "pyarrow.NativeFile",
    path: str,
) -> Iterable[memoryview]:
    """
    Read records from TFRecord file.

    A TFRecord file contains a sequence of records. The file can only be read
    sequentially. Each record is stored in the following formats:
        uint64 length
        uint32 masked_crc32_of_length
        byte   data[length]
        uint32 masked_crc32_of_data

    See https://www.tensorflow.org/tutorials/load_data/tfrecord#tfrecords_format_details
    for more details.
    """
    length_bytes = bytearray(8)
    crc_bytes = bytearray(4)
    datum_bytes = bytearray(1024 * 1024)
    row_count = 0
    while True:
        try:
            # Read "length" field.
            num_length_bytes_read = file.readinto(length_bytes)
            if num_length_bytes_read == 0:
                break
            elif num_length_bytes_read != 8:
                raise ValueError(
                    "Failed to read the length of record data. Expected 8 bytes but "
                    "got {num_length_bytes_read} bytes."
                )

            # Read "masked_crc32_of_length" field.
            num_length_crc_bytes_read = file.readinto(crc_bytes)
            if num_length_crc_bytes_read != 4:
                raise ValueError(
                    "Failed to read the length of CRC-32C hashes. Expected 4 bytes "
                    "but got {num_length_crc_bytes_read} bytes."
                )

            # Read "data[length]" field.
            (data_length,) = struct.unpack("<Q", length_bytes)
            if data_length > len(datum_bytes):
                datum_bytes = datum_bytes.zfill(int(data_length * 1.5))
            datum_bytes_view = memoryview(datum_bytes)[:data_length]
            num_datum_bytes_read = file.readinto(datum_bytes_view)
            if num_datum_bytes_read != data_length:
                raise ValueError(
                    f"Failed to read the record. Exepcted {data_length} bytes but got "
                    f"{num_datum_bytes_read} bytes."
                )

            # Read "masked_crc32_of_data" field.
            # TODO(chengsu): ideally we should check CRC-32C against the actual data.
            num_crc_bytes_read = file.readinto(crc_bytes)
            if num_crc_bytes_read != 4:
                raise ValueError(
                    "Failed to read the CRC-32C hashes. Expected 4 bytes but got "
                    f"{num_crc_bytes_read} bytes."
                )

            # Return the data.
            yield datum_bytes_view

            row_count += 1
            data_length = None
        except Exception as e:
            error_message = (
                f"Failed to read TFRecord file {path}. Please ensure that the "
                f"TFRecord file has correct format. Already read {row_count} rows."
            )
            if data_length is not None:
                error_message += f" Byte size of current record data is {data_length}."
            raise RuntimeError(error_message) from e


# Adapted from https://github.com/vahidk/tfrecord/blob/74b2d24a838081356d993ec0e147eaf59ccd4c84/tfrecord/writer.py#L57-L72  # noqa: E501
#
# MIT License
#
# Copyright (c) 2020 Vahid Kazemi
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


def _write_record(
    file: "pyarrow.NativeFile",
    example: "tf.train.Example",
) -> None:
    record = example.SerializeToString()
    length = len(record)
    length_bytes = struct.pack("<Q", length)
    file.write(length_bytes)
    file.write(_masked_crc(length_bytes))
    file.write(record)
    file.write(_masked_crc(record))


def _masked_crc(data: bytes) -> bytes:
    """CRC checksum."""
    import crc32c

    mask = 0xA282EAD8
    crc = crc32c.crc32(data)
    masked = ((crc >> 15) | (crc << 17)) + mask
    masked = np.uint32(masked & np.iinfo(np.uint32).max)
    masked_bytes = struct.pack("<I", masked)
    return masked_bytes
