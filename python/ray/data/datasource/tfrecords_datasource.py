<<<<<<< Updated upstream
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Union, Iterable, Iterator
=======
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Optional,
    Union,
    Iterable,
    Iterator,
    List
)
>>>>>>> Stashed changes
import struct

import numpy as np

from ray.util.annotations import PublicAPI
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow
    import tensorflow as tf


@PublicAPI(stability="alpha")
class TFRecordDatasource(FileBasedDatasource):

    _FILE_EXTENSION = "tfrecords"

    def _read_stream(
        self, f: "pyarrow.NativeFile", path: str, **reader_args
    ) -> Iterator[Block]:
        from google.protobuf.message import DecodeError
        import pyarrow as pa
        import tensorflow as tf

        tf_schema: Optional["schema_pb2.Schema"] = reader_args.get("tf_schema", None)

        batched_example_dicts = []
        example_batch_size = 256 * 8
        for record in _read_records(f, path):
            # if len(batched_example_dicts) == example_batch_size:
            #     yield pa.Table.from_pylist(batched_example_dicts)
            #     batched_example_dicts = []
            # else:
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
            batched_example_dicts.append(record_dict)
        yield pa.Table.from_pylist(batched_example_dicts)
            # yield pa.Table.from_pydict(_convert_example_to_dict(example, tf_schema))
            # yield [_convert_example_to_dict(example, tf_schema)] # 50s
            

    def _write_block(
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor,
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **writer_args,
    ) -> None:

        _check_import(self, module="crc32c", package="crc32c")

        arrow_table = block.to_arrow()

        # It seems like TFRecords are typically row-based,
        # https://www.tensorflow.org/tutorials/load_data/tfrecord#writing_a_tfrecord_file_2
        # so we must iterate through the rows of the block,
        # serialize to tf.train.Example proto, and write to file.

        examples = _convert_arrow_table_to_examples(arrow_table)

        # Write each example to the arrow file in the TFRecord format.
        for example in examples:
            _write_record(f, example)


def _convert_example_to_dict(
    example: "tf.train.Example",
    tf_schema: Optional["schema_pb2.Schema"],
) -> Dict[str, Union[List[Union[int, float, bytes]], Union[int, float, bytes]]]:
    record = {}
    for feature_name, feature in example.features.feature.items():
        value = _get_feature_value(feature)
        # Return value itself if the list has single value.
        # This is to give better user experience when writing preprocessing UDF on
        # these single-value lists.
        if len(value) == 1:
            value = value[0]
        record[feature_name] = [value]
    return record


def _convert_arrow_table_to_examples(
    arrow_table: "pyarrow.Table",
) -> Iterable["tf.train.Example"]:
    import tensorflow as tf

    # Serialize each row[i] of the block to a tf.train.Example and yield it.
    for i in range(arrow_table.num_rows):

        # First, convert row[i] to a dictionary.
        features: Dict[str, "tf.train.Feature"] = {}
        for name in arrow_table.column_names:
            features[name] = _value_to_feature(arrow_table[name][i].as_py())

        # Convert the dictionary to an Example proto.
        proto = tf.train.Example(features=tf.train.Features(feature=features))

        yield proto


def _get_feature_value(
    feature: "tf.train.Feature",
    schema_feature_type: Optional["schema_pb2.FeatureType"] = None,
)  -> Union[List[Union[int, float, bytes]], Union[int, float, bytes]]:
    import pyarrow as pa

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
        type_ = pa.binary()
    elif underlying_feature_type["float"]:
        value = feature.float_list.value
        type_ = pa.float32()
    elif underlying_feature_type["int"]:
        value = feature.int64_list.value
        type_ = pa.int64()
    else:
        value = []
        type_ = pa.null()
    value = list(value)
    if len(value) == 1 and schema_feature_type is None:
        # Use the value itself if the features contains a single value.
        # This is to give better user experience when writing preprocessing UDF on
        # these single-value lists.
        value = value[0]
    else:
        # If the feature value is empty and no type is specified in the user-provided
        # schema, set the type to null for now to allow pyarrow to construct a valid
        # Array; later, infer the type from other records which have non-empty values
        # for the feature.
        if len(value) == 0:
            type_ = pa.null()
        type_ = pa.list_(type_)
    return value


def _value_to_feature(
    value: Union["pyarrow.Scalar", "pyarrow.Array"],
    schema_feature_type: Optional["schema_pb2.FeatureType"] = None,
) -> "tf.train.Feature":
    import tensorflow as tf

    # A Feature stores a list of values.
    # If we have a single value, convert it to a singleton list first.
    values = [value] if not isinstance(value, list) else value

    if not values:
        raise ValueError(
            "Storing an empty value in a tf.train.Feature is not supported."
        )
    elif isinstance(values[0], bytes):
        return tf.train.Feature(bytes_list=tf.train.BytesList(value=values))
    elif isinstance(values[0], float):
        return tf.train.Feature(float_list=tf.train.FloatList(value=values))
    elif isinstance(values[0], int):
        return tf.train.Feature(int64_list=tf.train.Int64List(value=values))
    else:
        raise ValueError(
            f"Value is of type {type(values[0])}, "
            "which is not a supported tf.train.Feature storage type "
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
) -> Iterable[memoryview]:
    length_bytes = bytearray(8)
    crc_bytes = bytearray(4)
    datum_bytes = bytearray(1024 * 1024)
    while True:
        num_length_bytes_read = file.readinto(length_bytes)
        if num_length_bytes_read == 0:
            break
        elif num_length_bytes_read != 8:
            raise ValueError("Failed to read the record size.")
        if file.readinto(crc_bytes) != 4:
            raise ValueError("Failed to read the start token.")
        (length,) = struct.unpack("<Q", length_bytes)
        if length > len(datum_bytes):
            datum_bytes = datum_bytes.zfill(int(length * 1.5))
        datum_bytes_view = memoryview(datum_bytes)[:length]
        if file.readinto(datum_bytes_view) != length:
            raise ValueError("Failed to read the record.")
        if file.readinto(crc_bytes) != 4:
            raise ValueError("Failed to read the end token.")
        yield datum_bytes_view


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
