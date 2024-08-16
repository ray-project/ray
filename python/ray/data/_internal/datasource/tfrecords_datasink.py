import struct
from typing import TYPE_CHECKING, Dict, Iterable, Optional, Union

import numpy as np

from .tfrecords_datasource import _get_single_true_type
from ray.data._internal.util import _check_import
from ray.data.block import BlockAccessor
from ray.data.datasource.file_datasink import BlockBasedFileDatasink

if TYPE_CHECKING:
    import pyarrow
    import tensorflow as tf
    from tensorflow_metadata.proto.v0 import schema_pb2


class TFRecordDatasink(BlockBasedFileDatasink):
    def __init__(
        self,
        path: str,
        *,
        tf_schema: Optional["schema_pb2.Schema"] = None,
        file_format: str = "tar",
        **file_datasink_kwargs,
    ):
        super().__init__(path, file_format=file_format, **file_datasink_kwargs)

        _check_import(self, module="crc32c", package="crc32c")

        self.tf_schema = tf_schema

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        arrow_table = block.to_arrow()

        # It seems like TFRecords are typically row-based,
        # https://www.tensorflow.org/tutorials/load_data/tfrecord#writing_a_tfrecord_file_2
        # so we must iterate through the rows of the block,
        # serialize to tf.train.Example proto, and write to file.

        examples = _convert_arrow_table_to_examples(arrow_table, self.tf_schema)

        # Write each example to the arrow file in the TFRecord format.
        for example in examples:
            _write_record(file, example)


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


def _value_to_feature(
    value: Union["pyarrow.Scalar", "pyarrow.Array"],
    schema_feature_type: Optional["schema_pb2.FeatureType"] = None,
) -> "tf.train.Feature":
    import pyarrow as pa
    import tensorflow as tf

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
        "string": pa.types.is_string(value_type),
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
            "bytes": schema_feature_type == schema_pb2.FeatureType.BYTES
            and not underlying_value_type["string"],
            "string": schema_feature_type == schema_pb2.FeatureType.BYTES
            and underlying_value_type["string"],
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
    if underlying_value_type["string"]:
        value = [v.encode() for v in value]  # casting to bytes
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
