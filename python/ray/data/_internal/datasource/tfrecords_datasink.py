from typing import TYPE_CHECKING, Optional

from ray.data._internal.util import _check_import
from ray.data.block import BlockAccessor
from ray.data.datasource.file_datasink import BlockBasedFileDatasink
from ray.data.datasource.tfrecords_datasource import _write_record

if TYPE_CHECKING:
    import pyarrow
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
    arrow_table: pyarrow.Table,
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
