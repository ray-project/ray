from typing import TYPE_CHECKING, Any, Callable, Dict, List, Union, Iterable, Iterator
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

        for record in _read_records(f):
            example = tf.train.Example()
            try:
                example.ParseFromString(record)
            except DecodeError as e:
                raise ValueError(
                    "`TFRecordDatasource` failed to parse `tf.train.Example` "
                    f"record in '{path}'. This error can occur if your TFRecord "
                    f"file contains a message type other than `tf.train.Example`: {e}"
                )

            yield pa.Table.from_pydict(_convert_example_to_dict(example))

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
) -> Dict[
    str,
    Union[
        List[bytes],
        List[List[bytes]],
        List[float],
        List[List[float]],
        List[int],
        List[List[int]],
    ],
]:
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
) -> Union[List[bytes], List[float], List[int]]:
    values = (
        feature.bytes_list.value,
        feature.float_list.value,
        feature.int64_list.value,
    )
    # Exactly one of `bytes_list`, `float_list`, and `int64_list` should contain data.
    assert sum(bool(value) for value in values) == 1

    if feature.bytes_list.value:
        return list(feature.bytes_list.value)
    if feature.float_list.value:
        return list(feature.float_list.value)
    if feature.int64_list.value:
        return list(feature.int64_list.value)


def _value_to_feature(value: Union[bytes, float, int, List]) -> "tf.train.Feature":
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
