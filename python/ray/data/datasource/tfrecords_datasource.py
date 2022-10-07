from typing import TYPE_CHECKING, Dict, List, Union, Iterable, Iterator
import struct

from ray.util.annotations import PublicAPI
from ray.data.block import Block
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
        import pandas as pd
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

            yield pd.DataFrame([_convert_example_to_dict(example)])


def _convert_example_to_dict(
    example: "tf.train.Example",
) -> Dict[str, Union[bytes, List[bytes], float, List[float], int, List[int]]]:
    record = {}
    for feature_name, feature in example.features.feature.items():
        value = _get_feature_value(feature)
        if len(value) == 1:
            value = value[0]
        record[feature_name] = value
    return record


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


# Adapted from https://github.com/vahidk/tfrecord/blob/master/tfrecord/reader.py#L16-L96
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
