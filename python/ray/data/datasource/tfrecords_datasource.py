import logging
import struct
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Union

import numpy as np
import pyarrow

from ray.data.aggregate import AggregateFn
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pandas as pd
    import tensorflow as tf
    from tensorflow_metadata.proto.v0 import schema_pb2

    from ray.data.dataset import Dataset


# Default batch size to be used when using TFX BSL for reading tfrecord files
# Ray will use this parameter by default to read the tf.examples in batches.
DEFAULT_BATCH_SIZE = 2048

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
@dataclass
class TFXReadOptions:
    """
    Specifies read options when reading TFRecord files with TFX.
    """

    # An int representing the number of consecutive elements of
    # this dataset to combine in a single batch when tfx-bsl is used to read
    # the tfrecord files.
    batch_size: int = DEFAULT_BATCH_SIZE

    # Toggles the schema inference applied; applicable
    # only if tfx-bsl is used and tf_schema argument is missing.
    # Defaults to True.
    auto_infer_schema: bool = True


def _convert_example_to_dict(
    example: "tf.train.Example",
    tf_schema: Optional["schema_pb2.Schema"],
) -> Dict[str, pyarrow.Array]:
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
) -> pyarrow.Array:
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
    return pa.array([value], type=type_)


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


def _cast_large_list_to_list(batch: pyarrow.Table):
    """
    This function transform pyarrow.large_list into list and pyarrow.large_binary into
    pyarrow.binary so that all types resulting from the tfrecord_datasource are usable
    with dataset.to_tf().
    """
    old_schema = batch.schema
    fields = {}

    for column_name in old_schema.names:
        field_type = old_schema.field(column_name).type
        if type(field_type) == pyarrow.lib.LargeListType:
            value_type = field_type.value_type

            if value_type == pyarrow.large_binary():
                value_type = pyarrow.binary()

            fields[column_name] = pyarrow.list_(value_type)
        elif field_type == pyarrow.large_binary():
            fields[column_name] = pyarrow.binary()
        else:
            fields[column_name] = old_schema.field(column_name)

    new_schema = pyarrow.schema(fields)
    return batch.cast(new_schema)


def _infer_schema_and_transform(dataset: "Dataset"):
    list_sizes = dataset.aggregate(_MaxListSize(dataset.schema().names))

    return dataset.map_batches(
        _unwrap_single_value_lists,
        fn_kwargs={"col_lengths": list_sizes["max_list_size"]},
        batch_format="pyarrow",
    )


def _unwrap_single_value_lists(batch: pyarrow.Table, col_lengths: Dict[str, int]):
    """
    This function will transfrom the dataset converting list types that always
    contain single values to thery underlying data type
    (i.e. pyarrow.int64() and pyarrow.float64())
    """
    columns = {}

    for col in col_lengths:
        value_type = batch[col].type.value_type

        if col_lengths[col] == 1:
            if batch[col]:
                columns[col] = pyarrow.array(
                    [x.as_py()[0] if x.as_py() else None for x in batch[col]],
                    type=value_type,
                )
        else:
            columns[col] = batch[col]

    return pyarrow.table(columns)


class _MaxListSize(AggregateFn):
    def __init__(self, columns: List[str]):
        self._columns = columns
        super().__init__(
            init=self._init,
            merge=self._merge,
            accumulate_row=self._accumulate_row,
            finalize=lambda a: a,
            name="max_list_size",
        )

    def _init(self, k: str):
        return {col: 0 for col in self._columns}

    def _merge(self, acc1: Dict[str, int], acc2: Dict[str, int]):
        merged = {}
        for col in self._columns:
            merged[col] = max(acc1[col], acc2[col])

        return merged

    def _accumulate_row(self, acc: Dict[str, int], row: "pd.Series"):
        for k in row:
            value = row[k]
            if value:
                acc[k] = max(len(value), acc[k])

        return acc


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
