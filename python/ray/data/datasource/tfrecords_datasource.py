from multiprocessing.sharedctypes import Value
import tempfile
from typing import Dict, List, Union

import os
import pandas as pd
import pyarrow
import tensorflow as tf

from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource


class TFRecordDatasource(FileBasedDatasource):

    _FILE_EXTENSION = "tfrecords"

    def _read_file(self, f: "pyarrow.NativeFile", path: str, **reader_args) -> Block:
        from google.protobuf.message import DecodeError

        with tempfile.NamedTemporaryFile() as temporary_file:
            # TensorFlow doesn't expose a function for reading TFRecords with a file
            # handle; you can only read TFRecords with a file path. So, if the TFRecord
            # doesn't exist locally (e.g., because it's stored in S3), we have to
            # download a copy.
            local_path = path
            if not os.path.exists(local_path):
                f.write(temporary_file)
                local_path = temporary_file.name

            dataset = tf.data.TFRecordDataset([local_path])

            data = []
            for record in dataset:
                example = tf.train.Example()
                try:
                    example.ParseFromString(record.numpy())
                except DecodeError:
                    raise ValueError(
                        "`TFRecordDatasource` failed to parse `tf.train.Example` "
                        f"record in '{path}'. This error can occur if your TFRecord "
                        "file contains a message type other than `tf.train.Example`."
                    )

                data.append(_convert_example_to_dict(example))

            return pd.DataFrame.from_records(data)


def _convert_example_to_dict(example: tf.train.Example) -> pd.DataFrame:
    record = {}
    for feature_name, feature in example.features.feature.items():
        value = _get_feature_value(feature)
        if len(value) == 1:
            value = value[0]
        record[feature_name] = value
    return record


def _get_feature_value(
    feature: tf.train.Feature,
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
