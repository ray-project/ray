from typing import Dict, List, Union

import pandas as pd
import pyarrow
import tensorflow as tf

from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource


class TFRecordsDatasource(FileBasedDatasource):

    _FILE_EXTENSION = "tfrecords"

    def _read_file(self, f: "pyarrow.NativeFile", path: str, **reader_args) -> Block:
        dataset = tf.data.TFRecordDataset([path])

        data = []
        for record in dataset:
            example = tf.train.Example()
            example.ParseFromString(record.numpy())
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
        return feature.bytes_list.value
    if feature.float_list.value:
        return feature.float_list.value
    if feature.int64_list.value:
        return feature.int64_list.value
