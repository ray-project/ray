import logging
from typing import List

import tensorflow as tf

from ray.util.data.distributed_dataset import PandasDistributedDataset


class TFDataset:
    def __init__(self, pandas_ds: PandasDistributedDataset,
                 feature_columns: List[str],
                 feature_shapes: List[tf.TensorShape],
                 feature_types: List[tf.DType], label_column: str,
                 label_shape: tf.TensorShape, label_type: tf.DType):

        self._feature_columns = feature_columns
        self._feature_shapes = feature_shapes
        self._feature_types = feature_types
        self._label_column = label_column
        self._label_shape = label_shape
        self._label_type = label_type

        self._check_and_convert()

        self._ds = pandas_ds

    def _check_and_convert(self):
        # convert to list for convenience
        if not isinstance(self._feature_columns, list):
            self._feature_columns = [self._feature_columns]

        if self._feature_shapes:
            if not isinstance(self._feature_shapes, list):
                self._feature_shapes = [self._feature_shapes]

            assert len(self._feature_columns) == len(self._feature_shapes), \
                "The feature_shapes size must match the feature_columns"

        if self._feature_types:
            if not isinstance(self._feature_types, list):
                self._feature_types = [self._feature_types]

            assert len(self._feature_columns) == len(self._feature_types), \
                "The feature_types size must match the feature_columns"
            for i in range(len(self._feature_types)):
                assert (all(isinstance(dtype, tf.DType)
                            for dtype in self._feature_types)), \
                    "All value in feature_types should be tf.DType instance"

        if not self._feature_shapes:
            self._feature_shapes = [tf.TensorShape(
                ([]))] * len(self._feature_columns)

        if not self._feature_types:
            self._feature_types = [tf.float32] * len(self._feature_columns)

        if not self._label_type:
            self._label_type = tf.float32

        if not self._label_shape:
            self._label_shape = tf.TensorShape(([]))

    def set_num_shards(self, num_shards):
        """
        Reshards the iterator if necessary.
        """
        if num_shards != self._ds.num_shards():
            logging.info("Setting num shards", num_shards)
            self._ds = self._ds.repartition(num_shards)

    def get_shard(self,
                  shard_index: int,
                  batch_ms: int = 0,
                  num_async: int = 1,
                  repeat: bool = True,
                  shuffle: bool = False,
                  shuffle_buffer_size: int = 1,
                  seed: int = None) -> "tf.data.Dataset":
        def make_generator():
            it = self._ds.get_shard(shard_index, batch_ms, num_async, shuffle,
                                    shuffle_buffer_size, seed)
            for df in iter(it):
                num_rows = df.shape[0]
                feature_columns = [
                    df[col].values for col in self._feature_columns
                ]
                label_column = df[self._label_column].values
                for i in range(num_rows):
                    features = [f[i] for f in feature_columns]
                    yield tuple(features), label_column[i]

        output_shapes = self._feature_shapes.copy()
        output_shapes = (tuple(output_shapes), self._label_shape)

        output_types = self._feature_types.copy()
        output_types = (tuple(output_types), self._label_type)
        ds = tf.data.Dataset.from_generator(
            make_generator,
            output_types=output_types,
            output_shapes=output_shapes)
        if repeat:
            ds = ds.repeat()
        return ds
