import logging
from typing import Any, List, Optional

import tensorflow as tf

from ray.util.data import MLDataset


class TFMLDataset:
    """ A TFMLDataset which converted from MLDataset

    .. code-block:: python

        ds = ml_dataset.to_tf(feature_columns=["x"], label_column="y")
        shard = ds.get_shard(0) # the data as (x_value, y_value)

        ds = ml_dataset.to_tf(feature_columns=["x", "y"], label_column="z")
        shard = ds.get_shard(0) # the data as ((x_value, y_value), z_value)


    Args:
        ds (MLDataset): a MLDataset
        feature_columns (List[Any]): the feature columns' name
        feature_shapes (Optional[List[tf.TensorShape]]): the shape for each
            feature. If provide, it should match the size of feature_columns
        feature_types (Optional[List[tf.DType]]): the data type for each
            feature. If provide, it should match the size of feature_columns
        label_column (Any): the label column name
        label_shape (Optional[tf.TensorShape]): the shape for the label data
        label_type (Optional[tf.DType]): the data type for the label data
    """

    def __init__(self, ds: MLDataset, feature_columns: List[Any],
                 feature_shapes: Optional[List[tf.TensorShape]],
                 feature_types: Optional[List[tf.DType]], label_column: Any,
                 label_shape: Optional[tf.TensorShape],
                 label_type: Optional[tf.DType]):

        self._feature_columns = feature_columns
        self._feature_shapes = feature_shapes
        self._feature_types = feature_types
        self._label_column = label_column
        self._label_shape = label_shape
        self._label_type = label_type

        self._check_and_convert()

        self._ds = ds

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
        """ Repartition the MLDataset to given number of shards"""
        if num_shards != self._ds.num_shards():
            logging.info("Setting num shards", num_shards)
            self._ds = self._ds.repartition(num_shards)

    def get_shard(self,
                  shard_index: int,
                  batch_ms: int = 0,
                  num_async: int = 1,
                  shuffle: bool = False,
                  shuffle_buffer_size: int = 1,
                  seed: int = None) -> "tf.data.Dataset":
        """ Get the given shard data.

        Get a the given shard data from MLDataset and convert into a
        tensorflow.data.Dataset. If the shard_index is smaller than zero,
        it will collect all data as a tensorflow.data.Dataset.
        """
        it = self._ds.get_repeatable_shard(shard_index, batch_ms, num_async,
                                           shuffle, shuffle_buffer_size, seed)

        def make_generator():
            for df in iter(it):
                num_rows = df.shape[0]
                feature_columns = [
                    df[col].values for col in self._feature_columns
                ]
                label_column = df[self._label_column].values
                for i in range(num_rows):
                    features = [f[i] for f in feature_columns]
                    if len(features) > 1:
                        yield tuple(features), label_column[i]
                    else:
                        yield features[0], label_column[i]

        output_shapes = self._feature_shapes.copy()
        if len(output_shapes) > 1:
            output_shapes = (tuple(output_shapes), self._label_shape)
        else:
            output_shapes = (output_shapes[0], self._label_shape)

        output_types = self._feature_types.copy()
        if len(output_types) > 1:
            output_types = (tuple(output_types), self._label_type)
        else:
            output_types = output_types[0], self._label_type
        ds = tf.data.Dataset.from_generator(
            make_generator,
            output_types=output_types,
            output_shapes=output_shapes)
        return ds
