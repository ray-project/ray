from re import S
import pandas as pd
import pytest
import tensorflow as tf
import numpy as np

import ray


class TestToTF:

    def test_autosharding_is_disabled(self):    
        ds = ray.data.from_items([{"spam": 0, "ham": 0}])

        dataset = ds.to_tf(feature_columns="spam", label_columns="ham")
        
        actual_auto_shard_policy = dataset.options().experimental_distribute.auto_shard_policy
        expected_auto_shard_policy = tf.data.experimental.AutoShardPolicy.OFF
        assert actual_auto_shard_policy is expected_auto_shard_policy
        
    def test_element_spec_type(self):
        ds = ray.data.from_items([{"spam": 0, "ham": 0}])

        dataset = ds.to_tf(feature_columns="spam", label_columns="ham")

        feature_spec, label_spec = dataset.element_spec
        assert isinstance(feature_spec, tf.TypeSpec)
        assert isinstance(label_spec, tf.TypeSpec)

    def test_element_spec_type_with_multiple_columns(self):
        ds = ray.data.from_items([{"spam": 0, "ham": 0, "eggs": 0}])

        dataset = ds.to_tf(feature_columns=["spam", "ham"], label_columns="eggs")

        feature_output_signature, _ = dataset.element_spec
        assert isinstance(feature_output_signature, dict)
        assert feature_output_signature.keys() == {"spam", "ham"}
        assert all(isinstance(value, tf.TypeSpec) for value in feature_output_signature.values())
    
    def test_element_spec_name(self):
        ds = ray.data.from_items([{"spam": 0, "ham": 0}])

        dataset = ds.to_tf(feature_columns="spam", label_columns="ham")

        feature_spec, label_spec = dataset.element_spec
        assert feature_spec.name == "spam"
        assert label_spec.name == "ham"

    @pytest.mark.parametrize("data, expected_dtype", [
        (0, tf.int64),
        (0.0, tf.double),
        (False, tf.bool),
        ("eggs", tf.string),
        (np.zeros([2, 2], dtype=np.float32), tf.float32)
    ])
    def test_element_spec_dtype(self, data, expected_dtype):
        ds = ray.data.from_items([{"spam": data, "ham": data}])

        dataset = ds.to_tf(feature_columns="spam", label_columns="ham")

        feature_spec, label_spec = dataset.element_spec
        assert feature_spec.dtype == expected_dtype
        assert label_spec.dtype == expected_dtype

    def test_element_spec_shape(self):
        ds = ray.data.from_items([{"spam": 0, "ham": 0}])

        dataset = ds.to_tf(feature_columns="spam", label_columns="ham")

        feature_spec, label_spec = dataset.element_spec 
        assert feature_spec.shape == (None,)
        assert label_spec.shape == (None,)

    def test_element_spec_shape_with_tensors(self):
        ds = ray.data.from_items([{"spam": np.zeros([3, 32, 32]), "ham": None}])
        
        dataset = ds.to_tf(feature_columns="spam", label_columns="ham")

        feature_spec, _ = dataset.element_spec 
        assert feature_spec.shape == (None, 3, 32, 32)

    def test_invalid_column_raises_error(self):
        # `range_table` returns a dataset with one column named `value`.
        ds = ray.data.range_table(1)
        with pytest.raises(ValueError):
            ds.to_tf(feature_columns="spam", label_columns="ham")

    def test_simple_dataset_raises_error(self):
        # `range` returns a simple dataset containing `int`s.
        ds = ray.data.range(1)
        with pytest.raises(NotImplementedError):
            ds.to_tf(feature_columns="spam", label_columns="ham")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

