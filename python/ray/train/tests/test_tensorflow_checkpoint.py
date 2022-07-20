import pytest
import tensorflow as tf

from ray.train.tensorflow import TensorflowCheckpoint


def build_linear_model() -> tf.keras.Model:
        return tf.keras.Sequential(
            [
                tf.keras.layers.InputLayer(input_shape=(1,)),
                tf.keras.layers.Dense(1),
            ]
        )


def test_from_model_callable():
    expected_model = build_linear_model()
    checkpoint = TensorflowCheckpoint.from_model(expected_model)
    actual_model = checkpoint.get_model(build_linear_model)
    assert actual_model.get_weights() == expected_model.get_weights()


def test_from_model_kwargs():
    model = build_linear_model()
    checkpoint = TensorflowCheckpoint.from_model(model, epoch=0)
    assert checkpoint.to_dict()["epoch"] == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))