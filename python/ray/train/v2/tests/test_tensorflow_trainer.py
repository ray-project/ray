import sys

import pytest

import ray
from ray.data.preprocessors import Concatenator
from ray.train import ScalingConfig
from ray.train.constants import TRAIN_DATASET_KEY

if sys.version_info >= (3, 12):
    # Tensorflow is not installed for Python 3.12 because of keras compatibility.
    pass
else:
    from ray.train.examples.tf.tensorflow_regression_example import (
        train_func as tensorflow_linear_train_func,
    )
    from ray.train.tensorflow import TensorflowTrainer


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def build_model():
    import tensorflow as tf

    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=()),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(1),
        ]
    )

    return model


@pytest.mark.parametrize("num_workers", [1, 2])
def test_tensorflow_linear(ray_start_4_cpus, num_workers):
    """Also tests air Keras callback."""
    epochs = 1

    def train_func(config):
        result = tensorflow_linear_train_func(config)
        assert len(result) == epochs

    train_loop_config = {
        "lr": 1e-3,
        "batch_size": 32,
        "epochs": epochs,
    }
    scaling_config = ScalingConfig(num_workers=num_workers)
    dataset = ray.data.read_csv("s3://anonymous@air-example-data/regression.csv")
    columns_to_concatenate = [f"x{i:03}" for i in range(100)]
    preprocessor = Concatenator(columns=columns_to_concatenate, output_column_name="x")
    dataset = preprocessor.transform(dataset)

    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=train_loop_config,
        scaling_config=scaling_config,
        datasets={TRAIN_DATASET_KEY: dataset},
    )
    result = trainer.fit()
    assert not result.error
    assert result.checkpoint


if __name__ == "__main__":
    if sys.version_info >= (3, 12):
        # Tensorflow is not installed for Python 3.12 because of keras compatibility.
        sys.exit(0)
    else:
        sys.exit(pytest.main(["-v", "-x", __file__]))
