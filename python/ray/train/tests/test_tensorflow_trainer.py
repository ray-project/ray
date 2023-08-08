import os
import pytest

import ray
from ray import train
from ray.train import ScalingConfig
from ray.train.examples.tf.tensorflow_regression_example import (
    train_func as tensorflow_linear_train_func,
)
from ray.data.preprocessors import Concatenator
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.tensorflow import (
    TensorflowTrainer,
    TensorflowCheckpoint,
)
from ray.train.tests.dummy_preprocessor import DummyPreprocessor


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
    epochs = 3

    def train_func(config):
        result = tensorflow_linear_train_func(config)
        assert len(result) == epochs
        assert result[-1]["loss"] < result[0]["loss"]

    train_loop_config = {
        "lr": 1e-3,
        "batch_size": 32,
        "epochs": epochs,
    }
    scaling_config = ScalingConfig(num_workers=num_workers)
    dataset = ray.data.read_csv("s3://anonymous@air-example-data/regression.csv")
    preprocessor = Concatenator(exclude=["", "y"], output_column_name="x")

    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=train_loop_config,
        scaling_config=scaling_config,
        datasets={TRAIN_DATASET_KEY: dataset},
        preprocessor=preprocessor,
    )
    trainer.fit()


def test_tensorflow_e2e(ray_start_4_cpus):
    def train_func():
        model = build_model()
        train.report({}, checkpoint=TensorflowCheckpoint.from_model(model))

    scaling_config = ScalingConfig(num_workers=2)
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scaling_config,
        preprocessor=DummyPreprocessor(),
    )
    result = trainer.fit()
    assert isinstance(result.checkpoint.get_preprocessor(), DummyPreprocessor)


def test_report_and_load_using_ml_session(ray_start_4_cpus):
    def train_func():
        checkpoint = train.get_checkpoint()
        if checkpoint:
            with checkpoint.as_directory() as checkpoint_dir:
                import tensorflow as tf

                model = tf.keras.models.load_model(checkpoint_dir)
        else:
            model = build_model()

        model.save("my_model")
        train.report(
            metrics={"iter": 1},
            checkpoint=TensorflowCheckpoint.from_saved_model("my_model"),
        )

    scaling_config = ScalingConfig(num_workers=2)
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scaling_config,
        preprocessor=DummyPreprocessor(),
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    assert isinstance(checkpoint.get_preprocessor(), DummyPreprocessor)

    trainer2 = TensorflowTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scaling_config,
        resume_from_checkpoint=checkpoint,
        preprocessor=DummyPreprocessor(),
    )
    result = trainer2.fit()
    checkpoint = result.checkpoint
    assert isinstance(checkpoint.get_preprocessor(), DummyPreprocessor)
    with checkpoint.as_directory() as ckpt_dir:
        assert os.path.exists(os.path.join(ckpt_dir, "saved_model.pb"))
    assert result.metrics["iter"] == 1


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
