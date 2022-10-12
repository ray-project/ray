import os

import numpy as np
import pytest

import ray
from ray.air import session
from ray.air.examples.tf.tensorflow_regression_example import (
    get_dataset,
    train_func as tensorflow_linear_train_func,
)
from ray.air.config import ScalingConfig
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.tensorflow import (
    TensorflowCheckpoint,
    TensorflowPredictor,
    TensorflowTrainer,
)


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
            # Add feature dimension, expanding (batch_size,) to (batch_size, 1).
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(1),
        ]
    )

    return model


@pytest.mark.parametrize("num_workers", [1, 2])
def test_tensorflow_linear(ray_start_4_cpus, num_workers):
    """Also tests air Keras callback."""

    def train_func(config):
        result = tensorflow_linear_train_func(config)
        assert len(result) == epochs
        assert result[-1]["loss"] < result[0]["loss"]

    num_workers = num_workers
    epochs = 3
    scaling_config = ScalingConfig(num_workers=num_workers)
    config = {
        "lr": 1e-3,
        "batch_size": 32,
        "epochs": epochs,
    }
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
        datasets={TRAIN_DATASET_KEY: get_dataset()},
    )
    trainer.fit()


def test_tensorflow_e2e(ray_start_4_cpus):
    def train_func():
        model = build_model()
        session.report({}, checkpoint=TensorflowCheckpoint.from_model(model))

    scaling_config = ScalingConfig(num_workers=2)
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func, scaling_config=scaling_config
    )
    result = trainer.fit()

    class TensorflowScorer:
        def __init__(self):
            self.pred = TensorflowPredictor.from_checkpoint(
                result.checkpoint, build_model
            )

        def __call__(self, x):
            return self.pred.predict(x, dtype=np.float)

    predict_dataset = ray.data.range(3)
    predictions = predict_dataset.map_batches(
        TensorflowScorer, batch_format="pandas", compute="actors"
    )
    assert predictions.count() == 3


def test_report_and_load_using_ml_session(ray_start_4_cpus):
    def train_func():
        if session.get_checkpoint():
            with session.get_checkpoint().as_directory() as checkpoint_dir:
                import tensorflow as tf

                model = tf.keras.models.load_model(checkpoint_dir)
        else:
            model = build_model()

        model.save("my_model")
        session.report(
            metrics={"iter": 1},
            checkpoint=TensorflowCheckpoint.from_saved_model("my_model"),
        )

    scaling_config = ScalingConfig(num_workers=2)
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func, scaling_config=scaling_config
    )
    result = trainer.fit()

    trainer2 = TensorflowTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scaling_config,
        resume_from_checkpoint=result.checkpoint,
    )
    result = trainer2.fit()
    checkpoint = result.checkpoint
    with checkpoint.as_directory() as ckpt_dir:
        assert os.path.exists(os.path.join(ckpt_dir, "saved_model.pb"))
    assert result.metrics["iter"] == 1


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
