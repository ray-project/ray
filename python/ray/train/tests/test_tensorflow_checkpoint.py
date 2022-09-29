import tensorflow as tf

import ray
from ray.train.batch_predictor import BatchPredictor
from ray.train.tensorflow import (
    TensorflowCheckpoint, TensorflowTrainer, TensorflowPredictor
)
from ray.air import session
from ray.air.config import ScalingConfig


def test_tensorflow_checkpoint_saved_model():
    # The test passes if the following can run successfully.

    def train_fn():
        model = tf.keras.Sequential(
            [
                tf.keras.layers.InputLayer(input_shape=()),
                tf.keras.layers.Flatten(),
                tf.keras.layers.Dense(10),
                tf.keras.layers.Dense(1),
            ])
        model.save("my_model")
        checkpoint = TensorflowCheckpoint.from_saved_model("my_model")
        session.report({"my_metric": 1}, checkpoint=checkpoint)

    trainer = TensorflowTrainer(
        train_loop_per_worker=train_fn,
        scaling_config=ScalingConfig(num_workers=2))

    result_checkpoint = trainer.fit().checkpoint

    batch_predictor = BatchPredictor.from_checkpoint(
        result_checkpoint, TensorflowPredictor)
    batch_predictor.predict(ray.data.range(3))


def test_tensorflow_checkpoint_h5():
    # The test passes if the following can run successfully.

    def train_fn():
        model = tf.keras.Sequential(
            [
                tf.keras.layers.InputLayer(input_shape=()),
                tf.keras.layers.Flatten(),
                tf.keras.layers.Dense(10),
                tf.keras.layers.Dense(1),
            ])
        model.save("my_model")
        checkpoint = TensorflowCheckpoint.from_saved_model("my_model")
        session.report({"my_metric": 1}, checkpoint=checkpoint)

    trainer = TensorflowTrainer(
        train_loop_per_worker=train_fn,
        scaling_config=ScalingConfig(num_workers=2))

    result_checkpoint = trainer.fit().checkpoint

    batch_predictor = BatchPredictor.from_checkpoint(
        result_checkpoint, TensorflowPredictor)
    batch_predictor.predict(ray.data.range(3))
