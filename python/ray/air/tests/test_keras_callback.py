import numpy as np
import tensorflow as tf

import ray
from ray.air import session
from ray.air.callbacks.keras import Callback
from ray.air.constants import MODEL_KEY
from ray.train.constants import TRAIN_DATASET_KEY
from ray.air.config import ScalingConfig
from ray.train.tensorflow import (
    TensorflowTrainer,
    prepare_dataset_shard,
    TensorflowPredictor,
)


def get_dataset(a=5, b=10, size=1000):
    items = [i / size for i in range(size)]
    dataset = ray.data.from_items([{"x": x, "y": a * x + b} for x in items])
    return dataset


def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=()),
            # Add feature dimension, expanding (batch_size,) to (batch_size, 1).
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(10),
            tf.keras.layers.Dense(1),
        ]
    )
    return model


def train_func(config: dict):
    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        multi_worker_model = build_model()
        multi_worker_model.compile(
            optimizer=tf.keras.optimizers.SGD(learning_rate=config.get("lr", 1e-3)),
            loss=tf.keras.losses.mean_squared_error,
            metrics=[tf.keras.metrics.mean_squared_error],
        )

    dataset = session.get_dataset_shard("train")

    for _ in range(config.get("epoch", 3)):
        tf_dataset = prepare_dataset_shard(
            dataset.to_tf(
                label_column="y",
                output_signature=(
                    tf.TensorSpec(shape=(None, 1), dtype=tf.float32),
                    tf.TensorSpec(shape=(None), dtype=tf.float32),
                ),
                batch_size=32,
            )
        )
        multi_worker_model.fit(tf_dataset, callbacks=[Callback()])


def test_keras_callback_e2e():
    epochs = 3
    config = {
        "epochs": epochs,
    }
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={TRAIN_DATASET_KEY: get_dataset()},
    )
    checkpoint = trainer.fit().checkpoint
    checkpoint_dict = checkpoint.to_dict()
    assert MODEL_KEY in checkpoint_dict

    predictor = TensorflowPredictor.from_checkpoint(
        checkpoint, model_definition=build_model
    )

    items = np.random.uniform(0, 1, size=(10, 1))
    predictor.predict(data=items)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
