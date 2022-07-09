import numpy as np
import tensorflow as tf

from ray.air import session
from ray.air.callbacks.keras import Callback
from ray.air.constants import MODEL_KEY
from ray.air.examples.tf.tensorflow_linear_dataset_example import (
    build_model,
    get_dataset,
)
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.tensorflow import (
    TensorflowTrainer,
    prepare_dataset_shard,
    TensorflowPredictor,
)


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
    scaling_config = {"num_workers": 2}
    config = {
        "epochs": epochs,
    }
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
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
