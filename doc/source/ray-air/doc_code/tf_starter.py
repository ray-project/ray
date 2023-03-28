# flake8: noqa
# isort: skip_file

# __air_tf_train_start__
import ray
import tensorflow as tf

from ray.air import session
from ray.air.integrations.keras import Callback
from ray.train.tensorflow import TensorflowTrainer
from ray.air.config import ScalingConfig


# If using GPUs, set this to True.
use_gpu = False

a = 5
b = 10
size = 100


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
    batch_size = config.get("batch_size", 64)
    epochs = config.get("epochs", 3)

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

    results = []
    for _ in range(epochs):
        tf_dataset = dataset.to_tf(
            feature_columns="x", label_columns="y", batch_size=batch_size
        )
        history = multi_worker_model.fit(tf_dataset, callbacks=[Callback()])
        results.append(history.history)
    return results


config = {"lr": 1e-3, "batch_size": 32, "epochs": 4}

train_dataset = ray.data.from_items(
    [{"x": x / 200, "y": 2 * x / 200} for x in range(200)]
)
scaling_config = ScalingConfig(num_workers=2, use_gpu=use_gpu)
trainer = TensorflowTrainer(
    train_loop_per_worker=train_func,
    train_loop_config=config,
    scaling_config=scaling_config,
    datasets={"train": train_dataset},
)
result = trainer.fit()
print(result.metrics)
# __air_tf_train_end__

# __air_tf_batchpred_start__
import numpy as np

from ray.train.batch_predictor import BatchPredictor
from ray.train.tensorflow import TensorflowPredictor


batch_predictor = BatchPredictor.from_checkpoint(
    result.checkpoint, TensorflowPredictor, model_definition=build_model
)

items = [{"x": np.random.uniform(0, 1)} for _ in range(10)]
prediction_dataset = ray.data.from_items(items)

predictions = batch_predictor.predict(prediction_dataset, dtype=tf.float32)

print("PREDICTIONS")
predictions.show()

# __air_tf_batchpred_end__
