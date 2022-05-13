# flake8: noqa

# __air_tf_preprocess_start__
import ray

a = 5
b = 10
size = 1000

items = [i / size for i in range(size)]
dataset = ray.data.from_items([{"x": x, "y": a * x + b} for x in items])
# __air_tf_preprocess_end__


# __air_tf_train_start__
import tensorflow as tf
from tensorflow.keras.callbacks import Callback

import ray.train as train
from ray.train.tensorflow import prepare_dataset_shard
from ray.ml.train.integrations.tensorflow import TensorflowTrainer


def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(1,)),
            tf.keras.layers.Dense(10),
            tf.keras.layers.Dense(1),
        ]
    )
    return model


class TrainCheckpointReportCallback(Callback):
    def on_epoch_end(self, epoch, logs=None):
        train.save_checkpoint(**{"model": self.model.get_weights()})
        train.report(**logs)


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

    dataset = train.get_dataset_shard("train")

    results = []
    for _ in range(epochs):
        tf_dataset = prepare_dataset_shard(
            dataset.to_tf(
                label_column="y",
                output_signature=(
                    tf.TensorSpec(shape=(None, 1), dtype=tf.float32),
                    tf.TensorSpec(shape=(None), dtype=tf.float32),
                ),
                batch_size=batch_size,
            )
        )
        history = multi_worker_model.fit(
            tf_dataset, callbacks=[TrainCheckpointReportCallback()]
        )
        results.append(history.history)
    return results


num_workers = 2
use_gpu = False

config = {"lr": 1e-3, "batch_size": 32, "epochs": 4}

trainer = TensorflowTrainer(
    train_loop_per_worker=train_func,
    train_loop_config=config,
    scaling_config=dict(num_workers=num_workers, use_gpu=use_gpu),
    datasets={"train": dataset},
)
result = trainer.fit()
print(result.metrics)
# __air_tf_train_end__

# __air_tf_batchpred_start__
import numpy as np

from ray.ml.batch_predictor import BatchPredictor
from ray.ml.predictors.integrations.tensorflow import TensorflowPredictor


batch_predictor = BatchPredictor.from_checkpoint(
    result.checkpoint, TensorflowPredictor, model_definition=build_model
)

items = [{"x": np.random.uniform(0, 1)} for _ in range(10)]
prediction_dataset = ray.data.from_items(items)

predictions = batch_predictor.predict(prediction_dataset, dtype=tf.float32)

pandas_predictions = predictions.to_pandas(float("inf"))

print(f"PREDICTIONS\n{pandas_predictions}")
# __air_tf_batchpred_end__
