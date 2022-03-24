import argparse
import numpy as np
import pandas as pd


import tensorflow as tf
from tensorflow.keras.callbacks import Callback

import ray
import ray.train as train
from ray.data import Dataset
from ray.train.tensorflow import prepare_dataset_shard
from ray.ml.checkpoint import Checkpoint
from ray.ml.train.integrations.tensorflow import TensorflowTrainer
from ray.ml.predictors.integrations.tensorflow import TensorflowPredictor
from ray.ml.result import Result


class TrainCheckpointReportCallback(Callback):
    def on_epoch_end(self, epoch, logs=None):
        train.save_checkpoint(**{"model": self.model.get_weights()})
        train.report(**logs)


def get_dataset(a=5, b=10, size=1000) -> Dataset:
    items = [i / size for i in range(size)]
    dataset = ray.data.from_items([{"x": x, "y": a * x + b} for x in items])
    return dataset


def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(1,)),
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


def train_tensorflow_linear(num_workers: int = 2, use_gpu: bool = False) -> Result:
    dataset_pipeline = get_dataset()
    config = {"lr": 1e-3, "batch_size": 32, "epochs": 4}
    scaling_config = dict(num_workers=num_workers, use_gpu=use_gpu)
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
        datasets={"train": dataset_pipeline},
    )
    results = trainer.fit()
    print(results.metrics)
    return results


def predict_linear(result: Result) -> Dataset:
    items = [{"x": np.random.uniform(0, 1)}] * 10
    prediction_dataset = ray.data.from_items(items)

    checkpoint_object_ref = result.checkpoint.to_object_ref()

    class TFScorer:
        def __init__(self):
            self.predictor = TensorflowPredictor.from_checkpoint(
                Checkpoint.from_object_ref(checkpoint_object_ref),
                model_definition=build_model,
            )

        def __call__(self, batch) -> pd.DataFrame:
            return self.predictor.predict(batch, dtype=tf.float32)

    predictions = prediction_dataset.map_batches(
        TFScorer, compute="actors", batch_format="pandas"
    )

    pandas_predictions = predictions.to_pandas(float("inf"))

    print(f"PREDICTIONS\n{pandas_predictions}")

    return predictions


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )

    args, _ = parser.parse_known_args()

    if args.smoke_test:
        # 2 workers, 1 for trainer, 1 for datasets
        num_gpus = args.num_workers if args.use_gpu else 0
        ray.init(num_cpus=4, num_gpus=num_gpus)
        result = train_tensorflow_linear(num_workers=2, use_gpu=args.use_gpu)
    else:
        ray.init(address=args.address)
        result = train_tensorflow_linear(
            num_workers=args.num_workers, use_gpu=args.use_gpu
        )
    predict_linear(result)
